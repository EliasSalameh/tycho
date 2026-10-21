//! The simulated pre-state a streamed pAMM ladder was priced on.
//!
//! Titan's V2 quote stream publishes, per venue and pair, the account state it simulated the
//! venue on to produce a ladder. A consumer that carries that snapshot can re-simulate the venue
//! on what Titan says it priced, instead of on chain state that has since moved.
//!
//! That a snapshot and the ladder it is stamped onto belong to one simulation is the producer's
//! claim, taken on its word: the two arrive on separate sockets, joined by a sequence number
//! neither side can be checked against here. Validating that assumption is the release gate's
//! full-coverage simulation, not this module's.
//!
//! These types are serialized provenance: they travel with the state they were stamped onto,
//! through recordings and across process boundaries. No field is therefore wider than `u64` — a
//! `serde_json::Value` holds no wider integer, and neither does the buffer typetag reads an
//! adjacently tagged state through when the content arrives before the tag. The stream's sequence
//! numbers, which do not fit, stay in the transient code that matches a snapshot to a ladder and
//! are never persisted.
//!
//! `OverrideBuffer` is that transient code's store: it holds the validated snapshots of the
//! override socket until a ladder of the price level socket names one, indexed by the venue, the
//! pair and the sequence the three are joined on. The two sockets are independent, so the buffer
//! has to hold snapshots nothing ever asks for and snapshots whose ladder has already been
//! served; every retention rule it has — a bounded history per pair, a bounded number of pairs,
//! and a sweep that does not wait on a lookup — exists because what it retains is decided by the
//! stream rather than by a consumer.
//!
//! A snapshot stops being served one slot after Titan built it. That deadline is a function of
//! the entry's own stamp rather than of when the buffer admitted it, so every admission of one
//! build computes the same instant: re-sending a build the buffer already holds, or one it has
//! since dropped, can neither extend nor shorten it. The buffer therefore keeps no record of what
//! it has forgotten.
//!
//! The snapshot also names the block Titan simulated it for. The buffer does not check that
//! number, because it cannot disagree: `calculated_from` encodes the beacon slot in its high 64
//! bits, so a book of one block and an entry of another can never share a sequence. The number is
//! exported instead, and a consumer simulates at the block it names.
//!
//! A repeat is consequently a no-op rather than something to refuse. The identical envelope says
//! only that Titan sent it again — it carries the same build stamp, so it is indistinguishable
//! from a retransmit after a reconnect — and Titan states continued validity by re-stamping, which
//! arrives as a strictly newer build and supersedes.

#[cfg(test)]
use std::str::FromStr;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tycho_common::Bytes;

use super::{titan::VenueOverrides, tracker::Now, NANOS_PER_SECOND, SLOT};

/// The simulated pre-state of one account: the fields Titan overrode, and nothing else.
///
/// An absent field means the simulation read the chain's value, never that the value is zero, so
/// an account that overrode only storage carries no balance and no nonce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountOverride {
    /// The account's balance in wei, big-endian, as Titan sends it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<Bytes>,
    /// The account's nonce.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
    /// The overridden storage slots, keyed by slot. A slot the map does not name keeps its chain
    /// value: the diff is never a full storage image.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub state_diff: BTreeMap<Bytes, Bytes>,
}

/// The simulated pre-state one pAMM ladder was priced on, and the deadline past which it must not
/// be simulated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PammSimulationOverrides {
    /// The overridden accounts, keyed by address. An account the map does not name is simulated
    /// from chain state.
    pub accounts: BTreeMap<Bytes, AccountOverride>,
    /// The block the snapshot is to be simulated on, when the venue entry named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    /// The Unix second to simulate the block at, derived from the entry's beacon slot. `None`
    /// when the entry carried no slot: a simulation then uses the block's own timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_timestamp: Option<u64>,
    /// Unix nanoseconds at which the venue entry carrying the snapshot was built. Orders two
    /// snapshots of the same pair by source recency, which is not the order they were received in.
    pub built_at_ns: u64,
    /// The Unix second from which the snapshot must not be simulated: one slot after the build's
    /// own stamp, floored. A function of the build alone, so neither a repeat nor a re-admission
    /// after a removal moves it.
    ///
    /// The weaker of the two bounds a consumer has. [`Self::block_number`] is the stronger one:
    /// this snapshot describes that block, and nothing once a later one is built.
    pub expires_at: u64,
}
impl PammSimulationOverrides {
    /// Whether two snapshots describe the same simulated pre-state.
    ///
    /// Everything Titan sent is compared, destructured so that a field added to the snapshot has
    /// to be classified here rather than silently ignored. [`Self::expires_at`] is excluded
    /// because this process derives it from `built_at_ns`, which is compared.
    pub(super) fn same_simulated_state(&self, other: &Self) -> bool {
        let Self { accounts, block_number, block_timestamp, built_at_ns, expires_at: _ } = self;
        *accounts == other.accounts &&
            *block_number == other.block_number &&
            *block_timestamp == other.block_timestamp &&
            *built_at_ns == other.built_at_ns
    }
}

/// How many snapshots one `(venue, pair)` key holds.
///
/// **This is a memory ceiling, not a freshness rule, and it is sized so that it does not bind.**
/// The two sockets are independent: a venue entry arrives a median of 704 ms before the book that
/// names its sequence, so a key's entries are a queue of joins still waiting for their book, not a
/// history of superseded values. A newer sequence does not replace an older one — the older one's
/// book may still be in flight.
///
/// Measured on a 13 minute recording (2026-10-21, 65 blocks): one key receives **9.35 new
/// sequences per second**, so the one slot of lifetime below leaves about **112** snapshots live
/// per key. 128 clears that with headroom. An earlier revision set this to 8 on the false belief
/// that eight covered "a slot of one pair's history"; eight covers 0.86 s, and it discarded 22.7%
/// of the snapshots a consumer could otherwise have verified against.
///
/// Watch `override_buffer_total{event="evicted_entry"}`. It should sit near zero. If it moves,
/// Titan's sequence rate has risen and this number needs revisiting.
const MAX_KEY_DEPTH: usize = 128;

/// How many `(venue, pair)` keys the buffer retains. The 2026-10 recording streamed 12 venues with
/// 48 pairs between them, 21× below this, which is here to bound what a misbehaving upstream can
/// make this process hold rather than to bound Titan.
const MAX_KEYS: usize = 1_024;

/// What the buffer holds for a `(venue, pair, sequence)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MatchResult {
    /// The newest-built snapshot the venue priced that pair at that sequence.
    Matched(Arc<PammSimulationOverrides>),
    /// No snapshot of that `(venue, pair, sequence)` is held.
    Missing,
    /// One was held and its slot of lifetime has passed.
    Expired,
}

/// What one ingest did: one event per admission it attempted, in the order the message named its
/// entries and each entry named its pairs, plus what the per-key sweep dropped.
///
/// Each variant's name in snake case is the `event` the telemetry of an ingest labels it with.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum_macros::IntoStaticStr,
    strum_macros::EnumIter,
    strum_macros::EnumCount,
)]
#[strum(serialize_all = "snake_case")]
pub(super) enum IngestEvent {
    /// A snapshot the key did not hold is now held.
    Admitted,
    /// A newer build of a sequence replaced the one held for it.
    Restamped,
    /// The identical envelope arrived again. Nothing changed, its deadline included.
    Replayed,
    /// A build older than the one held for a sequence arrived, and did not replace it.
    OlderBuild,
    /// Titan re-published one build — the same venue and the same stamp — with a different set of
    /// overridden accounts. The buffer keeps the snapshot it already holds and counts this.
    ///
    /// Measured at 178 occurrences in 13 minutes, every one of them Titan re-publishing a build as
    /// a narrower slice about 100 ms later: same block, same beacon slot, fewer accounts, fewer
    /// pairs. None was a genuine disagreement about one pair's state. A sustained rise above that
    /// baseline means the producer's behaviour has changed and is worth looking at.
    ReissuedWithDifferentAccounts,
    /// The entry is stamped further ahead of the admitting clock than
    /// [`MAX_FUTURE_SKEW_NANOS`](super::MAX_FUTURE_SKEW_NANOS).
    RejectedFutureSkew,
    /// The entry's slot of lifetime had already passed when it arrived.
    RejectedExpired,
    /// A pair of one token with itself, which quotes nothing.
    DegeneratePair,
    /// A held snapshot was dropped to make room for another of its key. This should sit near zero:
    /// see [`MAX_KEY_DEPTH`].
    EvictedEntry,
    /// A key was dropped to make room for another key.
    EvictedKey,
    /// A record was dropped because its build's slot of lifetime has passed.
    SweptExpired,
}

/// Where the buffer indexes a snapshot: the venue it was built for and the pair it priced, the
/// pair's two tokens ordered so that the key does not depend on the orientation Titan named them
/// in. The sequence is not part of it — a key holds a pair's pending sequences together.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PairKey {
    venue: Bytes,
    low: Bytes,
    high: Bytes,
}

impl PairKey {
    fn new(venue: &Bytes, token_a: &Bytes, token_b: &Bytes) -> Self {
        let (low, high) = if token_a <= token_b { (token_a, token_b) } else { (token_b, token_a) };
        Self { venue: venue.clone(), low: low.clone(), high: high.clone() }
    }
}

/// One build of a snapshot a key retains.
#[derive(Debug, Clone)]
struct RetainedEntry {
    /// The sequence the build priced.
    sequence: u128,
    /// When Titan built the venue entry that carried it, which orders two builds of one pair.
    built_at_ns: u64,
    /// The monotonic instant from which the build must not be simulated: one slot from its own
    /// stamp. A function of the stamp alone, so every admission of one build computes it alike.
    deadline_mono: Instant,
    snapshot: Arc<PammSimulationOverrides>,
}

impl RetainedEntry {
    /// Whether the build's lifetime has passed. The boundary is inclusive: a build is refused at
    /// its deadline, not after it.
    fn expired(&self, now: Now) -> bool {
        now.monotonic >= self.deadline_mono
    }
}

/// One build of a snapshot as it arrives for one pair of the entry that carried it.
struct Arriving {
    sequence: u128,
    built_at_ns: u64,
    deadline_mono: Instant,
    snapshot: Arc<PammSimulationOverrides>,
}

impl Arriving {
    fn retained(&self) -> RetainedEntry {
        RetainedEntry {
            sequence: self.sequence,
            built_at_ns: self.built_at_ns,
            deadline_mono: self.deadline_mono,
            snapshot: self.snapshot.clone(),
        }
    }
}

/// What the admitting clock makes of a build's stamp.
enum Validity {
    /// The build has lifetime left, and these are the deadlines fixed for it.
    Live {
        deadline_mono: Instant,
        /// The exported wall-clock second a consumer reads, floored from the same instant
        /// `deadline_mono` names. See [`exported_second`].
        expires_at: u64,
    },
    /// Stamped further ahead of the clock than skew explains.
    FutureSkew,
    /// Built a whole slot or more ago: no lifetime is left to fix a deadline from.
    Elapsed,
}

/// The lifetime a build stamped `built_at_ns` has left when it is admitted at `now`.
///
/// Both deadlines are anchored to the stamp, not to the admission: a build dies one slot after
/// Titan built it, however late it is admitted and however often. That is what makes a repeat a
/// no-op — the identical envelope computes the identical deadline, so the buffer has nothing to
/// remember about a build it has dropped, and nothing to refuse.
///
/// A stamp ahead of the admitting clock is refused past
/// [`MAX_FUTURE_SKEW_NANOS`](super::MAX_FUTURE_SKEW_NANOS), which is also the allowance the price
/// level socket judges its own messages on. Measured over 71,762 entries, Titan's stamps were
/// always **behind** this host's clock, by at least 103 ms, so this refusal is not expected to
/// fire; it bounds how much lifetime a stamp from the future could claim.
///
/// That identity holds under the clock model the caller supplies: wall and monotonic readings that
/// advance together, each non-decreasing across calls. [`Now`] neither states nor enforces it, so a
/// wall clock stepped backwards between two admissions of one build is outside the model.
fn validity(built_at_ns: u64, now: Now) -> Validity {
    let ahead = built_at_ns.saturating_sub(now.wall_nanos);
    if ahead > super::MAX_FUTURE_SKEW_NANOS {
        return Validity::FutureSkew;
    }
    let age = Duration::from_nanos(
        now.wall_nanos
            .saturating_sub(built_at_ns),
    );
    let remaining = SLOT.saturating_sub(age);
    if remaining.is_zero() {
        return Validity::Elapsed;
    }
    Validity::Live {
        // `remaining + ahead` is the stamp's own slot measured from `now`: for a stamp in the past
        // `ahead` is zero, and for one ahead of the clock it restores what `age` clamped away.
        deadline_mono: now.monotonic + remaining + Duration::from_nanos(ahead),
        expires_at: exported_second(built_at_ns),
    }
}

/// The Unix second one slot after `built_at_ns`, floored.
///
/// A function of the stamp alone, so it is the same second at every admission of one build. The
/// flooring keeps the exported deadline at or before the monotonic one, never after it.
///
/// Widened to `u128` because the sum can pass `u64::MAX` by up to one slot while the second it
/// divides into cannot: the conversion back is therefore exact for every stamp Titan can send, and
/// saturates rather than wrapping if that ever stops being true.
fn exported_second(built_at_ns: u64) -> u64 {
    let deadline_nanos = u128::from(built_at_ns) + SLOT.as_nanos();
    u64::try_from(deadline_nanos / u128::from(NANOS_PER_SECOND)).unwrap_or(u64::MAX)
}

/// The builds one `(venue, pair)` key retains.
struct KeyHistory {
    /// At most [`MAX_KEY_DEPTH`] snapshots, in no particular order.
    entries: Vec<RetainedEntry>,
    /// The ingest at which traffic last named this key: the recency a key eviction orders by.
    ///
    /// Named, not admitted — traffic protects a key from eviction whether or not the build it
    /// carried was retained. Lookups do not touch it, because the buffer serves sockets rather
    /// than lookups: a pair Titan keeps pricing must not be evicted because nothing has asked for
    /// it yet.
    last_ingest: u64,
}

impl KeyHistory {
    fn new(last_ingest: u64) -> Self {
        Self { entries: Vec::new(), last_ingest }
    }

    /// Retains `arriving` under this key, and records what it did.
    fn apply(&mut self, arriving: &Arriving, now: Now, events: &mut Vec<IngestEvent>) {
        self.sweep(now, events);
        match self.holding(arriving.sequence) {
            Some(index) => self.resolve(index, arriving, events),
            None => self.insert(arriving, events),
        }
    }

    /// Drops every record whose build's lifetime has passed.
    ///
    /// Expiry costs a key no depth: because a deadline is a function of its build's stamp, a
    /// dropped record cannot be re-admitted on a longer lifetime, so there is nothing to keep in
    /// its place.
    fn sweep(&mut self, now: Now, events: &mut Vec<IngestEvent>) {
        let held = self.entries.len();
        self.entries
            .retain(|retained| !retained.expired(now));
        for _ in self.entries.len()..held {
            events.push(IngestEvent::SweptExpired);
        }
    }

    /// Retains `arriving` as a sequence this key does not hold, dropping the oldest build it holds
    /// once the key is at its depth.
    fn insert(&mut self, arriving: &Arriving, events: &mut Vec<IngestEvent>) {
        if self.entries.len() >= MAX_KEY_DEPTH {
            if let Some(oldest) = self.oldest() {
                self.entries.remove(oldest);
                events.push(IngestEvent::EvictedEntry);
            }
        }
        self.entries.push(arriving.retained());
        events.push(IngestEvent::Admitted);
    }

    /// The build this key holds for `sequence`.
    fn holding(&self, sequence: u128) -> Option<usize> {
        self.entries
            .iter()
            .position(|retained| retained.sequence == sequence)
    }

    /// The oldest-built entry, which is the one an arriving build displaces at the depth bound.
    fn oldest(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .min_by_key(|(_, retained)| retained.built_at_ns)
            .map(|(index, _)| index)
    }

    /// Applies `arriving` to the build already held for its sequence.
    fn resolve(&mut self, index: usize, arriving: &Arriving, events: &mut Vec<IngestEvent>) {
        match arriving
            .built_at_ns
            .cmp(&self.entries[index].built_at_ns)
        {
            Ordering::Greater => {
                self.entries[index] = arriving.retained();
                events.push(IngestEvent::Restamped);
            }
            Ordering::Less => events.push(IngestEvent::OlderBuild),
            Ordering::Equal => {
                let same = self.entries[index]
                    .snapshot
                    .same_simulated_state(&arriving.snapshot);
                events.push(if same {
                    IngestEvent::Replayed
                } else {
                    // Keep what is held. Measured, the second sighting of one build is a narrower
                    // slice of it — fewer accounts — so the first is the wider state to simulate
                    // on. Nothing is refused: a snapshot that does not describe the venue makes
                    // the consumer's own `eth_call` revert, which is visible to them, whereas
                    // refusing both would silently cost a check.
                    IngestEvent::ReissuedWithDifferentAccounts
                });
            }
        }
    }
}

/// The validated snapshots of the override socket, held until a ladder names one.
///
/// Reads no clock: every admission and every lookup is a function of the `Now` it is given, so
/// what the buffer holds is decided by its inputs alone.
#[derive(Default)]
pub(super) struct OverrideBuffer {
    keys: BTreeMap<PairKey, KeyHistory>,
    /// Counts ingests, so a key eviction can order keys by when traffic last named them.
    ingests: u64,
}

impl OverrideBuffer {
    /// Admits every validated venue entry of one message, each under the venue key the caller
    /// gives it, and returns what the message did in the order it named its entries.
    ///
    /// The caller canonicalizes the venue: the buffer indexes a snapshot under exactly the key it
    /// is handed, so a venue streamed under an alias is joined to its ladders only if both sides
    /// were canonicalized the same way.
    pub(super) fn ingest(
        &mut self,
        entries: impl IntoIterator<Item = (Bytes, VenueOverrides)>,
        now: Now,
    ) -> Vec<IngestEvent> {
        let mut events = Vec::new();
        self.ingests = self.ingests.saturating_add(1);
        for (venue, entry) in entries {
            self.ingest_entry(&venue, &entry, now, &mut events);
        }
        events
    }

    /// The newest-built snapshot the buffer holds for `sequence` on `venue`'s pair, or why it
    /// holds none.
    ///
    /// Expiry is judged here, independently of any sweep: a snapshot past its deadline is never
    /// returned, however long it has been since an ingest last visited its key.
    pub(super) fn matched(
        &self,
        venue: &Bytes,
        token_a: &Bytes,
        token_b: &Bytes,
        sequence: u128,
        now: Now,
    ) -> MatchResult {
        let Some(history) = self
            .keys
            .get(&PairKey::new(venue, token_a, token_b))
        else {
            return MatchResult::Missing;
        };
        let mut newest: Option<(u64, &Arc<PammSimulationOverrides>)> = None;
        let mut expired = false;
        for retained in &history.entries {
            if retained.sequence != sequence {
                continue;
            }
            if retained.expired(now) {
                expired = true;
                continue;
            }
            if newest.is_none_or(|(built_at_ns, _)| built_at_ns < retained.built_at_ns) {
                newest = Some((retained.built_at_ns, &retained.snapshot));
            }
        }
        match (newest, expired) {
            (Some((_, snapshot)), _) => MatchResult::Matched(snapshot.clone()),
            (None, true) => MatchResult::Expired,
            (None, false) => MatchResult::Missing,
        }
    }

    /// Admits one validated venue entry: one snapshot, shared by every pair the entry priced.
    ///
    /// The entry's stamp decides its lifetime once, before any pair of it is indexed, so one
    /// message cannot leave one pair of a venue holding a snapshot another pair of the same venue
    /// refused.
    fn ingest_entry(
        &mut self,
        venue: &Bytes,
        entry: &VenueOverrides,
        now: Now,
        events: &mut Vec<IngestEvent>,
    ) {
        let (deadline_mono, expires_at) = match validity(entry.built_at_ns, now) {
            Validity::Live { deadline_mono, expires_at } => (deadline_mono, expires_at),
            Validity::FutureSkew => {
                events.push(IngestEvent::RejectedFutureSkew);
                return;
            }
            Validity::Elapsed => {
                events.push(IngestEvent::RejectedExpired);
                return;
            }
        };
        let snapshot = Arc::new(entry.snapshot(expires_at));
        for pair in &entry.pairs {
            if pair.token_1 == pair.token_2 {
                events.push(IngestEvent::DegeneratePair);
                continue;
            }
            let arriving = Arriving {
                sequence: pair.sequence,
                built_at_ns: entry.built_at_ns,
                deadline_mono,
                snapshot: snapshot.clone(),
            };
            let key = PairKey::new(venue, &pair.token_1, &pair.token_2);
            self.admit(key, &arriving, now, events);
        }
    }

    /// Admits one build for one key, making room for the key first when the buffer is full.
    fn admit(
        &mut self,
        key: PairKey,
        arriving: &Arriving,
        now: Now,
        events: &mut Vec<IngestEvent>,
    ) {
        let ingest = self.ingests;
        if let Some(history) = self.keys.get_mut(&key) {
            history.last_ingest = ingest;
            history.apply(arriving, now, events);
            return;
        }
        if self.keys.len() >= MAX_KEYS {
            if let Some(victim) = self.evictable_key() {
                self.keys.remove(&victim);
                events.push(IngestEvent::EvictedKey);
            }
        }
        self.keys
            .entry(key)
            .or_insert_with(|| KeyHistory::new(ingest))
            .apply(arriving, now, events);
        let _ = now;
    }

    /// The key traffic named longest ago. Scanning every key is affordable because it happens only
    /// once the buffer is full, which the 2026-10 recording never came within 21× of.
    fn evictable_key(&self) -> Option<PairKey> {
        self.keys
            .iter()
            .min_by_key(|(_, history)| history.last_ingest)
            .map(|(key, _)| key.clone())
    }

    /// How many `(venue, pair)` keys the buffer holds.
    #[cfg(test)]
    fn retained_keys(&self) -> usize {
        self.keys.len()
    }

    /// How many snapshots the buffer holds across every key.
    #[cfg(test)]
    fn retained_entries(&self) -> usize {
        self.keys
            .values()
            .map(|history| history.entries.len())
            .sum()
    }
}

/// The Goliath venue entry of the captured override frame
/// `test_responses/pamm_quote_stream_v2_1792115920248724389.json`, transcribed: the venue
/// `0x4367985a…` as it stood at `timestamp` 1792115920127487452, on beacon slot 15440992. It is
/// one of the entries the joining price level frame names, so the join can be pinned to a
/// snapshot read off the capture by eye rather than to the parser's own output.
#[cfg(test)]
mod captured {
    /// The entry's one account that overrode storage, and the single word it wrote.
    pub const STORAGE_ACCOUNT: &str = "0xb261ef0a97bc7cf3af2025ab6b3792e6062d3d4a";
    pub const SLOT_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
    pub const WORD_1: &str = "0x000000006ad184d800003c06d28e200000000000000000000000000090db0604";

    /// Its one account that overrode only a balance and a nonce.
    pub const ACCOUNT_1: &str = "0x48165e588ec68e724315d3e0fe7b0f3b9b6c7a6d";
    pub const BALANCE_1: &str = "0x1b6008ea9711c3f3";
}

/// A hex literal of a captured frame as `Bytes`: an address, a balance, a storage slot or a
/// stored word.
#[cfg(test)]
fn hex(literal: &str) -> Bytes {
    Bytes::from_str(literal).expect("hex literal")
}

/// The snapshot the [`captured`] venue entry carried: two accounts, one balance and nonce only,
/// one carrying a single 32-byte storage write.
///
/// `block_timestamp` is the entry's beacon slot turned into a Unix second, and `expires_at` the
/// second of `built_at_ns` plus one slot — a function of the stamp, so it is the same whenever the
/// build is admitted.
#[cfg(test)]
pub(super) fn test_overrides() -> PammSimulationOverrides {
    PammSimulationOverrides {
        accounts: BTreeMap::from([
            (
                hex(captured::ACCOUNT_1),
                AccountOverride {
                    balance: Some(hex(captured::BALANCE_1)),
                    nonce: Some(0xe40),
                    state_diff: BTreeMap::new(),
                },
            ),
            (
                hex(captured::STORAGE_ACCOUNT),
                AccountOverride {
                    // The entry writes this balance `0x0`, one odd digit, which decodes to a
                    // single zero byte.
                    balance: Some(hex("0x00")),
                    nonce: Some(1),
                    state_diff: BTreeMap::from([(hex(captured::SLOT_1), hex(captured::WORD_1))]),
                },
            ),
        ]),
        block_number: Some(26_201_954),
        block_timestamp: Some(1_792_115_927),
        built_at_ns: 1_792_115_920_127_487_452,
        expires_at: 1_792_115_932,
    }
}

#[cfg(test)]
mod tests {

    use super::{
        super::{
            test_support::{address, USDC, USDT, WBTC, WETH},
            titan::{validate, EntryOutcome, EntryRejection, PairVersion},
        },
        captured::{BALANCE_1, SLOT_1, STORAGE_ACCOUNT, WORD_1},
        *,
    };

    /// The 9-venue override frame captured verbatim from the live `pamm_quote_stream_v2` socket
    /// (2026-10-16) — the same capture `titan.rs` validates.
    const CAPTURED_OVERRIDES: &str =
        include_str!("test_responses/pamm_quote_stream_v2_1792115920248724389.json");

    /// A venue and a pair the buffer tests key their synthetic entries by: TaurusFi's streamed
    /// address and the non-standard token it quotes. Identities only — these tests build their
    /// own entries rather than reading the capture, which carries no TaurusFi entry — with
    /// sequences above `u64::MAX`, the width the join must carry.
    const TAURUSFI: &str = "0x3ce2672aa806138585920421406bf0decb8130cb";
    const NVDAX: &str = "0xc845b2894dbddd03858fd2d643b4ef725fe0849d";
    const NVDAX_USDC_SEQUENCE: u128 = 283_851_543_383_726_791_766_048_793;
    /// The captured venue whose entry [`test_overrides`] transcribes, and the sequence it priced
    /// its one pair at.
    const GOLIATH: &str = "0x4367985a63063fc970b009c6e599e9225181f551";
    const GOLIATH_SEQUENCE: u128 = 284_836_027_668_196_596_826_243_132;

    /// The captured venue that stamps all eight of its pairs with one sequence.
    const BATCHED_VENUE: &str = "0xe844701934d9c6085b292e428f41b911cc0b3872";
    const BATCHED_SEQUENCE: u128 = 284_836_027_668_196_596_826_243_146;

    /// The nanosecond the captured Goliath entry was built at, and the base of the test clock:
    /// a snapshot admitted at `Clock::after(Duration::ZERO)` is admitted the instant Titan
    /// stamped it.
    const BASE_WALL_NANOS: u64 = 1_792_115_920_127_487_452;

    /// The second the captured snapshot expires at when it is admitted at the instant it was
    /// built: one slot later, as [`test_overrides`] transcribes it.
    const TAURUSFI_EXPIRES_AT: u64 = 1_792_115_932;

    /// A clock whose wall and monotonic readings advance together, based at the nanosecond the
    /// captured Goliath entry was built.
    ///
    /// Tests inject these readings: `Now.monotonic` is a `std::time::Instant`, which no tokio
    /// time control advances, so a buffer deadline can only be reached by handing the buffer a
    /// later `Now`.
    struct Clock {
        base: u64,
        start: Instant,
    }

    impl Clock {
        fn new() -> Self {
            Self::based_at(BASE_WALL_NANOS)
        }

        /// A clock based at `base` wall nanoseconds. The cases about which second a deadline
        /// floors to base theirs on a whole one, so the only fractions in their arithmetic are
        /// the ones they state.
        fn based_at(base: u64) -> Self {
            Self { base, start: Instant::now() }
        }

        fn after(&self, elapsed: Duration) -> Now {
            Now { wall_nanos: self.base + nanos(elapsed), monotonic: self.start + elapsed }
        }
    }

    fn nanos(elapsed: Duration) -> u64 {
        u64::try_from(elapsed.as_nanos()).expect("a test duration fits a u64 of nanoseconds")
    }

    /// The clock reading `seconds` after its base, which is what the fence cases count in: a
    /// build stamped at `stamped(6)` and admitted at `at(&clock, 0)` is served until
    /// `at(&clock, 12)` and may not be re-admitted before `at(&clock, 18)`.
    fn at(clock: &Clock, seconds: u64) -> Now {
        clock.after(Duration::from_secs(seconds))
    }

    /// Every accepted venue entry of the captured frame, under the venue address Titan named it
    /// by: canonicalization is the caller's, so the buffer is handed the key it is given.
    ///
    /// The capture's ninth entry carries neither a pair nor an account, and validation refuses it
    /// under `no_data`; the eight that carry data are what the buffer sees.
    fn captured_entries() -> Vec<(Bytes, VenueOverrides)> {
        validate(serde_json::from_str(CAPTURED_OVERRIDES).expect("the captured frame parses"))
            .into_iter()
            .filter_map(|outcome| match outcome {
                EntryOutcome::Accepted(entry) => Some((entry.quote_address.clone(), entry)),
                EntryOutcome::Rejected(EntryRejection::NoData) => None,
                EntryOutcome::Rejected(reason) => panic!("rejected a captured entry: {reason:?}"),
            })
            .collect()
    }

    fn version(token_1: &str, token_2: &str, sequence: u128) -> PairVersion {
        PairVersion { token_1: address(token_1), token_2: address(token_2), sequence }
    }

    /// A synthetic address holding `index`, for the venues and tokens a capacity case needs more
    /// of than any capture carries.
    fn synthetic_address(index: usize) -> Bytes {
        let mut bytes = [0u8; 20];
        bytes[12..].copy_from_slice(&(index + 1).to_be_bytes());
        Bytes::from(bytes)
    }

    /// A synthetic venue entry advertising `pairs`, stamped `built_at_ns`, overriding the nonce
    /// of one captured account: `nonce` is what a case varies to make two builds of one stamp
    /// disagree.
    fn entry(
        venue: &Bytes,
        built_at_ns: u64,
        nonce: u64,
        pairs: Vec<PairVersion>,
    ) -> (Bytes, VenueOverrides) {
        (
            venue.clone(),
            VenueOverrides {
                quote_address: venue.clone(),
                pairs,
                accounts: BTreeMap::from([(
                    hex(captured::ACCOUNT_1),
                    AccountOverride {
                        balance: Some(hex(BALANCE_1)),
                        nonce: Some(nonce),
                        state_diff: BTreeMap::new(),
                    },
                )]),
                block_number: Some(26_148_760),
                block_timestamp: Some(1_791_475_499),
                built_at_ns,
            },
        )
    }

    /// A one-pair TaurusFi entry: the state machine's subject.
    fn taurusfi(built_at_ns: u64, nonce: u64, sequence: u128) -> (Bytes, VenueOverrides) {
        entry(&address(TAURUSFI), built_at_ns, nonce, vec![version(NVDAX, USDC, sequence)])
    }

    /// What the buffer holds for the synthetic TaurusFi pair at `sequence`.
    fn lookup(buffer: &OverrideBuffer, sequence: u128, now: Now) -> MatchResult {
        buffer.matched(&address(TAURUSFI), &address(NVDAX), &address(USDC), sequence, now)
    }

    /// What the buffer holds for the captured Goliath entry's one pair, whose snapshot
    /// [`test_overrides`] transcribes.
    fn captured_lookup(buffer: &OverrideBuffer, now: Now) -> MatchResult {
        buffer.matched(&address(GOLIATH), &address(WETH), &address(USDC), GOLIATH_SEQUENCE, now)
    }

    /// The nonce the matched snapshot overrode: which build of a pair the buffer is serving.
    fn matched_nonce(result: &MatchResult) -> u64 {
        match result {
            MatchResult::Matched(snapshot) => snapshot
                .accounts
                .values()
                .next()
                .expect("an overridden account")
                .nonce
                .expect("an overridden nonce"),
            MatchResult::Missing | MatchResult::Expired => {
                panic!("no snapshot matched: {result:?}")
            }
        }
    }

    /// The second the matched snapshot must not be simulated from: the deadline a consumer of the
    /// stamped state reads, as distinct from the monotonic one the buffer serves by.
    fn matched_deadline(result: &MatchResult) -> u64 {
        match result {
            MatchResult::Matched(snapshot) => snapshot.expires_at,
            MatchResult::Missing | MatchResult::Expired => {
                panic!("no snapshot matched: {result:?}")
            }
        }
    }

    /// A snapshot is indexed by the venue, the pair and the sequence together, on live values: the
    /// pair's orientation is not part of the key, and changing any of the three misses. Titan
    /// stamps a batch of pairs with one sequence, so neither the venue nor the sequence alone
    /// identifies anything.
    #[test]
    fn a_captured_frame_is_indexed_by_venue_pair_and_sequence() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();
        buffer.ingest(captured_entries(), now);

        let snapshot = captured_lookup(&buffer, now);
        match &snapshot {
            MatchResult::Matched(overrides) => assert_eq!(**overrides, test_overrides()),
            MatchResult::Missing | MatchResult::Expired => {
                panic!("the captured snapshot is not served: {snapshot:?}")
            }
        }
        assert_eq!(
            buffer.matched(
                &address(GOLIATH),
                &address(USDC),
                &address(WETH),
                GOLIATH_SEQUENCE,
                now
            ),
            snapshot,
            "the pair's orientation is part of no key"
        );

        // The entry stamps all eight of its pairs with this one sequence, so the sequence alone
        // identifies nothing: a pair the entry does not carry stays unmatched under it.
        assert_eq!(
            buffer.matched(
                &address(BATCHED_VENUE),
                &address(WBTC),
                &address(USDT),
                BATCHED_SEQUENCE,
                now
            ),
            MatchResult::Missing,
            "a sequence must not reach a pair its entry never named"
        );
        // Nor does it reach another venue.
        assert_eq!(
            buffer.matched(
                &address(TAURUSFI),
                &address(WETH),
                &address(USDC),
                BATCHED_SEQUENCE,
                now
            ),
            MatchResult::Missing,
            "a sequence must not reach another venue"
        );
    }

    /// The deadline is fixed at the first admission of a build and is a function of when Titan
    /// built it, not of when it was admitted: a snapshot admitted later in its slot expires at
    /// the same second, and the boundary is inclusive — a snapshot is refused at its deadline,
    /// not after it.
    #[test]
    fn a_snapshot_expires_one_slot_after_it_was_built() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();
        buffer.ingest(captured_entries(), clock.after(Duration::from_millis(800)));

        let before = clock.after(SLOT - Duration::from_nanos(1));
        let matched = captured_lookup(&buffer, before);
        match &matched {
            MatchResult::Matched(overrides) => {
                assert_eq!(overrides.expires_at, TAURUSFI_EXPIRES_AT);
            }
            MatchResult::Missing | MatchResult::Expired => {
                panic!("expired before its deadline: {matched:?}")
            }
        }
        assert_eq!(captured_lookup(&buffer, clock.after(SLOT)), MatchResult::Expired);
    }

    /// A repeat never extends what it repeats: the deadline stays one slot from the build's own
    /// stamp, which is where the first admission put it. A strictly newer build gets its own.
    #[test]
    fn a_replay_never_refreshes_a_deadline_but_a_newer_build_gets_its_own() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();
        buffer.ingest(
            [taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)],
            clock.after(Duration::ZERO),
        );

        let replay = clock.after(SLOT / 2);
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], replay),
            vec![IngestEvent::Replayed]
        );
        assert_eq!(
            matched_nonce(&lookup(
                &buffer,
                NVDAX_USDC_SEQUENCE,
                clock.after(SLOT - Duration::from_nanos(1))
            )),
            1
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, clock.after(SLOT)), MatchResult::Expired);

        // A build stamped half a slot later: its own deadline outlives the one the repeat could
        // not extend.
        let built_later = BASE_WALL_NANOS + nanos(SLOT / 2);
        assert_eq!(
            buffer.ingest([taurusfi(built_later, 2, NVDAX_USDC_SEQUENCE)], clock.after(SLOT / 2)),
            vec![IngestEvent::Restamped]
        );
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, clock.after(SLOT))), 2);
        assert_eq!(
            lookup(&buffer, NVDAX_USDC_SEQUENCE, clock.after(SLOT + SLOT / 2)),
            MatchResult::Expired
        );
    }
    /// The depth bound is a ceiling, and a build dropped by it returns on the deadline it always
    /// had.
    ///
    /// Three properties in one walk, because they share a setup. A key admits up to
    /// [`MAX_KEY_DEPTH`] sequences without displacing anything — the bound is memory protection,
    /// not a working limit. Past it, the oldest build goes. A dropped build re-admitted later gets
    /// one slot from its own stamp, not from the re-admission, so the buffer needs to remember
    /// nothing about what it dropped.
    #[test]
    fn the_depth_bound_is_a_ceiling_and_a_dropped_build_keeps_its_deadline() {
        let clock = Clock::new();
        let now = at(&clock, 0);
        let mut buffer = OverrideBuffer::default();

        for sequence in 0..MAX_KEY_DEPTH as u128 {
            assert_eq!(
                buffer.ingest(
                    [taurusfi(
                        BASE_WALL_NANOS + sequence as u64,
                        1,
                        NVDAX_USDC_SEQUENCE + sequence
                    )],
                    now
                ),
                vec![IngestEvent::Admitted],
                "sequence {sequence} should not displace anything below the depth bound"
            );
        }
        assert_eq!(buffer.retained_entries(), MAX_KEY_DEPTH);

        // One past the bound displaces the oldest-built entry, which is the first one admitted.
        let past = MAX_KEY_DEPTH as u64;
        assert_eq!(
            buffer.ingest(
                [taurusfi(BASE_WALL_NANOS + past, 1, NVDAX_USDC_SEQUENCE + u128::from(past))],
                now
            ),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(buffer.retained_entries(), MAX_KEY_DEPTH);
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Missing);

        // The displaced build, re-admitted a second later, carries the deadline it always had.
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 1)),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(
            matched_deadline(&lookup(&buffer, NVDAX_USDC_SEQUENCE, at(&clock, 1))),
            exported_second(BASE_WALL_NANOS)
        );
    }

    /// A stamp near the end of the clock's range is admitted on arithmetic that does not wrap. The
    /// exported second is floored from a deadline that can pass `u64::MAX` by up to one slot,
    /// which is why it is computed wide.
    #[test]
    fn a_stamp_near_the_end_of_the_clock_does_not_wrap() {
        let near_the_end = Clock::based_at(u64::MAX - nanos(SLOT) / 2);
        let mut buffer = OverrideBuffer::default();
        let admitted = near_the_end.after(Duration::ZERO);

        assert_eq!(
            buffer.ingest([taurusfi(admitted.wall_nanos, 1, NVDAX_USDC_SEQUENCE)], admitted),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(
            matched_deadline(&lookup(&buffer, NVDAX_USDC_SEQUENCE, admitted)),
            18_446_744_079,
            "the second a deadline six seconds past u64::MAX falls in"
        );
    }

    /// A snapshot stamped further ahead of the admitting clock than one slot is refused. One
    /// inside the allowance is admitted, and lives until one slot past its own stamp.
    ///
    /// Measured over 71,762 entries, Titan's stamps were always behind this host's clock, by at
    /// least 103 ms, so neither branch is expected to fire in normal operation. The allowance is
    /// the same one the price level socket judges its own messages on.
    #[test]
    fn a_snapshot_stamped_too_far_ahead_is_refused() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();

        let beyond = BASE_WALL_NANOS + super::super::MAX_FUTURE_SKEW_NANOS + 1;
        assert_eq!(
            buffer.ingest([taurusfi(beyond, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::RejectedFutureSkew]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Missing);

        let allowed = BASE_WALL_NANOS + super::super::MAX_FUTURE_SKEW_NANOS;
        assert_eq!(
            buffer.ingest([taurusfi(allowed, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(
            matched_deadline(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)),
            exported_second(allowed)
        );
    }

    /// A snapshot whose slot has already elapsed is never retained: there is no lifetime left to
    /// fix a deadline from.
    #[test]
    fn a_snapshot_a_slot_old_is_refused() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();

        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], clock.after(SLOT)),
            vec![IngestEvent::RejectedExpired]
        );
        assert_eq!(buffer.retained_keys(), 0);
    }
    /// An entry naming no block is served for the whole of its deadline.
    ///
    /// Titan named a block on every one of 71,762 entries in the 2026-10 recording, so this is the
    /// shape of a producer that stops doing so. The block is carried for the consumer, never read
    /// here, so its absence changes nothing about what the buffer serves.
    #[test]
    fn an_entry_naming_no_block_is_served_for_its_whole_deadline() {
        let clock = Clock::new();
        let now = at(&clock, 0);
        let mut buffer = OverrideBuffer::default();
        let (venue, mut unblocked) = taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE);
        unblocked.block_number = None;

        assert_eq!(buffer.ingest([(venue, unblocked)], now), vec![IngestEvent::Admitted]);
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)), 1);
        assert_ne!(
            lookup(&buffer, NVDAX_USDC_SEQUENCE, clock.after(SLOT - Duration::from_nanos(1))),
            MatchResult::Expired
        );
        // Past the deadline it is held and unserved until an ingest sweeps it: a lookup judges
        // expiry itself and does not mutate the buffer.
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, at(&clock, 12)), MatchResult::Expired);
    }

    /// A pair of one token with itself quotes nothing, so it never takes a key — mirroring the
    /// price level side, which refuses to serve a book whose two symbols resolve to one asset.
    #[test]
    fn a_degenerate_self_pair_is_skipped() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();

        let self_paired = entry(
            &address(TAURUSFI),
            BASE_WALL_NANOS,
            1,
            vec![
                version(USDC, USDC, NVDAX_USDC_SEQUENCE),
                version(NVDAX, USDC, NVDAX_USDC_SEQUENCE),
            ],
        );
        assert_eq!(
            buffer.ingest([self_paired], now),
            vec![IngestEvent::DegeneratePair, IngestEvent::Admitted]
        );
        assert_eq!(buffer.retained_keys(), 1);
        assert_eq!(
            buffer.matched(
                &address(TAURUSFI),
                &address(USDC),
                &address(USDC),
                NVDAX_USDC_SEQUENCE,
                now
            ),
            MatchResult::Missing
        );
    }

    /// A key holds a bounded history of builds, and the build it drops to admit another is the
    /// oldest one it holds.
    #[test]
    fn a_full_key_drops_its_oldest_build() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();

        for index in 0..MAX_KEY_DEPTH as u64 {
            assert_eq!(
                buffer.ingest(
                    [taurusfi(BASE_WALL_NANOS + index, 1, NVDAX_USDC_SEQUENCE + u128::from(index))],
                    now
                ),
                vec![IngestEvent::Admitted]
            );
        }
        assert_eq!(buffer.retained_entries(), MAX_KEY_DEPTH);

        let past_depth = MAX_KEY_DEPTH as u64;
        assert_eq!(
            buffer.ingest(
                [taurusfi(
                    BASE_WALL_NANOS + past_depth,
                    1,
                    NVDAX_USDC_SEQUENCE + u128::from(past_depth)
                )],
                now
            ),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(buffer.retained_entries(), MAX_KEY_DEPTH);
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Missing);
        assert_ne!(
            lookup(&buffer, NVDAX_USDC_SEQUENCE + u128::from(past_depth), now),
            MatchResult::Missing
        );
    }
    /// Admits `count` keys of distinct synthetic venues, one ingest each, so that the venue of
    /// the lowest index is the one traffic named longest ago.
    fn fill_keys(buffer: &mut OverrideBuffer, count: usize, now: Now) {
        for index in 0..count {
            assert_eq!(
                buffer.ingest(
                    [entry(
                        &synthetic_address(index),
                        BASE_WALL_NANOS,
                        1,
                        vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE)]
                    )],
                    now
                ),
                vec![IngestEvent::Admitted]
            );
        }
    }

    /// Lookup of the synthetic venue `index`'s one pair.
    fn synthetic_lookup(buffer: &OverrideBuffer, index: usize, now: Now) -> MatchResult {
        buffer.matched(
            &synthetic_address(index),
            &address(NVDAX),
            &address(USDC),
            NVDAX_USDC_SEQUENCE,
            now,
        )
    }

    /// The buffer holds a bounded number of keys: past the cap, admitting a new key drops the one
    /// traffic named longest ago, and nothing else.
    #[test]
    fn a_full_buffer_drops_the_key_streamed_longest_ago() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();
        fill_keys(&mut buffer, 1_024, now);
        assert_eq!(buffer.retained_keys(), 1_024);

        assert_eq!(
            buffer.ingest(
                [entry(
                    &synthetic_address(1_024),
                    BASE_WALL_NANOS,
                    1,
                    vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE)]
                )],
                now
            ),
            vec![IngestEvent::EvictedKey, IngestEvent::Admitted]
        );
        assert_eq!(buffer.retained_keys(), 1_024);
        assert_eq!(synthetic_lookup(&buffer, 0, now), MatchResult::Missing);
        assert_ne!(synthetic_lookup(&buffer, 1, now), MatchResult::Missing);
        assert_ne!(synthetic_lookup(&buffer, 1_023, now), MatchResult::Missing);
        assert_ne!(synthetic_lookup(&buffer, 1_024, now), MatchResult::Missing);
    }
    /// An account that overrode only storage writes neither a balance nor a nonce, and one that
    /// overrode only its balance writes no storage map: an absent field means "take the chain's
    /// value", so writing it as a null or an empty map would claim an override Titan never sent.
    #[test]
    fn an_account_override_omits_the_fields_it_does_not_set() {
        let storage_only = AccountOverride {
            balance: None,
            nonce: None,
            state_diff: BTreeMap::from([(hex(SLOT_1), hex(WORD_1))]),
        };
        let json = serde_json::to_value(&storage_only).expect("serializes");
        let object = json.as_object().expect("an object");
        assert!(object.get("balance").is_none());
        assert!(object.get("nonce").is_none());
        assert_eq!(object.len(), 1);

        let balance_only = AccountOverride {
            balance: Some(hex(BALANCE_1)),
            nonce: None,
            state_diff: BTreeMap::new(),
        };
        let json = serde_json::to_value(&balance_only).expect("serializes");
        let object = json.as_object().expect("an object");
        assert_eq!(object.get("balance"), Some(&serde_json::json!(BALANCE_1)));
        assert_eq!(object.len(), 1);
    }

    /// The counterpart: an account entry naming no field at all reads back as an override of
    /// nothing, rather than as an account zeroed out.
    #[test]
    fn an_account_override_reads_an_absent_field_as_unset() {
        let read: AccountOverride = serde_json::from_str("{}").expect("reads back");
        assert_eq!(
            read,
            AccountOverride { balance: None, nonce: None, state_diff: BTreeMap::new() }
        );
    }

    /// Accounts and storage slots are `0x`-prefixed hex strings in the recording, as they are on
    /// the stream, not byte sequences: a consumer reading a recording finds the addresses it knows.
    #[test]
    fn accounts_and_slots_are_hex_string_keys() {
        let json = serde_json::to_value(test_overrides()).expect("serializes");
        assert_eq!(json["accounts"][STORAGE_ACCOUNT]["nonce"], serde_json::json!(1));
        assert_eq!(
            json["accounts"][STORAGE_ACCOUNT]["state_diff"][SLOT_1],
            serde_json::json!(WORD_1)
        );
    }

    /// A real snapshot survives both serde_json shapes. Nothing in it is wider than `u64`
    /// precisely so that it does: a `serde_json::Value` cannot hold a wider integer at all.
    #[test]
    fn overrides_round_trip_through_text_and_value() {
        let overrides = test_overrides();

        let text = serde_json::to_string(&overrides).expect("serializes");
        let from_text: PammSimulationOverrides =
            serde_json::from_str(&text).expect("reads back from text");
        assert_eq!(from_text, overrides);

        let value = serde_json::to_value(&overrides).expect("serializes into a Value");
        let from_value: PammSimulationOverrides =
            serde_json::from_value(value).expect("reads back from a Value");
        assert_eq!(from_value, overrides);
    }

    /// Titan names the block only on some entries, so a snapshot without either block field is
    /// complete and round-trips back to the same text.
    #[test]
    fn the_block_fields_are_optional() {
        let text = r#"{"accounts":{},"built_at_ns":1791475494775213874,"expires_at":1791475506}"#;
        let read: PammSimulationOverrides = serde_json::from_str(text).expect("reads back");
        assert_eq!(read.block_number, None);
        assert_eq!(read.block_timestamp, None);
        assert_eq!(serde_json::to_string(&read).expect("serializes"), text);
    }

    /// The accounts and the two stamps are not optional: a snapshot whose contents, source
    /// recency or deadline are unknown must be rejected, never defaulted to empty or to the epoch.
    #[test]
    fn a_snapshot_missing_a_required_field_is_rejected() {
        for text in [
            r#"{"built_at_ns":1791475494775213874,"expires_at":1791475506}"#,
            r#"{"accounts":{},"expires_at":1791475506}"#,
            r#"{"accounts":{},"built_at_ns":1791475494775213874}"#,
        ] {
            let error = serde_json::from_str::<PammSimulationOverrides>(text)
                .expect_err("a snapshot missing a required field is rejected");
            assert!(
                error
                    .to_string()
                    .contains("missing field"),
                "unexpected error: {error}"
            );
        }
    }
}
