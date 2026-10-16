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
//! A snapshot stops being served for one of two reasons, and the first is the one that matters:
//!
//! 1. **Its block has been built.** An entry names the block it was simulated for, and a snapshot
//!    describes nothing once the stream is quoting a later one. This bound compares two numbers
//!    both sockets send and consults no clock, so no skew can widen it. It is also the only bound
//!    that catches a quiet venue repeating one ladder for minutes: the sequence keeps matching, and
//!    the block does not.
//! 2. **One slot has passed since Titan built it.** The fallback for an entry naming no block, and
//!    a backstop for one whose block never arrives. The deadline is a function of the entry's own
//!    stamp, so every admission of one build computes the same instant: re-sending a build the
//!    buffer already holds, or one it has since dropped, can neither extend nor shorten it. The
//!    buffer therefore needs no record of what it has forgotten.
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
    /// Whether the snapshot still describes the block being quoted.
    ///
    /// An entry naming no block carries no such bound, and falls back to [`Self::expires_at`]
    /// alone. Every entry of both captured frames names one.
    fn covers(&self, block: u64) -> bool {
        self.block_number
            .is_none_or(|target| target >= block)
    }
}

/// How many builds of one `(venue, pair)` key the buffer retains, contradiction records included.
/// Titan re-stamps a pair several times a slot and batches pairs under one sequence, so eight
/// covers a slot of one pair's history; past it the oldest build it holds is dropped.
const MAX_KEY_DEPTH: usize = 8;

/// Furthest ahead of the admitting clock a build stamp may lie and still be admitted.
///
/// Titan's entries were observed at least 120 ms behind the local clock, so a stamp ahead of it is
/// this host's clock trailing Titan's rather than a claim about the future. Because the deadline is
/// derived from the stamp, this doubles as the most lifetime such a stamp can add: a second, on top
/// of the slot every build gets.
///
/// Deliberately tighter than the [`MAX_FUTURE_SKEW_NANOS`](super::MAX_FUTURE_SKEW_NANOS) the price
/// level socket judges its own frames on. The two cannot be the same number and also keep serving
/// inside one slot, and this is the side where that matters: the wider allowance bounds what is
/// quotable, this one bounds what a consumer simulates on. A host skewed past a second therefore
/// keeps quoting and stops carrying snapshots, which is the direction to fail in.
const MAX_BUILD_SKEW: Duration = Duration::from_secs(1);

/// How many `(venue, pair)` keys the buffer retains. The 2026-10 capture streamed nine venues
/// with 35 pairs between them, 29× below the cap, which is here to bound what a misbehaving
/// upstream can make this process hold rather than to bound Titan.
const MAX_KEYS: usize = 1_024;

/// How many venue entries of one frame are admitted. Titan streamed nine.
const MAX_FRAME_ENTRIES: usize = 16;

/// How many pairs of one venue entry are admitted. Titan's widest entry advertised eleven.
const MAX_ENTRY_PAIRS: usize = 32;

/// How many keys one ingest sweeps for expired entries. The cursor rotates, so a key that stops
/// being streamed is still visited within `MAX_KEYS / SWEEP_SHARE` ingests — 32 frames, under
/// three slots at Titan's 1 Hz — however long the buffer has gone without a lookup.
const SWEEP_SHARE: usize = 32;

/// What the buffer holds for a `(venue, pair, sequence)`.
///
/// The buffer itself produces every variant but [`Self::Ambiguous`]: its key is exact, and it
/// retains one live snapshot per sequence, so a lookup of it is never answered by two. That
/// variant is here because the caller joining a ladder to a snapshot reports the whole join
/// through this one enum, its own outcomes included, so that what a ladder was attached from — and
/// what it was not — is told in one vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MatchResult {
    /// The newest-built live snapshot the venue priced that pair at that sequence.
    Matched(Arc<PammSimulationOverrides>),
    /// No build of that `(venue, pair, sequence)` is held.
    Missing,
    /// The ladder cannot be attributed to one snapshot. Never produced by
    /// [`OverrideBuffer::matched`]: the caller that joins the two sockets decides this.
    Ambiguous,
    /// A build of it is held and no longer describes what is being quoted: the block it was
    /// simulated for has been built, or its one slot of lifetime has passed.
    Expired,
    /// Two payloads claimed to be the build that priced it, so what the venue was priced on is
    /// unknown until the build's lifetime passes.
    Poisoned,
}

/// What one ingest did: first what the rotating sweep dropped, then one event per admission it
/// attempted, in the order the frame named its entries and each entry named its pairs. The
/// rotating sweep runs before the frame is read, so its `SweptExpired` events come first; a key
/// outside the rotation's share is swept again when a frame entry names it, so above
/// [`SWEEP_SHARE`] keys a `SweptExpired` can also appear between admission events.
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
    /// A build the key did not hold is now held.
    Admitted,
    /// A newer build of a pair version replaced the one held for it.
    Restamped,
    /// The identical envelope arrived again: nothing changed, its deadline included.
    Replayed,
    /// A build older than the one held for a pair version arrived, and did not replace it.
    OlderBuild,
    /// A payload contradicting a held build arrived under that build's stamp, poisoning it for
    /// the rest of its lifetime.
    ConflictPoisoned,
    /// A build a contradiction record still bars arrived again: that build, or one stamped before
    /// it, which cannot be the evidence that settles the contradiction.
    RejectedContradicted,
    /// The entry is stamped further ahead of the admitting clock than [`MAX_BUILD_SKEW`].
    RejectedFutureSkew,
    /// The entry's slot of lifetime had already passed when it arrived.
    RejectedExpired,
    /// Live contradiction records fill the key's depth, so it admits nothing until one of them
    /// expires.
    RejectedKeySaturated,
    /// Every key the buffer holds is protecting such a record, so it has no slot for a new key.
    RejectedKeysExhausted,
    /// The frame named more venue entries than one frame may make the buffer retain. Once per
    /// such frame, however many entries it named past the cap.
    RejectedFrameEntryCap,
    /// The venue entry named more pairs than one entry may make the buffer retain. Once per such
    /// entry, however many pairs it named past the cap.
    RejectedEntryPairCap,
    /// A pair of one token with itself, which quotes nothing.
    DegeneratePair,
    /// A held build was dropped to make room for another build of its key.
    EvictedEntry,
    /// A key was dropped to make room for another key.
    EvictedKey,
    /// A record was dropped because its build's slot of lifetime has passed. One per record,
    /// whether it held a snapshot or barred a contradicted build.
    SweptExpired,
}

/// Where the buffer indexes a snapshot: the venue it was built for and the pair it priced, the
/// pair's two tokens ordered so that the key does not depend on the orientation Titan named
/// them in. The sequence is not part of it — a key holds a pair's recent sequences together.
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

/// What a retained entry holds.
#[derive(Debug, Clone)]
enum Content {
    /// The snapshot the venue was priced on, shared with every pair of the entry that carried it
    /// and with every state attached from it.
    Snapshot(Arc<PammSimulationOverrides>),
    /// Two payloads arrived under one build, so which state the venue was priced on is
    /// unknowable. The record is pinned for the build's remaining lifetime: dropping it would
    /// let the next copy of that build be admitted as if nothing had contradicted it.
    Contradicted,
}

impl Content {
    fn is_contradicted(&self) -> bool {
        match self {
            Self::Contradicted => true,
            Self::Snapshot(_) => false,
        }
    }

    /// The payload the record was admitted with, or `None` for a contradicted build.
    fn snapshot(&self) -> Option<&Arc<PammSimulationOverrides>> {
        match self {
            Self::Snapshot(snapshot) => Some(snapshot),
            Self::Contradicted => None,
        }
    }
}

/// One build of a snapshot a key retains.
#[derive(Debug, Clone)]
struct RetainedEntry {
    /// The pair version the build priced.
    sequence: u128,
    /// When Titan built the venue entry that carried it, which orders two builds of one pair.
    built_at_ns: u64,
    /// The monotonic instant from which the build must not be simulated: one slot from its own
    /// stamp. A function of the stamp alone, so every admission of one build computes it alike.
    deadline_mono: Instant,
    content: Content,
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
            content: Content::Snapshot(self.snapshot.clone()),
        }
    }
}

/// What the admitting clock makes of a build's stamp.
enum Validity {
    /// The build has lifetime left, and these are the deadlines fixed for it.
    Live {
        deadline_mono: Instant,
        /// The exported wall-clock second: the deadline a consumer of the attached state reads,
        /// floored from the same instant `deadline_mono` names, so it never outlasts the monotonic
        /// deadline. See [`exported_second`].
        expires_at: u64,
    },
    /// Stamped further ahead of the clock than [`MAX_BUILD_SKEW`] explains.
    FutureSkew,
    /// Built a whole slot or more ago: no lifetime is left to fix a deadline from.
    Elapsed,
}

/// The lifetime a build stamped `built_at_ns` has left when it is admitted at `now`, as the
/// deadlines fixed for it.
///
/// Both deadlines are anchored to the stamp, not to the admission: a build dies one slot after
/// Titan built it, however late it is admitted and however often. That is what makes a repeat a
/// no-op — the identical envelope computes the identical deadline, so the buffer has nothing to
/// remember about a build it has dropped, and nothing to refuse. Every step saturates, so no stamp
/// Titan can send turns into a longer lifetime than one slot plus [`MAX_BUILD_SKEW`].
///
/// That identity holds under the clock model the caller supplies: wall and monotonic readings that
/// advance together, each non-decreasing across calls. The coherence is this function's own
/// assumption — [`Now`] neither states nor enforces it — so a wall clock stepped backwards between
/// two admissions of one build is outside the model, and no deadline is promised across it.
fn validity(built_at_ns: u64, now: Now) -> Validity {
    let ahead = Duration::from_nanos(built_at_ns.saturating_sub(now.wall_nanos));
    if ahead > MAX_BUILD_SKEW {
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
        deadline_mono: now.monotonic + remaining + ahead,
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

/// Whether two builds describe the same simulated pre-state.
///
/// `expires_at` is excluded because it is derived: one slot after `built_at_ns`, which is compared,
/// so two builds agreeing on everything else cannot disagree on it. Everything Titan did send is
/// compared, destructured so that a field added to the snapshot has to be classified here rather
/// than silently ignored.
fn same_payload(retained: &PammSimulationOverrides, arriving: &PammSimulationOverrides) -> bool {
    let PammSimulationOverrides {
        accounts,
        block_number,
        block_timestamp,
        built_at_ns,
        expires_at: _,
    } = retained;
    *accounts == arriving.accounts &&
        *block_number == arriving.block_number &&
        *block_timestamp == arriving.block_timestamp &&
        *built_at_ns == arriving.built_at_ns
}

/// The builds one `(venue, pair)` key retains.
struct KeyHistory {
    /// At most [`MAX_KEY_DEPTH`] builds, contradiction records included, in no particular order.
    entries: Vec<RetainedEntry>,
    /// The ingest at which traffic last named this key: the recency a key eviction orders by.
    ///
    /// Named, not admitted — traffic protects a key from eviction whether or not the build it
    /// carried was retained, so a pair whose builds are being refused still counts as a pair
    /// Titan is pricing. Lookups, by contrast, do not touch it at all, because the buffer serves
    /// sockets rather than lookups: a pair Titan keeps pricing must not be evicted just because
    /// nothing has asked for it yet.
    last_ingest: u64,
}

impl KeyHistory {
    fn new(last_ingest: u64) -> Self {
        Self { entries: Vec::new(), last_ingest }
    }

    /// Retains `arriving` under this key, or refuses it, and records what it did.
    fn apply(&mut self, arriving: &Arriving, now: Now, events: &mut Vec<IngestEvent>) {
        self.sweep(now, events);
        if self.refused_by_contradiction(arriving) {
            events.push(IngestEvent::RejectedContradicted);
            return;
        }
        match self.active(arriving.sequence) {
            Some(index) => self.resolve(index, arriving, events),
            None => self.insert(arriving, events),
        }
    }

    /// Drops every record whose build's lifetime has passed.
    ///
    /// Expiry costs a key no depth: because a deadline is a function of its build's stamp, a
    /// dropped record cannot be re-admitted on a longer lifetime, so there is nothing to keep in
    /// its place. No contradiction record outlives the build it refuses either — once the deadline
    /// passes, [`validity`] refuses that build as `Elapsed` on its own.
    fn sweep(&mut self, now: Now, events: &mut Vec<IngestEvent>) {
        let held = self.entries.len();
        self.entries
            .retain(|retained| !retained.expired(now));
        for _ in self.entries.len()..held {
            events.push(IngestEvent::SweptExpired);
        }
    }

    /// Retains `arriving` as a build this key does not hold, dropping the oldest build it may
    /// forget once the key is at its depth.
    fn insert(&mut self, arriving: &Arriving, events: &mut Vec<IngestEvent>) {
        match self.room() {
            Room::Free => {}
            Room::Displacing(index) => {
                self.entries.remove(index);
                events.push(IngestEvent::EvictedEntry);
            }
            Room::Saturated => {
                events.push(IngestEvent::RejectedKeySaturated);
                return;
            }
        }
        self.entries.push(arriving.retained());
        events.push(IngestEvent::Admitted);
    }

    /// What admitting one more record under this key costs.
    fn room(&self) -> Room {
        if self.entries.len() < MAX_KEY_DEPTH {
            return Room::Free;
        }
        match self.oldest_evictable() {
            Some(index) => Room::Displacing(index),
            None => Room::Saturated,
        }
    }

    /// Whether a contradiction record of `arriving`'s pair version refuses it.
    ///
    /// A record refuses the build it was made for and every build **older** than that one: it is a
    /// recency bar, not a single-stamp veto. A build that predates a contradiction cannot be the
    /// evidence that settles it, and admitting one would make contradicting a pair version serve
    /// more than leaving it healthy does — against a healthy newer build the same stamp is the
    /// no-op [`IngestEvent::OlderBuild`] names. Builds stamped **newer** pass: a later build
    /// supersedes a contradiction as the served value.
    ///
    /// Every entry of the key is one the sweep left, because [`Self::apply`] sweeps before it
    /// asks, so each is still inside its build's lifetime.
    fn refused_by_contradiction(&self, arriving: &Arriving) -> bool {
        self.entries.iter().any(|retained| {
            retained.sequence == arriving.sequence &&
                retained.content.is_contradicted() &&
                retained.built_at_ns >= arriving.built_at_ns
        })
    }

    /// The one build serving `sequence`. A key holds at most one, however many records of that
    /// sequence's contradicted or removed builds it is also pinning.
    fn active(&self, sequence: u128) -> Option<usize> {
        self.entries
            .iter()
            .position(|retained| {
                retained.sequence == sequence && retained.content.snapshot().is_some()
            })
    }

    /// The oldest-built snapshot the key could drop, which is the only kind whose slot an arriving
    /// build can take: a contradiction record has to outlive the window in which the build it
    /// refuses could still be admitted.
    fn oldest_evictable(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, retained)| retained.content.snapshot().is_some())
            .min_by_key(|(_, retained)| retained.built_at_ns)
            .map(|(index, _)| index)
    }

    /// Whether the key still holds a contradiction record. Evicting the key would drop it, and the
    /// contradicted build would then be admitted as if nothing had contradicted it, so the key is
    /// protected while it holds one.
    fn holds_contradiction(&self, now: Now) -> bool {
        self.entries
            .iter()
            .any(|retained| !retained.expired(now) && retained.content.is_contradicted())
    }

    /// Applies `arriving` to the build already serving its sequence, held at `index`.
    fn resolve(&mut self, index: usize, arriving: &Arriving, events: &mut Vec<IngestEvent>) {
        match arriving
            .built_at_ns
            .cmp(&self.entries[index].built_at_ns)
        {
            Ordering::Greater => self.supersede(index, arriving, events),
            Ordering::Less => events.push(IngestEvent::OlderBuild),
            Ordering::Equal => self.replay(index, arriving, events),
        }
    }

    /// Replaces the build serving a sequence with the strictly newer one that arrived.
    ///
    /// The superseded build is dropped outright: its deadline is a function of its own stamp, so
    /// keeping a record of it would refuse a re-send that computes exactly the same deadline.
    fn supersede(&mut self, index: usize, arriving: &Arriving, events: &mut Vec<IngestEvent>) {
        self.entries[index] = arriving.retained();
        events.push(IngestEvent::Restamped);
    }

    /// Applies a build arriving under the stamp of the one serving its sequence.
    ///
    /// Only the identical envelope is a repeat, and it changes nothing — its deadline included.
    /// Anything else is two payloads claiming to be one build, and which of them the venue was
    /// priced on is unknowable from the stream, so the build is refused rather than one of them
    /// picked.
    fn replay(&mut self, index: usize, arriving: &Arriving, events: &mut Vec<IngestEvent>) {
        let retained = &mut self.entries[index];
        let replayed = match &retained.content {
            Content::Snapshot(snapshot) => same_payload(snapshot, &arriving.snapshot),
            // Unreachable: `active` names the build serving the sequence, which holds a payload,
            // and a contradicted build is refused before this. Refusing is the answer either way.
            Content::Contradicted => false,
        };
        if replayed {
            events.push(IngestEvent::Replayed);
        } else {
            retained.content = Content::Contradicted;
            events.push(IngestEvent::ConflictPoisoned);
        }
    }
}

/// What admitting one more record under a key at its depth would cost.
enum Room {
    /// The key holds fewer records than its depth.
    Free,
    /// The key is full, and this is the oldest snapshot it could drop to make room.
    Displacing(usize),
    /// Every record the key holds is a live contradiction record, so nothing more is admitted
    /// until one of them expires.
    Saturated,
}

/// The validated snapshots of the override socket, held until a ladder names one.
///
/// Reads no clock: every admission and every lookup is a function of the `Now` it is given, so
/// what the buffer holds is decided by its inputs alone.
#[derive(Default)]
pub(super) struct OverrideBuffer {
    keys: BTreeMap<PairKey, KeyHistory>,
    /// The key the next rotating sweep resumes at; `None` restarts the rotation.
    sweep_cursor: Option<PairKey>,
    /// Counts ingests, so a key eviction can order keys by when traffic last named them.
    ingests: u64,
}

impl OverrideBuffer {
    /// Admits every validated venue entry of one frame, each under the venue key the caller gives
    /// it, and returns what the frame did in the order it named its entries.
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
        self.sweep_rotating_share(now, &mut events);
        for (index, (venue, entry)) in entries.into_iter().enumerate() {
            // One event for the frame, not one per entry past the cap: the function that bounds
            // what a frame can make this process hold bounds what it says about it too.
            if index >= MAX_FRAME_ENTRIES {
                events.push(IngestEvent::RejectedFrameEntryCap);
                break;
            }
            self.ingest_entry(&venue, &entry, now, &mut events);
        }
        events
    }

    /// The newest-built snapshot the buffer holds for `sequence` on `venue`'s pair that still
    /// describes `block`, or why it holds none.
    ///
    /// Both bounds are judged here, independently of any sweep: a snapshot whose block has been
    /// built, or whose deadline has passed, is never returned, however long it has been since an
    /// ingest last visited its key. The block bound is the one that matters — a quiet venue
    /// repeats one ladder for minutes, so the sequence keeps matching long after the snapshot
    /// describes anything — and it is the reason a lookup takes the block it is quoting for.
    ///
    /// A contradiction record is reported only while nothing else serves the sequence: a newer
    /// build supersedes it as the served value without erasing it.
    pub(super) fn matched(
        &self,
        venue: &Bytes,
        token_a: &Bytes,
        token_b: &Bytes,
        sequence: u128,
        block: u64,
        now: Now,
    ) -> MatchResult {
        let Some(history) = self
            .keys
            .get(&PairKey::new(venue, token_a, token_b))
        else {
            return MatchResult::Missing;
        };
        let mut newest: Option<(u64, &Arc<PammSimulationOverrides>)> = None;
        let mut contradicted = false;
        let mut expired = false;
        for retained in &history.entries {
            if retained.sequence != sequence {
                continue;
            }
            if retained.expired(now) {
                expired = true;
                continue;
            }
            match &retained.content {
                Content::Contradicted => contradicted = true,
                Content::Snapshot(snapshot) => {
                    if !snapshot.covers(block) {
                        expired = true;
                        continue;
                    }
                    if newest.is_none_or(|(built_at_ns, _)| built_at_ns < retained.built_at_ns) {
                        newest = Some((retained.built_at_ns, snapshot));
                    }
                }
            }
        }
        match (newest, contradicted, expired) {
            (Some((_, snapshot)), _, _) => MatchResult::Matched(snapshot.clone()),
            (None, true, _) => MatchResult::Poisoned,
            (None, false, true) => MatchResult::Expired,
            (None, false, false) => MatchResult::Missing,
        }
    }

    /// Admits one validated venue entry: one snapshot, shared by every pair the entry priced.
    ///
    /// The entry's stamp decides its lifetime once, before any pair of it is indexed, so a frame
    /// cannot leave one pair of a venue holding a snapshot another pair of the same venue refused.
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
        for (index, pair) in entry.pairs.iter().enumerate() {
            // One event for the entry, as the frame cap does for the frame.
            if index >= MAX_ENTRY_PAIRS {
                events.push(IngestEvent::RejectedEntryPairCap);
                break;
            }
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
            let Some(victim) = self.evictable_key(now) else {
                events.push(IngestEvent::RejectedKeysExhausted);
                return;
            };
            self.keys.remove(&victim);
            events.push(IngestEvent::EvictedKey);
        }
        self.keys
            .entry(key)
            .or_insert_with(|| KeyHistory::new(ingest))
            .apply(arriving, now, events);
    }

    /// The key traffic named longest ago that is protecting no contradiction record, or `None`
    /// when every key it holds is.
    ///
    /// Such a key is never evicted: the record has to outlive the window in which the build it
    /// refuses could still be admitted, or a re-send of that build would pass as if nothing had
    /// contradicted it. That window ends one slot after the build's stamp, so a buffer that
    /// refuses new keys frees itself without help. Scanning every key is affordable because it
    /// happens only once the buffer is full.
    fn evictable_key(&self, now: Now) -> Option<PairKey> {
        self.keys
            .iter()
            .filter(|(_, history)| !history.holds_contradiction(now))
            .min_by_key(|(_, history)| history.last_ingest)
            .map(|(key, _)| key.clone())
    }

    /// Sweeps expired builds from the keys the cursor is at, and drops the keys left holding
    /// nothing.
    ///
    /// Traffic decides which keys an ingest touches and a lookup may never come, so without this
    /// rotation a pair that stops being streamed would hold its last build until its key was
    /// evicted. The cursor moves on by the share swept, so every key is visited within a full
    /// rotation however the traffic is distributed.
    fn sweep_rotating_share(&mut self, now: Now, events: &mut Vec<IngestEvent>) {
        let mut visiting: Vec<PairKey> = match &self.sweep_cursor {
            Some(cursor) => self
                .keys
                .range(cursor.clone()..)
                .take(SWEEP_SHARE + 1)
                .map(|(key, _)| key.clone())
                .collect(),
            None => self
                .keys
                .keys()
                .take(SWEEP_SHARE + 1)
                .cloned()
                .collect(),
        };
        // The one key past the share becomes where the next ingest resumes rather than being
        // swept now; a shorter run means the rotation reached the end and starts over.
        self.sweep_cursor = (visiting.len() > SWEEP_SHARE)
            .then(|| visiting.pop())
            .flatten();
        for key in visiting {
            self.sweep_key(&key, now, events);
        }
    }

    fn sweep_key(&mut self, key: &PairKey, now: Now, events: &mut Vec<IngestEvent>) {
        let Some(history) = self.keys.get_mut(key) else {
            return;
        };
        history.sweep(now, events);
        if history.entries.is_empty() {
            self.keys.remove(key);
        }
    }

    /// How many `(venue, pair)` keys the buffer holds.
    #[cfg(test)]
    fn retained_keys(&self) -> usize {
        self.keys.len()
    }

    /// How many builds the buffer holds across every key, poison records included.
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
/// second of `built_at_ns` plus one slot, as a deadline fixed at admission.
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
    use std::collections::HashSet;

    use super::{
        super::{
            test_support::{address, USDC, USDT, WBTC, WETH},
            titan::{validate, BlockTimestamp, EntryOutcome, EntryRejection, PairVersion},
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
    const WETH_USDC_SEQUENCE: u128 = 283_851_543_383_726_791_766_048_835;

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

    /// The block a lookup is quoting for, unless a case names its own: the block every entry
    /// [`entry`] builds is simulated for. A snapshot covers it, so each case exercises whatever
    /// else it is about; the cases that move it are the block-bound ones.
    const QUOTED_BLOCK: u64 = 26_148_760;

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

    /// The stamp of a build Titan built `seconds` after the test clock's base.
    fn stamped(seconds: u64) -> u64 {
        BASE_WALL_NANOS + seconds * NANOS_PER_SECOND
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

    /// The captured entries restamped as if Titan had built them at `built_at_ns`, which is how
    /// one capture stands in for a stream that keeps re-pricing the same pairs.
    fn restamped(built_at_ns: u64) -> Vec<(Bytes, VenueOverrides)> {
        captured_entries()
            .into_iter()
            .map(|(venue, entry)| (venue, VenueOverrides { built_at_ns, ..entry }))
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
                block_timestamp: BlockTimestamp::Derived(1_791_475_499),
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
        buffer.matched(
            &address(TAURUSFI),
            &address(NVDAX),
            &address(USDC),
            sequence,
            QUOTED_BLOCK,
            now,
        )
    }

    /// What the buffer holds for the captured Goliath entry's one pair, whose snapshot
    /// [`test_overrides`] transcribes.
    fn captured_lookup(buffer: &OverrideBuffer, now: Now) -> MatchResult {
        buffer.matched(
            &address(GOLIATH),
            &address(WETH),
            &address(USDC),
            GOLIATH_SEQUENCE,
            QUOTED_BLOCK,
            now,
        )
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
            MatchResult::Missing |
            MatchResult::Ambiguous |
            MatchResult::Expired |
            MatchResult::Poisoned => panic!("no snapshot matched: {result:?}"),
        }
    }

    /// The second the matched snapshot must not be simulated from: the deadline a consumer of the
    /// stamped state reads, as distinct from the monotonic one the buffer serves by.
    fn matched_deadline(result: &MatchResult) -> u64 {
        match result {
            MatchResult::Matched(snapshot) => snapshot.expires_at,
            MatchResult::Missing |
            MatchResult::Ambiguous |
            MatchResult::Expired |
            MatchResult::Poisoned => panic!("no snapshot matched: {result:?}"),
        }
    }

    fn counted(events: &[IngestEvent], event: IngestEvent) -> usize {
        events
            .iter()
            .filter(|recorded| **recorded == event)
            .count()
    }

    /// The retention rules, walked as one sequence on one `(venue, pair)` key: an exact hit; an
    /// identical replay that changes nothing; a newer build that replaces; an older build that
    /// never replaces; a payload contradicting a retained build, which poisons that build; a
    /// newer build superseding the poisoned one as the served value; that newer build evicted
    /// under depth pressure — after which the poisoned build is still refused, because a poison
    /// record outlives both the build that superseded it and the pressure that dropped it.
    ///
    /// Every build here is stamped before the clock that admits it, which is the lifecycle of
    /// ordinary traffic: Titan's stamps were observed at least 120 ms behind the local clock. A
    /// build stamped *ahead* of that clock is held to its first admission by a fence instead, and
    /// [`the_spent_fence_state_machine`] walks that lifecycle on its own.
    #[test]
    fn the_retention_state_machine() {
        let clock = Clock::new();
        let now = clock.after(Duration::from_secs(1));
        let mut buffer = OverrideBuffer::default();
        let (first, newer, newest) = (BASE_WALL_NANOS, BASE_WALL_NANOS + 1, BASE_WALL_NANOS + 2);

        assert_eq!(
            buffer.ingest([taurusfi(first, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)), 1);

        assert_eq!(
            buffer.ingest([taurusfi(first, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Replayed]
        );
        assert_eq!(
            buffer.ingest([taurusfi(newer, 2, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Restamped]
        );
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)), 2);
        assert_eq!(
            buffer.ingest([taurusfi(first, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::OlderBuild]
        );
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)), 2);

        // Two payloads claiming to be one build: which state the venue was priced on is
        // unknowable, so that build is refused for the rest of its lifetime.
        assert_eq!(
            buffer.ingest([taurusfi(newer, 3, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::ConflictPoisoned]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Poisoned);

        // A strictly newer build is admitted and served, without erasing the poison record.
        assert_eq!(
            buffer.ingest([taurusfi(newest, 4, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)), 4);
        assert_eq!(
            buffer.ingest([taurusfi(newer, 2, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::RejectedContradicted]
        );

        // Six more pair versions fill the key's depth; the seventh evicts the oldest build that
        // is not pinned poison, which is the build serving the poisoned sequence.
        for filler in 0..6 {
            assert_eq!(
                buffer.ingest(
                    [taurusfi(
                        newest + 1 + filler,
                        5,
                        NVDAX_USDC_SEQUENCE + 1 + u128::from(filler)
                    )],
                    now
                ),
                vec![IngestEvent::Admitted]
            );
        }
        assert_eq!(
            buffer.ingest([taurusfi(newest + 7, 5, NVDAX_USDC_SEQUENCE + 7)], now),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );

        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Poisoned);
        assert_eq!(
            buffer.ingest([taurusfi(newer, 2, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::RejectedContradicted]
        );

        // A build older than the contradicted one is not the evidence that resolves it. Admitting
        // it would make poisoning a pair version serve *more* than leaving it healthy does: the
        // same stamp against a healthy newer build is the no-op asserted above, so the mechanism
        // whose whole purpose is to fail closed would widen what this key serves.
        assert_eq!(
            buffer.ingest([taurusfi(first, 3, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::RejectedContradicted]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Poisoned);
    }

    /// The buffer answers with every variant of [`MatchResult`] but `Ambiguous`: its key is
    /// exact and it serves one build per sequence, so no lookup of it is answered by two
    /// snapshots. `Ambiguous` is part of the same vocabulary because the caller joining ladders
    /// to snapshots reports its own outcomes through it — a book it cannot attribute to one pair
    /// is ambiguous about which snapshot priced it — and that caller is what synthesizes it.
    #[test]
    fn the_buffer_never_answers_ambiguously() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();
        buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], now);
        buffer.ingest([taurusfi(BASE_WALL_NANOS, 2, NVDAX_USDC_SEQUENCE)], now);
        buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, WETH_USDC_SEQUENCE)], now);

        let answers = [
            lookup(&buffer, WETH_USDC_SEQUENCE, now),
            lookup(&buffer, NVDAX_USDC_SEQUENCE, now),
            lookup(&buffer, NVDAX_USDC_SEQUENCE + 99, now),
            lookup(&buffer, WETH_USDC_SEQUENCE, clock.after(SLOT)),
        ];
        assert_eq!(matched_nonce(&answers[0]), 1);
        assert_eq!(answers[1], MatchResult::Poisoned);
        assert_eq!(answers[2], MatchResult::Missing);
        assert_eq!(answers[3], MatchResult::Expired);
        for answer in &answers {
            assert_ne!(*answer, MatchResult::Ambiguous);
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
            MatchResult::Missing |
            MatchResult::Ambiguous |
            MatchResult::Expired |
            MatchResult::Poisoned => panic!("the captured snapshot is not served: {snapshot:?}"),
        }
        assert_eq!(
            buffer.matched(
                &address(GOLIATH),
                &address(USDC),
                &address(WETH),
                GOLIATH_SEQUENCE,
                QUOTED_BLOCK,
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
                QUOTED_BLOCK,
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
                QUOTED_BLOCK,
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
            MatchResult::Missing |
            MatchResult::Ambiguous |
            MatchResult::Expired |
            MatchResult::Poisoned => panic!("expired before its deadline: {matched:?}"),
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

    /// A stamp near the end of the clock's range is admitted on arithmetic that does not wrap.
    ///
    /// The exported second is floored from a deadline that can pass `u64::MAX` by up to one slot,
    /// which is why it is computed wide; the record's own deadline is a monotonic instant, so no
    /// wall-clock magnitude takes part in deciding when a build stops serving.
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

        // The same at a clock far from the end, for a stamp inside the skew allowance: the
        // lifetime runs from the stamp, so a repeat changes nothing and the sweep retires it one
        // slot past the stamp rather than one past any admission of it.
        let clock = Clock::based_at(u64::MAX - 30 * NANOS_PER_SECOND);
        let mut buffer = OverrideBuffer::default();
        let built_ahead = clock.after(Duration::ZERO).wall_nanos + NANOS_PER_SECOND;
        assert_eq!(
            buffer.ingest([taurusfi(built_ahead, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 0)),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(
            buffer.ingest([taurusfi(built_ahead, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 6)),
            vec![IngestEvent::Replayed]
        );
        assert_eq!(
            buffer.ingest([taurusfi(built_ahead, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 13)),
            vec![IngestEvent::SweptExpired, IngestEvent::RejectedExpired]
        );
    }

    /// Depth pressure drops the oldest-built snapshot, whichever admission put it there.
    ///
    /// Every record a key holds is droppable, because a deadline is a function of its build's own
    /// stamp: re-admitting what was dropped computes the same deadline, so there is nothing a
    /// re-send could gain by the drop and nothing to hold in its place. Only a contradiction
    /// record is exempt, and that case is its own test.
    #[test]
    fn depth_pressure_drops_the_oldest_build() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();

        // Eight builds of one pair, stamped a second apart, admitted newest-first so that stream
        // order cannot stand in for build order.
        for stamp in (1..9u64).rev() {
            assert_eq!(
                buffer.ingest(
                    [taurusfi(stamped(stamp), 1, NVDAX_USDC_SEQUENCE + u128::from(stamp))],
                    at(&clock, 9)
                ),
                vec![IngestEvent::Admitted]
            );
        }
        assert_eq!(buffer.retained_entries(), 8);

        // The ninth displaces the oldest build, which is the first one admitted and the last one
        // received.
        assert_eq!(
            buffer.ingest([taurusfi(stamped(9), 1, NVDAX_USDC_SEQUENCE + 9)], at(&clock, 9)),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE + 1, at(&clock, 9)), MatchResult::Missing);
        for stamp in 2..10u128 {
            assert_eq!(
                matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE + stamp, at(&clock, 9))),
                1
            );
        }

        // The displaced build is admitted again on the deadline it always had: one slot from its
        // own stamp, which the eviction did not move.
        assert_eq!(
            buffer.ingest([taurusfi(stamped(1), 1, NVDAX_USDC_SEQUENCE + 1)], at(&clock, 10)),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(
            matched_deadline(&lookup(&buffer, NVDAX_USDC_SEQUENCE + 1, at(&clock, 10))),
            exported_second(stamped(1)),
        );
    }

    /// A contradicted build is pinned until its own deadline, and no further.
    ///
    /// The record has to outlive the window in which the payload it contradicts could still be
    /// admitted, and that window closes at the build's deadline: past it [`validity`] turns the
    /// build away as `Elapsed` without consulting anything the buffer remembers. So the pin needs
    /// no separate bound, and the sweep that retires an ordinary build retires this record too.
    #[test]
    fn a_contradicted_build_stays_pinned_to_its_deadline() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();

        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 0)),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 2, NVDAX_USDC_SEQUENCE)], at(&clock, 0)),
            vec![IngestEvent::ConflictPoisoned]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, at(&clock, 0)), MatchResult::Poisoned);

        // Inside the window: the record refuses the payload it contradicts, and the builds that
        // predate the contradiction and so cannot settle it.
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 11)),
            vec![IngestEvent::RejectedContradicted]
        );
        assert_eq!(
            buffer.ingest(
                [taurusfi(BASE_WALL_NANOS - NANOS_PER_SECOND, 1, NVDAX_USDC_SEQUENCE)],
                at(&clock, 5)
            ),
            vec![IngestEvent::RejectedContradicted]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, at(&clock, 11)), MatchResult::Poisoned);

        // At the deadline the record goes, and arithmetic alone turns the same build away.
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 12)),
            vec![IngestEvent::SweptExpired, IngestEvent::RejectedExpired]
        );
        assert_eq!(buffer.retained_keys(), 0);
    }

    /// Every removal forgets a build whole, because every admission of it computes the same
    /// deadline: a re-send has nothing to gain by the removal, so nothing has to be remembered.
    #[test]
    fn a_removal_leaves_nothing_behind() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();

        for sequence in 0..8u128 {
            assert_eq!(
                buffer.ingest(
                    [taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE + sequence)],
                    at(&clock, 0)
                ),
                vec![IngestEvent::Admitted]
            );
        }

        // Depth pressure drops the oldest build whole, and the identical envelope is admitted
        // again — on the deadline it always had, one slot from the stamp rather than one from
        // this admission.
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE + 8)], at(&clock, 0)),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, at(&clock, 0)), MatchResult::Missing);
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 1)),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(
            matched_nonce(&lookup(
                &buffer,
                NVDAX_USDC_SEQUENCE,
                clock.after(SLOT - Duration::from_nanos(1))
            )),
            1
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, at(&clock, 12)), MatchResult::Expired);

        // Expiry leaves nothing either: every record goes at its deadline, and the key with the
        // last of them.
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], at(&clock, 12)),
            [vec![IngestEvent::SweptExpired; 8], vec![IngestEvent::RejectedExpired]].concat()
        );
        assert_eq!(buffer.retained_keys(), 0);
    }

    /// The exported deadline is the second the build's own deadline falls in, and a removal and
    /// re-admission between two whole seconds does not move it.
    ///
    /// The monotonic deadline of a build stamped in the past is already admission-invariant: it
    /// is one slot from the stamp, whenever the build is admitted. The exported second was not,
    /// because flooring the admitting clock and the remaining lifetime separately loses up to a
    /// second of each: a build stamped 0.6 s into a second and admitted 0.1 s later exported the
    /// second before the one its deadline falls in, and the identical envelope re-admitted after
    /// an eviction exported that later one — a second of exported lifetime an admission bought,
    /// which is what the fixed-deadline promise denies. Flooring the deadline itself, once, is
    /// what makes the two agree.
    #[test]
    fn the_exported_deadline_survives_a_removal_and_a_replay() {
        const WHOLE_SECOND: u64 = 1_791_475_494;
        let clock = Clock::based_at(WHOLE_SECOND * NANOS_PER_SECOND);
        let mut buffer = OverrideBuffer::default();
        let built = WHOLE_SECOND * NANOS_PER_SECOND + 600_000_000;
        let admitted = clock.after(Duration::from_millis(700));

        assert_eq!(
            buffer.ingest([taurusfi(built, 1, NVDAX_USDC_SEQUENCE)], admitted),
            vec![IngestEvent::Admitted]
        );
        let first = matched_deadline(&lookup(&buffer, NVDAX_USDC_SEQUENCE, admitted));
        assert_eq!(
            first,
            WHOLE_SECOND + 12,
            "the second the build's deadline — one slot from its 0.6 s stamp — falls in"
        );

        // Eight later builds of the same key, each its own pair version, displace this one: an
        // ordinary past-stamped record, which the key forgets whole.
        let pressure = at(&clock, 11);
        for sequence in 1..9u128 {
            buffer.ingest(
                [taurusfi(
                    WHOLE_SECOND * NANOS_PER_SECOND + sequence as u64 * NANOS_PER_SECOND,
                    1,
                    NVDAX_USDC_SEQUENCE + sequence,
                )],
                pressure,
            );
        }
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, pressure), MatchResult::Missing);

        // The identical envelope, re-admitted 200 ms later and inside its slot: the same build,
        // so the same exported second.
        let replayed = clock.after(Duration::from_millis(11_200));
        buffer.ingest([taurusfi(built, 1, NVDAX_USDC_SEQUENCE)], replayed);
        assert_eq!(
            matched_deadline(&lookup(&buffer, NVDAX_USDC_SEQUENCE, replayed)),
            first,
            "a removal and a re-admission moved the exported deadline"
        );
    }

    /// A snapshot stamped further ahead of the admitting clock than [`MAX_BUILD_SKEW`] is refused.
    /// One inside the allowance is admitted, and serves until one slot past its own stamp — which
    /// is how the allowance bounds the extra lifetime a trailing clock can buy.
    #[test]
    fn a_snapshot_stamped_too_far_ahead_is_refused() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();

        let beyond = BASE_WALL_NANOS + nanos(MAX_BUILD_SKEW) + 1;
        assert_eq!(
            buffer.ingest([taurusfi(beyond, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::RejectedFutureSkew]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Missing);

        let allowed = BASE_WALL_NANOS + nanos(MAX_BUILD_SKEW);
        assert_eq!(
            buffer.ingest([taurusfi(allowed, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Admitted]
        );
        assert_eq!(
            matched_nonce(&lookup(
                &buffer,
                NVDAX_USDC_SEQUENCE,
                clock.after(SLOT + MAX_BUILD_SKEW - Duration::from_nanos(1))
            )),
            1
        );
        assert_eq!(
            lookup(&buffer, NVDAX_USDC_SEQUENCE, clock.after(SLOT + MAX_BUILD_SKEW)),
            MatchResult::Expired
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

    /// A snapshot is not served once the block it was simulated for has been built, even while its
    /// deadline still has most of a slot to run.
    ///
    /// This is the bound that matters, and the clock cannot stand in for it: the same lookup one
    /// block later is refused at an instant where the time bound would still serve. The snapshot
    /// stays held — it describes a block, and `Expired` says so — rather than being swept, because
    /// the entry may still be the newest thing Titan has said about the pair.
    #[test]
    fn a_snapshot_whose_block_is_built_is_not_served() {
        let clock = Clock::new();
        let now = at(&clock, 1);
        let mut buffer = OverrideBuffer::default();
        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE)], now),
            vec![IngestEvent::Admitted]
        );

        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE, now)), 1);
        assert_eq!(
            buffer.matched(
                &address(TAURUSFI),
                &address(NVDAX),
                &address(USDC),
                NVDAX_USDC_SEQUENCE,
                QUOTED_BLOCK + 1,
                now,
            ),
            MatchResult::Expired,
            "a snapshot of the previous block was served for the block after it"
        );
        assert_eq!(buffer.retained_entries(), 1, "the snapshot was swept rather than held");
    }

    /// An entry naming no block falls back to its deadline, and is served for the whole of it.
    ///
    /// Titan named a block on every entry of both captures, so this is the shape of a producer
    /// that stops doing so: the weaker bound alone, rather than nothing or everything.
    #[test]
    fn an_entry_naming_no_block_falls_back_to_its_deadline() {
        let clock = Clock::new();
        let now = at(&clock, 0);
        let mut buffer = OverrideBuffer::default();
        let (venue, mut unblocked) = taurusfi(BASE_WALL_NANOS, 1, NVDAX_USDC_SEQUENCE);
        unblocked.block_number = None;

        assert_eq!(buffer.ingest([(venue, unblocked)], now), vec![IngestEvent::Admitted]);
        for block in [QUOTED_BLOCK, QUOTED_BLOCK + 1, QUOTED_BLOCK + 1_000] {
            assert_eq!(
                matched_nonce(&buffer.matched(
                    &address(TAURUSFI),
                    &address(NVDAX),
                    &address(USDC),
                    NVDAX_USDC_SEQUENCE,
                    block,
                    now,
                )),
                1,
                "block {block}"
            );
        }
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
                QUOTED_BLOCK,
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

        for index in 0..8 {
            assert_eq!(
                buffer.ingest(
                    [taurusfi(BASE_WALL_NANOS + index, 1, NVDAX_USDC_SEQUENCE + u128::from(index))],
                    now
                ),
                vec![IngestEvent::Admitted]
            );
        }
        assert_eq!(buffer.retained_entries(), 8);

        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS + 8, 1, NVDAX_USDC_SEQUENCE + 8)], now),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
        assert_eq!(buffer.retained_entries(), 8);
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, now), MatchResult::Missing);
        assert_ne!(lookup(&buffer, NVDAX_USDC_SEQUENCE + 8, now), MatchResult::Missing);
    }

    /// Poisons the key's whole depth: every one of its builds is contradicted while it is live.
    ///
    /// The eight builds are told apart by the pair version they priced, not by their stamp, so
    /// they share one: eight builds of one stamp have one deadline, which is what lets a case
    /// walk the whole depth past it in one step.
    fn poison_every_build(buffer: &mut OverrideBuffer, now: Now) {
        for index in 0..8u128 {
            let sequence = NVDAX_USDC_SEQUENCE + index;
            assert_eq!(
                buffer.ingest([taurusfi(BASE_WALL_NANOS, 1, sequence)], now),
                vec![IngestEvent::Admitted]
            );
            assert_eq!(
                buffer.ingest([taurusfi(BASE_WALL_NANOS, 2, sequence)], now),
                vec![IngestEvent::ConflictPoisoned]
            );
        }
    }

    /// Pinned poison saturating a key fails that key closed: admitting anything more would have
    /// to drop a record that is still refusing a build, so nothing more is admitted until the
    /// poison expires with the builds it refuses.
    #[test]
    fn a_key_saturated_by_poison_fails_closed() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();
        poison_every_build(&mut buffer, now);

        assert_eq!(
            buffer.ingest([taurusfi(BASE_WALL_NANOS + 8, 1, NVDAX_USDC_SEQUENCE + 8)], now),
            vec![IngestEvent::RejectedKeySaturated]
        );
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE + 8, now), MatchResult::Missing);
        assert_eq!(buffer.retained_entries(), 8);
    }

    /// A poison record lives exactly as long as the build it refuses: once that build could not
    /// be simulated anyway, the record is swept and the key is free again.
    #[test]
    fn poison_expires_with_the_build_it_refuses() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();
        poison_every_build(&mut buffer, clock.after(Duration::ZERO));

        let after = clock.after(SLOT);
        assert_eq!(lookup(&buffer, NVDAX_USDC_SEQUENCE, after), MatchResult::Expired);

        let built_later = BASE_WALL_NANOS + nanos(SLOT);
        assert_eq!(
            buffer.ingest([taurusfi(built_later, 1, NVDAX_USDC_SEQUENCE + 8)], after),
            [vec![IngestEvent::SweptExpired; 8], vec![IngestEvent::Admitted]].concat()
        );
        assert_eq!(buffer.retained_entries(), 1);
        assert_eq!(matched_nonce(&lookup(&buffer, NVDAX_USDC_SEQUENCE + 8, after)), 1);
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
            QUOTED_BLOCK,
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

    /// A key holding live poison is not a candidate for eviction, so a buffer whose every key
    /// holds poison refuses a new key rather than erasing a record that is still refusing a
    /// build. Both windows are bounded by one slot.
    #[test]
    fn a_new_key_is_refused_while_every_key_holds_poison() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();
        for index in 0..1_024 {
            let venue = synthetic_address(index);
            let pairs = vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE)];
            buffer.ingest([entry(&venue, BASE_WALL_NANOS, 1, pairs.clone())], now);
            assert_eq!(
                buffer.ingest([entry(&venue, BASE_WALL_NANOS, 2, pairs)], now),
                vec![IngestEvent::ConflictPoisoned]
            );
        }

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
            vec![IngestEvent::RejectedKeysExhausted]
        );
        assert_eq!(buffer.retained_keys(), 1_024);
        assert_eq!(synthetic_lookup(&buffer, 1_024, now), MatchResult::Missing);
        assert_eq!(synthetic_lookup(&buffer, 0, now), MatchResult::Poisoned);
    }

    /// A clock trailing Titan's costs the buffer nothing.
    ///
    /// Every build here is stamped ahead of the admission that takes it, which is what a host whose
    /// clock runs behind Titan's sees for every entry. The deadline comes from the stamp, so such a
    /// build is an ordinary one: it is evictable, the key cap keeps behaving as a cap, and a new
    /// venue is admitted by displacing the key streamed longest ago. An earlier revision anchored
    /// the deadline to the admission and kept a payload-less record to stop a re-send extending it,
    /// which made every one of these keys unevictable and stopped the buffer admitting anything at
    /// all until the records aged out.
    #[test]
    fn a_trailing_clock_does_not_lock_the_buffer() {
        let clock = Clock::based_at(BASE_WALL_NANOS - NANOS_PER_SECOND / 2);
        let admitted = at(&clock, 0);
        let pairs = vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE)];
        let mut buffer = OverrideBuffer::default();
        fill_keys(&mut buffer, 1_024, admitted);
        assert_eq!(buffer.retained_keys(), 1_024);

        assert_eq!(
            buffer.ingest(
                [entry(&synthetic_address(1_024), BASE_WALL_NANOS, 1, pairs.clone())],
                admitted
            ),
            vec![IngestEvent::EvictedKey, IngestEvent::Admitted]
        );
        assert_eq!(buffer.retained_keys(), 1_024);
        assert_eq!(matched_nonce(&synthetic_lookup(&buffer, 1_024, admitted)), 1);

        // And a new sequence of a key already at its depth displaces that key's oldest build,
        // rather than being refused while eight unevictable records hold every place.
        for sequence in 1..9u128 {
            buffer.ingest(
                [entry(
                    &synthetic_address(1_024),
                    BASE_WALL_NANOS + u64::try_from(sequence).unwrap(),
                    1,
                    vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE + sequence)],
                )],
                admitted,
            );
        }
        assert_eq!(
            buffer.ingest(
                [entry(&synthetic_address(1_024), BASE_WALL_NANOS + 9, 1, pairs)],
                admitted
            ),
            vec![IngestEvent::EvictedEntry, IngestEvent::Admitted]
        );
    }

    /// One frame admits a bounded number of venue entries, and one venue entry a bounded number
    /// of pairs: what a single frame can make this process retain is capped before anything in it
    /// is trusted — and so is what the ingest *says* about it, one event per over-cap frame and
    /// per over-cap entry rather than one per item dropped. Both cases name eight items past
    /// their cap, so a per-item event would be counted eight times here.
    #[test]
    fn the_per_frame_caps_bound_what_one_frame_admits() {
        let clock = Clock::new();
        let now = clock.after(Duration::ZERO);
        let mut buffer = OverrideBuffer::default();

        let entries: Vec<(Bytes, VenueOverrides)> = (0..24)
            .map(|index| {
                entry(
                    &synthetic_address(index),
                    BASE_WALL_NANOS,
                    1,
                    vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE)],
                )
            })
            .collect();
        let events = buffer.ingest(entries, now);
        assert_eq!(counted(&events, IngestEvent::Admitted), 16);
        assert_eq!(counted(&events, IngestEvent::RejectedFrameEntryCap), 1);
        assert_eq!(events.len(), 17, "one event per admission, one for the frame");
        assert_eq!(buffer.retained_keys(), 16);
        assert_eq!(synthetic_lookup(&buffer, 16, now), MatchResult::Missing);
        assert_eq!(synthetic_lookup(&buffer, 23, now), MatchResult::Missing);

        let pairs: Vec<PairVersion> = (0..40)
            .map(|index| PairVersion {
                token_1: synthetic_address(index),
                token_2: address(USDC),
                sequence: NVDAX_USDC_SEQUENCE,
            })
            .collect();
        let events = buffer.ingest([entry(&address(TAURUSFI), BASE_WALL_NANOS, 1, pairs)], now);
        assert_eq!(counted(&events, IngestEvent::Admitted), 32);
        assert_eq!(counted(&events, IngestEvent::RejectedEntryPairCap), 1);
        assert_eq!(events.len(), 33, "one event per admission, one for the entry");
        assert_eq!(
            buffer.matched(
                &address(TAURUSFI),
                &synthetic_address(32),
                &address(USDC),
                NVDAX_USDC_SEQUENCE,
                QUOTED_BLOCK,
                now
            ),
            MatchResult::Missing
        );
        assert_eq!(
            buffer.matched(
                &address(TAURUSFI),
                &synthetic_address(39),
                &address(USDC),
                NVDAX_USDC_SEQUENCE,
                QUOTED_BLOCK,
                now
            ),
            MatchResult::Missing
        );
    }

    /// Nothing ever looks a snapshot up — the stream can serve ladders for a venue whose
    /// overrides no consumer asks for — so retention cannot depend on lookups. A minute of
    /// override-only traffic holds one entry per streamed pair, and the pairs that stop being
    /// streamed are dropped within a slot of their last build.
    #[test]
    fn override_only_traffic_keeps_the_buffer_bounded() {
        let clock = Clock::new();
        let mut buffer = OverrideBuffer::default();
        let streamed: HashSet<(Bytes, Bytes, Bytes)> = captured_entries()
            .iter()
            .flat_map(|(venue, entry)| {
                entry
                    .pairs
                    .iter()
                    .map(|pair| (venue.clone(), pair.token_1.clone(), pair.token_2.clone()))
            })
            .collect();

        for second in 0..60 {
            let elapsed = Duration::from_secs(second);
            buffer.ingest(restamped(BASE_WALL_NANOS + nanos(elapsed)), clock.after(elapsed));
            assert_eq!(buffer.retained_keys(), streamed.len());
            assert_eq!(buffer.retained_entries(), streamed.len());
        }

        for second in 60..75 {
            let elapsed = Duration::from_secs(second);
            buffer.ingest(
                [entry(
                    &synthetic_address(0),
                    BASE_WALL_NANOS + nanos(elapsed),
                    1,
                    vec![version(NVDAX, USDC, NVDAX_USDC_SEQUENCE)],
                )],
                clock.after(elapsed),
            );
        }
        assert_eq!(buffer.retained_keys(), 1, "a key no traffic names is not swept");
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
