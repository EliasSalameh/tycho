//! The simulation overrides attached to a pAMM state, and the buffer that holds them until the
//! price levels they belong to arrive.
//!
//! Titan publishes a venue's price levels and the account state it priced them on over two
//! sockets. A book names the quote sequence it was priced at (`calculated_from`). A venue entry of
//! the quote stream names, for each pair, the quote sequence of the state it carries
//! (`pairSequences`). Titan does not publish an entry for every quote sequence, and its books
//! arrive about a second after its entries, so a book often names a quote sequence that no entry
//! carries. [`OverrideBuffer::find`] therefore returns the newest entry of the book's venue and
//! pair, from the book's slot, whose quote sequence is at or below the book's: the latest state
//! Titan published before it priced the book.
//!
//! A venue can price from more than one quote address. An entry's `pricedAddresses` lists every
//! venue whose swaps read that entry's state, so the caller stores an entry under each of them it
//! serves, and `find` merges the matching entries of the other quote addresses into the venue's
//! own entry. The venue's own entry wins wherever two of them set the same value.
//!
//! The buffer reads no clock. A quote sequence carries its beacon slot in its high 64 bits, and a
//! book only matches an entry of its own slot, so the buffer keeps only the slots a book can still
//! name: the slot of the latest price level message, the one before it, and the one after it.
//!
//! A match rests on what Titan publishes, not on anything this code can check: the two sockets
//! arrive separately, and nothing in either proves that one simulation produced both. The live
//! test `pamm_simulation_overrides` checks that claim against the chain.
//!
//! These types are serialized with the state that carries them, so no field is wider than `u64`:
//! a `serde_json::Value` holds no wider integer, and neither does the buffer typetag reads a state
//! through when its tag arrives after its content. Quote sequences do not fit, so they are only
//! used for matching and are never serialized.

#[cfg(test)]
use std::str::FromStr;
use std::{
    cmp::Ordering,
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tycho_common::Bytes;

use super::{titan::VenueOverrides, NANOS_PER_SECOND, SLOT};

/// The simulation overrides of one account: the fields Titan overrode, and nothing else.
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

/// The account state Titan simulated a venue on to price one state's levels, shaped for an
/// `eth_call` State Override Set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PammSimulationOverrides {
    /// The overridden accounts, keyed by address. An account the map does not name is simulated
    /// from chain state.
    pub accounts: BTreeMap<Bytes, AccountOverride>,
    /// The block Titan was building when it took this state, when the venue entry named one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    /// The Unix second that block is dated at, derived from the entry's beacon slot. `None` when
    /// the entry carried no slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_timestamp: Option<u64>,
    /// Unix nanoseconds at which Titan took this state. Orders two entries of one quote sequence,
    /// which is not the order they were received in.
    pub built_at_ns: u64,
    /// The Unix second one slot after [`Self::built_at_ns`], floored.
    ///
    /// Information only. Nothing in this crate acts on it: a state quotes for as long as its price
    /// levels are valid, whatever this says.
    pub expires_at: u64,
}

impl PammSimulationOverrides {
    /// Whether two sets of overrides describe the same simulated state.
    ///
    /// Everything Titan sent is compared, destructured so that a field added to the type has to
    /// be classified here. [`Self::expires_at`] is left out because it is derived from
    /// [`Self::built_at_ns`], which is compared.
    pub(super) fn same_simulated_state(&self, other: &Self) -> bool {
        let Self { accounts, block_number, block_timestamp, built_at_ns, expires_at: _ } = self;
        *accounts == other.accounts &&
            *block_number == other.block_number &&
            *block_timestamp == other.block_timestamp &&
            *built_at_ns == other.built_at_ns
    }

    /// `self` with the accounts of `others` added, `self`'s own values winning wherever both set
    /// one: a balance, a nonce or a storage slot. The block and timing fields stay `self`'s.
    fn merged<'a>(&'a self, others: impl IntoIterator<Item = &'a Self>) -> Self {
        let mut accounts: BTreeMap<Bytes, AccountOverride> = BTreeMap::new();
        for source in others.into_iter().chain([self]) {
            for (address, account) in &source.accounts {
                match accounts.entry(address.clone()) {
                    Entry::Vacant(vacant) => {
                        vacant.insert(account.clone());
                    }
                    Entry::Occupied(mut occupied) => {
                        let held = occupied.get_mut();
                        if account.balance.is_some() {
                            held.balance = account.balance.clone();
                        }
                        if account.nonce.is_some() {
                            held.nonce = account.nonce;
                        }
                        held.state_diff.extend(
                            account
                                .state_diff
                                .iter()
                                .map(|(slot, word)| (slot.clone(), word.clone())),
                        );
                    }
                }
            }
        }
        Self { accounts, ..self.clone() }
    }
}

/// The Unix second one slot after `built_at_ns`, floored.
///
/// Widened to `u128` because the sum can pass `u64::MAX` by up to one slot while the second it
/// divides into cannot, so the conversion back is exact for every timestamp Titan can send.
pub(super) fn expires_at(built_at_ns: u64) -> u64 {
    let deadline_nanos = u128::from(built_at_ns) + SLOT.as_nanos();
    u64::try_from(deadline_nanos / u128::from(NANOS_PER_SECOND)).unwrap_or(u64::MAX)
}

/// The beacon slot a quote sequence was priced in: its high 64 bits.
pub(super) fn slot_of(sequence: u128) -> u64 {
    // A `u128` shifted right by 64 bits always fits a `u64`.
    (sequence >> 64) as u64
}

/// The most entries the buffer holds across every venue and pair.
///
/// A memory ceiling against a misbehaving upstream, not a rule about Titan: replaying a 13 minute
/// recording (2026-10-21) through these rules, with every streamed venue served, peaked at 11,655
/// held entries.
const MAX_HELD: usize = 100_000;

/// What [`OverrideBuffer::find`] returns for a book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Found {
    /// The overrides to attach: the venue's own entry, with the other quote addresses' entries
    /// merged in.
    pub overrides: Arc<PammSimulationOverrides>,
    /// Whether the venue's own entry carried the book's quote sequence, rather than an earlier one
    /// of the same slot.
    pub same_sequence: bool,
    /// How many other quote addresses' entries were merged into the venue's own.
    pub merged_sources: usize,
}

/// What storing an entry, or moving the slot window, did to the buffer. One event per venue and
/// pair an entry is held under.
///
/// Each variant's name in snake case is the `event` label it is counted under.
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
pub(super) enum BufferEvent {
    /// The buffer now holds an entry it did not hold for that quote address, venue, pair and quote
    /// sequence.
    Added,
    /// A newer entry for the same quote address, venue, pair and quote sequence replaced the one
    /// held.
    ReplacedByNewer,
    /// An entry older than the one held for the same quote address, venue, pair and quote
    /// sequence arrived; the held one stays.
    OlderThanHeld,
    /// The same entry arrived again, identical. Nothing changed.
    ResentUnchanged,
    /// The same entry (same quote address and timestamp) arrived again with different state. The
    /// buffer keeps the copy it already holds.
    ///
    /// Titan does this a few times a second: in a 13 minute recording (2026-10-21) every resent
    /// copy carried a subset of the first copy's accounts and slots, with the same values, so the
    /// first copy is the more complete one.
    ResentWithDifferentState,
    /// A pair of one token with itself, which quotes nothing.
    SameTokenPair,
    /// The quote sequence names a slot more than one away from the latest price level message's.
    ArrivedOutsideSlotWindow,
    /// A held entry was dropped because the latest price level message moved the window past its
    /// slot.
    LeftSlotWindow,
    /// The buffer already held [`MAX_HELD`] entries, so the arriving one was not stored.
    BufferFull,
}

/// Where the buffer holds an entry: a venue and a pair, the pair's tokens ordered so that the key
/// does not depend on the order Titan names them in.
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

/// One entry, as the buffer holds it under one venue and pair.
#[derive(Debug, Clone)]
struct Held {
    /// The quote address Titan streamed the entry under, after alias normalization: the venue
    /// itself, or another quote address whose `pricedAddresses` list the venue.
    source: Bytes,
    sequence: u128,
    built_at_ns: u64,
    overrides: Arc<PammSimulationOverrides>,
}

/// The venue entries of the quote stream, held until the books that name them arrive.
pub(super) struct OverrideBuffer {
    held: BTreeMap<PairKey, Vec<Held>>,
    /// How many [`Held`] the map holds across every key.
    count: usize,
    max_held: usize,
    /// The slot of the latest price level message. The buffer holds entries of this slot and of
    /// the slots on either side of it. `None` until the first message, when every slot is held.
    slot: Option<u64>,
}

impl Default for OverrideBuffer {
    fn default() -> Self {
        Self::with_capacity(MAX_HELD)
    }
}

impl OverrideBuffer {
    fn with_capacity(max_held: usize) -> Self {
        Self { held: BTreeMap::new(), count: 0, max_held, slot: None }
    }

    /// Stores `entry`, streamed under `source`, under every venue in `venues` — the venues whose
    /// swaps read its state — and records what happened in `events`.
    ///
    /// The caller normalizes `source` and `venues` through the alias table, as it does a book's
    /// venue, so that the two sockets agree on what a venue is called.
    pub(super) fn store(
        &mut self,
        entry: &VenueOverrides,
        source: &Bytes,
        venues: &[Bytes],
        events: &mut Vec<BufferEvent>,
    ) {
        let overrides = Arc::new(entry.overrides());
        for pair in &entry.pairs {
            if pair.token_1 == pair.token_2 {
                events.push(BufferEvent::SameTokenPair);
                continue;
            }
            if !self.in_window(pair.sequence) {
                events.push(BufferEvent::ArrivedOutsideSlotWindow);
                continue;
            }
            for venue in venues {
                let held = Held {
                    source: source.clone(),
                    sequence: pair.sequence,
                    built_at_ns: entry.built_at_ns,
                    overrides: overrides.clone(),
                };
                let event = self.hold(PairKey::new(venue, &pair.token_1, &pair.token_2), held);
                events.push(event);
            }
        }
    }

    /// Moves the slot window to `slot`, the slot of the latest price level message, and drops
    /// every entry outside it.
    pub(super) fn advance(&mut self, slot: u64, events: &mut Vec<BufferEvent>) {
        self.slot = Some(slot);
        let before = self.count;
        self.held.retain(|_, held| {
            held.retain(|entry| in_window(slot, entry.sequence));
            !held.is_empty()
        });
        self.count = self.held.values().map(Vec::len).sum();
        for _ in self.count..before {
            events.push(BufferEvent::LeftSlotWindow);
        }
    }

    /// The overrides to attach to a book of `venue` pricing `token_a`/`token_b` at `sequence`, or
    /// `None` when the venue's own entries carry nothing for that pair in the book's slot at or
    /// below `sequence`.
    ///
    /// Of each quote address's entries, the one with the highest quote sequence at or below the
    /// book's is used, and of two with the same quote sequence the later-built one.
    pub(super) fn find(
        &self,
        venue: &Bytes,
        token_a: &Bytes,
        token_b: &Bytes,
        sequence: u128,
    ) -> Option<Found> {
        let held = self
            .held
            .get(&PairKey::new(venue, token_a, token_b))?;
        let mut newest: BTreeMap<&Bytes, &Held> = BTreeMap::new();
        for entry in held {
            if slot_of(entry.sequence) != slot_of(sequence) || entry.sequence > sequence {
                continue;
            }
            newest
                .entry(&entry.source)
                .and_modify(|best| {
                    if (entry.sequence, entry.built_at_ns) > (best.sequence, best.built_at_ns) {
                        *best = entry;
                    }
                })
                .or_insert(entry);
        }
        let own = newest.remove(venue)?;
        let overrides = if newest.is_empty() {
            own.overrides.clone()
        } else {
            let others = newest
                .values()
                .map(|other| other.overrides.as_ref());
            Arc::new(own.overrides.merged(others))
        };
        Some(Found {
            overrides,
            same_sequence: own.sequence == sequence,
            merged_sources: newest.len(),
        })
    }

    /// Whether a quote sequence's slot is one the buffer holds.
    fn in_window(&self, sequence: u128) -> bool {
        self.slot
            .is_none_or(|slot| in_window(slot, sequence))
    }

    /// Holds one entry under `key`, unless the buffer already holds that entry or is full.
    fn hold(&mut self, key: PairKey, arriving: Held) -> BufferEvent {
        if let Some(held) = self.held.get_mut(&key) {
            let same = held.iter_mut().find(|entry| {
                entry.source == arriving.source && entry.sequence == arriving.sequence
            });
            if let Some(entry) = same {
                return match arriving
                    .built_at_ns
                    .cmp(&entry.built_at_ns)
                {
                    Ordering::Greater => {
                        *entry = arriving;
                        BufferEvent::ReplacedByNewer
                    }
                    Ordering::Less => BufferEvent::OlderThanHeld,
                    Ordering::Equal
                        if entry
                            .overrides
                            .same_simulated_state(&arriving.overrides) =>
                    {
                        BufferEvent::ResentUnchanged
                    }
                    Ordering::Equal => BufferEvent::ResentWithDifferentState,
                };
            }
        }
        if self.count >= self.max_held {
            return BufferEvent::BufferFull;
        }
        self.held
            .entry(key)
            .or_default()
            .push(arriving);
        self.count += 1;
        BufferEvent::Added
    }

    /// How many entries the buffer holds across every venue and pair.
    #[cfg(test)]
    fn held_entries(&self) -> usize {
        self.count
    }
}

/// Whether a quote sequence's slot is within one of `slot`.
fn in_window(slot: u64, sequence: u128) -> bool {
    let sequence_slot = slot_of(sequence);
    sequence_slot.saturating_add(1) >= slot && sequence_slot <= slot.saturating_add(1)
}

/// The Goliath venue entry of the captured quote stream message
/// `test_responses/pamm_quote_stream_v2_1792115920248724389.json`, transcribed by hand: the venue
/// `0x4367985a…` as it stood at `timestamp` 1792115920127487452, in beacon slot 15440992. The
/// price level message captured with it names this entry, so tests can pin a match to values read
/// off the capture rather than to the parser's own output.
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

/// A hex literal of a captured message as `Bytes`: an address, a balance, a storage slot or a
/// stored word.
#[cfg(test)]
fn hex(literal: &str) -> Bytes {
    Bytes::from_str(literal).expect("hex literal")
}

/// The overrides the [`captured`] venue entry carried: two accounts, one with a balance and a
/// nonce only, one with a single 32-byte storage write.
///
/// `block_timestamp` is the entry's beacon slot as a Unix second, and `expires_at` the second one
/// slot after `built_at_ns`.
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
            config::canonical_pamm_address,
            test_support::{address, USDC, USDT, WBTC, WETH},
            titan::{validate, EntryOutcome, EntryRejection, PairSequence},
        },
        captured::{BALANCE_1, SLOT_1, STORAGE_ACCOUNT, WORD_1},
        *,
    };

    /// The 9-venue quote stream message captured verbatim from the live socket (2026-10-16).
    const CAPTURED_OVERRIDES: &str =
        include_str!("test_responses/pamm_quote_stream_v2_1792115920248724389.json");

    /// The slot both captures were taken in, and the quote sequences the captured Goliath entry
    /// and the captured Metric entry priced their pairs at.
    const CAPTURED_SLOT: u64 = 15_440_992;
    const GOLIATH: &str = "0x4367985a63063fc970b009c6e599e9225181f551";
    const GOLIATH_SEQUENCE: u128 = 284_836_027_668_196_596_826_243_132;

    /// Metric's public identity, the quote address its main entry is streamed under, and the
    /// second quote address whose `pricedAddresses` list Metric's router.
    const METRIC: &str = "0x99ab7c1c5eab47801f7937075998527a550fba8a";
    const METRIC_SECOND_SOURCE: &str = "0x30a88bf894d1b806125a23ebf23d715496fd7102";
    const METRIC_SEQUENCE: u128 = 284_836_027_668_196_596_826_243_146;

    /// A synthetic venue and the account its entries override.
    const VENUE: &str = "0x3ce2672aa806138585920421406bf0decb8130cb";
    const OTHER_SOURCE: &str = "0x00000000000000000000000000000000000000aa";
    const ACCOUNT: &str = "0xba443ecec71dc7123bd68c2082d13e3299cde896";
    const BUILT_AT: u64 = 1_792_115_920_000_000_000;

    /// The quote sequence numbered `n` in `slot`: the slot in the high 64 bits, as Titan's are.
    fn sequence(slot: u64, n: u64) -> u128 {
        (u128::from(slot) << 64) | u128::from(n)
    }

    /// Every accepted entry of the captured message, stored the way the tracker stores it when it
    /// serves every venue: under its own quote address and every venue its `pricedAddresses`
    /// name, each normalized.
    fn captured_buffer() -> OverrideBuffer {
        let mut buffer = OverrideBuffer::default();
        let mut events = Vec::new();
        for outcome in validate(serde_json::from_str(CAPTURED_OVERRIDES).expect("parses")) {
            let entry = match outcome {
                EntryOutcome::Accepted(entry) => entry,
                EntryOutcome::Rejected(EntryRejection::Empty) => continue,
                EntryOutcome::Rejected(reason) => panic!("rejected a captured entry: {reason:?}"),
            };
            let source = canonical_pamm_address(&entry.quote_address);
            let venues = venues_of(&source, &entry.priced_addresses);
            buffer.store(&entry, &source, &venues, &mut events);
        }
        buffer
    }

    /// `source` followed by every other venue `priced` names, each normalized and listed once.
    fn venues_of(source: &Bytes, priced: &[Bytes]) -> Vec<Bytes> {
        let mut seen = HashSet::new();
        [source.clone()]
            .into_iter()
            .chain(
                priced
                    .iter()
                    .map(canonical_pamm_address),
            )
            .filter(|venue| seen.insert(venue.clone()))
            .collect()
    }

    /// A synthetic entry streamed under `source`: it priced each of `pairs` at the given quote
    /// sequence, at `built_at_ns`, overriding `ACCOUNT`'s storage slot 1 to `word` and its nonce
    /// to `nonce`.
    fn entry(
        source: &str,
        built_at_ns: u64,
        nonce: u64,
        word: u8,
        pairs: &[(&str, &str, u128)],
    ) -> VenueOverrides {
        VenueOverrides {
            quote_address: address(source),
            priced_addresses: Vec::new(),
            pairs: pairs
                .iter()
                .map(|(token_1, token_2, sequence)| PairSequence {
                    token_1: address(token_1),
                    token_2: address(token_2),
                    sequence: *sequence,
                })
                .collect(),
            accounts: BTreeMap::from([(
                address(ACCOUNT),
                AccountOverride {
                    balance: None,
                    nonce: Some(nonce),
                    state_diff: BTreeMap::from([(word_bytes(1), word_bytes(word))]),
                },
            )]),
            block_number: Some(26_201_954),
            slot: Some(CAPTURED_SLOT),
            block_timestamp: Some(1_792_115_927),
            built_at_ns,
        }
    }

    /// A 32-byte word whose last byte is `byte`.
    fn word_bytes(byte: u8) -> Bytes {
        let mut word = [0u8; 32];
        word[31] = byte;
        Bytes::from(word)
    }

    /// Stores `entry` under its own quote address only, as the tracker stores an entry whose
    /// `pricedAddresses` name no other venue.
    fn store(buffer: &mut OverrideBuffer, entry: &VenueOverrides) -> Vec<BufferEvent> {
        let mut events = Vec::new();
        let source = entry.quote_address.clone();
        buffer.store(entry, &source, std::slice::from_ref(&source), &mut events);
        events
    }

    fn find(buffer: &OverrideBuffer, sequence: u128) -> Option<Found> {
        buffer.find(&address(VENUE), &address(WBTC), &address(USDC), sequence)
    }

    /// The nonce the found overrides set, which is what tells two synthetic entries apart.
    fn nonce_of(found: &Found) -> u64 {
        found.overrides.accounts[&address(ACCOUNT)]
            .nonce
            .expect("a nonce")
    }

    /// The captured Goliath entry is found by the book that names its quote sequence, with the
    /// values transcribed by hand from the capture.
    #[test]
    fn a_captured_entry_is_found_by_venue_pair_and_quote_sequence() {
        let buffer = captured_buffer();

        let found = buffer
            .find(&address(GOLIATH), &address(WETH), &address(USDC), GOLIATH_SEQUENCE)
            .expect("the Goliath entry");

        assert!(found.same_sequence);
        assert_eq!(found.merged_sources, 0);
        assert_eq!(*found.overrides, test_overrides());
        // The pair's token order does not matter.
        assert_eq!(
            buffer.find(&address(GOLIATH), &address(USDC), &address(WETH), GOLIATH_SEQUENCE),
            Some(found)
        );
    }

    /// A book names a quote sequence no entry carries most of the time, so the newest earlier
    /// entry of the same slot is the one found — never a later one, and never one of another
    /// slot.
    #[test]
    fn the_newest_entry_at_or_below_the_quote_sequence_is_found() {
        let mut buffer = OverrideBuffer::default();
        for (n, nonce) in [(5, 5), (8, 8), (12, 12)] {
            let pairs = [(WBTC, USDC, sequence(CAPTURED_SLOT, n))];
            store(&mut buffer, &entry(VENUE, BUILT_AT + n, nonce, 1, &pairs));
        }
        let previous_slot = [(WBTC, USDC, sequence(CAPTURED_SLOT - 1, 20))];
        store(&mut buffer, &entry(VENUE, BUILT_AT, 99, 1, &previous_slot));

        let exact = find(&buffer, sequence(CAPTURED_SLOT, 8)).expect("the entry at 8");
        assert!(exact.same_sequence);
        assert_eq!(nonce_of(&exact), 8);

        let earlier = find(&buffer, sequence(CAPTURED_SLOT, 10)).expect("the entry at 8");
        assert!(!earlier.same_sequence);
        assert_eq!(nonce_of(&earlier), 8);

        // Below every entry of the slot: the previous slot's entry is never used.
        assert_eq!(find(&buffer, sequence(CAPTURED_SLOT, 4)), None);
    }

    /// Of two entries carrying one quote sequence, the later-built one is found, whichever
    /// arrived first.
    #[test]
    fn of_two_entries_of_one_quote_sequence_the_later_built_is_found() {
        let pairs = [(WBTC, USDC, sequence(CAPTURED_SLOT, 7))];
        let mut buffer = OverrideBuffer::default();

        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT + 1, 2, 1, &pairs)),
            [BufferEvent::Added]
        );
        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &pairs)),
            [BufferEvent::OlderThanHeld]
        );
        assert_eq!(nonce_of(&find(&buffer, sequence(CAPTURED_SLOT, 7)).expect("found")), 2);

        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT + 2, 3, 1, &pairs)),
            [BufferEvent::ReplacedByNewer]
        );
        assert_eq!(nonce_of(&find(&buffer, sequence(CAPTURED_SLOT, 7)).expect("found")), 3);
        assert_eq!(buffer.held_entries(), 1);
    }

    /// The same entry sent twice changes nothing; sent again with different state, the copy
    /// already held is kept.
    #[test]
    fn a_resent_entry_keeps_the_copy_already_held() {
        let pairs = [(WBTC, USDC, sequence(CAPTURED_SLOT, 7))];
        let mut buffer = OverrideBuffer::default();
        store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &pairs));

        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &pairs)),
            [BufferEvent::ResentUnchanged]
        );
        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT, 2, 1, &pairs)),
            [BufferEvent::ResentWithDifferentState]
        );
        assert_eq!(nonce_of(&find(&buffer, sequence(CAPTURED_SLOT, 7)).expect("found")), 1);
    }

    /// Another quote address's entry is merged into the venue's own, and the venue's own entry
    /// wins where both set a value. Without an entry of its own, the venue gets nothing.
    #[test]
    fn other_sources_merge_into_the_venue_s_own_entry() {
        let pairs = [(WBTC, USDC, sequence(CAPTURED_SLOT, 7))];
        let mut buffer = OverrideBuffer::default();
        let mut events = Vec::new();
        let mut other = entry(OTHER_SOURCE, BUILT_AT, 50, 50, &pairs);
        // The other source also writes a slot the venue's own entry does not.
        other
            .accounts
            .get_mut(&address(ACCOUNT))
            .expect("the account")
            .state_diff
            .insert(word_bytes(2), word_bytes(2));
        buffer.store(
            &other,
            &address(OTHER_SOURCE),
            &[address(OTHER_SOURCE), address(VENUE)],
            &mut events,
        );
        assert_eq!(
            find(&buffer, sequence(CAPTURED_SLOT, 7)),
            None,
            "found without the venue's own"
        );

        store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &pairs));
        let found = find(&buffer, sequence(CAPTURED_SLOT, 7)).expect("found");

        assert_eq!(found.merged_sources, 1);
        let account = &found.overrides.accounts[&address(ACCOUNT)];
        assert_eq!(account.nonce, Some(1), "the other source's nonce won");
        assert_eq!(
            account.state_diff[&word_bytes(1)],
            word_bytes(1),
            "the other source's word won"
        );
        assert_eq!(
            account.state_diff[&word_bytes(2)],
            word_bytes(2),
            "the other source's slot is lost"
        );
    }

    /// The captured Metric book's pairs find Metric's main entry with its second quote address's
    /// entry merged in, for the pairs that second entry carries.
    #[test]
    fn the_captured_metric_entries_merge() {
        let buffer = captured_buffer();
        let metric = address(METRIC);

        let merged = buffer
            .find(&metric, &address(WETH), &address(USDC), METRIC_SEQUENCE)
            .expect("Metric's WETH/USDC");
        assert_eq!(merged.merged_sources, 1);
        assert!(merged
            .overrides
            .accounts
            .contains_key(&address(METRIC_SECOND_SOURCE)));

        // The second source carries no USDC/USDT pair, so that pair is Metric's entry alone.
        let alone = buffer
            .find(&metric, &address(USDC), &address(USDT), METRIC_SEQUENCE)
            .expect("Metric's USDC/USDT");
        assert_eq!(alone.merged_sources, 0);
    }

    /// The window holds the latest message's slot and one slot on either side: older entries are
    /// dropped when it moves, and arrivals outside it are not stored.
    #[test]
    fn the_buffer_holds_one_slot_either_side_of_the_latest_message() {
        let mut buffer = OverrideBuffer::default();
        for slot in [CAPTURED_SLOT - 2, CAPTURED_SLOT - 1, CAPTURED_SLOT, CAPTURED_SLOT + 1] {
            store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &[(WBTC, USDC, sequence(slot, 1))]));
        }

        let mut events = Vec::new();
        buffer.advance(CAPTURED_SLOT, &mut events);

        assert_eq!(events, [BufferEvent::LeftSlotWindow]);
        assert_eq!(buffer.held_entries(), 3);
        assert_eq!(find(&buffer, sequence(CAPTURED_SLOT - 2, 1)), None);
        assert!(find(&buffer, sequence(CAPTURED_SLOT - 1, 1)).is_some());
        for slot in [CAPTURED_SLOT - 2, CAPTURED_SLOT + 2] {
            let pairs = [(WBTC, USDC, sequence(slot, 2))];
            assert_eq!(
                store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &pairs)),
                [BufferEvent::ArrivedOutsideSlotWindow]
            );
        }
    }

    /// A full buffer stores nothing new but keeps serving what it holds, and stores again once
    /// the window moves and frees room.
    #[test]
    fn a_full_buffer_stores_again_once_the_window_frees_room() {
        let mut buffer = OverrideBuffer::with_capacity(2);
        let old = [(WBTC, USDC, sequence(CAPTURED_SLOT - 1, 1))];
        let current = [(WBTC, USDC, sequence(CAPTURED_SLOT, 1))];
        let next = [(WBTC, USDC, sequence(CAPTURED_SLOT + 1, 1))];
        store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &old));
        store(&mut buffer, &entry(VENUE, BUILT_AT, 2, 1, &current));

        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT, 3, 1, &next)),
            [BufferEvent::BufferFull]
        );
        assert!(find(&buffer, sequence(CAPTURED_SLOT, 1)).is_some());

        buffer.advance(CAPTURED_SLOT + 1, &mut Vec::new());
        assert_eq!(store(&mut buffer, &entry(VENUE, BUILT_AT, 3, 1, &next)), [BufferEvent::Added]);
    }

    /// A pair of one token with itself quotes nothing, so it is not stored, and the entry's other
    /// pairs still are.
    #[test]
    fn a_same_token_pair_is_skipped() {
        let mut buffer = OverrideBuffer::default();
        let pairs =
            [(WBTC, WBTC, sequence(CAPTURED_SLOT, 1)), (WBTC, USDC, sequence(CAPTURED_SLOT, 1))];

        assert_eq!(
            store(&mut buffer, &entry(VENUE, BUILT_AT, 1, 1, &pairs)),
            [BufferEvent::SameTokenPair, BufferEvent::Added]
        );
        assert!(find(&buffer, sequence(CAPTURED_SLOT, 1)).is_some());
    }

    /// `expires_at` is one slot after the timestamp, floored, and never wraps near the end of the
    /// clock.
    #[test]
    fn expires_at_is_one_slot_after_the_timestamp() {
        assert_eq!(expires_at(1_792_115_920_127_487_452), 1_792_115_932);
        assert_eq!(expires_at(u64::MAX), (u128::from(u64::MAX) / 1_000_000_000 + 12) as u64);
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

    /// Accounts and storage slots are `0x`-prefixed hex strings in a recording, as they are on
    /// the stream, not byte sequences: a consumer reading a recording finds the addresses it
    /// knows.
    #[test]
    fn accounts_and_slots_are_hex_string_keys() {
        let json = serde_json::to_value(test_overrides()).expect("serializes");
        assert_eq!(json["accounts"][STORAGE_ACCOUNT]["nonce"], serde_json::json!(1));
        assert_eq!(
            json["accounts"][STORAGE_ACCOUNT]["state_diff"][SLOT_1],
            serde_json::json!(WORD_1)
        );
    }

    /// Real overrides survive both serde_json shapes. Nothing in them is wider than `u64`
    /// precisely so that they do: a `serde_json::Value` cannot hold a wider integer at all.
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

    /// The block fields are optional, and overrides without them round-trip to the same text.
    #[test]
    fn the_block_fields_are_optional() {
        let text = r#"{"accounts":{},"built_at_ns":1791475494775213874,"expires_at":1791475506}"#;
        let read: PammSimulationOverrides = serde_json::from_str(text).expect("reads back");
        assert_eq!(read.block_number, None);
        assert_eq!(read.block_timestamp, None);
        assert_eq!(serde_json::to_string(&read).expect("serializes"), text);
    }

    /// The accounts and the two timestamps are required: overrides whose contents or timing are
    /// unknown are rejected, never defaulted to empty or to the epoch.
    #[test]
    fn overrides_missing_a_required_field_are_rejected() {
        for text in [
            r#"{"built_at_ns":1791475494775213874,"expires_at":1791475506}"#,
            r#"{"accounts":{},"expires_at":1791475506}"#,
            r#"{"accounts":{},"built_at_ns":1791475494775213874}"#,
        ] {
            let error = serde_json::from_str::<PammSimulationOverrides>(text)
                .expect_err("overrides missing a required field are rejected");
            assert!(
                error
                    .to_string()
                    .contains("missing field"),
                "unexpected error: {error}"
            );
        }
    }
}
