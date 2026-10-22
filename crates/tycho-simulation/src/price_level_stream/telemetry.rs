//! Metrics of the price level stream, emitted through the `metrics` facade. A consumer that
//! installs a `metrics` recorder receives them with no further setup; without a recorder every
//! call is a no-op. Label values are registered venue names, the name of one of the two Titan
//! sockets ([`TitanMessage::SOCKET`](super::titan::TitanMessage::SOCKET)), or the enumerations
//! below, never values from the stream, with one exception: an auto-detected venue is named by its
//! address, so its `venue` label carries the address the frame named it by.

use metrics::{counter, gauge, histogram};
use strum::IntoEnumIterator;
use strum_macros::IntoStaticStr;

use super::{
    overrides::BufferEvent,
    titan::{EntryRejection, TitanMessage, TitanOverrideMessage, TitanPriceLevelMessage},
};

/// The two `socket` label values, taken from the frame types that name them so that a socket is
/// named identically in a log and in a series.
const PRICE_LEVEL_SOCKET: &str = TitanPriceLevelMessage::SOCKET;
const OVERRIDE_SOCKET: &str = TitanOverrideMessage::SOCKET;

/// Counter, no labels. Incremented once per frame the tracker accepts.
pub(super) const FRAMES_ACCEPTED: &str = "price_level_stream_frames_accepted_total";
/// Counter, labels `socket` and `reason`, one of [`RejectReason`]. Incremented once per frame the
/// stream drops. `socket` names which Titan socket sent it: only `parse_error` reaches this
/// counter from the override socket, because every other reason is a freshness check the tracker
/// makes of a price level frame.
pub(super) const FRAMES_REJECTED: &str = "price_level_stream_frames_rejected_total";
/// Histogram, no labels. The age of every accepted frame at acceptance, in seconds: the local
/// wall clock minus the frame's `timestamp`, negative when the frame is stamped ahead of
/// the local clock. It measures Titan's lag plus delivery delay, and a clock skew between Titan
/// and this host; a frame older than one slot yields states that refuse to quote.
pub(super) const FRAME_AGE: &str = "price_level_stream_frame_age_seconds";
/// Gauge, label `venue`. The frame's `timestamp` of the newest accepted frame that carried the
/// venue, in seconds since the Unix epoch; 0 until the first such frame. `venue` is the
/// registered venue name, or the address of an auto-detected venue.
pub(super) const LAST_SEEN: &str = "price_level_stream_last_seen_timestamp_seconds";
/// Gauge, label `venue`. The number of components the stream currently serves for the venue;
/// 0 for every registered venue from the start. `venue` is as on `LAST_SEEN`.
pub(super) const SERVED_COMPONENTS: &str = "price_level_stream_served_components";
/// Counter, label `venue`. Incremented once per component that turns stale at its `stale_at`
/// deadline and is emitted in `removed_pairs`. `venue` is as on `LAST_SEEN`.
pub(super) const STALE_REMOVALS: &str = "price_level_stream_stale_removals_total";
/// Gauge, no labels. The stream's serving state as a [`ServingState`] number: 0 = unserved,
/// 1 = serving.
pub(super) const SERVING_STATE: &str = "price_level_stream_serving_state";
/// Counter, labels `socket` and `reason`, one of [`ReconnectReason`]. Incremented once per Titan
/// connection the stream gives up on, or fails to establish, before backing off. Both sockets are
/// maintained by one function, so `socket` is what tells an override socket that keeps dropping
/// from the price level socket flapping — only the second puts quoting at risk.
pub(super) const RECONNECTS: &str = "price_level_stream_reconnects_total";
/// Counter, no labels. Incremented once per venue an accepted frame carries books of that is
/// neither registered nor served under auto-detection — once per venue and frame, however many
/// books the frame carried for it.
pub(super) const UNREGISTERED_PAMM_ENTRIES: &str =
    "price_level_stream_unregistered_pamm_entries_total";
/// Counter, label `reason`, one of [`SkipReason`]. Incremented once per book of a served venue
/// whose token addresses do not name a pair this consumer can serve, so the book is not priced at
/// all. Every reason starts at zero. The address itself is never a label — it appears only in the
/// accompanying DEBUG log.
pub(super) const SKIPPED_BOOKS: &str = "price_level_stream_skipped_books_total";
/// Counter, label `reason`, one of [`SideRejectReason`]. Incremented once per book side whose
/// levels are not a usable price function and which is therefore dropped whole, leaving that one
/// trade direction unquotable while the opposite one is still served. Every reason starts at
/// zero.
pub(super) const REJECTED_SIDES: &str = "price_level_stream_rejected_sides_total";
/// Counter, labels `venue` and `outcome`, one of [`MatchOutcome`]. Incremented once per pair a
/// price level message serves while simulation overrides are on, saying whether the pair's state
/// got overrides and how close a match they were, so the sum over `outcome` equals the pairs
/// served. `venue` is as on [`LAST_SEEN`]. Every outcome of every registered venue starts at zero.
pub(super) const OVERRIDE_MATCHES: &str = "price_level_stream_override_match_total";
/// Counter, label `event`, one of [`BufferEvent`]. Incremented once per venue and pair an entry is
/// held under, and once per held entry the slot window drops. It counts what the buffer holds,
/// never whether a state got overrides, which is [`OVERRIDE_MATCHES`], so the two must not be
/// divided by one another. Every event starts at zero while overrides are on.
pub(super) const OVERRIDE_BUFFER: &str = "price_level_stream_override_buffer_total";
/// Counter, label `reason`, one of [`EntryRejection`]. Incremented once per venue entry refused
/// whole before the buffer saw it, meaning Titan sent something this parser does not accept.
/// `empty` has a permanent baseline of about one a second (see [`EntryRejection::Empty`]), so
/// alerts must exclude it. Every reason starts at zero while overrides are on.
pub(super) const REFUSED_OVERRIDE_ENTRIES: &str =
    "price_level_stream_refused_override_entries_total";
/// Counter, no labels. Incremented once per **served** book whose `calculated_from` this parser
/// cannot read: a book whose tokens did not resolve never has it read. A book naming none is not
/// counted, so a change in Titan's format stands out instead of hiding among books that name
/// nothing. Counted per book, where [`OVERRIDE_MATCHES`] counts per pair, so the two must not be
/// divided by one another. Registered whether or not overrides are on, because the field is read
/// off the price level socket either way.
pub(super) const UNREADABLE_QUOTE_SEQUENCE: &str =
    "price_level_stream_unreadable_quote_sequence_total";
/// Counter, label `reason`, one of [`QuoteRefusedReason`]. Incremented once per call to
/// `spot_price`, `get_amount_out` or `get_limits` that a state refused, before any node is
/// involved. Counted per call, so its rate follows how often a router asks, not how many states
/// refused. `ladders_expired` is the reason to watch: a consumer that quotes promptly should
/// almost never see it, and a rising rate means states go unrefreshed for a slot.
pub(super) const QUOTES_REFUSED: &str = "price_level_stream_quotes_refused_total";
/// Counter, label `socket`. Incremented once per quote sequence whose high 64 bits are not the
/// beacon slot of the message carrying it: a book's on `price_levels`, a venue entry's pair on
/// `quote_stream`.
///
/// **This watches an assumption.** Titan does not document that encoding, and matching relies on
/// it: a book only matches an entry of its own slot, and the buffer keeps entries by slot. It
/// held for 29,772 books and 324,956 entry pairs on 2026-10-21. If it moves, matching needs
/// revisiting.
pub(super) const SEQUENCE_SLOT_MISMATCH: &str = "price_level_stream_sequence_slot_mismatch_total";
/// Counter, label `field`, one of [`MissingField`]. Incremented once per accepted venue entry
/// that omits a field a consumer needs to simulate a quote: the block number, or the slot its
/// timestamp comes from. Titan sent both on all 77,377 entries measured on 2026-10-21, so a
/// non-zero rate is a change on Titan's side.
pub(super) const OVERRIDE_ENTRY_MISSING_FIELD: &str =
    "price_level_stream_override_entry_missing_field_total";
/// Counter, no labels. Incremented once per price level message that carries two or more books
/// for one venue and pair, where the last one is served. Never observed in 795 messages on
/// 2026-10-21; counted because the replacement changes a served price silently.
pub(super) const DUPLICATE_BOOKS: &str = "price_level_stream_duplicate_books_total";
/// Counter, no labels. Incremented once per accepted venue entry streamed under a quote address
/// this consumer does not serve. Such an entry is still held under any served venue its
/// `pricedAddresses` name, as Metric's second quote address's entries are, and dropped
/// otherwise.
pub(super) const UNSERVED_VENUE_ENTRIES: &str = "price_level_stream_unserved_venue_entries_total";
/// Histogram. The storage words one accepted venue entry overrode, so growth toward the
/// per-entry cap of 512 shows before the cap refuses anything. Median 7 and maximum 16 on
/// 2026-10-21.
pub(super) const OVERRIDE_ENTRY_SLOTS: &str = "price_level_stream_override_entry_storage_slots";
/// Histogram. The accounts one accepted venue entry overrode, against the per-entry cap of 32.
/// Median 3 and maximum 9 on 2026-10-21.
pub(super) const OVERRIDE_ENTRY_ACCOUNTS: &str = "price_level_stream_override_entry_accounts";
/// Counter, no labels. Incremented once per state whose attached overrides name a block other
/// than the price level message's. A book only matches an entry of its own slot, so this stays
/// at zero while Titan's encoding holds; it watches the block a consumer will simulate at
/// directly, where [`SEQUENCE_SLOT_MISMATCH`] watches the encoding.
pub(super) const OVERRIDE_BLOCK_MISMATCH: &str = "price_level_stream_override_block_mismatch_total";
/// Counter, no labels. Incremented by the number of other quote addresses' entries merged into a
/// state's overrides. Only Metric does this today: its router reads the state of two quote
/// addresses.
pub(super) const OVERRIDE_MERGED_SOURCES: &str = "price_level_stream_override_merged_sources_total";

/// The `reason` label of [`FRAMES_REJECTED`]: why the stream dropped a frame. The label value
/// is the variant name in snake case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum RejectReason {
    /// The text did not parse as a frame.
    ParseError,
    /// The frame's `timestamp` is `stale_after` old or older.
    TooOld,
    /// The frame's `timestamp` is more than one slot ahead of the local wall clock.
    InFuture,
    /// The frame's `timestamp` is older than the newest accepted frame's.
    OutOfOrder,
    /// The block is below the newest accepted block.
    BlockRegression,
    /// The block is above the newest accepted block by more than one block per elapsed slot
    /// plus 2.
    BlockJump,
}

impl RejectReason {
    /// Every reason, so [`init_socket_counters`] can register all of them against the price level
    /// socket. A new variant belongs here too.
    const ALL: [Self; 6] = [
        Self::ParseError,
        Self::TooOld,
        Self::InFuture,
        Self::OutOfOrder,
        Self::BlockRegression,
        Self::BlockJump,
    ];
}

/// The `reason` label of [`RECONNECTS`]: why the stream gave up on a Titan connection. The
/// label value is the variant name in snake case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum ReconnectReason {
    /// No frame parsed within the idle timeout.
    IdleTimeout,
    /// The server hung up without a close frame.
    Ended,
    /// The server sent a close frame.
    Closed,
    /// A transport or protocol error.
    ReadError,
    /// The connection was refused, or the TLS handshake failed.
    ConnectFailed,
    /// The handshake did not complete within the connect timeout.
    ConnectTimeout,
}

impl ReconnectReason {
    /// Every reason, so [`init_socket_counters`] can register all of them against each socket this
    /// consumer reads. A new variant belongs here too.
    const ALL: [Self; 6] = [
        Self::IdleTimeout,
        Self::Ended,
        Self::Closed,
        Self::ReadError,
        Self::ConnectFailed,
        Self::ConnectTimeout,
    ];
}

/// The `reason` label of [`SKIPPED_BOOKS`]: why a book named no pair this consumer can serve.
/// The label value is the variant name in snake case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum SkipReason {
    /// One of the two addresses is not 20 bytes, so it identifies no asset. A producer-side
    /// fault, kept apart from an unadmitted token so it cannot hide inside the ordinary miss rate.
    MalformedAddress,
    /// The consumer's token metadata does not carry one of the two addresses.
    UnadmittedToken,
    /// Both addresses name one asset, so the book names no pair to quote.
    OneAssetPair,
}

impl SkipReason {
    /// Every reason, so [`init_book_counters`] can register all of them. A new variant belongs
    /// here too.
    const ALL: [Self; 3] = [Self::MalformedAddress, Self::UnadmittedToken, Self::OneAssetPair];
}

/// The `reason` label of [`REJECTED_SIDES`]: why a book side was dropped whole. The label value
/// is the variant name in snake case. Mirrors the conversion's own rejection reasons, which
/// [`tracker`](super::tracker) maps onto these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum SideRejectReason {
    /// A level's stream price or volume is not a finite positive number.
    UnsupportedLevel,
    /// A level's amount overflowed `f64` to infinity.
    NonFiniteAmount,
    /// A level rounds to a zero amount, which is not a quote.
    ZeroAmount,
    /// Two levels round to the same input amount but disagree on the output.
    ConflictingKnots,
}

impl SideRejectReason {
    /// Every reason, so [`init_book_counters`] can register all of them. A new variant belongs
    /// here too.
    const ALL: [Self; 4] =
        [Self::UnsupportedLevel, Self::NonFiniteAmount, Self::ZeroAmount, Self::ConflictingKnots];
}

/// The `outcome` label of [`OVERRIDE_MATCHES`]: whether a served pair's state got simulation
/// overrides. The label value is the variant name in snake case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum MatchOutcome {
    /// The venue's entry carried the book's own quote sequence.
    SameSequence,
    /// The venue's newest entry for the pair in the book's slot carried an earlier quote sequence.
    EarlierSequence,
    /// The venue had no entry for the pair in the book's slot at or below its quote sequence, so
    /// the state carries no overrides.
    NoneInSlot,
    /// The book named no quote sequence, or one this parser cannot read, so nothing was looked
    /// up. Counted so that the sum over this label equals the pairs served.
    NoQuoteSequence,
}

impl MatchOutcome {
    /// Every outcome, so [`init_override_matches`] can register all of them. A new variant
    /// belongs here too.
    const ALL: [Self; 4] =
        [Self::SameSequence, Self::EarlierSequence, Self::NoneInSlot, Self::NoQuoteSequence];
}

/// The `reason` label of [`QUOTES_REFUSED`]: why a state refused a quote, before any node call.
/// The label value is the variant name in snake case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum QuoteRefusedReason {
    /// The message that carried the price levels is one slot old.
    LaddersExpired,
    /// The quotes either side of the amount asked for do not increase in output.
    NotMonotonic,
    /// The two tokens asked for are not this component's pair.
    UnknownPair,
    /// The direction asked for has no quotes: a one-sided book, or a side the conversion
    /// declined.
    NoLiquidity,
    /// The amount asked for is below the smallest quote the venue gave.
    BelowSmallestQuote,
    /// A quote amount does not fit the `f64` a spot price is returned in.
    SpotPriceUnrepresentable,
}

impl QuoteRefusedReason {
    /// Every reason, so [`init_book_counters`] can register all of them.
    const ALL: [Self; 6] = [
        Self::LaddersExpired,
        Self::NotMonotonic,
        Self::UnknownPair,
        Self::NoLiquidity,
        Self::BelowSmallestQuote,
        Self::SpotPriceUnrepresentable,
    ];
}

/// The `field` label of [`OVERRIDE_ENTRY_MISSING_FIELD`]: the field an accepted venue entry
/// omitted. The label value is the variant name in snake case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum MissingField {
    /// `blockNumber`: which block the state was taken for.
    BlockNumber,
    /// `slot`: the only source of that block's timestamp.
    Slot,
}

impl MissingField {
    const ALL: [Self; 2] = [Self::BlockNumber, Self::Slot];
}

/// The stream's serving state, exported as the numeric value of `SERVING_STATE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ServingState {
    /// No component is served.
    Unserved = 0,
    /// At least one component is served.
    Serving = 1,
}

pub(super) fn record_frame_accepted() {
    counter!(FRAMES_ACCEPTED).increment(1);
}

pub(super) fn record_frame_rejected(socket: &'static str, reason: RejectReason) {
    counter!(FRAMES_REJECTED, "socket" => socket, "reason" => <&'static str>::from(reason))
        .increment(1);
}

pub(super) fn record_frame_age(age_seconds: f64) {
    histogram!(FRAME_AGE).record(age_seconds);
}

pub(super) fn record_last_seen(venue: &str, unix_seconds: u64) {
    gauge!(LAST_SEEN, "venue" => venue.to_string()).set(unix_seconds as f64);
}

pub(super) fn record_served_components(venue: &str, count: usize) {
    gauge!(SERVED_COMPONENTS, "venue" => venue.to_string()).set(count as f64);
}

pub(super) fn record_stale_removal(venue: &str) {
    counter!(STALE_REMOVALS, "venue" => venue.to_string()).increment(1);
}

/// Registers the [`STALE_REMOVALS`] series of `venue` at zero, so a venue that never loses a
/// component to staleness reads as a visible zero rather than a missing series — matching the
/// per-venue gauges, which are set to zero for the same reason.
pub(super) fn init_stale_removals(venue: &str) {
    counter!(STALE_REMOVALS, "venue" => venue.to_string()).increment(0);
}

pub(super) fn record_serving_state(state: ServingState) {
    gauge!(SERVING_STATE).set(state as u8 as f64);
}

pub(super) fn record_reconnect(socket: &'static str, reason: ReconnectReason) {
    counter!(RECONNECTS, "socket" => socket, "reason" => <&'static str>::from(reason)).increment(1);
}

pub(super) fn record_unregistered_pamm() {
    counter!(UNREGISTERED_PAMM_ENTRIES).increment(1);
}

pub(super) fn record_skipped_book(reason: SkipReason) {
    counter!(SKIPPED_BOOKS, "reason" => <&'static str>::from(reason)).increment(1);
}

pub(super) fn record_rejected_side(reason: SideRejectReason) {
    counter!(REJECTED_SIDES, "reason" => <&'static str>::from(reason)).increment(1);
}

pub(super) fn record_override_match(venue: &str, outcome: MatchOutcome) {
    let outcome = <&'static str>::from(outcome);
    counter!(OVERRIDE_MATCHES, "venue" => venue.to_string(), "outcome" => outcome).increment(1);
}

pub(super) fn record_buffer_event(event: BufferEvent) {
    counter!(OVERRIDE_BUFFER, "event" => <&'static str>::from(event)).increment(1);
}

pub(super) fn record_refused_override_entry(reason: EntryRejection) {
    counter!(REFUSED_OVERRIDE_ENTRIES, "reason" => <&'static str>::from(reason)).increment(1);
}

pub(super) fn record_quote_refused(reason: QuoteRefusedReason) {
    counter!(QUOTES_REFUSED, "reason" => <&'static str>::from(reason)).increment(1);
}

pub(super) fn record_sequence_slot_mismatch(socket: &'static str) {
    counter!(SEQUENCE_SLOT_MISMATCH, "socket" => socket).increment(1);
}

pub(super) fn record_override_entry_missing_field(field: MissingField) {
    counter!(OVERRIDE_ENTRY_MISSING_FIELD, "field" => <&'static str>::from(field)).increment(1);
}

pub(super) fn record_duplicate_books() {
    counter!(DUPLICATE_BOOKS).increment(1);
}

pub(super) fn record_unserved_venue_entry() {
    counter!(UNSERVED_VENUE_ENTRIES).increment(1);
}

pub(super) fn record_override_entry_size(accounts: usize, slots: usize) {
    histogram!(OVERRIDE_ENTRY_ACCOUNTS).record(accounts as f64);
    histogram!(OVERRIDE_ENTRY_SLOTS).record(slots as f64);
}

pub(super) fn record_override_block_mismatch() {
    counter!(OVERRIDE_BLOCK_MISMATCH).increment(1);
}

pub(super) fn record_override_merged_sources(sources: usize) {
    counter!(OVERRIDE_MERGED_SOURCES).increment(sources as u64);
}

pub(super) fn record_unreadable_quote_sequence() {
    counter!(UNREADABLE_QUOTE_SEQUENCE).increment(1);
}

/// Registers every series read off a price level book at zero — every `reason` of
/// [`SKIPPED_BOOKS`], [`REJECTED_SIDES`] and [`QUOTES_REFUSED`], plus
/// [`UNREADABLE_QUOTE_SEQUENCE`], [`DUPLICATE_BOOKS`] and the `price_levels` series of
/// [`SEQUENCE_SLOT_MISMATCH`] — so a reason that never fires reads as a visible zero instead of a
/// missing series, as the per-venue gauges do for a venue that never appears.
///
/// This consumer reads the price level socket whether or not overrides are on, so all of these
/// are registered unconditionally.
pub(super) fn init_book_counters() {
    for reason in SkipReason::ALL {
        counter!(SKIPPED_BOOKS, "reason" => <&'static str>::from(reason)).increment(0);
    }
    for reason in SideRejectReason::ALL {
        counter!(REJECTED_SIDES, "reason" => <&'static str>::from(reason)).increment(0);
    }
    for reason in QuoteRefusedReason::ALL {
        counter!(QUOTES_REFUSED, "reason" => <&'static str>::from(reason)).increment(0);
    }
    counter!(UNREADABLE_QUOTE_SEQUENCE).increment(0);
    counter!(SEQUENCE_SLOT_MISMATCH, "socket" => PRICE_LEVEL_SOCKET).increment(0);
    counter!(DUPLICATE_BOOKS).increment(0);
}

/// Registers every series of the overrides path at zero, except the per-venue match outcomes,
/// which [`init_override_matches`] registers per venue.
///
/// Called only while overrides are on: otherwise no quote stream socket is connected, none of
/// these can ever move, and a flat zero would advertise a path this process does not run. The
/// buffer events and refusal reasons are enumerated from their enums, so a new variant cannot
/// miss its zero.
pub(super) fn init_override_counters() {
    for event in BufferEvent::iter() {
        counter!(OVERRIDE_BUFFER, "event" => <&'static str>::from(event)).increment(0);
    }
    for reason in EntryRejection::iter() {
        counter!(REFUSED_OVERRIDE_ENTRIES, "reason" => <&'static str>::from(reason)).increment(0);
    }
    for field in MissingField::ALL {
        let field = <&'static str>::from(field);
        counter!(OVERRIDE_ENTRY_MISSING_FIELD, "field" => field).increment(0);
    }
    counter!(SEQUENCE_SLOT_MISMATCH, "socket" => OVERRIDE_SOCKET).increment(0);
    counter!(UNSERVED_VENUE_ENTRIES).increment(0);
    counter!(OVERRIDE_BLOCK_MISMATCH).increment(0);
    counter!(OVERRIDE_MERGED_SOURCES).increment(0);
}

/// Registers every match outcome of `venue` at zero, while overrides are on.
pub(super) fn init_override_matches(venue: &str) {
    for outcome in MatchOutcome::ALL {
        let outcome = <&'static str>::from(outcome);
        counter!(OVERRIDE_MATCHES, "venue" => venue.to_string(), "outcome" => outcome).increment(0);
    }
}

/// Registers the rejection and reconnect series of the sockets this consumer reads, at zero, so a
/// socket that has never failed reads as zeros rather than as a gap — the distinction an operator
/// needs to tell a healthy socket from one that was never configured.
///
/// Asymmetric on purpose. `parse_error` is the only [`RejectReason`] the override socket can ever
/// carry, because every other one is a freshness check [`tracker`](super::tracker) makes of a price
/// level frame; registering the rest against it would advertise five series no code path can reach.
/// Reconnects, by contrast, are recorded by the one function that maintains both sockets, so every
/// reason belongs to each socket that exists.
pub(super) fn init_socket_counters(override_socket: bool) {
    for reason in RejectReason::ALL {
        let reason = <&'static str>::from(reason);
        counter!(FRAMES_REJECTED, "socket" => PRICE_LEVEL_SOCKET, "reason" => reason).increment(0);
    }
    for reason in ReconnectReason::ALL {
        let reason = <&'static str>::from(reason);
        counter!(RECONNECTS, "socket" => PRICE_LEVEL_SOCKET, "reason" => reason).increment(0);
    }
    if !override_socket {
        return;
    }
    let parse_error = <&'static str>::from(RejectReason::ParseError);
    counter!(FRAMES_REJECTED, "socket" => OVERRIDE_SOCKET, "reason" => parse_error).increment(0);
    for reason in ReconnectReason::ALL {
        let reason = <&'static str>::from(reason);
        counter!(RECONNECTS, "socket" => OVERRIDE_SOCKET, "reason" => reason).increment(0);
    }
}

/// Reads a `DebuggingRecorder` snapshot back into plain values for assertions.
#[cfg(test)]
pub(super) mod recorded {
    use std::{
        collections::{BTreeMap, HashMap},
        future::Future,
    };

    use metrics_util::{
        debugging::{DebugValue, DebuggingRecorder, Snapshot},
        MetricKind,
    };

    pub(in super::super) type SnapshotMap =
        HashMap<(MetricKind, String, BTreeMap<String, String>), DebugValue>;

    /// Runs `future` on a current-thread runtime with a `DebuggingRecorder` installed and
    /// returns its output together with every metric it recorded. `metrics::with_local_recorder`
    /// takes a sync closure, which is why this cannot be a `#[tokio::test]`.
    pub(in super::super) fn record_async<T>(future: impl Future<Output = T>) -> (T, SnapshotMap) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let output = metrics::with_local_recorder(&recorder, || runtime.block_on(future));
        (output, snapshot_map(snapshotter.snapshot()))
    }

    pub(in super::super) fn snapshot_map(snapshot: Snapshot) -> SnapshotMap {
        snapshot
            .into_vec()
            .into_iter()
            .map(|(composite_key, _unit, _description, value)| {
                let name = composite_key.key().name().to_string();
                let labels = composite_key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                ((composite_key.kind(), name, labels), value)
            })
            .collect()
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    pub(in super::super) fn counter_value(
        snapshot: &SnapshotMap,
        name: &str,
        label_pairs: &[(&str, &str)],
    ) -> u64 {
        match snapshot.get(&(MetricKind::Counter, name.to_string(), labels(label_pairs))) {
            Some(DebugValue::Counter(value)) => *value,
            Some(DebugValue::Gauge(_)) | Some(DebugValue::Histogram(_)) | None => 0,
        }
    }

    /// Whether the counter series was registered at all, whatever its value. A series registered
    /// at zero and a series that was never touched both read as 0 through [`counter_value`], so
    /// only this tells a visible zero from a missing series.
    pub(in super::super) fn counter_registered(
        snapshot: &SnapshotMap,
        name: &str,
        label_pairs: &[(&str, &str)],
    ) -> bool {
        snapshot.contains_key(&(MetricKind::Counter, name.to_string(), labels(label_pairs)))
    }

    /// How many series of `name` the snapshot holds, whatever their labels. Paired with per-value
    /// lookups it pins a label vocabulary exactly rather than as a lower bound.
    pub(in super::super) fn registered_series(snapshot: &SnapshotMap, name: &str) -> usize {
        snapshot
            .keys()
            .filter(|(_kind, series, _labels)| series == name)
            .count()
    }

    /// Every label value recorded on any metric, for checking that no value from the stream became
    /// a label.
    pub(in super::super) fn all_label_values(snapshot: &SnapshotMap) -> Vec<&str> {
        snapshot
            .keys()
            .flat_map(|(_kind, _name, labels)| labels.values().map(String::as_str))
            .collect()
    }

    pub(in super::super) fn gauge_value(
        snapshot: &SnapshotMap,
        name: &str,
        label_pairs: &[(&str, &str)],
    ) -> f64 {
        match snapshot.get(&(MetricKind::Gauge, name.to_string(), labels(label_pairs))) {
            Some(DebugValue::Gauge(value)) => value.into_inner(),
            Some(DebugValue::Counter(_)) | Some(DebugValue::Histogram(_)) | None => f64::NAN,
        }
    }

    /// Every value recorded on the histogram, in recording order; empty when nothing was
    /// recorded.
    pub(in super::super) fn histogram_values(
        snapshot: &SnapshotMap,
        name: &str,
        label_pairs: &[(&str, &str)],
    ) -> Vec<f64> {
        match snapshot.get(&(MetricKind::Histogram, name.to_string(), labels(label_pairs))) {
            Some(DebugValue::Histogram(values)) => values
                .iter()
                .map(|value| value.into_inner())
                .collect(),
            Some(DebugValue::Counter(_)) | Some(DebugValue::Gauge(_)) | None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    // The enum's own count, which only the vocabulary test reads.
    use strum::EnumCount;

    use super::{recorded::*, *};

    /// The metric names and label values are the contract with dashboards and alerts, so the
    /// lookups spell them out instead of reusing the constants and enums the helpers emit under.
    #[test]
    fn every_helper_emits_its_named_metric() {
        let ((), snapshot) = record_async(async {
            record_frame_accepted();
            record_frame_rejected("price_levels", RejectReason::TooOld);
            record_frame_rejected("price_levels", RejectReason::TooOld);
            record_frame_age(0.25);
            record_last_seen("fermiswap", 1_700_000_000);
            record_served_components("fermiswap", 3);
            record_stale_removal("fermiswap");
            record_serving_state(ServingState::Serving);
            record_reconnect("quote_stream", ReconnectReason::IdleTimeout);
            record_unregistered_pamm();
            record_skipped_book(SkipReason::MalformedAddress);
            record_rejected_side(SideRejectReason::ConflictingKnots);
            record_override_match("fermiswap", MatchOutcome::EarlierSequence);
            record_buffer_event(BufferEvent::ResentWithDifferentState);
            record_refused_override_entry(EntryRejection::PairsAndSequencesDiffer);
            record_unreadable_quote_sequence();
            record_quote_refused(QuoteRefusedReason::BelowSmallestQuote);
            record_sequence_slot_mismatch("quote_stream");
            record_override_entry_missing_field(MissingField::BlockNumber);
            record_duplicate_books();
            record_unserved_venue_entry();
            record_override_entry_size(3, 7);
            record_override_block_mismatch();
            record_override_merged_sources(2);
        });
        assert_eq!(counter_value(&snapshot, "price_level_stream_frames_accepted_total", &[]), 1);
        assert_eq!(
            counter_value(
                &snapshot,
                "price_level_stream_frames_rejected_total",
                &[("socket", "price_levels"), ("reason", "too_old")]
            ),
            2
        );
        assert_eq!(
            histogram_values(&snapshot, "price_level_stream_frame_age_seconds", &[]),
            vec![0.25]
        );
        assert_eq!(
            gauge_value(
                &snapshot,
                "price_level_stream_last_seen_timestamp_seconds",
                &[("venue", "fermiswap")]
            ),
            1_700_000_000.0
        );
        assert_eq!(
            gauge_value(
                &snapshot,
                "price_level_stream_served_components",
                &[("venue", "fermiswap")]
            ),
            3.0
        );
        assert_eq!(
            counter_value(
                &snapshot,
                "price_level_stream_stale_removals_total",
                &[("venue", "fermiswap")]
            ),
            1
        );
        assert_eq!(gauge_value(&snapshot, "price_level_stream_serving_state", &[]), 1.0);
        assert_eq!(
            counter_value(
                &snapshot,
                "price_level_stream_reconnects_total",
                &[("socket", "quote_stream"), ("reason", "idle_timeout")]
            ),
            1
        );
        assert_eq!(
            counter_value(&snapshot, "price_level_stream_unregistered_pamm_entries_total", &[]),
            1
        );
        assert_eq!(
            counter_value(
                &snapshot,
                "price_level_stream_skipped_books_total",
                &[("reason", "malformed_address")]
            ),
            1
        );
        assert_eq!(
            counter_value(
                &snapshot,
                "price_level_stream_rejected_sides_total",
                &[("reason", "conflicting_knots")]
            ),
            1
        );
        for (name, labels, value) in [
            (
                "price_level_stream_override_match_total",
                vec![("venue", "fermiswap"), ("outcome", "earlier_sequence")],
                1,
            ),
            (
                "price_level_stream_override_buffer_total",
                vec![("event", "resent_with_different_state")],
                1,
            ),
            (
                "price_level_stream_refused_override_entries_total",
                vec![("reason", "pairs_and_sequences_differ")],
                1,
            ),
            ("price_level_stream_unreadable_quote_sequence_total", vec![], 1),
            (
                "price_level_stream_quotes_refused_total",
                vec![("reason", "below_smallest_quote")],
                1,
            ),
            (
                "price_level_stream_sequence_slot_mismatch_total",
                vec![("socket", "quote_stream")],
                1,
            ),
            (
                "price_level_stream_override_entry_missing_field_total",
                vec![("field", "block_number")],
                1,
            ),
            ("price_level_stream_duplicate_books_total", vec![], 1),
            ("price_level_stream_unserved_venue_entries_total", vec![], 1),
            ("price_level_stream_override_block_mismatch_total", vec![], 1),
            ("price_level_stream_override_merged_sources_total", vec![], 2),
        ] {
            assert_eq!(counter_value(&snapshot, name, &labels), value, "{name}");
        }
        assert_eq!(
            histogram_values(&snapshot, "price_level_stream_override_entry_accounts", &[]),
            vec![3.0]
        );
        assert_eq!(
            histogram_values(&snapshot, "price_level_stream_override_entry_storage_slots", &[]),
            vec![7.0]
        );
    }

    /// The `reason` label values of the two book counters are the contract with dashboards, so
    /// they are spelled out here rather than derived from the enums that emit them. Every one
    /// exists at zero before anything is skipped, so a reason that never fires is a visible zero.
    #[test]
    fn book_counters_start_at_zero_under_every_reason() {
        let ((), snapshot) = record_async(async { init_book_counters() });

        for reason in ["malformed_address", "unadmitted_token", "one_asset_pair"] {
            let labels = [("reason", reason)];
            assert!(
                counter_registered(&snapshot, SKIPPED_BOOKS, &labels),
                "skipped_books {reason} is a missing series"
            );
            assert_eq!(counter_value(&snapshot, SKIPPED_BOOKS, &labels), 0);
        }
        for reason in ["unsupported_level", "non_finite_amount", "zero_amount", "conflicting_knots"]
        {
            let labels = [("reason", reason)];
            assert!(
                counter_registered(&snapshot, REJECTED_SIDES, &labels),
                "rejected_sides {reason} is a missing series"
            );
            assert_eq!(counter_value(&snapshot, REJECTED_SIDES, &labels), 0);
        }
        for reason in [
            "ladders_expired",
            "not_monotonic",
            "unknown_pair",
            "no_liquidity",
            "below_smallest_quote",
            "spot_price_unrepresentable",
        ] {
            let labels = [("reason", reason)];
            assert!(
                counter_registered(&snapshot, QUOTES_REFUSED, &labels),
                "quotes_refused {reason} is a missing series"
            );
        }
        for (name, labels) in [
            (UNREADABLE_QUOTE_SEQUENCE, vec![]),
            (DUPLICATE_BOOKS, vec![]),
            (SEQUENCE_SLOT_MISMATCH, vec![("socket", "price_levels")]),
        ] {
            assert!(counter_registered(&snapshot, name, &labels), "{name} is a missing series");
            assert_eq!(counter_value(&snapshot, name, &labels), 0, "{name}");
        }
    }

    /// The label values of the overrides series, spelled out for the same reason: they are the
    /// contract with dashboards, and every one exists at zero before anything is stored or
    /// matched, so an outcome that never happens is a visible zero.
    ///
    /// Each labelled counter's series are also counted, so the vocabulary is pinned exactly
    /// rather than as a lower bound, and the two enum-derived lists are checked against their
    /// enum's `COUNT` so that a variant added to either fails here, naming the list to extend.
    #[test]
    fn override_counters_start_at_zero_under_every_label() {
        let ((), snapshot) = record_async(async {
            init_override_counters();
            init_override_matches("fermiswap");
        });
        let outcomes = ["same_sequence", "earlier_sequence", "none_in_slot", "no_quote_sequence"];
        let events = [
            "added",
            "replaced_by_newer",
            "older_than_held",
            "resent_unchanged",
            "resent_with_different_state",
            "same_token_pair",
            "arrived_outside_slot_window",
            "left_slot_window",
            "buffer_full",
        ];
        let refusals = [
            "undecodable",
            "not_a_venue",
            "not_an_account",
            "malformed_pair",
            "malformed_storage",
            "malformed_quantity",
            "no_timestamp",
            "pairs_and_sequences_differ",
            "empty",
            "unsupported_override",
            "over_cap",
        ];

        assert_eq!(events.len(), BufferEvent::COUNT, "a BufferEvent is missing from this list");
        assert_eq!(
            refusals.len(),
            EntryRejection::COUNT,
            "an EntryRejection is missing from this list"
        );
        for outcome in outcomes {
            let labels = [("venue", "fermiswap"), ("outcome", outcome)];
            assert!(counter_registered(&snapshot, OVERRIDE_MATCHES, &labels), "{outcome}");
        }
        assert_eq!(registered_series(&snapshot, OVERRIDE_MATCHES), outcomes.len());
        for (name, key, values) in [
            (OVERRIDE_BUFFER, "event", events.as_slice()),
            (REFUSED_OVERRIDE_ENTRIES, "reason", refusals.as_slice()),
            (OVERRIDE_ENTRY_MISSING_FIELD, "field", ["block_number", "slot"].as_slice()),
        ] {
            for value in values {
                let labels = [(key, *value)];
                assert!(
                    counter_registered(&snapshot, name, &labels),
                    "{name} {value} is a missing series"
                );
                assert_eq!(counter_value(&snapshot, name, &labels), 0, "{name} {value}");
            }
            assert_eq!(
                registered_series(&snapshot, name),
                values.len(),
                "{name} registered a series its label vocabulary does not name"
            );
        }
        for (name, labels) in [
            (SEQUENCE_SLOT_MISMATCH, vec![("socket", "quote_stream")]),
            (UNSERVED_VENUE_ENTRIES, vec![]),
            (OVERRIDE_BLOCK_MISMATCH, vec![]),
            (OVERRIDE_MERGED_SOURCES, vec![]),
        ] {
            assert!(counter_registered(&snapshot, name, &labels), "{name} is a missing series");
        }
    }

    /// The socket series are registered for exactly what each socket can record, so a socket that
    /// has never failed reads as zeros while one that was never configured has no series at all.
    ///
    /// The asymmetry is the point: `parse_error` is the only rejection reason the override socket
    /// can carry, so the other five must not exist under it. The counts pin that exactly — any
    /// cross-product would register more series than there are reachable ones.
    #[rstest]
    #[case::with_the_override_socket(true)]
    #[case::price_level_socket_only(false)]
    fn socket_counters_start_at_zero_for_every_reachable_series(#[case] override_socket: bool) {
        let ((), snapshot) = record_async(async { init_socket_counters(override_socket) });
        let reject_reasons = [
            "parse_error",
            "too_old",
            "in_future",
            "out_of_order",
            "block_regression",
            "block_jump",
        ];
        let reconnect_reasons =
            ["idle_timeout", "ended", "closed", "read_error", "connect_failed", "connect_timeout"];

        for reason in reject_reasons {
            let labels = [("socket", "price_levels"), ("reason", reason)];
            assert!(counter_registered(&snapshot, FRAMES_REJECTED, &labels), "{reason}");
            assert_eq!(counter_value(&snapshot, FRAMES_REJECTED, &labels), 0, "{reason}");
        }
        for reason in reconnect_reasons {
            for socket in ["price_levels", "quote_stream"] {
                let labels = [("socket", socket), ("reason", reason)];
                let reachable = socket == "price_levels" || override_socket;
                assert_eq!(
                    counter_registered(&snapshot, RECONNECTS, &labels),
                    reachable,
                    "{socket} {reason}"
                );
            }
        }
        assert_eq!(
            counter_registered(
                &snapshot,
                FRAMES_REJECTED,
                &[("socket", "quote_stream"), ("reason", "parse_error")]
            ),
            override_socket
        );
        for reason in ["too_old", "in_future", "out_of_order", "block_regression", "block_jump"] {
            assert!(
                !counter_registered(
                    &snapshot,
                    FRAMES_REJECTED,
                    &[("socket", "quote_stream"), ("reason", reason)]
                ),
                "{reason} cannot be recorded against the override socket, yet it is registered"
            );
        }
        assert_eq!(
            registered_series(&snapshot, FRAMES_REJECTED),
            if override_socket { reject_reasons.len() + 1 } else { reject_reasons.len() }
        );
        assert_eq!(
            registered_series(&snapshot, RECONNECTS),
            if override_socket { reconnect_reasons.len() * 2 } else { reconnect_reasons.len() }
        );
    }
}
