//! Bebop's pricing stream in two halves: [`BebopPricesFeed`] holds the WebSocket and publishes
//! each frame's parsed prices, and [`BebopBookAssembler`] turns prices into the simulate-ready
//! books [`BebopFeed`](super::feed::BebopFeed) publishes. Bebop allows one pricing connection
//! per API key, so a service can run one `BebopPricesFeed` and send its prices to each process
//! running a `BebopBookAssembler`.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use chrono::DateTime;
use http::Request;
use itertools::Itertools;
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use tycho_common::{models::Chain, Bytes};

pub use crate::rfq::protocols::bebop::models::BebopBook;
use crate::{
    book::{
        all_pairs::{BookCollector, Venue, VenueBook},
        component::pair_component_id,
        wire::EntryReader,
        Book, BookFeedConfig, Books,
    },
    rfq::protocols::bebop::{
        client::{BebopClient, BebopPricingClient},
        feed::BebopFeedBuilder,
        models::{BebopPricingUpdate, BebopSupportedChain},
        state::{BebopAllPairsState, BebopState},
        PROTOCOL_TYPE,
    },
    snapshot_feed::{
        errors::FeedError,
        ws::{run_ws_feed, WsFeedConfig, WsPayload, WsSource},
        Publisher, SnapshotFeed,
    },
};

/// Pairs from one Bebop pricing frame, excluding pairs with invalid levels and preserving each
/// pair's published orientation.
///
/// The serde form follows the venue API models, so the writer and the reader must run the same
/// tycho-simulation version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BebopPrices {
    pub pairs: Vec<BebopBook>,
}

impl BebopPrices {
    /// Reads every pair of `update`, dropping the ones whose levels are invalid. Fails only when
    /// none is valid.
    fn from_update(update: BebopPricingUpdate) -> Result<Self, FeedError> {
        let mut reader = EntryReader::new();
        let pairs = reader.read(update.pairs, BebopBook::try_from);
        reader
            .finish()
            .map_err(|reason| FeedError::Parsing(format!("Bebop pricing update: {reason}")))?;
        debug!(pairs = pairs.len(), "decoded pricing update");
        Ok(BebopPrices { pairs })
    }
}

/// Decodes one pricing frame. A text frame carries no prices and yields `None`.
fn decode_frame(payload: WsPayload) -> Result<Option<BebopPrices>, FeedError> {
    let data = match payload {
        WsPayload::Binary(data) => data,
        // The protobuf stream carries pricing only in binary frames; a text frame is most
        // likely a server-side notice worth seeing in the logs.
        WsPayload::Text(text) => {
            warn!(%text, "unexpected text frame on the pricing stream");
            return Ok(None);
        }
    };
    let update = BebopPricingUpdate::decode(&data[..])
        .map_err(|e| FeedError::Parsing(format!("Failed to parse protobuf message: {e}")))?;
    BebopPrices::from_update(update).map(Some)
}

/// Streams Bebop's pricing WebSocket and publishes every frame as [`BebopPrices`]. Requires only
/// the chain and API key. [`BebopBookAssembler`] applies the token list and TVL floor. `Debug`
/// output omits the API key.
#[derive(Clone, Debug)]
pub struct BebopPricesFeed {
    feed_config: WsFeedConfig,
    source: BebopPricesSource,
}

/// Reads the pricing WebSocket through a [`BebopPricingClient`].
#[derive(Clone, Debug)]
pub(super) struct BebopPricesSource {
    client: BebopPricingClient,
}

impl BebopPricesSource {
    pub(super) fn new(client: BebopPricingClient) -> Self {
        Self { client }
    }
}

impl BebopPricesFeed {
    /// Creates a feed that streams Bebop's pricing WebSocket for `chain` with `ws_key`.
    ///
    /// Fails with [`FeedError::Fatal`] for a chain Bebop does not serve. The feed tuning starts
    /// from [`BebopFeedBuilder::default_feed_config`].
    pub fn new(chain: Chain, ws_key: String) -> Result<Self, FeedError> {
        let chain = BebopSupportedChain::try_from(chain).map_err(FeedError::Fatal)?;
        Ok(Self {
            feed_config: BebopFeedBuilder::default_feed_config(),
            source: BebopPricesSource::new(BebopPricingClient::new(chain, ws_key)),
        })
    }

    /// Tune the connection loop (timeouts, backoff, failure limit).
    pub fn feed_config(mut self, feed_config: WsFeedConfig) -> Self {
        self.feed_config = feed_config;
        self
    }
}

impl WsSource for BebopPricesSource {
    type Snapshot = BebopPrices;

    fn request(&self) -> Result<Request<()>, FeedError> {
        self.client
            .pricing_handshake()
            // No retry builds a different request, so the feed ends rather than reconnecting.
            .map_err(|e| FeedError::Fatal(format!("Failed to build the pricing handshake: {e}")))
    }

    fn decode(&mut self, payload: WsPayload) -> Result<Option<BebopPrices>, FeedError> {
        decode_frame(payload)
    }
}

impl SnapshotFeed for BebopPricesFeed {
    type Snapshot = BebopPrices;
    type Error = FeedError;

    fn run(
        self,
        publisher: Publisher<Self::Snapshot>,
    ) -> impl Future<Output = Result<(), FeedError>> + Send + 'static {
        run_ws_feed(self.feed_config, publisher, self.source)
    }
}

/// Turns [`BebopPrices`] into the books [`BebopFeed`](super::feed::BebopFeed) would publish for
/// the same frame: each pair between the configured tokens whose normalized TVL clears the
/// threshold, as simulate-ready books whose states request firm quotes through the assembler's
/// client. Created by [`BebopFeedBuilder::build_book_assembler`] with the protocol system, book
/// configuration, USD quote tokens, quote client and all-pairs setting.
#[derive(Clone, Debug)]
pub struct BebopBookAssembler {
    /// The protocol system stamped on the components: the venue's own, or its fallback-router
    /// one for an assembler made with `with_fallback_router()`.
    pub(super) protocol_system: &'static str,
    pub(super) book_config: BookFeedConfig,
    /// USD-priced tokens the book's TVL is normalized into before the threshold applies.
    pub(super) usd_quote_tokens: Arc<HashSet<Bytes>>,
    /// Requests binding quotes; shared with every state built here.
    pub(super) client: Arc<BebopClient>,
    /// Whether Bebop's pairs are published as one component or one each. Bebop names no
    /// market maker, so that component is quoted once per venue.
    pub(super) all_pairs: bool,
}

impl BebopBookAssembler {
    /// Returns the books for `prices`, keyed by component id. USD conversion uses pairs from the
    /// whole update, including pairs outside the configured tokens, to price each book's quote
    /// token.
    pub fn books(&self, prices: BebopPrices) -> HashMap<String, Book> {
        self.collect(prices).into_map()
    }

    pub(super) fn collect(&self, prices: BebopPrices) -> Books {
        let pairs = prices.pairs;
        // Measure all pairs before consuming any, because USD conversion can use other pairs in
        // the list. Each result contains the tokens, TVL and quote-token price needed to build a
        // book.
        let measured = pairs
            .iter()
            .map(|book| {
                let (base_token, quote_token) = self
                    .book_config
                    .pair_tokens(&book.base, &book.quote)?;
                let (tvl, usd_per_quote_token) = self.measure_in_usd(book, &pairs)?;
                Some((base_token, quote_token, tvl, usd_per_quote_token))
            })
            .collect_vec();

        let mut collector = BookCollector::new(
            self.protocol_system,
            PROTOCOL_TYPE,
            self.book_config.chain,
            self.all_pairs.then_some(()),
        );
        for (book, (base_token, quote_token, tvl, usd_per_quote_token)) in pairs
            .into_iter()
            .zip(measured)
            .filter_map(|(book, measured)| measured.map(|measured| (book, measured)))
        {
            let component_id = pair_component_id(self.protocol_system, &book.base, &book.quote);
            if !self
                .book_config
                .clears_min_tvl(tvl, &component_id)
            {
                continue;
            }
            // `last_update_ts` is milliseconds on the wire (Bebop's spec says seconds; 13-digit
            // values observed live).
            let updated_at = DateTime::from_timestamp_millis(book.last_update_ts as i64);
            let state = BebopState::new(
                base_token.clone(),
                quote_token.clone(),
                book,
                usd_per_quote_token,
                Arc::clone(&self.client),
            );
            collector.push(component_id, VenueBook { signer: Venue, updated_at, state });
        }
        collector.finish::<BebopAllPairsState>()
    }

    /// What the book is worth in USD and what one of its quote tokens is worth there: its TVL,
    /// and the price the venue's dollar floor is read against. Both rest on the same pair — the
    /// one pricing the book's quote token in a USD quote token, or none at all when the quote
    /// token is one itself — so a book that has one has both. `None` when nothing in the update
    /// prices that token, or when the pair that does is one-sided and names no price.
    ///
    /// The two are priced at different sizes on purpose: a book's TVL is worth what converting
    /// all of it would fetch, where a floor of one dollar is a trade at the top of the ladder.
    fn measure_in_usd(&self, book: &BebopBook, pairs: &[BebopBook]) -> Option<(f64, f64)> {
        if self
            .usd_quote_tokens
            .contains(&book.quote)
        {
            return Some((book.calculate_tvl(None), 1.0));
        }
        let Some(quote_book) = self.conversion_book(book, pairs) else {
            debug!(
                base = %book.base,
                quote = %book.quote,
                "skipping pair, no book prices its quote token in a USD quote token"
            );
            return None;
        };
        let usd_per_quote_token = quote_book.get_mid_price(1.0, &book.quote)?;
        Some((book.calculate_tvl(Some(quote_book)), usd_per_quote_token))
    }

    /// A pair in the same update that prices `book`'s quote token in a USD quote token, in
    /// either orientation.
    fn conversion_book<'a>(
        &self,
        book: &BebopBook,
        pairs: &'a [BebopBook],
    ) -> Option<&'a BebopBook> {
        self.usd_quote_tokens
            .iter()
            .find_map(|usd_quote_token| {
                pairs.iter().find(|pair| {
                    (pair.base == book.quote && pair.quote == *usd_quote_token) ||
                        (pair.quote == book.quote && pair.base == *usd_quote_token)
                })
            })
    }
}
