//! Hashflow's price levels in two halves: [`HashflowPricesFeed`] polls the market makers' levels
//! and publishes each poll's answer, and [`HashflowBookAssembler`] turns those levels into the
//! simulate-ready books [`HashflowFeed`](super::feed::HashflowFeed) publishes. A service can run
//! one `HashflowPricesFeed` and send its prices to each process running a
//! `HashflowBookAssembler`.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tracing::debug;
use tycho_common::{models::Chain, Bytes};

pub use crate::rfq::protocols::hashflow::models::{HashflowMarketMakerLevels, HashflowPair};
use crate::{
    book::{
        all_pairs::{BookCollector, ComponentLayout, MarketMaker, QuoteRule, VenueBook},
        chain_id,
        component::maker_component_id,
        tvl, Book, BookFeedConfig, Books,
    },
    rfq::protocols::hashflow::{
        client::{HashflowClient, HashflowLevelsClient},
        feed::HashflowFeedBuilder,
        state::{HashflowAllPairsState, HashflowState},
        PROTOCOL_TYPE,
    },
    snapshot_feed::{
        errors::FeedError,
        http::{run_http_poll_feed, HttpFeedConfig, HttpSource},
        Publisher, SnapshotFeed,
    },
};

/// Every market maker's price levels as one Hashflow poll returned them, keyed by maker name.
///
/// The serde form follows the venue API models, so the writer and the reader must run the same
/// tycho-simulation version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HashflowPrices {
    pub levels_by_maker: HashMap<String, Vec<HashflowMarketMakerLevels>>,
}

/// Polls Hashflow's taker API and publishes every poll as [`HashflowPrices`]. Requires only the
/// chain and credentials. [`HashflowBookAssembler`] applies the token list and TVL floor. `Debug`
/// output omits the credentials.
#[derive(Clone, Debug)]
pub struct HashflowPricesFeed {
    feed_config: HttpFeedConfig,
    source: HashflowPricesSource,
}

/// Reads the market makers and their price levels through a [`HashflowLevelsClient`].
#[derive(Clone, Debug)]
pub(super) struct HashflowPricesSource {
    client: HashflowLevelsClient,
}

impl HashflowPricesSource {
    pub(super) fn new(client: HashflowLevelsClient) -> Self {
        Self { client }
    }
}

impl HashflowPricesFeed {
    /// Creates a feed that polls Hashflow's taker API for `chain` with the given credentials.
    ///
    /// Fails with [`FeedError::Fatal`] for a chain with no numeric chain id, which is all
    /// Hashflow's API is addressed by. Hashflow reports whether it prices the chain on the first
    /// poll. The feed tuning starts from [`HashflowFeedBuilder::default_feed_config`].
    pub fn new(chain: Chain, auth_user: String, auth_key: String) -> Result<Self, FeedError> {
        let client = HashflowLevelsClient::new(None, chain_id(chain)?, auth_user, auth_key);
        Ok(Self {
            feed_config: HashflowFeedBuilder::default_feed_config(),
            source: HashflowPricesSource::new(client),
        })
    }

    /// Tune the poll loop (poll cadence, request timeout, failure limit).
    pub fn feed_config(mut self, feed_config: HttpFeedConfig) -> Self {
        self.feed_config = feed_config;
        self
    }
}

impl HttpSource for HashflowPricesSource {
    type Snapshot = HashflowPrices;

    /// Fetches the market makers, then their price levels, in one poll.
    async fn fetch(&self) -> Result<HashflowPrices, FeedError> {
        let market_makers = self
            .client
            .fetch_market_makers()
            .await?;
        let levels_by_maker = self
            .client
            .fetch_price_levels(&market_makers)
            .await?;
        debug!(market_makers = levels_by_maker.len(), "fetched price levels");
        Ok(HashflowPrices { levels_by_maker })
    }
}

impl SnapshotFeed for HashflowPricesFeed {
    type Snapshot = HashflowPrices;
    type Error = FeedError;

    fn run(
        self,
        publisher: Publisher<Self::Snapshot>,
    ) -> impl Future<Output = Result<(), FeedError>> + Send + 'static {
        run_http_poll_feed(self.feed_config, publisher, self.source)
    }
}

/// Turns [`HashflowPrices`] into the books [`HashflowFeed`](super::feed::HashflowFeed) would
/// publish for the same poll: every market maker's levels between the configured tokens whose
/// normalized TVL clears the threshold, one simulate-ready book per maker and pair, whose states
/// request firm quotes through the assembler's client. Created by
/// [`HashflowFeedBuilder::build_book_assembler`] with the protocol system, book configuration,
/// USD quote tokens, quote client and component layout.
#[derive(Clone, Debug)]
pub struct HashflowBookAssembler {
    /// The protocol system stamped on the components: the venue's own, or its fallback-router
    /// one for an assembler made with `with_fallback_router()`.
    pub(super) protocol_system: &'static str,
    pub(super) book_config: BookFeedConfig,
    /// USD-priced tokens the book's TVL is normalized into before the threshold applies.
    pub(super) usd_quote_tokens: Arc<HashSet<Bytes>>,
    /// Requests binding quotes; shared with every state built here.
    pub(super) client: Arc<HashflowClient>,
    /// Whether the venue's pairs are published as one component or one each.
    pub(super) component_layout: ComponentLayout<QuoteRule>,
}

impl HashflowBookAssembler {
    /// Returns the books for `prices`, keyed by component id.
    pub fn books(&self, prices: HashflowPrices) -> HashMap<String, Book> {
        self.collect(prices).into_map()
    }

    pub(super) fn collect(&self, prices: HashflowPrices) -> Books {
        let levels_by_maker = prices.levels_by_maker;
        let mut collector = BookCollector::new(
            self.protocol_system,
            PROTOCOL_TYPE,
            self.book_config.chain,
            self.component_layout,
        );
        for (maker_name, maker_levels) in levels_by_maker.iter() {
            for maker_level in maker_levels {
                let base_bytes = &maker_level.pair.base_token;
                let quote_bytes = &maker_level.pair.quote_token;
                let Some((base_token, quote_token)) = self
                    .book_config
                    .pair_tokens(base_bytes, quote_bytes)
                else {
                    continue;
                };

                // Hashflow's levels price one direction, so the reverse pair is a different book.
                let component_id =
                    maker_component_id(self.protocol_system, base_bytes, quote_bytes, maker_name);
                let tvl = maker_level.levels.notional();
                let Some(normalized_tvl) = self.normalized_tvl(tvl, quote_bytes, &levels_by_maker)
                else {
                    debug!(
                        %component_id,
                        market_maker = %maker_name,
                        "skipping pair, no levels price its quote token in a USD quote token"
                    );
                    continue;
                };

                if !self
                    .book_config
                    .clears_min_tvl(normalized_tvl, &component_id)
                {
                    continue;
                }

                // `levels_by_maker` stays borrowed for the TVL normalization lookups above.
                let state = HashflowState::new(
                    base_token.clone(),
                    quote_token.clone(),
                    maker_level.clone(),
                    maker_name.clone(),
                    Arc::clone(&self.client),
                );
                collector.push(
                    component_id,
                    VenueBook { signer: MarketMaker(maker_name.clone()), updated_at: None, state },
                );
            }
        }
        collector.finish::<HashflowAllPairsState>()
    }

    /// The TVL in USD quote-token units, converted through every pair the response priced —
    /// each market maker's level set is one such pair.
    fn normalized_tvl(
        &self,
        raw_tvl: f64,
        quote_token: &Bytes,
        levels_by_maker: &HashMap<String, Vec<HashflowMarketMakerLevels>>,
    ) -> Option<f64> {
        tvl::tvl_in_usd(
            raw_tvl,
            quote_token,
            &self.usd_quote_tokens,
            levels_by_maker
                .values()
                .flatten()
                .map(|maker_level| {
                    (
                        &maker_level.pair.base_token,
                        &maker_level.pair.quote_token,
                        &maker_level.levels,
                    )
                }),
        )
    }
}
