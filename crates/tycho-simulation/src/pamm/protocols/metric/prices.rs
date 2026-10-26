//! Metric's pool prices in two halves: [`MetricPricesFeed`] polls the pools' metadata and quotes
//! and publishes each poll's answer, and [`MetricBookAssembler`] turns those prices into the
//! simulate-ready books [`MetricFeed`](super::feed::MetricFeed) publishes. A service can run one
//! `MetricPricesFeed` and send its prices to each process running a `MetricBookAssembler`.

use std::{collections::HashMap, future::Future, mem, num::NonZeroUsize};

use chrono::DateTime;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use tycho_common::models::{token::Token, Chain};

pub use crate::pamm::protocols::metric::models::{MetricBidAskResponse, MetricMetadata};
use crate::{
    book::{chain_id, component::pair_book, Book, BookFeedConfig, Books},
    pamm::protocols::metric::{
        client::{MetricClient, DEFAULT_BID_ASK_CONCURRENCY},
        feed::MetricFeedBuilder,
        state::MetricState,
        FALLBACK_PROTOCOL_SYSTEM, PROTOCOL_SYSTEM, PROTOCOL_TYPE,
    },
    snapshot_feed::{
        errors::FeedError,
        http::{run_http_poll_feed, HttpFeedConfig, HttpSource},
        Publisher, SnapshotFeed,
    },
};

/// The pools one Metric poll quoted, in whatever order the venue answered.
///
/// The serde form follows the venue API models, so the writer and the reader must run the same
/// tycho-simulation version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricPrices {
    pub pools: Vec<MetricPoolPrice>,
}

/// One pool's metadata and bid/ask quote.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricPoolPrice {
    pub metadata: MetricMetadata,
    pub bid_ask: MetricBidAskResponse,
}

/// Whether Metric routes to `pool`, which it does only at pause level `0`.
fn is_unpaused(pool: &MetricMetadata) -> bool {
    if !pool.is_unpaused() {
        debug!(pool = %pool.pool_address, pause_level = ?pool.pause_level, "skipping paused pool");
    }
    pool.is_unpaused()
}

/// Returns quotes for the pools whose requests succeeded. Logs and omits failed requests. If every
/// requested pool fails, returns an error: publishing an empty book would withdraw every pair the
/// venue was serving and treat an outage as a successful poll.
async fn quote_pools(
    client: &MetricClient,
    pools: Vec<MetricMetadata>,
) -> Result<MetricPrices, FeedError> {
    let polled = client.fetch_bid_ask_each(pools).await;
    let requested = polled.len();
    let mut failures: Vec<FeedError> = Vec::new();
    let mut answered = Vec::with_capacity(requested);
    for (metadata, bid_ask) in polled {
        match bid_ask {
            Ok(bid_ask) => answered.push(MetricPoolPrice { metadata, bid_ask }),
            Err(e) => {
                warn!(
                    pool = %metadata.pool_address,
                    error = %e,
                    "skipping pool, bid/ask fetch failed"
                );
                failures.push(e);
            }
        }
    }
    if requested > 0 && failures.len() == requested {
        return Err(every_pool_failed(failures));
    }
    Ok(MetricPrices { pools: answered })
}

/// What a poll reports when none of the pools it asked answered: the pools' own verdict where
/// they agree on it, and an unreachable venue where they do not. One pool calling its failure
/// fatal is about that pool; every pool calling it fatal is about the venue, and nothing is
/// gained by polling on. The individual failures are logged as they arrive.
fn every_pool_failed(failures: Vec<FeedError>) -> FeedError {
    let summary =
        format!("none of Metric's {} quotable pools answered its bid/ask request", failures.len());
    let mut failures = failures.into_iter();
    let Some(first) = failures.next() else {
        return FeedError::Connection(summary);
    };
    if failures.all(|other| mem::discriminant(&other) == mem::discriminant(&first)) {
        first.in_context(summary)
    } else {
        FeedError::Connection(summary)
    }
}

/// Rejects empty bearer tokens before starting the feed. Metric reports `Unknown chainId` for both
/// an empty token and an unsupported chain, but returns 401 for a rejected nonempty key. Without
/// this check, an empty token would be mistaken for an unsupported chain and end the feed.
pub(super) fn check_api_key(api_key: &str) -> Result<(), FeedError> {
    if api_key.is_empty() {
        return Err(FeedError::InvalidInput("Metric trading key is empty".to_string()));
    }
    Ok(())
}

/// Rejects a base URL that does not parse. Every poll would fail the same way, so the feed does
/// not start.
pub(super) fn check_base_url(base_url: &str) -> Result<(), FeedError> {
    Url::parse(base_url)
        .map(drop)
        .map_err(|error| FeedError::InvalidInput(format!("Metric base URL {base_url}: {error}")))
}

/// Polls Metric's metadata and batch bid/ask endpoints and publishes every poll as
/// [`MetricPrices`]: each unpaused pool that answered its quote request. Requires only the chain
/// and trading key. [`MetricBookAssembler`] applies the token list, TVL floor and quote-usability
/// checks.
///
/// A poll succeeds while any pool answers; a pool that fails is left out, so a reader loses only
/// that pool's pairs. `Debug` output omits the API key.
#[derive(derive_more::Debug, Clone)]
pub struct MetricPricesFeed {
    feed_config: HttpFeedConfig,
    chain_id: u64,
    #[debug(skip)]
    api_key: String,
    base_url: Option<String>,
    bid_ask_concurrency: NonZeroUsize,
    source: MetricPricesSource,
}

/// Reads the pools' metadata and quotes through a [`MetricClient`].
#[derive(Clone, Debug)]
pub(super) struct MetricPricesSource {
    client: MetricClient,
}

impl MetricPricesSource {
    pub(super) fn new(client: MetricClient) -> Self {
        Self { client }
    }

    /// Polls the metadata of every pool, then quotes the unpaused ones for which `keep` holds.
    ///
    /// A metadata failure fails the poll, and so does losing every pool; losing some only drops
    /// those from the prices.
    pub(super) async fn poll(
        &self,
        keep: impl Fn(&MetricMetadata) -> bool,
    ) -> Result<MetricPrices, FeedError> {
        let pools = self
            .client
            .fetch_metadata()
            .await?
            .into_iter()
            .filter(|pool| keep(pool))
            .collect();
        quote_pools(&self.client, pools).await
    }
}

impl MetricPricesFeed {
    /// Creates a feed that polls Metric for `chain` with `api_key`.
    ///
    /// Fails with [`FeedError::InvalidInput`] for an empty trading key and with
    /// [`FeedError::Fatal`] for a chain with no numeric chain id, which is all Metric's API is
    /// addressed by. Metric reports whether it serves the chain on the first poll. The feed
    /// tuning starts from [`MetricFeedBuilder::default_feed_config`].
    pub fn new(chain: Chain, api_key: String) -> Result<Self, FeedError> {
        check_api_key(&api_key)?;
        let chain_id = chain_id(chain)?;
        let bid_ask_concurrency = DEFAULT_BID_ASK_CONCURRENCY;
        let client = MetricClient::new(None, chain_id, api_key.clone(), bid_ask_concurrency);
        Ok(Self {
            feed_config: MetricFeedBuilder::default_feed_config(),
            chain_id,
            api_key,
            base_url: None,
            bid_ask_concurrency,
            source: MetricPricesSource::new(client),
        })
    }

    /// Points the feed at another Metric deployment; fails with [`FeedError::InvalidInput`] for
    /// a value that is not a URL.
    pub fn base_url(mut self, base_url: String) -> Result<Self, FeedError> {
        check_base_url(&base_url)?;
        self.base_url = Some(base_url);
        Ok(self.with_rebuilt_client())
    }

    /// How many of a poll's batch bid/ask requests run at once. See
    /// [`MetricFeedBuilder::bid_ask_concurrency`].
    pub fn bid_ask_concurrency(mut self, requests_in_flight: NonZeroUsize) -> Self {
        self.bid_ask_concurrency = requests_in_flight;
        self.with_rebuilt_client()
    }

    /// Tune the poll loop (poll cadence, request timeout, failure limit).
    pub fn feed_config(mut self, feed_config: HttpFeedConfig) -> Self {
        self.feed_config = feed_config;
        self
    }

    fn with_rebuilt_client(mut self) -> Self {
        let client = MetricClient::new(
            self.base_url.as_deref(),
            self.chain_id,
            self.api_key.clone(),
            self.bid_ask_concurrency,
        );
        self.source = MetricPricesSource::new(client);
        self
    }
}

impl HttpSource for MetricPricesSource {
    type Snapshot = MetricPrices;

    async fn fetch(&self) -> Result<MetricPrices, FeedError> {
        self.poll(is_unpaused).await
    }
}

impl SnapshotFeed for MetricPricesFeed {
    type Snapshot = MetricPrices;
    type Error = FeedError;

    fn run(
        self,
        publisher: Publisher<Self::Snapshot>,
    ) -> impl Future<Output = Result<(), FeedError>> + Send + 'static {
        run_http_poll_feed(self.feed_config, publisher, self.source)
    }
}

/// Turns [`MetricPrices`] into the books [`MetricFeed`](super::feed::MetricFeed) would publish
/// for the same poll: every unpaused, quotable pool between the configured tokens whose USD TVL
/// clears the threshold, as simulate-ready books. Created by
/// [`MetricFeedBuilder::build_book_assembler`] with the feed builder's protocol system, chain,
/// tokens and TVL floor, or by [`new`](Self::new) where no trading key is at hand.
#[derive(Clone, Debug)]
pub struct MetricBookAssembler {
    /// The protocol system stamped on the components: the venue's own, or its fallback-router
    /// one for an assembler made with `with_fallback_router()`.
    pub(super) protocol_system: &'static str,
    pub(super) book_config: BookFeedConfig,
}

impl MetricBookAssembler {
    /// Creates an assembler that serves the tokens and TVL floor of `book_config`. It needs no
    /// trading key, since it makes no API call.
    pub fn new(book_config: BookFeedConfig) -> Self {
        Self { protocol_system: PROTOCOL_SYSTEM, book_config }
    }

    /// Settles this venue's swaps through Tycho's `MetricFallbackRouter`, which retries
    /// against a solver-named fallback pool when the venue reverts. Components then carry
    /// [`FALLBACK_PROTOCOL_SYSTEM`] rather than [`PROTOCOL_SYSTEM`], which is what selects
    /// the fallback encoder. Off by default.
    pub fn with_fallback_router(mut self) -> Self {
        self.protocol_system = FALLBACK_PROTOCOL_SYSTEM;
        self
    }

    /// Returns the books for `prices`, keyed by component id.
    pub fn books(&self, prices: MetricPrices) -> HashMap<String, Book> {
        self.collect(prices).into_map()
    }

    /// Returns the token pair of `pool` when this assembler serves it: both tokens are in the
    /// token list, its TVL clears the floor and it is unpaused.
    pub(super) fn pool_tokens(&self, pool: &MetricMetadata) -> Option<(&Token, &Token)> {
        let tokens = self
            .book_config
            .pair_tokens(&pool.token0, &pool.token1)?;
        // v1 metadata carries the USD TVL directly, so no cross-pool price normalization is
        // needed.
        let clears_floor = self
            .book_config
            .clears_min_tvl(pool.tvl_fiat.unwrap_or(0.0), &pool.pool_address);
        (clears_floor && is_unpaused(pool)).then_some(tokens)
    }

    pub(super) fn collect(&self, prices: MetricPrices) -> Books {
        let mut books = Books::new();
        for MetricPoolPrice { metadata, bid_ask } in prices.pools {
            let Some((token0, token1)) = self.pool_tokens(&metadata) else {
                continue;
            };
            if !bid_ask.is_quotable() {
                debug!(pool = %metadata.pool_address, "skipping pool, not quotable");
                continue;
            }

            let component_id = metadata.pool_address.clone();
            let updated_at = DateTime::from_timestamp(bid_ask.server_ts as i64, 0);
            let state = MetricState {
                base_token: token0.clone(),
                quote_token: token1.clone(),
                metadata,
                bid_ask,
            };
            books.insert(pair_book(
                self.protocol_system,
                PROTOCOL_TYPE,
                self.book_config.chain,
                component_id,
                state,
                updated_at,
            ));
        }
        books
    }
}
