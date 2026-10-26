use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tracing::debug;
use tycho_common::Bytes;

use crate::{
    book::{
        all_pairs::{BookCollector, ComponentLayout, MarketMaker, QuoteRule, VenueBook},
        component::maker_component_id,
        tvl, BookFeedConfig, Books,
    },
    rfq::protocols::hashflow::{
        client::HashflowClient,
        models::HashflowMarketMakerLevels,
        state::{HashflowAllPairsState, HashflowState},
        PROTOCOL_TYPE,
    },
};

/// Builds the complete book from one poll: every market maker's levels between the configured
/// tokens whose normalized TVL clears the threshold, one simulate-ready book per maker and
/// pair.
#[derive(Clone, Debug)]
pub struct HashflowBookAssembler {
    /// The protocol system stamped on this source's components: the venue's own, or its
    /// fallback-router one for a feed built with `with_fallback_router()`.
    pub(super) protocol_system: &'static str,
    pub(super) book_config: BookFeedConfig,
    /// USD-priced tokens the book's TVL is normalized into before the threshold applies.
    pub(super) usd_quote_tokens: Arc<HashSet<Bytes>>,
    /// Requests binding quotes; shared with every state this source emits.
    pub(super) client: Arc<HashflowClient>,
    /// Whether the venue's pairs are published as one component or one each.
    pub(super) component_layout: ComponentLayout<QuoteRule>,
}

impl HashflowBookAssembler {
    /// The TVL in USD quote-token units, converted through every pair the response priced —
    /// each market maker's level set is one such pair.
    fn normalized_tvl(
        &self,
        raw_tvl: f64,
        quote_token: &Bytes,
        levels_by_mm: &HashMap<String, Vec<HashflowMarketMakerLevels>>,
    ) -> Option<f64> {
        tvl::tvl_in_usd(
            raw_tvl,
            quote_token,
            &self.usd_quote_tokens,
            levels_by_mm
                .values()
                .flatten()
                .map(|mm_level| {
                    (&mm_level.pair.base_token, &mm_level.pair.quote_token, &mm_level.levels)
                }),
        )
    }

    /// Builds the books for the levels of one poll.
    pub(super) fn build_books(
        &self,
        levels_by_mm: HashMap<String, Vec<HashflowMarketMakerLevels>>,
    ) -> Books {
        let mut collector = BookCollector::new(
            self.protocol_system,
            PROTOCOL_TYPE,
            self.book_config.chain,
            self.component_layout,
        );
        for (mm_name, mm_levels) in levels_by_mm.iter() {
            for mm_level in mm_levels {
                let base_bytes = &mm_level.pair.base_token;
                let quote_bytes = &mm_level.pair.quote_token;
                let Some((base_token, quote_token)) = self
                    .book_config
                    .pair_tokens(base_bytes, quote_bytes)
                else {
                    continue;
                };

                // Hashflow's levels price one direction, so the reverse pair is a different book.
                let component_id =
                    maker_component_id(self.protocol_system, base_bytes, quote_bytes, mm_name);
                let tvl = mm_level.levels.notional();
                let Some(normalized_tvl) = self.normalized_tvl(tvl, quote_bytes, &levels_by_mm)
                else {
                    debug!(
                        %component_id,
                        market_maker = %mm_name,
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

                // `levels_by_mm` stays borrowed for the TVL normalization lookups above.
                let state = HashflowState::new(
                    base_token.clone(),
                    quote_token.clone(),
                    mm_level.clone(),
                    mm_name.clone(),
                    Arc::clone(&self.client),
                );
                collector.push(
                    component_id,
                    VenueBook { signer: MarketMaker(mm_name.clone()), updated_at: None, state },
                );
            }
        }
        collector.finish::<HashflowAllPairsState>()
    }
}
