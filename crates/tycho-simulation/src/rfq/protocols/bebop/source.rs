use http::Request;

use crate::{
    book::{BookSnapshot, ReceivedAt},
    rfq::protocols::bebop::prices::{BebopBookAssembler, BebopPricesSource},
    snapshot_feed::{
        errors::FeedError,
        ws::{WsPayload, WsSource},
    },
};

/// Streams Bebop's pricing WebSocket and decodes every pricing frame into the complete book its
/// [`BebopBookAssembler`] makes of it.
#[derive(Clone, Debug)]
pub struct BebopBookSource {
    pub(super) prices: BebopPricesSource,
    pub(super) assembler: BebopBookAssembler,
}

impl WsSource for BebopBookSource {
    type Snapshot = BookSnapshot<ReceivedAt>;

    fn request(&self) -> Result<Request<()>, FeedError> {
        self.prices.request()
    }

    fn decode(
        &mut self,
        payload: WsPayload,
    ) -> Result<Option<BookSnapshot<ReceivedAt>>, FeedError> {
        Ok(self
            .prices
            .decode(payload)?
            .map(|prices| BookSnapshot::received_now(self.assembler.collect(prices))))
    }
}
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;
    use tycho_common::models::Chain;

    use super::*;
    use crate::{
        book::{decode_swap_directions, test_token_map, SWAP_DIRECTIONS_ATTRIBUTE},
        rfq::protocols::bebop::{
            client::QuoteOrigin,
            models::{BebopPriceData, BebopSupportedChain},
            FALLBACK_PROTOCOL_SYSTEM, PROTOCOL_SYSTEM,
        },
    };

    fn test_client() -> Arc<BebopClient> {
        Arc::new(BebopClient::new(
            BebopSupportedChain::Ethereum,
            "test_key".to_string(),
            std::time::Duration::from_secs(5),
            QuoteOrigin::default(),
        ))
    }

    /// A WETH/USDC source with USDC as the only USD quote token.
    fn test_source(tvl: f64) -> BebopBookSource {
        let weth: Bytes = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            .parse()
            .unwrap();
        let usdc: Bytes = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
            .parse()
            .unwrap();
        let tokens = crate::book::test_token_map(&[(&weth, "WETH", 18), (&usdc, "USDC", 6)]);
        BebopBookSource {
            all_pairs: false,
            protocol_system: PROTOCOL_SYSTEM,
            book_config: BookFeedConfig {
                chain: tycho_common::models::Chain::Ethereum,
                tokens: Arc::new(tokens),
                min_tvl_usd: tvl,
            },
            usd_quote_tokens: Arc::new(HashSet::from([usdc])),
            client: test_client(),
        }
    }

    #[cfg(test)]
    fn weth_usdc_price_data(
        bid_price: f32,
    ) -> crate::rfq::protocols::bebop::models::BebopPriceData {
        use crate::rfq::protocols::bebop::models::BebopPriceData;

        BebopPriceData {
            base: hex::decode("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
            quote: hex::decode("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap(),
            last_update_ts: 1752617378,
            bids: vec![bid_price, 0.325717f32],
            asks: vec![bid_price + 0.5f32, 0.325717f32],
        }
    }

    #[rstest::rstest]
    #[case::approved_quote_included(0.0, 3000.0, true)]
    #[case::below_tvl_threshold_excluded(1e12, 3000.0, false)]
    fn build_books_tvl_threshold(
        #[case] tvl_threshold: f64,
        #[case] bid_price: f32,
        #[case] expect_included: bool,
    ) {
        let source = test_source(tvl_threshold);

        let update = BebopPricingUpdate { pairs: vec![weth_usdc_price_data(bid_price)] };
        let books = source.build_books(update).unwrap();

        assert_eq!(books.len(), usize::from(expect_included));
        if expect_included {
            let book = books.values().next().unwrap();
            assert_eq!(book.component.protocol_system, "rfq:bebop");
        }
    }

    #[rstest]
    #[case::per_pair(
        false,
        // "fallback:rfq:bebop" in ASCII, then WETH and USDC.
        "0x66616c6c6261636b3a7266713a6265626f70\
           c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2\
           a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    )]
    #[case::all_pairs(true, "0x66616c6c6261636b3a7266713a6265626f70")]
    fn a_fallback_source_labels_its_components_for_the_fallback_router(
        #[case] all_pairs: bool,
        #[case] expected_id: &str,
    ) {
        let source = BebopBookSource {
            protocol_system: FALLBACK_PROTOCOL_SYSTEM,
            all_pairs,
            ..test_source(0.0)
        };
        let update = BebopPricingUpdate { pairs: vec![weth_usdc_price_data(3000.0)] };

        let books = source.build_books(update).unwrap();

        let book = books.values().next().expect("one book");
        assert_eq!(book.component.protocol_system, "fallback:rfq:bebop");
        assert_eq!(book.component.id.to_string(), expected_id);
    }

    #[test]
    fn build_books_skips_unknown_tokens() {
        let source = test_source(0.0);

        let mut price_data = weth_usdc_price_data(3000.0);
        price_data.base = hex::decode("1111111111111111111111111111111111111111").unwrap();
        let update = BebopPricingUpdate { pairs: vec![price_data] };

        assert!(source
            .build_books(update)
            .unwrap()
            .is_empty());
    }

    /// Bebop sends its whole book in one frame, so a level it got wrong on one pair costs that
    /// pair and no other — including when the bad pair is one nobody asked for.
    #[test]
    fn build_books_drops_the_pair_whose_level_is_invalid() {
        let source = test_source(0.0);

        let mut unconfigured = weth_usdc_price_data(3000.0);
        unconfigured.base = hex::decode("1111111111111111111111111111111111111111").unwrap();
        unconfigured.quote = hex::decode("2222222222222222222222222222222222222222").unwrap();
        unconfigured.bids = vec![0.0f32, 1.0f32];
        let update = BebopPricingUpdate { pairs: vec![weth_usdc_price_data(3000.0), unconfigured] };

        let books = source.build_books(update).unwrap();

        let weth = Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
        let usdc = Bytes::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap();
        assert_eq!(
            books.keys().collect::<Vec<_>>(),
            vec![&pair_component_id(PROTOCOL_SYSTEM, &weth, &usdc).to_string()]
        );
    }

    /// A frame with nothing readable in it fails the poll: a published snapshot is complete, so
    /// one carrying no books withdraws every pair Bebop was serving.
    #[test]
    fn build_books_rejects_an_update_with_nothing_valid_in_it() {
        let source = test_source(0.0);

        let mut price_data = weth_usdc_price_data(3000.0);
        price_data.bids = vec![0.0f32, 1.0f32];
        let update = BebopPricingUpdate { pairs: vec![price_data] };

        assert!(matches!(source.build_books(update), Err(FeedError::Parsing(_))));
    }

    #[test]
    fn build_books_emits_each_published_orientation_as_its_own_component() {
        // Bebop streams some pairs from both sides. Each orientation is its own component,
        // identified by the pair in the order it was published.
        let source = test_source(0.0);
        let weth_usdc = weth_usdc_price_data(3000.0);
        let usdc_weth = BebopPriceData {
            base: weth_usdc.quote.clone(),
            quote: weth_usdc.base.clone(),
            last_update_ts: 1752617378,
            bids: vec![1.0 / 3000.5, 1000.0],
            asks: vec![1.0 / 3000.0, 1000.0],
        };
        let weth = Bytes::from(weth_usdc.base.clone());
        let usdc = Bytes::from(weth_usdc.quote.clone());

        let update = BebopPricingUpdate { pairs: vec![usdc_weth, weth_usdc] };
        let books = source.build_books(update).unwrap();

        assert_eq!(books.len(), 2);
        let forward = &books[&pair_component_id(PROTOCOL_SYSTEM, &weth, &usdc).to_string()];
        assert_eq!(forward.component.tokens[0].symbol, "WETH");
        assert_eq!(forward.component.tokens[1].symbol, "USDC");
        let reverse = &books[&pair_component_id(PROTOCOL_SYSTEM, &usdc, &weth).to_string()];
        assert_eq!(reverse.component.tokens[0].symbol, "USDC");
        assert_eq!(reverse.component.tokens[1].symbol, "WETH");
        for book in [forward, reverse] {
            assert!(
                !book
                    .component
                    .static_attributes
                    .contains_key(SWAP_DIRECTIONS_ATTRIBUTE),
                "both sides of the book have levels, so its two tokens are the directions"
            );
        }
    }

    #[test]
    fn build_books_normalizes_unapproved_quote() {
        // WETH/WBTC quotes in WBTC (not approved); a WBTC/USDC pair provides the one-hop
        // price for normalization.
        let weth = Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
        let wbtc = Bytes::from_str("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599").unwrap();
        let usdc = Bytes::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap();
        let tokens = test_token_map(&[(&weth, "WETH", 18), (&wbtc, "WBTC", 8)]);
        let source = BebopBookSource {
            all_pairs: false,
            protocol_system: PROTOCOL_SYSTEM,
            book_config: BookFeedConfig {
                chain: Chain::Ethereum,
                tokens: Arc::new(tokens),
                min_tvl_usd: 0.0,
            },
            usd_quote_tokens: Arc::new(HashSet::from([usdc])),
            client: test_client(),
        };

        let weth_wbtc = BebopPriceData {
            base: hex::decode("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap(),
            quote: hex::decode("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599").unwrap(),
            last_update_ts: 1752617378,
            bids: vec![0.05f32, 1.0f32],
            asks: vec![0.051f32, 1.0f32],
        };
        let wbtc_usdc = BebopPriceData {
            base: hex::decode("2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599").unwrap(),
            quote: hex::decode("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap(),
            last_update_ts: 1752617378,
            bids: vec![60000.0f32, 1.0f32],
            asks: vec![60050.0f32, 1.0f32],
        };

        // With the normalization pair present, WETH/WBTC is emitted.
        let update = BebopPricingUpdate { pairs: vec![weth_wbtc.clone(), wbtc_usdc] };
        let books = source.build_books(update).unwrap();
        assert_eq!(books.len(), 1);

        // The same pair prices the venue's dollar floor: one WBTC is worth the mid of the
        // 60 000 bid and the 60 050 ask it is quoted at.
        let state = books
            .values()
            .next()
            .unwrap()
            .state
            .as_any()
            .downcast_ref::<BebopState>()
            .expect("a Bebop state");
        assert_eq!(state.usd_per_quote_token, 60_025.0);

        // Without it, the unapproved quote cannot be priced and the pair is skipped.
        let update = BebopPricingUpdate { pairs: vec![weth_wbtc] };
        assert!(source
            .build_books(update)
            .unwrap()
            .is_empty());
    }

    /// Published as one component, Bebop's pairs are one book advertising both directions of
    /// each of them, and the venue signs one quote for the route.
    #[test]
    fn build_books_publishes_every_pair_under_one_component() {
        let source = BebopBookSource { all_pairs: true, ..test_source(0.0) };
        let update = BebopPricingUpdate { pairs: vec![weth_usdc_price_data(3000.0)] };

        let books = source.build_books(update).unwrap();

        assert_eq!(books.len(), 1);
        let book = books.values().next().unwrap();
        let weth = Bytes::from_str("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").unwrap();
        let usdc = Bytes::from_str("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap();
        // The venue's protocol system in ASCII, which is the whole id.
        assert_eq!(book.component.id.to_string(), "0x7266713a6265626f70");
        assert_eq!(
            decode_swap_directions(&book.component.static_attributes["swap_directions"]).unwrap(),
            vec![(usdc.clone(), weth.clone()), (weth.clone(), usdc.clone())]
        );

        let tokens = &source.book_config.tokens;
        // A tenth of a WETH, well inside the published ladder.
        let swap = book
            .state
            .get_amount_out(
                num_bigint::BigUint::from(10u64).pow(17),
                &tokens[&weth],
                &tokens[&usdc],
            )
            .unwrap();
        let second = swap.new_state.get_amount_out(
            num_bigint::BigUint::from(10u64).pow(17),
            &tokens[&weth],
            &tokens[&usdc],
        );
        assert!(
            matches!(
                second,
                Err(tycho_common::simulation::errors::SimulationError::RecoverableError(_))
            ),
            "Bebop signs once per route, got {second:?}"
        );
    }
}
