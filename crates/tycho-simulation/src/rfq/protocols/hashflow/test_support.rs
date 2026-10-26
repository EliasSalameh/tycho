//! Fixtures shared by the tests of the Hashflow prices and sources.

use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use tycho_common::{models::Chain, Bytes};

use crate::{
    book::{all_pairs::ComponentLayout, test_token_map, BookFeedConfig},
    rfq::protocols::hashflow::{
        client::HashflowClient, prices::HashflowBookAssembler, PROTOCOL_SYSTEM,
    },
    snapshot_feed::http::test_support::{spawn_http_server, MockHttpServer},
};

pub(super) const USDC: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
pub(super) const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
pub(super) const WBTC: &str = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599";
pub(super) const UNPRICED: &str = "0x1234567890123456789012345678901234567890";

/// An assembler whose quote client lives under `endpoint`, serving `tokens` (18 decimals each)
/// above `min_tvl_usd`, with USDC and USDT as the USD quote tokens.
pub(super) fn assembler_for(
    endpoint: &str,
    tokens: &[&Bytes],
    min_tvl_usd: f64,
) -> HashflowBookAssembler {
    let quote_tokens = HashSet::from([
        Bytes::from_str(USDC).unwrap(),
        Bytes::from_str("0xdAC17F958D2ee523a2206206994597C13D831ec7").unwrap(), // USDT
    ]);
    let entries: Vec<_> = tokens
        .iter()
        .map(|address| (*address, "T", 18))
        .collect();
    HashflowBookAssembler {
        component_layout: ComponentLayout::PerPair,
        protocol_system: PROTOCOL_SYSTEM,
        book_config: BookFeedConfig {
            chain: Chain::Ethereum,
            tokens: Arc::new(test_token_map(&entries)),
            min_tvl_usd,
        },
        usd_quote_tokens: Arc::new(quote_tokens),
        client: Arc::new(HashflowClient::new(
            Some(endpoint),
            1,
            "test_user".to_string(),
            "test_key".to_string(),
            Duration::from_secs(5),
        )),
    }
}

/// A server answering a poll with two makers: mm1 prices WETH/USDC, mm2 a WBTC/WETH pair
/// outside the `[WETH, USDC]` universe the sources here serve.
pub(super) async fn two_maker_server() -> MockHttpServer {
    let market_makers = r#"{"marketMakers":["mm1","mm2"]}"#.to_string();
    let price_levels = format!(
        r#"{{"status":"success","levels":{{
            "mm1":[{{"pair":{{"baseToken":"{WETH}","quoteToken":"{USDC}"}},"levels":[{{"q":"1","p":"3000"}}]}}],
            "mm2":[{{"pair":{{"baseToken":"{WBTC}","quoteToken":"{WETH}"}},"levels":[{{"q":"1","p":"10"}}]}}]
        }}}}"#
    );
    spawn_http_server(move |request| {
        if request
            .target
            .starts_with("/taker/v3/market-makers")
        {
            Some(("200 OK", market_makers.clone()))
        } else {
            Some(("200 OK", price_levels.clone()))
        }
    })
    .await
}
