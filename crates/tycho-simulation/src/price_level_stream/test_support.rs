//! Helpers shared by the price level stream tests.

use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{future::BoxFuture, SinkExt};
use num_bigint::BigUint;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use tycho_common::{
    models::{token::Token, Chain},
    Bytes,
};

use super::{
    config::PriceLevelStreamConfig,
    titan::{TitanBook, TitanOverrideMessage, TitanPriceLevelMessage},
};

/// The FermiSwapper router, one of the default venues.
pub(super) const PAMM: &str = "0x5979458912f80b96d30d4220af8e2e4925a33320";
/// The KipseliPropAMMWrapper router: Kipseli's canonical public identity, and one of the default
/// venues.
pub(super) const KIPSELI: &str = "0x71e790dd841c8a9061487cb3e78c288e75ce0b3d";
/// The address Titan streams Kipseli's books under, aliased onto [`KIPSELI`].
pub(super) const KIPSELI_STREAMED: &str = "0xc9a956b5196dfe110debe8781857bc8b97d32091";
pub(super) const WBTC: &str = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599";
pub(super) const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
pub(super) const WETH: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
pub(super) const USDT: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
pub(super) const CBBTC: &str = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";
/// Quoted by TaurusFi in the capture, and admitted by no token map these tests build, so a book
/// naming it is skipped as `unadmitted_token`.
pub(super) const NVDAX: &str = "0xc845b2894dbddd03858fd2d643b4ef725fe0849d";

pub(super) fn address(hex: &str) -> Bytes {
    Bytes::from_str(hex).expect("hex address")
}

/// The address Titan streams beside `symbol`, for the assets these tests build books from.
///
/// Test ergonomics only: a test names a pair the way a reader reads it, while the book it builds
/// carries the address the parser resolves. Nothing in production resolves a symbol.
pub(super) fn symbol_address(symbol: &str) -> &'static str {
    match symbol {
        "WBTC" => WBTC,
        "USDC" => USDC,
        "WETH" => WETH,
        "USDT" => USDT,
        "cbBTC" => CBBTC,
        "NVDAx" => NVDAX,
        other => panic!("no test address for {other}"),
    }
}

pub(super) fn token(address: &str, symbol: &str, decimals: u32) -> Token {
    Token::new(
        &Bytes::from_str(address).unwrap(),
        symbol,
        decimals,
        0,
        &[Some(10_000)],
        Chain::Ethereum,
        100,
    )
}

pub(super) fn token_map(tokens: impl IntoIterator<Item = Token>) -> HashMap<Bytes, Token> {
    tokens
        .into_iter()
        .map(|token| (token.address.clone(), token))
        .collect()
}

/// The bound assets of the streamed symbols the tests price, with the decimals of their Ethereum
/// deployments, as a consumer's token metadata.
pub(super) fn tokens() -> HashMap<Bytes, Token> {
    token_map([token(WBTC, "WBTC", 8), token(USDC, "USDC", 6), token(WETH, "WETH", 18)])
}

/// Every bound asset the 2026-10-16 captures quote, as a consumer's token metadata: [`tokens`]
/// plus USDT and cbBTC, so no captured book goes unserved for want of a token the consumer admits.
/// `NVDAx`, the one other symbol the capture quotes, is bound by nothing and stays unserved.
pub(super) fn captured_tokens() -> HashMap<Bytes, Token> {
    token_map([
        token(WBTC, "WBTC", 8),
        token(USDC, "USDC", 6),
        token(WETH, "WETH", 18),
        token(USDT, "USDT", 6),
        token(CBBTC, "cbBTC", 8),
    ])
}

pub(super) type FakeConnection = WebSocketStream<TcpStream>;

/// A scripted stand-in for Titan's WebSocket endpoint on `127.0.0.1`. Every accepted
/// connection runs `handler(connection_index, socket)`.
pub(super) struct FakeTitan {
    addr: SocketAddr,
    pub(super) connections: Arc<AtomicUsize>,
    accept_task: Option<JoinHandle<()>>,
}

impl FakeTitan {
    pub(super) async fn spawn<H, Fut>(handler: H) -> Self
    where
        H: Fn(usize, FakeConnection) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener
            .local_addr()
            .expect("local addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        let handler: Arc<dyn Fn(usize, FakeConnection) -> BoxFuture<'static, ()> + Send + Sync> =
            Arc::new(move |index, socket| Box::pin(handler(index, socket)));
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let handler = handler.clone();
                tokio::spawn(async move {
                    let Ok(socket) = tokio_tungstenite::accept_async(socket).await else {
                        return;
                    };
                    handler(index, socket).await;
                });
            }
        });
        Self { addr, connections, accept_task: Some(accept_task) }
    }

    /// A fake that accepts TCP connections and never completes the WebSocket handshake, so a
    /// client's connect hangs until its own [`connect_timeout`] fires.
    ///
    /// [`connect_timeout`]: super::stream::PriceLevelStreamBuilder::connect_timeout
    pub(super) async fn spawn_hung() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener
            .local_addr()
            .expect("local addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        let accept_task = tokio::spawn(async move {
            let mut accepted = Vec::new();
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                // Held rather than answered: dropping the socket would let the client's connect
                // fail at once instead of hanging, which is the opposite of the case.
                accepted.push(socket);
            }
        });
        Self { addr, connections, accept_task: Some(accept_task) }
    }

    pub(super) fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Stops accepting: the listener is dropped, so every later connect is refused at TCP
    /// level. Connections already handed to a handler keep running.
    pub(super) fn shutdown(&mut self) {
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
    }
}

impl Drop for FakeTitan {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Nanoseconds since the Unix epoch, for building frames Titan would stamp right now.
pub(super) fn wall_nanos_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after epoch")
            .as_nanos(),
    )
    .expect("unix nanoseconds fit in u64")
}

/// The FermiSwap venue at [`PAMM`], registered under the protocol name `fermiswap`.
pub(super) fn fermiswap() -> PriceLevelStreamConfig {
    PriceLevelStreamConfig::new("fermiswap", address(PAMM), BigUint::from(120_000u64))
}

/// A [`FakeTitan`] handler that sends `first` once, then `repeat` every `interval` until the
/// socket closes.
pub(super) fn frame_then_repeat(
    first: Message,
    repeat: Message,
    interval: Duration,
) -> impl Fn(usize, FakeConnection) -> BoxFuture<'static, ()> + Send + Sync + 'static {
    move |_, mut socket| {
        let first = first.clone();
        let repeat = repeat.clone();
        Box::pin(async move {
            if socket.send(first).await.is_err() {
                return;
            }
            while socket
                .send(repeat.clone())
                .await
                .is_ok()
            {
                tokio::time::sleep(interval).await;
            }
        })
    }
}

/// A valid frame carrying the FermiSwap WBTC/USDC book two-sided: 1 WBTC sells for 99,900 USDC
/// and 100,100 USDC buys 1 WBTC.
pub(super) fn frame_text(block: u64, ts: u64) -> String {
    format!(
        r#"{{"slot": 1, "block": {block}, "ts": {ts}, "books": [
            {{"maker": "Fermi", "quoteAddress": "{PAMM}", "token1": "WBTC", "token2": "USDC",
              "token1Address": "{WBTC}", "token2Address": "{USDC}",
              "bids": [{{"price": 99900.0, "cumVolume": 1.0}}],
              "asks": [{{"price": 100100.0, "cumVolume": 1.0}}]}}
        ]}}"#
    )
}

/// A frame captured verbatim from the live stream (2026-10-16); the file name carries the frame's
/// `ts` field.
const CAPTURED_MESSAGE: &str =
    include_str!("test_responses/pamm_price_levels_v2_1792115918290990905.json");

/// The books of the captured live frame.
pub(super) fn captured_books() -> Vec<TitanBook> {
    serde_json::from_str::<TitanPriceLevelMessage>(CAPTURED_MESSAGE)
        .expect("the captured frame parses")
        .books
}

/// Every captured book `maker` quoted, in capture order.
pub(super) fn captured_books_of(maker: &str) -> Vec<TitanBook> {
    let books: Vec<TitanBook> = captured_books()
        .into_iter()
        .filter(|book| book.maker.as_deref() == Some(maker))
        .collect();
    assert!(!books.is_empty(), "no captured book of {maker}");
    books
}

/// The captured book `maker` quoted for `token1`/`token2`.
pub(super) fn captured_book(maker: &str, token1: &str, token2: &str) -> TitanBook {
    captured_books_of(maker)
        .into_iter()
        .find(|book| book.token1 == token1 && book.token2 == token2)
        .expect("captured book")
}

/// A frame stamped with `block`/`ts` carrying every captured book of `maker`, each verbatim from
/// the capture — so a stream reading it parses what Titan actually sent, including the venue
/// address Titan streams the books under.
pub(super) fn captured_frame_text(maker: &str, block: u64, ts: u64) -> String {
    let frame: serde_json::Value =
        serde_json::from_str(CAPTURED_MESSAGE).expect("the captured frame parses");
    let books: Vec<serde_json::Value> = frame["books"]
        .as_array()
        .expect("the capture carries books")
        .iter()
        .filter(|book| book["maker"] == maker)
        .cloned()
        .collect();
    assert!(!books.is_empty(), "no captured book of {maker}");
    serde_json::json!({ "slot": 1, "block": block, "ts": ts, "books": books }).to_string()
}

/// The price level message of the 2026-10-16 capture whose books [`CAPTURED_OVERRIDES`] priced.
/// Of its 30 books on admitted tokens, 27 find their venue's entry in that quote stream message:
/// 16 at their own quote sequence and 11 at an earlier one of the same slot. [`CAPTURED_MESSAGE`],
/// captured earlier, belongs to the previous slot and matches none of them.
const MATCHING_MESSAGE: &str =
    include_str!("test_responses/pamm_price_levels_v2_1792115920294943036.json");

/// The quote stream message captured verbatim from the live `pamm_quote_stream_v2` socket
/// (2026-10-16), one entry per venue Titan priced in that round.
const CAPTURED_OVERRIDES: &str =
    include_str!("test_responses/pamm_quote_stream_v2_1792115920248724389.json");

/// The books of [`MATCHING_MESSAGE`], verbatim.
pub(super) fn matching_books() -> Vec<TitanBook> {
    serde_json::from_str::<TitanPriceLevelMessage>(MATCHING_MESSAGE)
        .expect("the captured message parses")
        .books
}

/// [`MATCHING_MESSAGE`] with its `block` and `ts` replaced, as raw JSON: every book verbatim, so a
/// stream reading it matches exactly what Titan sent, while its `ts` passes the freshness checks
/// of the reading host's clock.
pub(super) fn matching_frame_text(block: u64, ts: u64) -> String {
    let frame = restamped(MATCHING_MESSAGE, "block", |_| block);
    restamped(&frame, "ts", |_| ts)
}

/// [`CAPTURED_OVERRIDES`], parsed.
pub(super) fn captured_override_frame() -> TitanOverrideMessage {
    serde_json::from_str(CAPTURED_OVERRIDES).expect("the captured quote stream message parses")
}

/// [`CAPTURED_OVERRIDES`] as raw JSON, verbatim.
pub(super) fn captured_override_text() -> &'static str {
    CAPTURED_OVERRIDES
}

/// `frame` with the value of every `"key": <digits>` field replaced by `restamp` of it, as text.
///
/// Text surgery rather than a `serde_json::Value` round trip, which is the obvious way and the
/// wrong one: a `Value` holds no integer wider than a `u64`, so the quote sequences of the
/// capture — every one of them above `u64::MAX` — would come back out as an `f64`, and nothing
/// would match any more.
fn restamped(frame: &str, key: &str, restamp: impl Fn(u64) -> u64) -> String {
    let field = format!("\"{key}\":");
    let mut restamped = String::with_capacity(frame.len());
    let mut rest = frame;
    let mut fields = 0;
    while let Some(start) = rest.find(&field) {
        let (head, tail) = rest.split_at(start + field.len());
        restamped.push_str(head);
        // The capture is compact, as Titan sends it, but a formatted one must restamp too.
        let spaces = tail.len() - tail.trim_start_matches(' ').len();
        let (padding, tail) = tail.split_at(spaces);
        restamped.push_str(padding);
        let digits = tail.len() -
            tail.trim_start_matches(|character: char| character.is_ascii_digit())
                .len();
        let (value, tail) = tail.split_at(digits);
        let value: u64 = value
            .parse()
            .unwrap_or_else(|_| panic!("\"{key}\" is not an integer field"));
        restamped.push_str(&restamp(value).to_string());
        rest = tail;
        fields += 1;
    }
    assert!(fields > 0, "the captured frame carries no \"{key}\" field");
    restamped.push_str(rest);
    restamped
}
