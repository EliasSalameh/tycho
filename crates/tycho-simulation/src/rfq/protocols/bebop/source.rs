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
