use std::{collections::HashSet, sync::Arc};

use chrono::DateTime;
use itertools::Itertools;
use tracing::debug;
use tycho_common::Bytes;

use crate::{
    book::{
        all_pairs::{BookCollector, Venue, VenueBook},
        component::pair_component_id,
        wire::EntryReader,
        BookFeedConfig, Books,
    },
    rfq::protocols::bebop::{
        client::BebopClient,
        models::{BebopBook, BebopPricingUpdate},
        state::{BebopAllPairsState, BebopState},
        PROTOCOL_TYPE,
    },
    snapshot_feed::errors::FeedError,
};

/// Builds the complete book from every pricing frame: each pair between the configured tokens
/// whose normalized TVL clears the threshold, as simulate-ready books.
#[derive(Clone, Debug)]
pub struct BebopBookAssembler {
    /// The protocol system stamped on this source's components: the venue's own, or its
    /// fallback-router one for a feed built with `with_fallback_router()`.
    pub protocol_system: &'static str,
    pub book_config: BookFeedConfig,
    /// USD-priced tokens the book's TVL is normalized into before the threshold applies.
    pub usd_quote_tokens: Arc<HashSet<Bytes>>,
    /// Requests binding quotes; shared with every state this source emits. Its key also
    /// authenticates the pricing WebSocket.
    pub client: Arc<BebopClient>,
    /// Whether Bebop's pairs are published as one component or one each. Bebop names no
    /// market maker, so that component is quoted once per venue.
    pub all_pairs: bool,
}

impl BebopBookAssembler {
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

    /// Builds the complete book for one pricing update: every pair between requested tokens
    /// whose TVL (normalized into an approved quote token) clears the threshold. Each pair the
    /// update carries becomes its own component, in the orientation it was published in. Drops
    /// a pair whose levels are invalid, and fails only when no pair in the update is valid.
    pub(super) fn build_books(&self, update: BebopPricingUpdate) -> Result<Books, FeedError> {
        let mut reader = EntryReader::new();
        let pairs = reader.read(update.pairs, BebopBook::try_from);
        reader
            .finish()
            .map_err(|reason| FeedError::Parsing(format!("Bebop pricing update: {reason}")))?;
        debug!(pairs = pairs.len(), "decoded pricing update");

        // Pricing a book in USD needs the whole update, since the pair that prices its quote
        // token is another of the pairs the venue just sent — including one that is not itself
        // between configured tokens, which is why every pair stays in the list and is measured
        // before any of them is consumed. What comes out is what a book needs to exist at all.
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
        Ok(collector.finish::<BebopAllPairsState>())
    }
}
