use std::mem;

use tracing::debug;

use crate::{pamm::protocols::metric::models::MetricMetadata, snapshot_feed::errors::FeedError};

/// Whether Metric routes to `pool`, which it does only at pause level `0`.
pub(super) fn is_unpaused(pool: &MetricMetadata) -> bool {
    if !pool.is_unpaused() {
        debug!(pool = %pool.pool_address, pause_level = ?pool.pause_level, "skipping paused pool");
    }
    pool.is_unpaused()
}

/// What a poll reports when none of the pools it asked answered: the pools' own verdict where
/// they agree on it, and an unreachable venue where they do not. One pool calling its failure
/// fatal is about that pool; every pool calling it fatal is about the venue, and nothing is
/// gained by polling on. The individual failures are logged as they arrive.
pub(super) fn every_pool_failed(failures: Vec<FeedError>) -> FeedError {
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
