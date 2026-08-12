//! Public regression suite for the findings in the Issue #554 review.
//!
//! Run the complete suite with:
//!
//! ```text
//! cargo test -p canton-ledger --all-features --test issue_554_regressions
//! ```
//!
//! These tests state the behavior required to close each finding. They are
//! intentionally expected to fail against the reviewed v0.1.4 release.
#![allow(
    clippy::expect_used,
    clippy::large_futures,
    clippy::panic,
    clippy::unwrap_used
)]

// Regression modules, kept in review order.
// LEDGER-01 — generated command IDs and full change-ID matching.
#[path = "issue_554_regressions/ledger_01.rs"]
mod ledger_01;

// STREAM-01 — checkpoint update recovery.
#[path = "issue_554_regressions/stream_01.rs"]
mod stream_01;

// STREAM-03 — source error preservation after reconnect exhaustion.
#[path = "issue_554_regressions/stream_03.rs"]
mod stream_03;

// OBS-01 — failures reported after a stream opens must remain observable.
#[path = "issue_554_regressions/obs_01.rs"]
mod obs_01;

// OBS-02 — trace context propagation in the WebSocket handshake.
#[cfg(all(feature = "ws", feature = "otel"))]
#[path = "issue_554_regressions/obs_02.rs"]
mod obs_02;
