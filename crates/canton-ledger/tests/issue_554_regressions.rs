//! Public regression suite for the findings in the Issue #554 review.
//!
//! Run the complete suite with:
//!
//! ```text
//! cargo test -p canton-ledger --test issue_554_regressions
//! ```
//!
//! These tests state the behavior required to close each finding. They are
//! intentionally expected to fail against the reviewed v0.1.4 release.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

// Regression modules, kept in review order.
// LEDGER-01 — generated command IDs and full change-ID matching.
#[path = "issue_554_regressions/ledger_01.rs"]
mod ledger_01;

// STREAM-01 — checkpoint update recovery.
#[path = "issue_554_regressions/stream_01.rs"]
mod stream_01;
