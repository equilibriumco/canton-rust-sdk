//! Public `canton-core` regression suite for the findings in the Issue #554 review.
//!
//! Run this suite with:
//!
//! ```text
//! cargo test -p canton-core --test issue_554_regressions
//! ```
//!
//! These tests state the behavior required to close each finding. They are
//! intentionally expected to fail against the reviewed v0.1.4 release.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

// CORE-02 — safe handling of oversized server retry information.
#[path = "issue_554_regressions/core_02.rs"]
mod core_02;
