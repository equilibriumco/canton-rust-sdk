//! CORE-02: Oversized retry information can panic error handling.
//!
//! A Canton JSON API error can include `retryInfo`, which tells the client how
//! long to wait before retrying. The value comes from the server and therefore
//! must be validated before it is converted into Rust's `Duration` type.
//!
//! The current parser accepts a finite but unrepresentably large value such as
//! `1e300 seconds` and passes it to `Duration::from_secs_f64`, which panics.
//! This can terminate an application while it is handling an ordinary server
//! error.
//!
//! The SDK should preserve the server error and its retriable classification,
//! but ignore retry advice that cannot be represented safely. The test below
//! constructs that public error, proves its useful fields remain available,
//! and then asserts that reading its malformed retry advice returns `None`
//! without panicking.

use std::panic::{AssertUnwindSafe, catch_unwind};

use canton_core::{Error, ErrorCategory};

// CORE-02: ignore an oversized retry delay without panicking.
#[test]
fn core_02_oversized_retry_information_does_not_panic() {
    let correlation_id = "core-02-correlation-id";
    let oversized_retry_info = "1e300 seconds";

    // Build the transient JSON error exactly as the public error model receives it.
    let body = format!(
        r#"{{
            "code": "SERVICE_UNAVAILABLE",
            "cause": "the participant is temporarily unavailable",
            "correlationId": "{correlation_id}",
            "errorCategory": 1,
            "grpcCodeValue": 14,
            "retryInfo": "{oversized_retry_info}"
        }}"#
    );
    let error = Error::Http { status: 503, body };

    // The valid parts of the server error still identify a retriable failure.
    assert_eq!(
        error.category(),
        Some(ErrorCategory::TransientServerFailure),
        "CORE-02: the valid error category must remain available"
    );
    assert!(
        error.is_retriable(),
        "CORE-02: malformed retry advice must not change the error classification"
    );
    assert_eq!(
        error.correlation_id().as_deref(),
        Some(correlation_id),
        "CORE-02: the valid correlation ID must remain available"
    );

    // Reading invalid retry advice must not panic while handling the error.
    let retry_delay = match catch_unwind(AssertUnwindSafe(|| error.retry_delay())) {
        Ok(retry_delay) => retry_delay,
        Err(_) => panic!("CORE-02: parsing server retryInfo {oversized_retry_info:?} panicked"),
    };

    // An unrepresentable delay has no safe value for the retry scheduler to use.
    assert_eq!(
        retry_delay, None,
        "CORE-02: oversized server retry information must be ignored"
    );
}
