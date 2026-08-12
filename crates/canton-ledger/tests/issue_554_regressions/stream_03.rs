//! STREAM-03: Update recovery replaces the source error after exhaustion.
//!
//! [Issue #554] requires the SDK to distinguish retriable errors from
//! non-retriable errors. A gRPC `Unavailable` status is retriable, and the
//! SDK's error type normally preserves its status code, message, structured
//! details, correlation ID, and server retry advice for the caller.
//!
//! A resumable update subscription correctly retries when it receives
//! `Unavailable`. If every reconnect receives the same error, however, the
//! current code eventually replaces that source error with
//! `UnexpectedResponse`. The replacement has no gRPC status, so all of those
//! details disappear and the failure is classified as non-retriable.
//!
//! The SDK correctly stops reconnecting when it reaches the configured limit.
//! It should not then change what caused recovery to fail. The final
//! `Unavailable` must still be returned so the caller can inspect it and decide
//! whether a higher-level recovery policy should try again later.
//!
//! The test below runs a node that returns the same `Unavailable` status from
//! every update subscription. After automatic recovery stops, it asserts that
//! the caller receives the complete source error unchanged.
//!
//! [Issue #554]: https://github.com/canton-foundation/canton-dev-fund/issues/554

use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use canton_ledger::{CantonClient, Config, Error, ErrorCategory, RetryConfig};
use canton_proto::com::daml::ledger::api::v2 as pb;
use pb::update_service_server::{UpdateService, UpdateServiceServer};
use tokio::net::TcpListener;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt as _};

// Shared type for the node's server-streaming update RPC.
type UpdateStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::GetUpdatesResponse, Status>> + Send>>;

// Fail every update subscription with the same retriable status.
#[derive(Clone)]
struct AlwaysUnavailable {
    failure: Status,
}

#[tonic::async_trait]
impl UpdateService for AlwaysUnavailable {
    type GetUpdatesStream = UpdateStream;

    async fn get_updates(
        &self,
        _request: Request<pb::GetUpdatesRequest>,
    ) -> Result<Response<Self::GetUpdatesStream>, Status> {
        // Open the subscription, then report the transient stream failure.
        let stream = tokio_stream::once(Err(self.failure.clone()));
        Ok(Response::new(Box::pin(stream)))
    }

    // Unary update reads are outside this streaming regression's scope.
    async fn get_update_by_offset(
        &self,
        _request: Request<pb::GetUpdateByOffsetRequest>,
    ) -> Result<Response<pb::GetUpdateResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn get_update_by_id(
        &self,
        _request: Request<pb::GetUpdateByIdRequest>,
    ) -> Result<Response<pb::GetUpdateResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn get_update_by_hash(
        &self,
        _request: Request<pb::GetUpdateByHashRequest>,
    ) -> Result<Response<pb::GetUpdateResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }

    async fn get_updates_page(
        &self,
        _request: Request<pb::GetUpdatesPageRequest>,
    ) -> Result<Response<pb::GetUpdatesPageResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }
}

// Serve the update mock across a real gRPC boundary.
async fn start_update_server(mock: AlwaysUnavailable) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .serve_with_incoming(
                UpdateServiceServer::new(mock),
                TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    format!("http://{address}")
}

// STREAM-03: preserve the last retriable error after reconnect exhaustion.
#[tokio::test]
async fn stream_03_exhaustion_preserves_the_source_error() {
    let original_begin_offset = 0;
    let subscribed_party = "SubscribedParty".to_string();
    let failure_message = "the update service is temporarily unavailable";
    let failure_reason = "PARTICIPANT_TEMPORARILY_UNAVAILABLE";
    let failure_domain = "canton.participant";
    let failure_category = ErrorCategory::TransientServerFailure;
    let failure_category_id = "1";
    let correlation_id = "stream-03-correlation-id";
    let server_retry_delay = Duration::from_millis(750);
    let configured_max_attempts = 1;

    // Attach Canton's structured diagnostics to the retriable Unavailable status.
    let mut metadata = HashMap::new();
    metadata.insert("category".to_string(), failure_category_id.to_string());
    let mut details = ErrorDetails::with_error_info(failure_reason, failure_domain, metadata);
    details.set_request_info(correlation_id, "");
    details.set_retry_info(Some(server_retry_delay));
    let source_failure = Status::with_error_details(Code::Unavailable, failure_message, details);

    // Make every update subscription fail with that same complete status.
    let node = AlwaysUnavailable {
        failure: source_failure,
    };
    let endpoint = start_update_server(node).await;

    // Keep the configured attempt limit and backoff small so exhaustion is immediate.
    let client = CantonClient::connect_lazy(
        Config::new(endpoint).with_retry(
            RetryConfig::default()
                .with_max_attempts(configured_max_attempts)
                .with_initial_backoff(Duration::from_millis(1)),
        ),
    )
    .unwrap();

    // Start the resumable subscription and wait until automatic recovery stops.
    let updates = client.updates_resumable(vec![subscribed_party], original_begin_offset);
    tokio::pin!(updates);
    let error = tokio::time::timeout(Duration::from_secs(2), updates.next())
        .await
        .expect("timed out while waiting for automatic recovery to stop")
        .expect("the stream must return its source error after exhaustion")
        .expect_err("every subscription attempt must fail");

    // Automatic retries stop here, but the final failure remains Unavailable.
    let Error::Status(status) = &error else {
        panic!("STREAM-03: reconnect exhaustion replaced the source error with {error:?}");
    };

    // The status code and message must survive reconnect exhaustion unchanged.
    assert_eq!(
        status.code(),
        Code::Unavailable,
        "STREAM-03: the source status code must survive reconnect exhaustion"
    );
    assert_eq!(
        status.message(),
        failure_message,
        "STREAM-03: the source message must survive reconnect exhaustion"
    );

    // Canton's structured error identity must survive reconnect exhaustion.
    assert_eq!(
        error.category(),
        Some(failure_category),
        "STREAM-03: the source error category must survive reconnect exhaustion"
    );
    let error_info = error
        .error_info()
        .expect("STREAM-03: the source ErrorInfo must survive reconnect exhaustion");
    assert_eq!(
        error_info.reason, failure_reason,
        "STREAM-03: the source error reason must survive reconnect exhaustion"
    );
    assert_eq!(
        error_info.domain, failure_domain,
        "STREAM-03: the source error domain must survive reconnect exhaustion"
    );

    // Correlation and retry metadata must remain available to the caller.
    assert_eq!(
        error.correlation_id().as_deref(),
        Some(correlation_id),
        "STREAM-03: the source correlation ID must survive reconnect exhaustion"
    );
    assert_eq!(
        error.retry_delay(),
        Some(server_retry_delay),
        "STREAM-03: the source retry advice must survive reconnect exhaustion"
    );

    // The terminal result must retain the source error's retriable classification.
    assert!(
        error.is_retriable(),
        "STREAM-03: Unavailable must remain retriable after automatic recovery stops"
    );
}
