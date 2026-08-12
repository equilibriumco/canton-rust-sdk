//! OBS-01: A failure after a gRPC stream opens is not counted.
//!
//! [Issue #554] requires tracing, metrics, and trace-aware logs across the
//! SDK's gRPC and JSON transports. The SDK's existing telemetry contract counts
//! a failed client operation in `canton_client_errors_total`, including whether
//! the failure is retriable.
//!
//! Opening an update subscription and consuming it happen at different times.
//! The current telemetry covers only the first part: once the node accepts the
//! subscription, the `updates` call is recorded as successful and returns a
//! stream to the caller. If that stream later reports `Unavailable`, the error
//! happens after the telemetry scope has ended and is not counted.
//!
//! The test below starts an in-process gRPC node. The node accepts an update
//! subscription, sends transaction 10, and then returns `Unavailable`. The test
//! confirms that the client receives transaction 10 followed by `Unavailable`,
//! then checks the SDK's telemetry. The correct behavior is to increase the
//! existing `updates` error counter by one. The current implementation leaves
//! that counter at zero because it stopped observing after the stream opened.
//!
//! [Issue #554]: https://github.com/canton-foundation/canton-dev-fund/issues/554

use std::pin::Pin;

use canton_core::telemetry::{METRIC_ERRORS, TRANSPORT_GRPC};
use canton_ledger::{CantonClient, Config};
use canton_proto::com::daml::ledger::api::v2 as pb;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use pb::update_service_server::{UpdateService, UpdateServiceServer};
use tokio::net::TcpListener;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

// Shared type for the node's server-streaming update RPC.
type UpdateStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::GetUpdatesResponse, Status>> + Send>>;

// Send one transaction after accepting the subscription, then fail the stream.
#[derive(Clone)]
struct UpdateThenUnavailable {
    transaction_offset: i64,
    failure_message: String,
}

#[tonic::async_trait]
impl UpdateService for UpdateThenUnavailable {
    type GetUpdatesStream = UpdateStream;

    async fn get_updates(
        &self,
        _request: Request<pb::GetUpdatesRequest>,
    ) -> Result<Response<Self::GetUpdatesStream>, Status> {
        // Accept the subscription before sending its update and later failure.
        let transaction = pb::GetUpdatesResponse {
            update: Some(pb::get_updates_response::Update::Transaction(
                pb::Transaction {
                    offset: self.transaction_offset,
                    ..Default::default()
                },
            )),
        };
        let stream = tokio_stream::iter(vec![
            Ok(transaction),
            Err(Status::unavailable(self.failure_message.clone())),
        ]);
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
async fn start_update_server(mock: UpdateThenUnavailable) -> String {
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

// Read one counter with the exact labels named by the test.
fn counter_value(snapshotter: &Snapshotter, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            let metric = key.key();
            metric.name() == name
                && metric.labels().count() == labels.len()
                && labels.iter().all(|(expected_key, expected_value)| {
                    metric.labels().any(|label| {
                        label.key() == *expected_key && label.value() == *expected_value
                    })
                })
        })
        .filter_map(|(_, _, _, value)| match value {
            DebugValue::Counter(count) => Some(count),
            _ => None,
        })
        .sum()
}

// OBS-01: count a retriable error reported by an already-open update stream.
#[tokio::test]
async fn obs_01_stream_failure_is_recorded_as_an_error() {
    let subscribed_party = "SubscribedParty".to_string();
    let begin_offset = 0;
    let transaction_offset = 10;
    let failure_message = "the open update stream became unavailable";

    // Capture every metric emitted by this test process.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install metrics recorder");

    // No retriable gRPC error has been recorded for updates yet.
    let error_labels = [
        ("method", "updates"),
        ("transport", TRANSPORT_GRPC),
        ("retriable", "true"),
    ];
    let errors_before_failure = counter_value(&snapshotter, METRIC_ERRORS, &error_labels);
    assert_eq!(errors_before_failure, 0);

    // Accept the subscription, send transaction 10, and then report Unavailable.
    let node = UpdateThenUnavailable {
        transaction_offset,
        failure_message: failure_message.to_string(),
    };
    let endpoint = start_update_server(node).await;
    let client = CantonClient::connect_lazy(Config::new(endpoint)).unwrap();

    // Open the subscription successfully before its stream reports the failure.
    let updates = client
        .updates(vec![subscribed_party], begin_offset)
        .await
        .expect("the node must accept the update subscription");
    tokio::pin!(updates);

    // The first stream item proves that the accepted subscription was usable.
    let update = updates
        .next()
        .await
        .expect("the stream must send transaction 10")
        .expect("transaction 10 must arrive before the failure");
    let pb::get_updates_response::Update::Transaction(transaction) = update else {
        panic!("the first stream item must be transaction 10");
    };
    assert_eq!(transaction.offset, transaction_offset);

    // The next item is the retriable failure that telemetry must observe.
    let failure = updates
        .next()
        .await
        .expect("the stream must report its failure")
        .expect_err("the item after transaction 10 must be Unavailable");
    assert_eq!(failure.code(), Some(Code::Unavailable));
    assert!(
        failure.to_string().contains(failure_message),
        "the caller must receive the node's failure message"
    );

    // Telemetry must record the stream failure as one retriable gRPC updates error.
    let errors_after_failure = counter_value(&snapshotter, METRIC_ERRORS, &error_labels);
    assert_eq!(
        errors_after_failure,
        errors_before_failure + 1,
        "OBS-01: receiving Unavailable from the stream must increment canton_client_errors_total"
    );
}
