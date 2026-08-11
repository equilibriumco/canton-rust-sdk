//! STREAM-01: Update resumption ignores offset checkpoints.
//!
//! A Canton participant node sends ledger updates over a long-lived gRPC
//! stream.
//!
//! [Issue #554] requires resilient and resumable streams. The approved
//! proposal specifically requires update subscriptions to resume after an
//! interruption from their last known position instead of restarting from zero.
//!
//! The current resumable API handles this correctly when a stream reports a
//! retriable error after yielding a transaction: it opens another stream after
//! that transaction's offset.
//!
//! The node can advance that position with an offset checkpoint. A checkpoint
//! does not represent a transaction for the caller; it tells the client which
//! offset is safe to use when it next asks the node for updates.
//!
//! The correctness gap appears when the node sends checkpoint 10, then the gRPC
//! stream fails with `Unavailable`. The current update API removes checkpoints
//! before the resumable layer can record them. The resumable layer therefore
//! reconnects from the original offset 0 instead of the safe checkpoint offset
//! 10.
//!
//! The test below reproduces the failure with an in-process gRPC node. It records
//! the starting offset of each request and asserts `[0, 10]`.
//!
//! [Issue #554]: https://github.com/canton-foundation/canton-dev-fund/issues/554

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use canton_ledger::{CantonClient, Config, RetryConfig};
use canton_proto::com::daml::ledger::api::v2 as pb;
use pb::update_service_server::{UpdateService, UpdateServiceServer};
use tokio::net::TcpListener;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

// Shared type for the node's server-streaming update RPC.
type UpdateStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::GetUpdatesResponse, Status>> + Send>>;

// Send a checkpoint before a transient failure and record every requested begin offset.
#[derive(Clone)]
struct CheckpointThenUnavailable {
    checkpoint_offset: i64,
    update_after_checkpoint: i64,
    subscribed_party: String,
    streams_opened: Arc<AtomicUsize>,
    requested_begin_offsets: Arc<Mutex<Vec<i64>>>,
}

impl CheckpointThenUnavailable {
    fn new(checkpoint_offset: i64, update_after_checkpoint: i64, subscribed_party: String) -> Self {
        Self {
            checkpoint_offset,
            update_after_checkpoint,
            subscribed_party,
            streams_opened: Arc::new(AtomicUsize::new(0)),
            requested_begin_offsets: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[tonic::async_trait]
impl UpdateService for CheckpointThenUnavailable {
    type GetUpdatesStream = UpdateStream;

    async fn get_updates(
        &self,
        request: Request<pb::GetUpdatesRequest>,
    ) -> Result<Response<Self::GetUpdatesStream>, Status> {
        // Count which subscription the client is opening.
        let stream_number = self.streams_opened.fetch_add(1, Ordering::SeqCst) + 1;

        // Record the offset after which the client asks the node for updates.
        self.requested_begin_offsets
            .lock()
            .unwrap()
            .push(request.into_inner().begin_exclusive);

        let stream: UpdateStream = match stream_number {
            // The initial stream advertises a safe restart point, then fails.
            1 => Box::pin(tokio_stream::iter(vec![
                Ok(offset_checkpoint(self.checkpoint_offset)),
                Err(Status::unavailable(format!(
                    "transient stream failure after checkpoint {}",
                    self.checkpoint_offset
                ))),
            ])),
            // The reconnected subscription returns one update and remains open.
            _ => Box::pin(
                tokio_stream::once(Ok(transaction(
                    self.update_after_checkpoint,
                    &self.subscribed_party,
                )))
                .chain(tokio_stream::pending()),
            ),
        };

        Ok(Response::new(stream))
    }

    // Unary update reads are outside these streaming regressions' scope.
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

// Build a protocol-valid transaction visible to the subscribed party.
fn transaction(offset: i64, subscribed_party: &str) -> pb::GetUpdatesResponse {
    let template_id = pb::Identifier {
        package_id: "test-package-id".to_string(),
        module_name: "Test".to_string(),
        entity_name: "Asset".to_string(),
    };
    let timestamp = prost_types::Timestamp::default();
    let created_event = pb::CreatedEvent {
        offset,
        node_id: 0,
        contract_id: format!("contract-{offset}"),
        template_id: Some(template_id.clone()),
        create_arguments: Some(pb::Record {
            record_id: Some(template_id),
            fields: Vec::new(),
        }),
        witness_parties: vec![subscribed_party.to_string()],
        signatories: vec![subscribed_party.to_string()],
        created_at: Some(timestamp.clone()),
        package_name: "test-package".to_string(),
        acs_delta: true,
        representative_package_id: "test-package-id".to_string(),
        ..Default::default()
    };

    pb::GetUpdatesResponse {
        update: Some(pb::get_updates_response::Update::Transaction(
            pb::Transaction {
                update_id: format!("update-{offset}"),
                effective_at: Some(timestamp.clone()),
                events: vec![pb::Event {
                    event: Some(pb::event::Event::Created(created_event)),
                }],
                offset,
                synchronizer_id: "synchronizer-id".to_string(),
                record_time: Some(timestamp),
                ..Default::default()
            },
        )),
    }
}

// Build the offset checkpoint that should advance the restart position.
fn offset_checkpoint(offset: i64) -> pb::GetUpdatesResponse {
    pb::GetUpdatesResponse {
        update: Some(pb::get_updates_response::Update::OffsetCheckpoint(
            pb::OffsetCheckpoint {
                offset,
                ..Default::default()
            },
        )),
    }
}

// Serve the update mock across a real gRPC boundary.
async fn start_update_server(mock: CheckpointThenUnavailable) -> String {
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

// STREAM-01: resume after the latest offset checkpoint.
#[tokio::test]
async fn stream_01_reconnect_starts_after_the_latest_offset_checkpoint() {
    let original_begin_offset = 0;
    let checkpoint_offset = 10;
    let update_after_checkpoint = 15;
    let subscribed_party = "SubscribedParty".to_string();

    // The first subscription returns checkpoint 10 and Unavailable; the next
    // returns transaction 15. Record the begin offset requested for each one.
    let node = CheckpointThenUnavailable::new(
        checkpoint_offset,
        update_after_checkpoint,
        subscribed_party.clone(),
    );
    let requested_begin_offsets = node.requested_begin_offsets.clone();
    let endpoint = start_update_server(node).await;

    // Give stream recovery a retry budget with a one-millisecond backoff.
    let client = CantonClient::connect_lazy(
        Config::new(endpoint).with_retry(
            RetryConfig::default()
                .with_max_attempts(2)
                .with_initial_backoff(Duration::from_millis(1)),
        ),
    )
    .unwrap();

    // Start an update subscription from offset 0.
    let updates = client.updates_resumable(vec![subscribed_party], original_begin_offset);
    tokio::pin!(updates);

    // Wait for transaction 15 from the reconnected subscription.
    let update = tokio::time::timeout(Duration::from_secs(2), updates.next())
        .await
        .expect("timed out while waiting for the update after reconnect")
        .expect("the unbounded stream must stay open")
        .expect("the stream must recover after Unavailable");
    let pb::get_updates_response::Update::Transaction(transaction) = update else {
        panic!("the reconnected subscription must return transaction 15");
    };
    assert_eq!(transaction.offset, update_after_checkpoint);

    // The initial request starts at 0; the reconnect must start at checkpoint 10.
    assert_eq!(
        *requested_begin_offsets.lock().unwrap(),
        [original_begin_offset, checkpoint_offset],
        "STREAM-01: checkpoint 10 is the node's safe restart point and must not be discarded before the resumable layer sees it"
    );
}
