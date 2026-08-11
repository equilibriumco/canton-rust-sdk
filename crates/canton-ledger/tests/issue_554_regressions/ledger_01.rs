//! LEDGER-01: Command recovery has two correctness gaps.
//!
//! A Canton participant node accepts commands and publishes their outcomes as
//! completions. Applications read those completions when a network failure
//! prevents a submission response from reaching them.
//!
//! 1. When the caller does not provide a command ID, the SDK generates one and
//!    sends the command to the node, which can then accept it. If a network
//!    failure occurs on the way back, the response will never reach the SDK.
//!    In this case, the SDK safely retries with the same ID, and the node will
//!    correctly reject the retry as a duplicate. The submission API returns that
//!    duplicate error to the caller, but won't reveal the SDK-generated ID unless
//!    the response was successful. The caller therefore has no ID with which to
//!    find the initially accepted command's completion.
//!
//! 2. Canton identifies a command by its user, acting parties, and command ID
//!    together. Two commands from the same user may therefore share a command ID
//!    when their acting-party sets differ. Both completions can appear in the
//!    same stream when their acting parties overlap the requested party filter.
//!    The recovery helper checks only the command ID, so it returns whichever
//!    same-ID completion arrives first, even if its full identity is different.
//!
//! Each test below isolates one gap with an in-process gRPC node and
//! asserts the required behavior through the SDK's public API.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use canton_ledger::{CantonClient, Config, RetryConfig, Submit, create, identifier, record};
use canton_proto::com::daml::ledger::api::v2 as pb;
use pb::command_completion_service_server::{
    CommandCompletionService, CommandCompletionServiceServer,
};
use pb::command_submission_service_server::{
    CommandSubmissionService, CommandSubmissionServiceServer,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

// Shared type for the node's server-streaming completion RPC.
type CompletionStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::CompletionStreamResponse, Status>> + Send>>;

// Build an ordinary command whose generated identity is the value under test.
fn test_submission() -> Submit {
    Submit::new("SubmittingParty").add_command(create(
        identifier("test-package", "Issue554", "RecoveryMarker"),
        record(vec![]),
    ))
}

// Fail the network request after accepting the command, then reject the retry.
#[derive(Clone, Default)]
struct AcceptedBeforeNetworkFailure {
    calls: Arc<AtomicUsize>,
    seen_command_ids: Arc<Mutex<Vec<String>>>,
}

#[tonic::async_trait]
impl CommandSubmissionService for AcceptedBeforeNetworkFailure {
    async fn submit(
        &self,
        request: Request<pb::SubmitRequest>,
    ) -> Result<Response<pb::SubmitResponse>, Status> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let command_id = request
            .into_inner()
            .commands
            .expect("the SDK must send Commands")
            .command_id;
        self.seen_command_ids.lock().unwrap().push(command_id);

        match call {
            // The node accepts the command before the network request fails.
            1 => Err(Status::unavailable("network failed after acceptance")),
            // The retry reuses the command ID, so the node rejects it as a duplicate.
            _ => Err(Status::already_exists("duplicate command")),
        }
    }

    // Reassignment submission is outside this regression's scope.
    async fn submit_reassignment(
        &self,
        _request: Request<pb::SubmitReassignmentRequest>,
    ) -> Result<Response<pb::SubmitReassignmentResponse>, Status> {
        Err(Status::unimplemented("not used by this test"))
    }
}

// Serve the submission mock across the same gRPC boundary used in production.
async fn start_submission_server(mock: AcceptedBeforeNetworkFailure) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .serve_with_incoming(
                CommandSubmissionServiceServer::new(mock),
                TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    format!("http://{address}")
}

// LEDGER-01: preserve a generated ID after an ambiguous outcome.
#[tokio::test]
async fn ledger_01_generated_command_id_survives_an_ambiguous_submission() {
    // Arrange a node that commits once and rejects the retry as a duplicate.
    let node = AcceptedBeforeNetworkFailure::default();
    let calls = node.calls.clone();
    let seen_command_ids = node.seen_command_ids.clone();
    let endpoint = start_submission_server(node).await;
    let client = CantonClient::connect_lazy(
        Config::new(endpoint).with_retry(
            RetryConfig::default()
                .with_max_attempts(2)
                .with_initial_backoff(Duration::from_millis(1))
                .with_max_backoff(Duration::from_millis(1)),
        ),
    )
    .unwrap();

    // Submit without a caller-supplied command ID.
    let outcome = client.submit(test_submission()).await;

    // First prove that one stable command ID protected the ledger from a double commit.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the node must see the accepted attempt and its de-duplicated retry"
    );
    let seen = seen_command_ids.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(
        seen[0], seen[1],
        "retrying with the same command ID is what prevents a double commit"
    );

    // Then prove that the caller retains that same ID for completion recovery.
    let recoverable_command_id = outcome.expect(
        "LEDGER-01: the accepted command's generated ID must remain available after the retry is rejected as a duplicate",
    );
    assert_eq!(recoverable_command_id, seen[0]);
}

// Return a fixed sequence of completions from the node.
#[derive(Clone)]
struct CompletionFeed {
    items: Vec<Result<pb::CompletionStreamResponse, Status>>,
}

#[tonic::async_trait]
impl CommandCompletionService for CompletionFeed {
    type CompletionStreamStream = CompletionStream;

    async fn completion_stream(
        &self,
        _request: Request<pb::CompletionStreamRequest>,
    ) -> Result<Response<Self::CompletionStreamStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::iter(
            self.items.clone(),
        ))))
    }

    // The public recovery helper uses CompletionStream, so GetCompletions is outside this test.
    type GetCompletionsStream = CompletionStream;

    async fn get_completions(
        &self,
        _request: Request<pb::GetCompletionsRequest>,
    ) -> Result<Response<Self::GetCompletionsStream>, Status> {
        Err(Status::unimplemented("the SDK uses CompletionStream"))
    }
}

// Build completions with the three fields that form the command's change identity.
fn completion(command_id: &str, user_id: &str, act_as: &[&str], update_id: &str) -> pb::Completion {
    pb::Completion {
        command_id: command_id.to_string(),
        user_id: user_id.to_string(),
        act_as: act_as.iter().map(ToString::to_string).collect(),
        update_id: update_id.to_string(),
        offset: 42,
        ..Default::default()
    }
}

// Wrap a completion in the node's streaming response type.
fn completion_response(completion: pb::Completion) -> pb::CompletionStreamResponse {
    pb::CompletionStreamResponse {
        completion_response: Some(
            pb::completion_stream_response::CompletionResponse::Completion(completion),
        ),
    }
}

// Serve the completion mock across a real gRPC boundary.
async fn start_completion_server(mock: CompletionFeed) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .serve_with_incoming(
                CommandCompletionServiceServer::new(mock),
                TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });
    format!("http://{address}")
}

// LEDGER-01: match the full change identity, not only command_id.
#[tokio::test]
async fn ledger_01_completion_recovery_matches_the_complete_change_identity() {
    // Both changes have the same user and command ID, but different acting-party sets.
    let command_id = "shared-command-id";
    let overlapping_change = completion(
        command_id,
        "target-user",
        &["TargetParty", "OtherParty"],
        "other-update",
    );
    let target_change = completion(command_id, "target-user", &["TargetParty"], "target-update");
    // Both include TargetParty, so both may appear in a stream filtered to TargetParty.
    let endpoint = start_completion_server(CompletionFeed {
        items: vec![
            Ok(completion_response(overlapping_change)),
            Ok(completion_response(target_change)),
        ],
    })
    .await;
    let client =
        CantonClient::connect_lazy(Config::new(endpoint).with_token("dummy-target-user-token"))
            .unwrap();

    // Ask the public recovery API for the target command.
    let recovered = client
        .await_completion(
            command_id,
            vec!["TargetParty".to_string()],
            0,
            Duration::from_secs(2),
        )
        .await
        .expect("LEDGER-01: the intended completion must remain recoverable");

    // Prove that the overlapping change's same-ID completion did not win.
    assert_eq!(
        recovered.update_id, "target-update",
        "LEDGER-01: command_id alone is not a change identity; a completion with a different act_as set must not win"
    );
    assert_eq!(recovered.user_id, "target-user");
    assert_eq!(recovered.act_as, ["TargetParty"]);
}
