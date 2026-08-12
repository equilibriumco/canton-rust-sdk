//! OBS-02: WebSocket handshakes omit the active trace context.
//!
//! [Issue #554] requires tracing across the SDK's gRPC and JSON transports.
//! The SDK correctly propagates an active OpenTelemetry trace through ordinary
//! JSON requests by adding the W3C `traceparent` header before sending them.
//!
//! A JSON update subscription starts with a WebSocket upgrade request. That
//! handshake is built on a separate path, and the current code does not add the
//! active trace context to its headers. The server therefore cannot connect the
//! long-lived subscription to the client operation that opened it.
//!
//! The test below installs an OpenTelemetry tracer and records the trace ID of
//! an active span. It opens a public `ws_updates` subscription inside that span,
//! while an in-process WebSocket server reads the `traceparent` header from the
//! upgrade request. The test then asserts that the header contains the active
//! span's exact trace ID. That proves the handshake propagated the active trace.
//!
//! [Issue #554]: https://github.com/canton-foundation/canton-dev-fund/issues/554

use canton_ledger::JsonClient;
use futures_util::StreamExt as _;
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing::Instrument as _;
use tracing::subscriber::set_default;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::layer::SubscriberExt as _;

// Accept one subscription and return the traceparent from its upgrade request.
async fn start_websocket_server() -> (String, oneshot::Receiver<Option<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (send_traceparent, receive_traceparent) = oneshot::channel();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &Request, mut response: Response| {
                // Return exactly what the server receives in the WebSocket handshake.
                let traceparent = request
                    .headers()
                    .get("traceparent")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let _ = send_traceparent.send(traceparent);

                // Echo the subprotocol required by the SDK's WebSocket client.
                response.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    HeaderValue::from_static("daml.ws.auth"),
                );
                Ok(response)
            },
        )
        .await
        .unwrap();

        // Receive the subscription message so the public call completes normally.
        let _ = websocket.next().await;
    });

    (format!("http://{address}"), receive_traceparent)
}

// OBS-02: propagate the active trace through the WebSocket upgrade request.
#[tokio::test(flavor = "current_thread")]
async fn obs_02_websocket_handshake_carries_the_active_trace() {
    let subscribed_party = "SubscribedParty".to_string();
    let begin_offset = 0;
    let end_offset = 10;

    // Start a server that returns the traceparent from the WebSocket handshake.
    let (endpoint, receive_traceparent) = start_websocket_server().await;

    // Connect tracing spans to an OpenTelemetry tracer for this current-thread test.
    let provider = opentelemetry_sdk::trace::TracerProvider::builder().build();
    let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("obs-02-test"));
    let subscriber = tracing_subscriber::registry().with(otel_layer);
    let _subscriber_guard = set_default(subscriber);

    // Record the exact trace ID that the WebSocket handshake must propagate.
    let client = JsonClient::new(endpoint);
    let active_trace = tracing::info_span!("obs_02_active_trace");
    let active_context = active_trace.context();
    let active_trace_id = active_context.span().span_context().trace_id().to_string();

    // Open the public WebSocket subscription inside that active trace.
    let updates = client
        .ws_updates(vec![subscribed_party], begin_offset, Some(end_offset))
        .instrument(active_trace)
        .await
        .expect("the WebSocket subscription must open successfully");
    drop(updates);

    // Read the traceparent that the server received in the upgrade request.
    let handshake_traceparent = receive_traceparent
        .await
        .expect("the server must inspect the WebSocket handshake")
        .expect("OBS-02: the WebSocket handshake must include a traceparent header");

    // Read the trace ID from: version-trace_id-parent_span_id-trace_flags.
    let fields = handshake_traceparent.split('-').collect::<Vec<_>>();
    let [_, handshake_trace_id, _, _] = fields.as_slice() else {
        panic!("OBS-02: the WebSocket handshake must carry a valid W3C traceparent header");
    };

    // The handshake must carry the trace ID of the span that opened it.
    assert_eq!(
        *handshake_trace_id, active_trace_id,
        "OBS-02: the WebSocket handshake must propagate the active span's trace ID"
    );
}
