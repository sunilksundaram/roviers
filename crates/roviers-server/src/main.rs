use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use futures::stream::Stream;
use roviers_core::RoviersEngine;
use roviers_proto::{Receipt, ServiceOutcome, ServiceRequest, SignedCitizenToken};
use serde::Deserialize;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Roviers: High-throughput Civic Protocol Server")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    bind: String,

    #[arg(short, long, default_value = "./roviers_data")]
    data_dir: String,
}

#[derive(Clone)]
struct AppState {
    engine: Arc<RoviersEngine>,
}

#[derive(Deserialize)]
struct IssueCitizenRequest {
    citizen_id: String,
    full_name: String,
    validity_days: i64,
}

#[derive(Deserialize)]
struct PickupRequest {
    citizen_token: SignedCitizenToken,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    tracing::info!("Initializing Roviers engine at {}...", args.data_dir);
    let engine = Arc::new(RoviersEngine::new(&args.data_dir)?);
    let state = AppState {
        engine: Arc::clone(&engine),
    };

    // Spawn the background worker pool to demonstrate async task execution
    spawn_worker_pool(Arc::clone(&engine));

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/v1/auth/citizen", post(issue_citizen_token))
        .route("/api/v1/services/{service_name}", post(submit_service))
        .route("/api/v1/receipts/{receipt_id}", get(get_receipt))
        .route("/api/v1/receipts/{receipt_id}/pickup", post(pickup_outcome))
        .route("/api/v1/citizens/{citizen_id}/stream", get(stream_citizen_intimations))
        .route("/api/v1/citizens/{citizen_id}/receipts", get(list_citizen_receipts))
        .with_state(state);

    let addr: SocketAddr = args.bind.parse()?;
    tracing::info!("🏛️ Roviers Service Counter listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "Roviers Civic Counter: OK"
}

/// Endpoint to issue official citizen ID cards (signed by the government key)
async fn issue_citizen_token(
    State(state): State<AppState>,
    Json(req): Json<IssueCitizenRequest>,
) -> Result<Json<SignedCitizenToken>, (StatusCode, String)> {
    let validity_secs = req.validity_days * 86400;
    let token = state
        .engine
        .issue_citizen_id(&req.citizen_id, &req.full_name, validity_secs)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(token))
}

/// Service Counter: Citizen presents ID and request, receives Receipt (HTTP 202 Accepted)
async fn submit_service(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
    Json(mut req): Json<ServiceRequest>,
) -> Result<(StatusCode, Json<Receipt>), (StatusCode, String)> {
    req.service_name = service_name;

    match state.engine.submit_service_request(req).await {
        Ok(receipt) => Ok((StatusCode::ACCEPTED, Json(receipt))),
        Err(roviers_core::CoreError::Auth(e)) => Err((StatusCode::UNAUTHORIZED, e.to_string())),
        Err(roviers_core::CoreError::DuplicateRequest(id)) => {
            Err((StatusCode::CONFLICT, format!("Duplicate request: {}", id)))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// Check receipt status
async fn get_receipt(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
) -> Result<Json<Receipt>, StatusCode> {
    state
        .engine
        .get_receipt(receipt_id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Pickup outcome by presenting the receipt ID and Citizen ID card
async fn pickup_outcome(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
    Json(req): Json<PickupRequest>,
) -> Result<Json<ServiceOutcome>, (StatusCode, String)> {
    state
        .engine
        .pickup_outcome(receipt_id, &req.citizen_token)
        .map(Json)
        .map_err(|e| match e {
            roviers_core::CoreError::Auth(_) | roviers_core::CoreError::CitizenMismatch => {
                (StatusCode::FORBIDDEN, e.to_string())
            }
            roviers_core::CoreError::OutcomeNotReady(_) => {
                (StatusCode::ACCEPTED, "Outcome still in process".to_string())
            }
            roviers_core::CoreError::ReceiptNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })
}

/// List all active receipts for a citizen
async fn list_citizen_receipts(
    State(state): State<AppState>,
    Path(citizen_id): Path<String>,
) -> Json<Vec<Receipt>> {
    let receipts = state.engine.list_citizen_receipts(&citizen_id);
    Json(receipts)
}

/// Real-time Server-Sent Events (SSE) intimation channel:
/// Citizen holds an open lightweight SSE stream to hear back when an outcome is ready.
/// Zero polling, zero heavy sockets on worker servers.
async fn stream_citizen_intimations(
    State(state): State<AppState>,
    Path(citizen_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.engine.subscribe_intimations();
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let expected_citizen = citizen_id.clone();
        match item {
            Ok(notice) => {
                if notice.citizen_id == expected_citizen {
                    let json = serde_json::to_string(&notice).unwrap_or_default();
                    Some(Ok(Event::default().event("intimation").data(json)))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Background worker simulation:
/// Pulls work from the topic trie and completes tasks asynchronously.
fn spawn_worker_pool(engine: Arc<RoviersEngine>) {
    let mut rx = engine.topic_trie.subscribe("service.*");

    tokio::spawn(async move {
        tracing::info!("👷 Async worker pool started, waiting for service requests...");

        while let Ok(record_offset) = rx.recv().await {
            let engine = Arc::clone(&engine);

            tokio::spawn(async move {
                // Read the transaction from the ledger
                let record = {
                    let mut ledger = engine.ledger.lock().await;
                    ledger.read_record(record_offset).ok().flatten()
                };

                if let Some(record) = record {
                    if let Ok(req) = serde_json::from_slice::<ServiceRequest>(&record.payload) {
                        tracing::info!(
                            "🛠️ Worker executing service '{}' for receipt #{}...",
                            req.service_name,
                            req.request_id
                        );

                        // Simulate processing time (e.g., identity verification, document generation, HITL)
                        tokio::time::sleep(Duration::from_millis(1500)).await;

                        let outcome = ServiceOutcome {
                            receipt_id: req.request_id,
                            citizen_id: req.citizen_token.credentials.citizen_id.clone(),
                            service_name: req.service_name.clone(),
                            completed_at: chrono::Utc::now().timestamp(),
                            result: serde_json::json!({
                                "status": "APPROVED",
                                "document_number": format!("DOC-{:X}", rand::random::<u32>()),
                                "seal": "GOV_OFFICIAL_VERIFIED",
                                "details": req.payload
                            }),
                        };

                        if let Err(e) = engine.complete_service(outcome) {
                            tracing::error!("Failed to complete service: {:?}", e);
                        }
                    }
                }
            });
        }
    });
}

