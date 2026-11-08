use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures::stream::Stream;
use roviers_core::RoviersEngine;
use roviers_proto::{
    BlobReference, CancellationRequest, ClusterTopologyResponse,
    ParkForInputRequest, PeerHeartbeatRequest, Receipt, ServiceBackoff, ServiceOutcome,
    ServiceRequest, SessionResumeRequest, SetBackoffRequest, SignedAgentToken, SignedResumeToken,
    WorkerHeartbeatRequest, WorkerLeaseRequest, WorkerLeaseResponse,
};
use serde::Deserialize;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<RoviersEngine>,
}

#[derive(Deserialize)]
pub struct IssueAgentRequest {
    pub agent_id: String,
    #[serde(alias = "full_name", default = "default_agent_name")]
    pub agent_name: String,
    #[serde(default = "default_validity_days")]
    pub validity_days: i64,
}

fn default_agent_name() -> String {
    "Autonomous Agent".to_string()
}

fn default_validity_days() -> i64 {
    365
}

#[derive(Deserialize)]
pub struct PickupRequest {
    #[serde(alias = "citizen_token")]
    pub agent_token: SignedAgentToken,
}

#[derive(Deserialize)]
pub struct CompleteRequest {
    pub result: serde_json::Value,
    #[serde(default)]
    pub blob_ref: Option<BlobReference>,
}

#[derive(Deserialize, Default)]
pub struct CompactVaultRequest {
    #[serde(default)]
    pub max_age_secs: Option<i64>,
}

pub fn create_app(engine: Arc<RoviersEngine>) -> Router {
    let state = AppState { engine };

    Router::new()
        .route("/health", get(health_check))
        // Agent Auth
        .route("/api/v1/auth/agent", post(issue_agent_token))
        .route("/api/v1/auth/citizen", post(issue_agent_token)) // Backwards compatibility
        // Service Intake & Backpressure Management
        .route("/api/v1/services/{service_name}", post(submit_service))
        .route(
            "/api/v1/services/{service_name}/backoff",
            post(set_service_backoff),
        )
        .route(
            "/api/v1/services/{service_name}/resume",
            post(unset_service_backoff),
        )
        .route(
            "/api/v1/services/{service_name}/status",
            get(get_service_status),
        )
        // Receipts & Outcome Vault
        .route("/api/v1/receipts/{receipt_id}", get(get_receipt))
        .route("/api/v1/receipts/{receipt_id}/pickup", post(pickup_outcome))
        .route(
            "/api/v1/receipts/{receipt_id}/complete",
            post(complete_receipt),
        )
        // Interactive Human-In-The-Loop (HITL) Parking & Resumption
        .route("/api/v1/receipts/{receipt_id}/park", post(park_for_input))
        .route("/api/v1/sessions/{session_id}/resume", post(resume_session))
        // In-Flight Task Cancellation
        .route("/api/v1/receipts/{receipt_id}/cancel", post(cancel_receipt))
        // Business Key Idempotency & Case Locking
        .route("/api/v1/cases/{business_key}", get(get_business_case).delete(release_business_case))
        // Worker Capacity Leases & Heartbeats
        .route("/api/v1/workers/lease", post(lease_tasks))
        .route("/api/v1/workers/heartbeat", post(worker_heartbeat))
        .route("/api/v1/workers/reclaim", post(reclaim_expired_leases))
        // Cluster HA & Node Topology
        .route("/api/v1/cluster/nodes", get(get_cluster_nodes))
        .route("/api/v1/cluster/heartbeat", post(cluster_peer_heartbeat))
        // Dead Letter Queue
        .route("/api/v1/dlq", get(list_dlq))
        // Vault Lifecycle & Compaction
        .route("/api/v1/vault/compact", post(compact_vault))
        // SSE Real-Time Completion Signals
        .route(
            "/api/v1/agents/{agent_id}/stream",
            get(stream_agent_signals),
        )
        .route(
            "/api/v1/tenants/{tenant_id}/stream",
            get(stream_tenant_signals),
        )
        .route(
            "/api/v1/sessions/{session_id}/stream",
            get(stream_session_signals),
        )
        .route(
            "/api/v1/agents/{agent_id}/receipts",
            get(list_agent_receipts),
        )
        .route(
            "/api/v1/citizens/{citizen_id}/stream",
            get(stream_citizen_signals),
        )
        .route(
            "/api/v1/citizens/{citizen_id}/receipts",
            get(list_citizen_receipts),
        )
        .with_state(state)
}

pub async fn health_check() -> &'static str {
    "Roviers Broker: OK"
}

/// Endpoint to issue official agent identity credentials (signed by the broker key)
pub async fn issue_agent_token(
    State(state): State<AppState>,
    Json(req): Json<IssueAgentRequest>,
) -> Result<Json<SignedAgentToken>, (StatusCode, String)> {
    let validity_secs = req.validity_days * 86400;
    let token = state
        .engine
        .issue_agent_token(&req.agent_id, &req.agent_name, validity_secs)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(token))
}

/// Broker Ingress: Agent presents token and request, receives Receipt (HTTP 202 Accepted)
pub async fn submit_service(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
    headers: HeaderMap,
    Json(mut req): Json<ServiceRequest>,
) -> Result<(StatusCode, HeaderMap, Json<Receipt>), (StatusCode, String)> {
    req.service_name = service_name;

    // Phase 12: W3C Distributed Tracing propagation
    if req.traceparent.is_none() {
        if let Some(tp) = headers.get("traceparent").and_then(|v| v.to_str().ok()) {
            req.traceparent = Some(tp.to_string());
        } else {
            let trace_id = format!("{:032x}", rand::random::<u128>());
            let parent_id = format!("{:016x}", rand::random::<u64>());
            req.traceparent = Some(format!("00-{}-{}-01", trace_id, parent_id));
        }
    }
    if req.tracestate.is_none()
        && let Some(ts) = headers.get("tracestate").and_then(|v| v.to_str().ok())
    {
        req.tracestate = Some(ts.to_string());
    }

    match state.engine.submit_service_request(req).await {
        Ok(receipt) => {
            let mut resp_headers = HeaderMap::new();
            if let Some(ref tp) = receipt.traceparent
                && let Ok(val) = HeaderValue::from_str(tp)
            {
                resp_headers.insert("traceparent", val);
            }
            if let Some(ref ts) = receipt.tracestate
                && let Ok(val) = HeaderValue::from_str(ts)
            {
                resp_headers.insert("tracestate", val);
            }
            Ok((StatusCode::ACCEPTED, resp_headers, Json(receipt)))
        }
        Err(roviers_core::CoreError::Auth(e)) => Err((StatusCode::UNAUTHORIZED, e.to_string())),
        Err(roviers_core::CoreError::DuplicateRequest(id)) => {
            Err((StatusCode::CONFLICT, format!("Duplicate request: {}", id)))
        }
        Err(roviers_core::CoreError::BusinessCaseLocked(k, id)) => {
            Err((
                StatusCode::CONFLICT,
                serde_json::json!({
                    "error": "BusinessCaseLocked",
                    "business_key": k,
                    "existing_receipt_id": id
                })
                .to_string(),
            ))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// Dynamically trip or set backoff for a downstream service
pub async fn set_service_backoff(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
    Json(req): Json<SetBackoffRequest>,
) -> Json<ServiceBackoff> {
    let backoff = state.engine.set_service_backoff(
        &service_name,
        req.duration_secs,
        req.reason,
        req.concurrency_cap,
    );
    Json(backoff)
}

/// Clear backoff and resume steady-state dispatch for a downstream service
pub async fn unset_service_backoff(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let cleared = state.engine.unset_service_backoff(&service_name);
    (
        StatusCode::OK,
        Json(serde_json::json!({ "service_name": service_name, "backoff_cleared": cleared })),
    )
}

/// Query current backoff and capacity status for a service
pub async fn get_service_status(
    State(state): State<AppState>,
    Path(service_name): Path<String>,
) -> Json<serde_json::Value> {
    if let Some(backoff) = state.engine.get_service_backoff(&service_name) {
        let now = chrono::Utc::now().timestamp();
        let remaining_secs = (backoff.backed_off_until - now).max(0);
        Json(serde_json::json!({
            "service_name": service_name,
            "status": "BACKING_OFF",
            "backed_off_until": backoff.backed_off_until,
            "remaining_secs": remaining_secs,
            "reason": backoff.reason,
            "concurrency_cap": backoff.concurrency_cap
        }))
    } else {
        Json(serde_json::json!({
            "service_name": service_name,
            "status": "NORMAL"
        }))
    }
}

/// Check receipt status
pub async fn get_receipt(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
) -> Result<(HeaderMap, Json<Receipt>), StatusCode> {
    let receipt = state.engine.get_receipt(receipt_id).ok_or(StatusCode::NOT_FOUND)?;
    let mut resp_headers = HeaderMap::new();
    if let Some(ref tp) = receipt.traceparent
        && let Ok(val) = HeaderValue::from_str(tp)
    {
        resp_headers.insert("traceparent", val);
    }
    if let Some(ref ts) = receipt.tracestate
        && let Ok(val) = HeaderValue::from_str(ts)
    {
        resp_headers.insert("tracestate", val);
    }
    Ok((resp_headers, Json(receipt)))
}

/// Pickup outcome by presenting the receipt ID and Agent token
pub async fn pickup_outcome(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
    Json(req): Json<PickupRequest>,
) -> Result<Json<ServiceOutcome>, (StatusCode, String)> {
    state
        .engine
        .pickup_outcome(receipt_id, &req.agent_token)
        .map(Json)
        .map_err(|e| match e {
            roviers_core::CoreError::Auth(_) | roviers_core::CoreError::AgentMismatch => {
                (StatusCode::FORBIDDEN, e.to_string())
            }
            roviers_core::CoreError::OutcomeNotReady(_) => {
                (StatusCode::ACCEPTED, "Outcome still in process".to_string())
            }
            roviers_core::CoreError::ReceiptNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })
}

/// Worker completion: deposit outcome into the Vault
pub async fn complete_receipt(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
    Json(req): Json<CompleteRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let receipt = state.engine.get_receipt(receipt_id).ok_or((
        StatusCode::NOT_FOUND,
        format!("Receipt {} not found", receipt_id),
    ))?;

    let outcome = ServiceOutcome {
        receipt_id,
        agent_id: receipt.agent_id,
        service_name: receipt.service_name,
        completed_at: chrono::Utc::now().timestamp(),
        result: req.result,
        blob_ref: req.blob_ref,
    };

    state
        .engine
        .complete_service(outcome)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::OK)
}

/// Worker parks task awaiting interactive human/agent input (HITL)
pub async fn park_for_input(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
    Json(req): Json<ParkForInputRequest>,
) -> Result<Json<SignedResumeToken>, (StatusCode, String)> {
    let ttl_secs = req.ttl_secs.unwrap_or(86400);
    state
        .engine
        .park_for_input(
            receipt_id,
            req.question,
            req.schema_for_answer,
            req.checkpoint_uri,
            ttl_secs,
        )
        .map(Json)
        .map_err(|e| match e {
            roviers_core::CoreError::ReceiptNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })
}

/// User/client supplies answer and SignedResumeToken to resume parked task
pub async fn resume_session(
    State(state): State<AppState>,
    Path(_session_id): Path<String>,
    Json(req): Json<SessionResumeRequest>,
) -> Result<Json<Receipt>, (StatusCode, String)> {
    state
        .engine
        .resume_session(req)
        .map(Json)
        .map_err(|e| match e {
            roviers_core::CoreError::Auth(_) | roviers_core::CoreError::AgentMismatch => {
                (StatusCode::FORBIDDEN, e.to_string())
            }
            roviers_core::CoreError::ReceiptNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            roviers_core::CoreError::NotParked(_) => (StatusCode::BAD_REQUEST, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })
}

/// In-flight task cancellation
pub async fn cancel_receipt(
    State(state): State<AppState>,
    Path(receipt_id): Path<Uuid>,
    Json(req): Json<CancellationRequest>,
) -> Result<Json<Receipt>, (StatusCode, String)> {
    state
        .engine
        .cancel_receipt(receipt_id, &req.agent_token, req.reason)
        .map(Json)
        .map_err(|e| match e {
            roviers_core::CoreError::Auth(_) | roviers_core::CoreError::AgentMismatch => {
                (StatusCode::FORBIDDEN, e.to_string())
            }
            roviers_core::CoreError::ReceiptNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })
}

/// Worker capacity lease acquisition
pub async fn lease_tasks(
    State(state): State<AppState>,
    Json(req): Json<WorkerLeaseRequest>,
) -> Result<Json<WorkerLeaseResponse>, (StatusCode, String)> {
    state
        .engine
        .lease_tasks(req, 45)
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// Worker heartbeat keepalive
pub async fn worker_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<WorkerHeartbeatRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let extend_secs = req.extend_secs.unwrap_or(45);
    state
        .engine
        .worker_heartbeat(req.lease_id, extend_secs)
        .map(|_| StatusCode::OK)
        .map_err(|e| match e {
            roviers_core::CoreError::ReceiptNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })
}

/// Reclaim expired worker leases, re-queuing orphaned tasks or moving exceeded retries to DLQ
pub async fn reclaim_expired_leases(State(state): State<AppState>) -> Json<Vec<Uuid>> {
    let reclaimed = state.engine.reclaim_expired_leases();
    Json(reclaimed)
}

/// List all dead-lettered / permanently failed receipts
pub async fn list_dlq(State(state): State<AppState>) -> Json<Vec<Receipt>> {
    let dlq = state.engine.list_dlq();
    Json(dlq)
}

/// Compact / purge expired completed or failed receipts and outcomes from vault (Phase 11)
pub async fn compact_vault(
    State(state): State<AppState>,
    Json(req): Json<CompactVaultRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let max_age = req.max_age_secs.unwrap_or(86400);
    let purged = state.engine.purge_expired_receipts(max_age);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "purged_receipts": purged,
            "max_age_secs": max_age
        })),
    )
}

/// List all active receipts for an agent
pub async fn list_agent_receipts(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
) -> Json<Vec<Receipt>> {
    let receipts = state.engine.list_agent_receipts(&agent_id);
    Json(receipts)
}

pub async fn list_citizen_receipts(
    State(state): State<AppState>,
    Path(citizen_id): Path<String>,
) -> Json<Vec<Receipt>> {
    list_agent_receipts(State(state), Path(citizen_id)).await
}

/// Real-time Server-Sent Events (SSE) signal channel:
/// Agent holds an open lightweight SSE stream to hear back when an outcome is ready.
/// Zero polling, zero heavy sockets on worker servers.
pub async fn stream_agent_signals(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.engine.subscribe_signals();
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let expected_agent = agent_id.clone();
        match item {
            Ok(signal) => {
                if signal.agent_id == expected_agent {
                    let json = serde_json::to_string(&signal).unwrap_or_default();
                    Some(Ok(Event::default().event("signal").data(json)))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

pub async fn stream_citizen_signals(
    State(state): State<AppState>,
    Path(citizen_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    stream_agent_signals(State(state), Path(citizen_id)).await
}

/// Tenant-scoped real-time SSE signal stream
pub async fn stream_tenant_signals(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.engine.subscribe_signals();
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let expected_tenant = tenant_id.clone();
        match item {
            Ok(signal) => {
                if signal.tenant_id.as_deref() == Some(&expected_tenant) {
                    let json = serde_json::to_string(&signal).unwrap_or_default();
                    Some(Ok(Event::default().event("signal").data(json)))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Session-scoped real-time SSE signal stream (for sticky interactive HITL turns)
pub async fn stream_session_signals(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.engine.subscribe_signals();
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let expected_session = session_id.clone();
        match item {
            Ok(signal) => {
                if signal.session_id.as_deref() == Some(&expected_session) {
                    let json = serde_json::to_string(&signal).unwrap_or_default();
                    Some(Ok(Event::default().event("signal").data(json)))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Retrieve the active receipt currently leasing a business key
pub async fn get_business_case(
    State(state): State<AppState>,
    Path(business_key): Path<String>,
) -> Result<Json<Receipt>, StatusCode> {
    state
        .engine
        .get_business_case(&business_key)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Explicitly unlock/release a business key lock
pub async fn release_business_case(
    State(state): State<AppState>,
    Path(business_key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let released = state.engine.release_business_case(&business_key);
    (
        StatusCode::OK,
        Json(serde_json::json!({ "business_key": business_key, "released": released })),
    )
}

/// Retrieve the active cluster node topology and partition assignments
pub async fn get_cluster_nodes(State(state): State<AppState>) -> Json<ClusterTopologyResponse> {
    Json(state.engine.get_cluster_topology())
}

/// Register or renew a peer broker node heartbeat
pub async fn cluster_peer_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<PeerHeartbeatRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    state.engine.register_or_heartbeat_peer(req);
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "acknowledged" })),
    )
}

/// Background worker simulation:
/// Pulls work from the topic trie and completes tasks asynchronously.
pub fn spawn_worker_pool(engine: Arc<RoviersEngine>) {
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
                    let req_opt = serde_json::from_slice::<ServiceRequest>(&record.payload).ok();
                    if let Some(req) = req_opt {
                        tracing::info!(
                            "🛠️ Worker executing service '{}' for receipt #{}...",
                            req.service_name,
                            req.request_id
                        );

                        // Simulate processing time
                        tokio::time::sleep(Duration::from_millis(1500)).await;

                        let outcome = ServiceOutcome {
                            receipt_id: req.request_id,
                            agent_id: req.agent_token.credentials.agent_id.clone(),
                            service_name: req.service_name.clone(),
                            completed_at: chrono::Utc::now().timestamp(),
                            result: serde_json::json!({
                                "status": "APPROVED",
                                "document_number": format!("DOC-{:X}", rand::random::<u32>()),
                                "seal": "BROKER_VERIFIED",
                                "details": req.payload
                            }),
                            blob_ref: None,
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
