use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use roviers_core::RoviersEngine;
use roviers_proto::{
    BlobReference, CancellationRequest, ClusterTopologyResponse, ParkForInputRequest,
    PeerHeartbeatRequest, Receipt, ReceiptStatus, ServiceOutcome, ServiceRequest,
    SessionResumeRequest, SignedAgentToken, SignedResumeToken, WorkerHeartbeatRequest,
    WorkerLeaseRequest, WorkerLeaseResponse,
};
use roviers_server::create_app;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn test_server_full_http_lifecycle() {
    let temp_dir = std::env::temp_dir().join(format!("roviers_http_test_{}", Uuid::now_v7()));
    let engine = Arc::new(RoviersEngine::new(&temp_dir).expect("Failed to init engine"));
    let app = create_app(Arc::clone(&engine));

    // 1. Health check
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2. Issue Agent Token
    let auth_body = serde_json::to_vec(&serde_json::json!({
        "agent_id": "AGENT-HTTP-TEST-01",
        "agent_name": "Integration Test Agent",
        "validity_days": 30
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/agent")
                .header("content-type", "application/json")
                .body(Body::from(auth_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let token: SignedAgentToken = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(token.credentials.agent_id, "AGENT-HTTP-TEST-01");

    // 3. Submit Service Request
    let req = ServiceRequest::new(
        "compute.matrix_invert",
        token.clone(),
        serde_json::json!({ "matrix_size": 256 }),
        None,
    );
    let req_id = req.request_id;
    let submit_body = serde_json::to_vec(&req).unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/services/compute.matrix_invert")
                .header("content-type", "application/json")
                .body(Body::from(submit_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let receipt: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipt.receipt_id, req_id);
    assert_eq!(receipt.agent_id, "AGENT-HTTP-TEST-01");

    // 4. Status Check
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/receipts/{}", req_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let r: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(r.status, ReceiptStatus::Queued);

    // 5. Worker deposits completed outcome into the Vault
    let complete_body = serde_json::to_vec(&serde_json::json!({
        "result": { "inverted": true, "determinant": 42.0 }
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/complete", req_id))
                .header("content-type", "application/json")
                .body(Body::from(complete_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 6. Pickup Outcome from Vault
    let pickup_body = serde_json::to_vec(&serde_json::json!({
        "agent_token": token
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/pickup", req_id))
                .header("content-type", "application/json")
                .body(Body::from(pickup_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let outcome: ServiceOutcome = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(outcome.result["inverted"], true);
    assert_eq!(outcome.result["determinant"], 42.0);

    // 7. Impostor pickup fails
    let impostor_token = engine
        .issue_agent_token("AGENT-IMPOSTOR", "Malicious Actor", 3600)
        .unwrap();
    let impostor_body = serde_json::to_vec(&serde_json::json!({
        "agent_token": impostor_token
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/pickup", req_id))
                .header("content-type", "application/json")
                .body(Body::from(impostor_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // 8. List receipts for agent
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/agents/AGENT-HTTP-TEST-01/receipts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let receipts: Vec<Receipt> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].status, ReceiptStatus::Fulfilled);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_server_hitl_park_and_resume_http() {
    let temp_dir = std::env::temp_dir().join(format!("roviers_http_hitl_{}", Uuid::now_v7()));
    let engine = Arc::new(RoviersEngine::new(&temp_dir).expect("Failed to init engine"));
    let app = create_app(Arc::clone(&engine));

    // 1. Issue Agent Token
    let auth_body = serde_json::to_vec(&serde_json::json!({
        "agent_id": "AGENT-HITL-01",
        "agent_name": "HITL Test Agent",
        "validity_days": 7
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/agent")
                .header("content-type", "application/json")
                .body(Body::from(auth_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let token: SignedAgentToken = serde_json::from_slice(&bytes).unwrap();

    // 2. Submit initial request
    let req = ServiceRequest::new(
        "insurance.claim_review",
        token.clone(),
        serde_json::json!({ "claim_id": "CLM-1002" }),
        None,
    );
    let req_id = req.request_id;
    let submit_body = serde_json::to_vec(&req).unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/services/insurance.claim_review")
                .header("content-type", "application/json")
                .body(Body::from(submit_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    // 3. Worker parks task awaiting missing incident report (HITL)
    let park_body = serde_json::to_vec(&ParkForInputRequest {
        receipt_id: req_id,
        question: "Please upload or specify incident police report number".into(),
        schema_for_answer: Some(serde_json::json!({ "type": "string" })),
        checkpoint_uri: Some("vault://checkpoint/clm-1002-step1".into()),
        ttl_secs: Some(3600),
    })
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/park", req_id))
                .header("content-type", "application/json")
                .body(Body::from(park_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let resume_token: SignedResumeToken = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(resume_token.payload.receipt_id, req_id);

    // 4. Verify receipt status is now ParkedAwaitingInput
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/receipts/{}", req_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let r: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert!(matches!(r.status, ReceiptStatus::ParkedAwaitingInput { .. }));

    // 5. User submits answer to resume the session
    let resume_body = serde_json::to_vec(&SessionResumeRequest {
        resume_token,
        answer: serde_json::json!({ "police_report": "PR-998822" }),
        agent_token: token.clone(),
    })
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/sessions/sess-{}/resume", req_id))
                .header("content-type", "application/json")
                .body(Body::from(resume_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let resumed: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(resumed.status, ReceiptStatus::Processing);

    // 6. Worker completes task
    let complete_body = serde_json::to_vec(&serde_json::json!({
        "result": { "status": "SETTLED", "payout": 3500.00 }
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/complete", req_id))
                .header("content-type", "application/json")
                .body(Body::from(complete_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 7. Pickup outcome
    let pickup_body = serde_json::to_vec(&serde_json::json!({
        "agent_token": token
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/pickup", req_id))
                .header("content-type", "application/json")
                .body(Body::from(pickup_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let outcome: ServiceOutcome = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(outcome.result["status"], "SETTLED");
    assert_eq!(outcome.result["payout"], 3500.00);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_server_backpressure_and_dynamic_backoff_http() {
    let temp_dir = std::env::temp_dir().join(format!("roviers_http_backoff_{}", Uuid::now_v7()));
    let engine = Arc::new(RoviersEngine::new(&temp_dir).expect("Failed to init engine"));
    let app = create_app(Arc::clone(&engine));

    // 1. Issue Agent Token
    let auth_body = serde_json::to_vec(&serde_json::json!({
        "agent_id": "AGENT-ERP-CLIENT",
        "agent_name": "ERP Integration Client",
        "validity_days": 10
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/agent")
                .header("content-type", "application/json")
                .body(Body::from(auth_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let token: SignedAgentToken = serde_json::from_slice(&bytes).unwrap();

    let service = "erp.invoice_sync";

    // 2. Normal status before backoff
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/services/{}/status", service))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let status_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status_val["status"], "NORMAL");

    // 3. Trip backoff for erp.invoice_sync via HTTP endpoint
    let backoff_body = serde_json::to_vec(&serde_json::json!({
        "duration_secs": 120,
        "reason": "Downstream ERP returned HTTP 503 Service Unavailable",
        "concurrency_cap": 1
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/services/{}/backoff", service))
                .header("content-type", "application/json")
                .body(Body::from(backoff_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. Submit task during active backoff: successfully queued with extended ETA
    let req = ServiceRequest::new(
        service,
        token.clone(),
        serde_json::json!({ "invoice_id": "INV-2026-001" }),
        None,
    );
    let submit_body = serde_json::to_vec(&req).unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/services/{}", service))
                .header("content-type", "application/json")
                .body(Body::from(submit_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let receipt: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipt.status, ReceiptStatus::Queued);
    assert!(receipt.estimated_ready_secs.unwrap() >= 100);

    // 5. Query status during backoff
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/services/{}/status", service))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let status_val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status_val["status"], "BACKING_OFF");

    // 6. Resume service / unset backoff
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/services/{}/resume", service))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_server_cancellation_and_worker_leases_http() {
    let temp_dir = std::env::temp_dir().join(format!("roviers_http_cancel_{}", Uuid::now_v7()));
    let engine = Arc::new(RoviersEngine::new(&temp_dir).expect("Failed to init engine"));
    let app = create_app(Arc::clone(&engine));

    // 1. Issue Agent Token
    let auth_body = serde_json::to_vec(&serde_json::json!({
        "agent_id": "AGENT-WORKER-01",
        "agent_name": "Batch Worker Agent",
        "validity_days": 10
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/agent")
                .header("content-type", "application/json")
                .body(Body::from(auth_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let token: SignedAgentToken = serde_json::from_slice(&bytes).unwrap();

    // 2. Submit task and test cancellation
    let req = ServiceRequest::new(
        "batch.crunch",
        token.clone(),
        serde_json::json!({ "dataset": "huge_csv" }),
        None,
    );
    let req_id = req.request_id;
    let submit_body = serde_json::to_vec(&req).unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/services/batch.crunch")
                .header("content-type", "application/json")
                .body(Body::from(submit_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    // Cancel the task
    let cancel_body = serde_json::to_vec(&CancellationRequest {
        receipt_id: req_id,
        agent_token: token.clone(),
        reason: Some("Pruned by user".into()),
    })
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/cancel", req_id))
                .header("content-type", "application/json")
                .body(Body::from(cancel_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let cancelled: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert!(matches!(cancelled.status, ReceiptStatus::Failed(_)));

    // 3. Submit tasks and test worker lease acquisition
    for i in 0..2 {
        let r = ServiceRequest::new(
            "worker.task",
            token.clone(),
            serde_json::json!({ "task_index": i }),
            None,
        );
        let b = serde_json::to_vec(&r).unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/services/worker.task")
                    .header("content-type", "application/json")
                    .body(Body::from(b))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    let lease_body = serde_json::to_vec(&WorkerLeaseRequest {
        worker_id: "WORKER-AUTONOMOUS-01".into(),
        service_filter: Some("worker.task".into()),
        max_tasks: 2,
        available_tpm_budget: None,
        active_inflight: 0,
    })
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/workers/lease")
                .header("content-type", "application/json")
                .body(Body::from(lease_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let lease: WorkerLeaseResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(lease.worker_id, "WORKER-AUTONOMOUS-01");

    // 4. Test worker heartbeat keepalive
    let hb_body = serde_json::to_vec(&WorkerHeartbeatRequest {
        worker_id: "WORKER-AUTONOMOUS-01".into(),
        lease_id: lease.lease_id,
        extend_secs: Some(60),
    })
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/workers/heartbeat")
                .header("content-type", "application/json")
                .body(Body::from(hb_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_server_dlq_and_two_tier_blob_vault_http() {
    let temp_dir = std::env::temp_dir().join(format!("roviers_http_dlq_test_{}", Uuid::now_v7()));
    let engine = Arc::new(RoviersEngine::new(&temp_dir).expect("Failed to init engine"));
    let app = create_app(Arc::clone(&engine));

    // 1. Issue Agent Token
    let auth_body = serde_json::to_vec(&serde_json::json!({
        "agent_id": "AGENT-BLOB-TEST",
        "agent_name": "Blob Test Agent",
        "validity_days": 30
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/agent")
                .header("content-type", "application/json")
                .body(Body::from(auth_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let token: SignedAgentToken = serde_json::from_slice(&bytes).unwrap();

    // 2. Submit service request
    let req = ServiceRequest::new(
        "report.render_pdf",
        token.clone(),
        serde_json::json!({ "doc_id": 999 }),
        None,
    );
    let req_id = req.request_id;
    let b = serde_json::to_vec(&req).unwrap();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/services/report.render_pdf")
                .header("content-type", "application/json")
                .body(Body::from(b))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    // 3. Complete with two-tier BlobReference
    let blob = BlobReference {
        blob_url: "s3://roviers-vault/reports/doc_999.pdf".into(),
        content_type: "application/pdf".into(),
        size_bytes: 8_200_100,
        sha256_hash: "abcdef1234567890abcdef1234567890".into(),
    };
    let complete_body = serde_json::to_vec(&serde_json::json!({
        "result": { "page_count": 42 },
        "blob_ref": blob
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/complete", req_id))
                .header("content-type", "application/json")
                .body(Body::from(complete_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 4. Pickup outcome and verify blob metadata
    let pickup_body = serde_json::to_vec(&serde_json::json!({ "agent_token": token })).unwrap();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/receipts/{}/pickup", req_id))
                .header("content-type", "application/json")
                .body(Body::from(pickup_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let outcome: ServiceOutcome = serde_json::from_slice(&bytes).unwrap();
    assert!(outcome.blob_ref.is_some());
    assert_eq!(outcome.blob_ref.unwrap().size_bytes, 8_200_100);

    // 5. Test reclaim & DLQ via HTTP
    let dlq_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/dlq")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(dlq_res.status(), StatusCode::OK);
    let bytes = dlq_res.into_body().collect().await.unwrap().to_bytes();
    let dlq_list: Vec<Receipt> = serde_json::from_slice(&bytes).unwrap();
    assert!(dlq_list.is_empty());

    let reclaim_res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/workers/reclaim")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reclaim_res.status(), StatusCode::OK);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_server_case_locking_and_cluster_http() {
    let temp_dir = std::env::temp_dir().join(format!("roviers_case_cluster_test_{}", Uuid::now_v7()));
    let engine = Arc::new(RoviersEngine::new(&temp_dir).expect("Failed to init engine"));
    let app = create_app(Arc::clone(&engine));

    // 1. Issue Agent Token
    let auth_body = serde_json::to_vec(&serde_json::json!({
        "agent_id": "AGENT-CASE-HTTP-01",
        "agent_name": "Case Agent",
        "validity_days": 30
    }))
    .unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/agent")
                .header("content-type", "application/json")
                .body(Body::from(auth_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let token: SignedAgentToken = serde_json::from_slice(&bytes).unwrap();

    // 2. Submit initial request with business_key
    let business_key = "case:CLAIM-2026-X";
    let req = ServiceRequest::new(
        "insurance.claim",
        token.clone(),
        serde_json::json!({ "claim_id": "CLM-2026-X", "amount": 1200 }),
        None,
    )
    .with_business_key(business_key);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/services/insurance.claim")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let receipt: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipt.business_key, Some(business_key.to_string()));

    // 3. GET /api/v1/cases/{business_key} -> returns active receipt
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/cases/{}", business_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let fetched_case: Receipt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(fetched_case.receipt_id, receipt.receipt_id);

    // 4. Submitting a duplicate request with the same business_key -> returns 409 CONFLICT
    let dup_req = ServiceRequest::new(
        "insurance.claim",
        token.clone(),
        serde_json::json!({ "claim_id": "CLM-2026-X", "amount": 1500 }),
        None,
    )
    .with_business_key(business_key);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/services/insurance.claim")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&dup_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);

    // 5. Cluster topology: GET /api/v1/cluster/nodes
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/cluster/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let topology: ClusterTopologyResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(topology.nodes.len(), 1);

    // 6. Cluster peer heartbeat: POST /api/v1/cluster/heartbeat
    let peer_hb = PeerHeartbeatRequest {
        node_id: "broker-node-02".into(),
        listen_addr: "http://127.0.0.1:9091".into(),
        is_leader: false,
        assigned_partitions: vec![8, 9, 10, 11, 12, 13, 14, 15],
    };
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/cluster/heartbeat")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&peer_hb).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Verify updated topology has 2 nodes
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/cluster/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let topology2: ClusterTopologyResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(topology2.nodes.len(), 2);

    // 7. Explicitly release business case lock: DELETE /api/v1/cases/{business_key}
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/cases/{}", business_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 8. GET /api/v1/cases/{business_key} now returns 404 NOT_FOUND
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/v1/cases/{}", business_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let _ = std::fs::remove_dir_all(&temp_dir);
}
