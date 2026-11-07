use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use roviers_core::RoviersEngine;
use roviers_proto::{Receipt, ReceiptStatus, ServiceOutcome, ServiceRequest, SignedAgentToken};
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
