use criterion::{Criterion, black_box, criterion_group, criterion_main};
use roviers_core::RoviersEngine;
use roviers_proto::{ServiceOutcome, ServiceRequest};
use uuid::Uuid;

fn bench_token_verification(c: &mut Criterion) {
    let temp_dir = std::env::temp_dir().join(format!("roviers_bench_verify_{}", Uuid::now_v7()));
    let engine = RoviersEngine::new(&temp_dir).unwrap();
    let token = engine
        .issue_agent_token("AGENT-BENCH-01", "Bench Agent", 86400)
        .unwrap();

    c.bench_function("stateless_ed25519_token_verification", |b| {
        b.iter(|| {
            let res = token.verify(&engine.broker_verifying_key).unwrap();
            black_box(res);
        })
    });

    let _ = std::fs::remove_dir_all(&temp_dir);
}

fn bench_request_intake(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = std::env::temp_dir().join(format!("roviers_bench_intake_{}", Uuid::now_v7()));
    let engine = RoviersEngine::new(&temp_dir).unwrap();
    let token = engine
        .issue_agent_token("AGENT-BENCH-02", "Bench Submitter Agent", 86400)
        .unwrap();

    let mut group = c.benchmark_group("engine_intake");
    group.sample_size(50);

    group.bench_function("submit_service_request_throughput", |b| {
        b.to_async(&rt).iter(|| {
            let req = ServiceRequest::new(
                "bench.computation.task",
                token.clone(),
                serde_json::json!({
                    "task": "matrix_multiply",
                    "dimension": 128,
                    "urgency": "normal"
                }),
                None,
            );
            async {
                let receipt = engine.submit_service_request(req).await.unwrap();
                black_box(receipt);
            }
        })
    });

    group.finish();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

fn bench_vault_redemption(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = std::env::temp_dir().join(format!("roviers_bench_vault_{}", Uuid::now_v7()));
    let engine = RoviersEngine::new(&temp_dir).unwrap();
    let token = engine
        .issue_agent_token("AGENT-BENCH-03", "Vault Collector Agent", 86400)
        .unwrap();

    // Pre-populate 500 completed outcomes in the vault
    let mut receipt_ids = Vec::new();
    rt.block_on(async {
        for i in 0..500 {
            let req = ServiceRequest::new(
                "bench.storage.fetch",
                token.clone(),
                serde_json::json!({ "index": i }),
                None,
            );
            let req_id = req.request_id;
            receipt_ids.push(req_id);
            engine.submit_service_request(req).await.unwrap();

            let outcome = ServiceOutcome {
                receipt_id: req_id,
                agent_id: "AGENT-BENCH-03".into(),
                service_name: "bench.storage.fetch".into(),
                completed_at: chrono::Utc::now().timestamp(),
                result: serde_json::json!({ "computed_hash": "a8f3b299e120" }),
            };
            engine.complete_service(outcome).unwrap();
        }
    });

    let mut group = c.benchmark_group("vault_operations");
    let mut idx = 0;

    group.bench_function("pickup_outcome_from_vault", |b| {
        b.iter(|| {
            let id = receipt_ids[idx % receipt_ids.len()];
            idx += 1;
            let outcome = engine.pickup_outcome(id, &token);
            let _ = black_box(outcome);
        })
    });

    group.finish();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

criterion_group!(
    benches,
    bench_token_verification,
    bench_request_intake,
    bench_vault_redemption
);
criterion_main!(benches);
