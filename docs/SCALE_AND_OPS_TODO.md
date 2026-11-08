# Roviers Scale, Ops & Multi-Agent Architecture TODO

This document tracks identified architectural gaps, real-world multi-agent failure modes, and implementation plans required to take **Roviers** from an ultra-low-latency single-node broker to a production chassis-grade multi-tenant event fabric.

---

## 1. Identified Scale & Ops Gaps

| # | Gap Area | Current State in Roviers | Production Risk / Failure Mode | Target Resolution |
|---|---|---|---|---|
| **GAP-1** | **Single-Node Data Directory** | Local `--data-dir` with mmap append-only log segments and local JSON vault. | Single point of failure (SPOF); no clustering; replica split-brain; no multi-broker failover. | Partitioned Raft consensus or distributed log tier (NATS JetStream / Kafka / Raft cluster) with decoupled SSE fan-out replicas. |
| **GAP-2** | **Multi-Tenant SSE Scoping** | SSE streams are indexed 1:1 by flat `agent_id`. | 10k end-users either require 10k individual SSE connections or pooled service tokens resulting in noisy streams and cross-tenant data leaks. | Hierarchical scoping (`TenantID:Namespace:AgentID:SessionID`) with granular JWT/Ed25519 claims and topic-filtered channels. |
| **GAP-3** | **Interactive HITL & Provider Q&A** | One-way `callback_url` or single claim-check pickup. | Multi-turn Human-In-The-Loop (HITL) workflows park awaiting input. Subsequent user answers route to different workers, causing "no waiter / 404" failures. | Session Rendezvous Protocol with stateful `resume_token`, parked state machine (`AwaitingInput`), and distributed turn correlation. |
| **GAP-4** | **Large Binary & Blob Vaulting** | In-memory cache + local filesystem disk vault (`/vault/{receipt_id}.json`). | Storing large quote templates, PDFs, multi-MB prompt artifacts, or images bloats the local disk and degrades broker performance. | Two-tier Claim-Check: inline JSON (<64KB) vs Blob Storage (S3 / GCS / Azure Blob) with presigned URLs and Blake3/SHA256 integrity proofs. |
| **GAP-5** | **Business Key Idempotency** | Sliding window ring-buffer (default 1M entries, ~60s retention under heavy burst). | Transient retries succeed, but multi-hour / multi-day business processes ("one open claim per user session") cannot be enforced. | Persistent Business Case Leasing (`business_key` + distributed lease lock with configurable TTL up to 30 days). |
| **GAP-6** | **LLM Quota Stampedes & Backpressure** | Roviers accepts all requests up to broker network/disk I/O ceiling. | Workers concurrently picking tasks stampede upstream LLM rate limits (Azure OpenAI TPM/RPM, Apister inflight limits), triggering cascading 429 errors. | Quota-aware admission control, token-bucket rate limiters per upstream model, and worker capacity leases. |
| **GAP-7** | **Benchmark Realism & Sizing** | Raw ingestion/SSE tested at 100k+ ops/sec. | Ingress throughput gives a false sense of capacity; actual system throughput is constrained by LLM inference (10-50 turns/sec/cluster). | Tiered benchmark suite distinguishing **Broker Ingress Throughput** from **Chassis End-to-End Orchestration Capacity**. |
| **GAP-8** | **Worker Crash, Lease Expiration & DLQ** | No worker heartbeat or lease timeout on in-flight tasks. | Worker crash (Kubernetes OOMKill, spot eviction) leaves task stuck in `Processing` forever; poison pills crash repeated workers in an infinite loop. | Ephemeral worker leases with heartbeat renewal (`keepalive`), automatic lease reclaim, and Dead Letter Queue (DLQ) after $N$ failed attempts. |
| **GAP-9** | **In-Flight Cancellation & Client Abort** | No mechanism to cancel a running task or revoke an issued receipt. | User cancels prompt in UI or evaluator prunes an agent branch, but worker continues expensive 30s LLM inference loop, burning quota. | First-class cancellation API (`POST /receipts/{id}/cancel`), SSE `AbortSignal` broadcast, and cooperative token cancellation in client SDKs. |
| **GAP-10** | **Multi-Tenant Fair Queuing (Noisy Neighbors)** | Global FIFO queue for all incoming service requests. | One tenant dumping 50,000 batch indexing jobs completely starves another tenant's real-time interactive user turns. | Multi-tenant Deficit Round-Robin (DRR) / Weighted Fair Queuing (WFQ) ensuring guaranteed fair-share worker dispatch per tenant. |
| **GAP-11** | **Vault Retention, Compaction & Lifecycle GC** | Vault files accumulate indefinitely on disk. | High-throughput production clusters run out of disk inodes and storage capacity over weeks/months of operation. | Automated retention & compaction engine: hot in-memory $\to$ warm local disk $\to$ cold compressed S3/GCS archive $\to$ TTL purge. |
| **GAP-12** | **End-to-End Distributed Tracing (W3C)** | Tracing is internal to server log spans. | Cannot correlate a slow or failed turn across User UI $\to$ Gateway $\to$ Roviers $\to$ Worker $\to$ LLM/APISTER $\to$ SSE. | Propagate W3C `traceparent` and `tracestate` across `ServiceRequest`, `Receipt`, and `AgentSignal` into OpenTelemetry collectors. |

---

## 2. Multi-Agent & Multi-User Failure Modes

```
[FAILURE SCENARIO 1: HITL Routing Disconnection]
User A ────► Roviers Ingress ────► Worker 1 picks task
                                        │
                                   Agent 1 asks question (HITL Parked)
                                        │
User A sends answer ───► Gateway ───► Worker 2 (Agent 2) ──► 💥 404 / No Waiter Found!

[FAILURE SCENARIO 2: Worker Quota Stampede]
500 Clients ──► Roviers (Queues instantly @ 100k/s)
                     │
         50 Worker Nodes pull simultaneously
                     │
          All call Azure OpenAI (TPM: 120k) ──► 💥 Cascading HTTP 429 Too Many Requests!

[FAILURE SCENARIO 3: Worker Node Crash & Zombie Tasks]
Client ────► Roviers (Dispatches Task-X) ────► Worker Node A
                                                    │
                                     Worker Node A OOMKilled by OS!
                                                    │
                                       (Task-X stuck in "Processing")
                                                    │
Client polls /status ──► Never completes (Forever pending until manual reset)

[FAILURE SCENARIO 4: Noisy Neighbor Starvation]
Tenant A dumps 50,000 batch doc-indexing jobs ──┐
                                                ├──► FIFO Queue ──► Workers 1..10
Tenant B submits 1 urgent interactive prompt ────┘      (Tenant B waits 15 minutes!)
```

---

## 3. Detailed Architectural Implementation Plan

### Phase 1: Interactive Human-In-The-Loop (HITL) & Session Rendezvous
*Goal: Prevent broken conversation turns and lost waiters when users answer questions.*

- [ ] **Stateful Session Turn Machine**:
  - Add `session_id`, `turn_id`, and `parent_receipt_id` to `roviers-proto::ServiceRequest`.
  - Introduce lifecycle states: `Dispatched`, `ParkedAwaitingInput`, `Resumed`, `Completed`, `TimedOut`.
- [ ] **Resumption Tokens (`SignedResumeToken`)**:
  - When a worker pauses for human/agent input, it issues a signed `ResumeToken` containing:
    `{ session_id, turn_id, schema_for_answer, expiry, checkpoint_uri }`.
  - User/client POSTs answer to `/api/v1/sessions/{session_id}/resume` supplying the `ResumeToken`.
- [ ] **Distributed Waiter Rendezvous**:
  - Support two worker architectures:
    1. **Sticky Routing**: Route resumption signal directly to the worker holding the in-memory agent.
    2. **Stateless Checkpointing (Recommended)**: Worker checkpoints agent memory (e.g. LangGraph state) to persistent storage upon parking; any worker can pick up the resumption turn.

### Phase 2: Upstream LLM Quota-Aware Leases & Adaptive Backpressure
*Goal: Prevent agent worker stampedes against shared LLM API limits (TPM / RPM / Inflight).*

- [ ] **Broker-Managed Capacity Leases**:
  - Introduce `WorkerHeartbeat` with capacity telemetry (`available_slots`, `active_turns`).
  - Workers lease tasks using `/api/v1/workers/lease` rather than blind polling.
- [ ] **Token Bucket Rate Limiting per Provider**:
  - Add configuration block in `roviers.toml` for upstream model budgets:
    ```toml
    [upstream_limits.azure_openai_gpt4o]
    max_tpm = 120_000
    max_rpm = 1_200
    max_inflight_turns = 40
    ```
  - Roviers estimates or tracks tokens per request and throttles task dispatch into the worker pool.
- [ ] **Prioritized Task Queuing**:
  - Interactive user turns receive `Priority::High`; background batch tasks receive `Priority::Low`.
  - When LLM quota is near exhaustion, hold background tasks while serving interactive user turns.

### Phase 3: Multi-Broker High Availability & Distributed Clustering
*Goal: Move from single-node `--data-dir` to chassis-grade high availability without split-brain.*

- [ ] **Partitioned Log Architecture**:
  - Replace single monolithic commit log with virtual topic partitions (`partition_id = hash(tenant_id, session_id) % num_partitions`).
- [ ] **Consensus & Replication**:
  - Implement Raft replication across broker replicas (or provide official integration adapter for NATS JetStream / Kafka storage engines).
  - Leader election per partition; automatic failover if node crashes.
- [ ] **Read/Write Segregation for SSE**:
  - Write Leaders: Handle ingestion, deduplication, and receipt generation.
  - Read Followers: Fan out SSE streams to thousands of connected clients, eliminating connection-drain overhead on the write path.

### Phase 4: Multi-Tenant Identity & Scoped SSE Fan-Out
*Goal: Securely isolate 10,000+ end-users across tenants without stream leakage or connection exhaustion.*

- [ ] **Hierarchical Subject Addressing**:
  - Format: `{tenant_id}/{workspace_id}/{agent_id}/{session_id}`.
- [ ] **Scoped Cryptographic Tokens**:
  - Extend `SignedAgentToken` claims to include `allowed_topics` wildcard patterns and tenant boundaries.
  - SSE listener endpoint enforces strict prefix matching (e.g., Tenant A agent cannot listen to Tenant B events).
- [ ] **Connection Multiplexing / SSE Gateway**:
  - Support client heartbeat pings, stream reconnects with `Last-Event-ID`, and server-side connection coalescing.

### Phase 5: Two-Tier Claim-Check Vault (Inlined vs External Blob Storage)
*Goal: Efficiently handle large payloads (PDFs, images, multi-MB context windows) without bloating disk.*

- [ ] **Tiered Storage Threshold**:
  - Threshold configurable via `vault.inline_threshold_bytes` (default: 64 KB).
  - Outcomes $\le$ 64 KB: Stored directly in encrypted local/distributed JSON vault.
  - Outcomes $>$ 64 KB: Offloaded to Object Storage (S3 / GCS / Azure Blob).
- [ ] **Presigned Pickup Protocol**:
  - Receipt outcome returns `BlobReference { url, method, headers, sha256_digest, size_bytes }`.
  - Client retrieves binary directly from Cloud Storage, preserving broker bandwidth.

### Phase 6: Long-Lived Business Idempotency & Case Leasing
*Goal: Guarantee business rules (e.g. "only one active loan application per citizen session") over days/weeks.*

- [ ] **Persistent Idempotency Registry**:
  - Store `business_key` mappings in RocksDB / SQLite / Postgres with custom TTL (e.g. 24 hours to 30 days).
- [ ] **Stateful Mutex & Atomic Guard**:
  - Prevent duplicate submissions while an existing case is still in `InProgress` state.
  - Return existing active `receipt_id` if a duplicate submission arrives during processing.

### Phase 7: Benchmark Realism & Sizing Documentation
*Goal: Set realistic expectations for engineering teams sizing infrastructure.*

- [ ] **Document Dual Throughput Reality**:
  - Clearly document the distinction between **Broker Ingress/Egress** (100k+ msg/sec) and **End-to-End LLM Agent Turns** (~10-100 turns/sec governed by LLM latency and concurrency quotas).
- [ ] **End-to-End Soak Test Suite**:
  - Add simulated agent workload harness with synthetic 1.5s LLM latency, 2KB prompt payloads, and 5% HITL interruptions to measure real chassis behavior under stress.

### Phase 8: Worker Liveness, Lease Heartbeats & Dead Letter Queue (DLQ)
*Goal: Automatically recover from worker crashes and isolate poison pill payloads.*

- [ ] **Lease Keepalive & Heartbeat Protocol**:
  - Workers refresh lease periodically via `POST /api/v1/workers/{worker_id}/heartbeat`.
  - Default lease TTL: 45 seconds. Broker background sweeper reclaims expired leases and resets task status to `Queued`.
- [ ] **Dead Letter Queue (DLQ) & Max Retry Guard**:
  - Track `delivery_attempts` per task.
  - If a task causes $N \ge 3$ consecutive worker crashes or unhandled panics, immediately move it to `DLQ`.
  - Deposit a structured error outcome in the Vault so the client receives a deterministic failure notification.

### Phase 9: In-Flight Task Cancellation & Abort Propagation
*Goal: Stop expensive downstream LLM inference loops when users cancel or navigate away.*

- [ ] **Cancellation API**:
  - Add `POST /api/v1/receipts/{receipt_id}/cancel` endpoint accepting signed agent credential.
  - Marks receipt status as `Cancelled`.
- [ ] **Worker Abort Broadcast**:
  - Broadcast an `AbortSignal` over internal worker channel / SSE.
  - Python and TypeScript SDKs attach `AbortController` / `tokio::select!` cancellation triggers to interrupt running LLM API requests and release GPU/TPM quota immediately.

### Phase 10: Multi-Tenant Fair Queuing & Starvation Prevention
*Goal: Prevent batch workloads from degrading interactive real-time user turns.*

- [ ] **Deficit Round-Robin (DRR) Scheduler**:
  - Maintain per-tenant queue partitions in memory.
  - Round-robin task dispatch across tenants with active workloads, ensuring single high-volume tenants cannot consume more than their fair slice of available worker leases.
- [ ] **Priority Classes**:
  - Support `Priority::Interactive` vs `Priority::Batch` in `ServiceRequest`.

### Phase 11: Vault Lifecycle, Retention & Cold Archival
*Goal: Prevent disk exhaustion in continuous high-throughput deployments.*

- [ ] **Automated Compaction & Pruning**:
  - Configurable retention in `roviers.toml`:
    ```toml
    [vault.lifecycle]
    hot_cache_ttl_secs = 300
    local_disk_retention_days = 7
    archive_to_cold_storage = true
    cold_storage_bucket = "s3://my-roviers-vault-archive"
    ```
  - Scheduled background sweeper zips aged partitions and moves them to cold storage before deleting local files.

### Phase 12: W3C Distributed Tracing (OpenTelemetry)
*Goal: Provide end-to-end observability across the entire multi-agent call chain.*

- [ ] **Trace Context Injection & Extraction**:
  - Add optional `traceparent` and `tracestate` fields to `ServiceRequest` and `Receipt`.
  - Propagate context into Axum request spans, ledger commit logs, and SSE broadcast signals.
  - Enable seamless correlation in Jaeger, Datadog, Zipkin, and Google Cloud Trace.

---

## 4. Priority Roadmap

```
           Q1 2026: Reliability, HITL & Quotas
┌────────────────────────────────────────────────────────┐
│  Phase 1: HITL & Session Rendezvous                    │
│  Phase 2: LLM Quota Leases & Adaptive Backpressure     │
│  Phase 8: Worker Liveness, Heartbeats & DLQ            │
│  Phase 9: In-Flight Cancellation & Abort Signals       │
│  Phase 5: Two-Tier Blob Claim-Check Vault              │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
           Q2 2026: Multi-Tenancy & Operations
┌────────────────────────────────────────────────────────┐
│  Phase 4: Hierarchical Multi-Tenant SSE Scoping        │
│  Phase 6: Long-Lived Business Idempotency & Case Locks │
│  Phase 10: Multi-Tenant Fair Queuing (Noisy Neighbors) │
│  Phase 11: Vault Lifecycle, Retention & Cold Archival  │
│  Phase 12: W3C Distributed Tracing (OpenTelemetry)     │
│  Phase 7: Real-World Soak Benchmarks                   │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
           Q3 2026: Chassis-Grade HA
┌────────────────────────────────────────────────────────┐
│  Phase 3: Multi-Broker Partition Clustering            │
│           & Distributed Raft Consensus                 │
└────────────────────────────────────────────────────────┘
```

