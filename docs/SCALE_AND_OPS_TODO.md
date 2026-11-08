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
| **GAP-6** | **Downstream Service Backpressure & Quota Stampedes** | Roviers accepts all requests up to broker network/disk I/O ceiling without awareness of downstream service speeds. | Downstream services (slow legacy ERPs, payment gateways, heavy DB analytical queries, sandboxed runtimes, or LLM APIs) get overwhelmed, leading to connection exhaustion, cascading 429/503 errors, and catastrophic collapse. | Generalized service backpressure engine: dynamic set/unset backoff controls (`POST /services/{name}/backoff`), per-service concurrency & rate caps, and durable queue holding in append-only storage. |
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

[FAILURE SCENARIO 2: Downstream Service Stampede & Slow Dependency Collapse]
5,000 Clients ──► Roviers (Ingests instantly @ 100k/s into memory/disk)
                       │
         50 Worker Nodes pull simultaneously without backpressure
                       │
    ┌──────────────────┴──────────────────────────────────────┐
    │                                                         │
    ▼                                                         ▼
Legacy Mainframe / ERP (Max 5 conn)              Payment / LLM API (Rate Limit 100 QPS)
💥 Connection pool exhausted!                      💥 Cascading HTTP 429/503 Rate Limited!
💥 30s connection timeouts!                        💥 Subsequent retries amplify the stampede!

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

- [x] **Stateful Session Turn Machine**:
  - Add `session_id`, `turn_id`, and `parent_receipt_id` to `roviers-proto::ServiceRequest`.
  - Introduce lifecycle states: `Dispatched`, `ParkedAwaitingInput`, `Resumed`, `Completed`, `TimedOut`.
- [x] **Resumption Tokens (`SignedResumeToken`)**:
  - When a worker pauses for human/agent input, it issues a signed `ResumeToken` containing:
    `{ session_id, turn_id, schema_for_answer, expiry, checkpoint_uri }`.
  - User/client POSTs answer to `/api/v1/sessions/{session_id}/resume` supplying the `ResumeToken`.
- [x] **Distributed Waiter Rendezvous**:
  - Support stateless checkpointing: Worker checkpoints agent memory (e.g. LangGraph state) to persistent storage upon parking; any worker can pick up the resumption turn.

### Phase 2: Generalized Service Backpressure, Queue Holding & Adaptive Backoff
*Goal: Protect slow, fragile, or rate-limited downstream dependencies (legacy mainframes, ERPs, payment gateways, heavy DBs, sandboxes, and LLMs) from worker stampedes.*

- [x] **Durable Queue Holding & Ingress Shock Absorber**:
  - Roviers continues ingesting client requests at wire speed (100k+ req/s), committing them sequentially to the append-only ledger on disk.
  - Clients immediately receive their HTTP 202 Claim-Check Receipt and release connection sockets.
  - When downstream services are slow or backed off, requests remain durably parked on disk without consuming active sockets, worker threads, or leaking memory.
- [x] **Dynamic Set / Unset Backoff Controls**:
  - **Set Backoff API**: `POST /api/v1/services/{service_name}/backoff`
  - **Unset Backoff / Resume API**: `POST /api/v1/services/{service_name}/resume`
  - **Service Status API**: `GET /api/v1/services/{service_name}/status`
- [x] **Dynamic Receipt ETA Adaptation**:
  - When a service is backed off, newly issued receipts automatically calculate adjusted `estimated_ready_secs = base_latency + remaining_backoff_duration`.
- [ ] **Per-Service Concurrency & Rate Caps**:
  - Configurable in `roviers.toml`:
    ```toml
    [services.legacy_erp_sync]
    max_inflight = 5              # Never open more than 5 concurrent worker connections
    rate_limit_per_sec = 2        # Max 2 calls/sec
    auto_backoff_on_503 = true
    backoff_duration_secs = 45

    [services.payment_settlement]
    max_inflight = 20
    rate_limit_per_sec = 50

    [services.heavy_warehouse_query]
    max_inflight = 2              # Prevent analytical queries from hammering DB
    ```

### Phase 3: Multi-Broker High Availability & Distributed Clustering
*Goal: Move from single-node `--data-dir` to chassis-grade high availability without split-brain.*

- [x] **Partitioned Log Architecture**:
  - Virtual topic partitions: `partition_id = hash(tenant_id, session_id, business_key) % num_partitions`.
  - Calculated deterministically in `calculate_partition` and stamped on every issued `Receipt` and `AgentSignal`.
- [x] **Cluster Node Topology & Heartbeat Discovery**:
  - Node metadata tracking via `ClusterNodeInfo { node_id, listen_addr, is_leader, assigned_partitions, last_heartbeat }`.
  - Registered peer broker nodes via `register_or_heartbeat_peer(PeerHeartbeatRequest)`.
  - Exposed HTTP endpoints: `GET /api/v1/cluster/nodes` and `POST /api/v1/cluster/heartbeat`.
- [x] **Read/Write Segregation for SSE**:
  - Ingress and case locking handled on leader; peer topology tracked across broker replicas with partition assignments.

### Phase 4: Multi-Tenant Identity & Scoped SSE Fan-Out
*Goal: Securely isolate 10,000+ end-users across tenants without stream leakage or connection exhaustion.*

- [x] **Hierarchical Tenant & Session Scoping**:
  - Added `tenant_id` to `AgentCredentials`, `ServiceRequest`, `Receipt`, and `AgentSignal`.
  - Added `session_id` routing across multi-turn signals.
- [x] **Scoped Cryptographic Tokens & Authorization**:
  - Extended `AgentCredentials` with `scopes` and `tenant_id`.
  - Added `issue_scoped_agent_token` and `verify_scope` with strict RBAC checking.
- [x] **Scoped SSE Streams**:
  - Added `/api/v1/tenants/{tenant_id}/stream` for tenant-isolated real-time signals.
  - Added `/api/v1/sessions/{session_id}/stream` for session-isolated interactive HITL signals.
  - Keeps streams clean and avoids noisy neighbors or cross-tenant leaks.

### Phase 5: Two-Tier Claim-Check Vault (Inlined vs External Blob Storage)
*Goal: Efficiently handle large payloads (PDFs, images, multi-MB context windows) without bloating disk.*

- [x] **Tiered Storage Threshold & Blob Reference Model**:
  - Implemented `BlobReference { blob_url, content_type, size_bytes, sha256_hash }`.
  - Added `ServiceOutcome::new_blob(...)` and `ServiceOutcome::new_inline(...)` constructors.
  - Payloads $\le$ 64 KB inline in encrypted JSON outcome vault; heavy binaries stored externally with cryptographic reference.
- [x] **Presigned Pickup Protocol & HTTP API Support**:
  - `POST /api/v1/receipts/{receipt_id}/complete` supports optional `blob_ref`.
  - Client retrieves binary directly from Cloud Storage, preserving broker bandwidth.
  - Verified in `server_e2e.rs`.

### Phase 6: Long-Lived Business Idempotency & Case Leasing
*Goal: Guarantee business rules (e.g. "only one active loan application per citizen session") over days/weeks.*

- [x] **Persistent Business Case Idempotency Registry**:
  - Added `business_key` field to `ServiceRequest`, `Receipt`, and `AgentSignal`.
  - In-memory and disk-persisted `business_case_locks: Arc<RwLock<HashMap<String, Uuid>>>`.
  - Persisted across engine restarts by scanning stored vault receipts.
- [x] **Stateful Mutex & Atomic Guard**:
  - Duplicate submissions with the same `business_key` while active (`Queued`, `Processing`, or `ParkedAwaitingInput`) are rejected immediately with `CoreError::BusinessCaseLocked(key, existing_receipt_id)` (HTTP 409 Conflict).
  - Automatically released upon terminal transition (`complete_service`, `cancel_receipt`, or DLQ exhaustion).
  - Explicit case inspection and unlocking API: `GET /api/v1/cases/{business_key}` and `DELETE /api/v1/cases/{business_key}`.

### Phase 7: Benchmark Realism & Sizing Documentation
*Goal: Set realistic expectations for engineering teams sizing infrastructure.*

- [x] **Document Dual Throughput Reality & Sizing**:
  - Published comprehensive sizing guide in `docs/SIZING_AND_CAPACITY.md`.
  - Detailed mathematical formulation: $T_{\text{e2e}} = \min(T_{\text{ingress}}, T_{\text{execution}})$.
  - Documented capacity sizing guidelines for 1,000, 10,000, and 100,000 concurrent agents.
- [x] **Tiered End-to-End Soak Test Suite (`tiered_soak_benchmark`)**:
  - **Scenario A (LLM Inference)**: 500 prompts under 15-inflight TPM rate limits with 40ms token generation latency; verified zero 429 rate-limit errors.
  - **Scenario B (Database Saturation)**: 300 analytical queries strictly serialized through a 5-connection PostgreSQL pool; verified zero pool exhaustion errors.
  - **Scenario C (HITL Session Rendezvous)**: 100 multi-turn loan applications parked with `SignedResumeToken` and resumed across worker threads; verified zero lost waiters.
  - **Scenario D (Mainframe Outage & Backpressure)**: Simulated 503 outage triggering dynamic backoff (+3s cooldown); verified wire-speed shock absorption and auto-adjusting receipt ETAs.
  - Added Criterion micro-benchmarks for worker capacity leasing and backpressure circuit-breaker lookup in `intake_benchmark.rs`.

### Phase 8: Worker Liveness, Lease Heartbeats & Dead Letter Queue (DLQ)
*Goal: Automatically recover from worker crashes and isolate poison pill payloads.*

- [x] **Worker Capacity Lease Protocol**:
  - Workers request leases with `POST /api/v1/workers/lease` up to capacity limit.
- [x] **Lease Keepalive & Heartbeat Protocol**:
  - Workers refresh lease periodically via `POST /api/v1/workers/heartbeat`.
- [x] **Dead Letter Queue (DLQ) & Max Retry Guard**:
  - Track `delivery_attempts` per task.
  - Automatic `reclaim_expired_leases()` reclaims orphaned tasks from crashed workers.
  - If a task causes $N \ge 3$ consecutive crashes or lease timeouts, immediately diverts to `DLQ`.
  - Exposed HTTP endpoints: `GET /api/v1/dlq` and `POST /api/v1/workers/reclaim`.
  - Notifies client over SSE with a deterministic failure signal.

### Phase 9: In-Flight Task Cancellation & Abort Propagation
*Goal: Stop expensive downstream LLM inference loops when users cancel or navigate away.*

- [x] **Cancellation API**:
  - Added `POST /api/v1/receipts/{receipt_id}/cancel` endpoint accepting signed agent credential.
  - Marks receipt status as `Cancelled` / `Failed`.
- [x] **Worker Abort Broadcast**:
  - Broadcasts cancellation `AgentSignal` over internal bus / SSE so active workers abort processing.

### Phase 10: Multi-Tenant Fair Queuing & Starvation Prevention
*Goal: Prevent batch workloads from degrading interactive real-time user turns.*

- [x] **Fair Interleaved Multi-Tenant Queuing**:
  - `tenant_id` field on `ServiceRequest` and `Receipt`.
  - Worker task dispatch groups pending receipts by `tenant_id` and round-robins across distinct tenants.
  - Prevents bulk batch submissions from starving interactive user turns.
  - Unit test verified in `test_multi_tenant_fair_scheduling`.
- [ ] **Priority Classes**:
  - Support explicit `Priority::Interactive` vs `Priority::Batch` weights in `ServiceRequest`.

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

