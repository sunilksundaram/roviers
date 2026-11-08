# Roviers Sizing, Capacity Planning & Benchmark Realism Guide

## 1. Executive Summary: The Dual-Throughput Reality

When deploying agentic systems in production, engineering teams frequently fall into the **"Ingress Illusion" trap**: assuming that because an event broker can ingest 100,000+ requests per second, the overall multi-agent system can sustain 100,000 operations per second.

In reality, modern autonomous agents are strictly bottlenecked by **downstream dependencies**:
1. **Large Language Models (LLMs)**: High latency (500ms – 5,000ms per turn), token rate limits (TPM), and request rate limits (RPM).
2. **Databases (RDBMS & Lakehouses)**: Strict connection pool limits (typically 20 – 100 max connections), query lock contention, and disk IOPS.
3. **Interactive Human-In-The-Loop (HITL)**: Human reaction times (minutes, hours, or days) during multi-step review or clarification turns.
4. **External Services & Mainframes**: Fragile legacy endpoints subject to transient timeouts, rate limits, and outages.

**Roviers** is engineered specifically to bridge this gap: it serves as an **ultra-high-speed shock absorber** that ingests bursts at wire speed into durable append-only storage, issues instantaneous cryptographic claim-check receipts, and meters work out to downstream execution workers strictly within downstream capacity limits.

---

## 2. Mathematical Formulation of Agent Capacity

The end-to-end system throughput of any agent deployment can be rigorously modeled as:

$$T_{\text{e2e}} = \min\left( T_{\text{ingress}}, \; T_{\text{execution}} \right)$$

Where:
- **$T_{\text{ingress}}$**: Broker wire ingestion throughput ($> 100,000 \text{ req/sec}$ per multi-broker node using mmap commit logs and sub-microsecond Ed25519 verification).
- **$T_{\text{execution}}$**: Downstream service execution capacity, governed by:

$$T_{\text{execution}} = \min\left( \frac{N_{\text{workers}} \times C_{\text{inflight}}}{L_{\text{service}}}, \; Q_{\text{provider}} \right)$$

- $N_{\text{workers}}$: Number of active worker processes or pods.
- $C_{\text{inflight}}$: Maximum concurrent tasks leased per worker.
- $L_{\text{service}}$: Round-trip execution latency of the downstream dependency (in seconds).
- $Q_{\text{provider}}$: Provider-enforced rate ceiling (e.g., Azure OpenAI TPM/RPM quotas).

### Key Takeaway
Because $T_{\text{ingress}} \gg T_{\text{execution}}$, **sizing your cluster is determined by your downstream service limits, not the broker**. Roviers ensures that when $T_{\text{ingress}} > T_{\text{execution}}$, excess requests are safely absorbed and queued in durable disk storage rather than crashing downstream services or dropping client connections.

---

## 3. Real-World Benchmark Scenarios

### Scenario A: Generative AI & LLM Inference Bottlenecks

```
[Burst Traffic] 2,000 Agents ──► Roviers Ingress (Ingested in 20ms @ 100k/s)
                                       │
                         Append-Only Disk Queue (Zero dropped sockets)
                                       │
                      Worker Capacity Leases (Max 15 concurrent turns)
                                       │
                         Azure OpenAI GPT-4o (Strict RPM/TPM limit)
                         Turns: 50 turns/sec | Latency: 1.2s per turn
```

* **Premise**: 2,000 autonomous agents trigger complex chain-of-thought (CoT) reasoning tasks simultaneously.
* **Downstream Constraints**:
  - LLM latency: $1.2\text{ seconds}$ per reasoning turn.
  - LLM concurrency ceiling: $20\text{ concurrent requests}$ (Azure OpenAI quota: 60,000 TPM / 500 RPM).
* **Without Roviers**:
  - 2,000 concurrent HTTP requests hit the LLM API directly.
  - 1,980 requests fail with `HTTP 429 Too Many Requests`.
  - Client retry storms amplify the stampede, causing complete cluster outage.
* **With Roviers**:
  - All 2,000 requests are accepted by Roviers in **18 milliseconds** ($> 110,000 \text{ req/sec}$).
  - Clients receive an Ed25519 Claim-Check Receipt (`HTTP 202 Accepted`) and release connection sockets.
  - Workers use `POST /api/v1/workers/lease` to draw batches of 5 tasks strictly matching the 20-inflight ceiling.
  - Zero 429 errors; clients listen over lightweight Server-Sent Events (SSE) for completion signals.

---

### Scenario B: Relational Database & Warehouse Query Saturation

```
[Bursty Reports] 500 Analysts ──► Roviers Ingress (Ingested in 4ms)
                                       │
                         Append-Only Queue (Shock Absorber)
                                       │
                      Worker Pool Gate (Max 5 DB connections leased)
                                       │
                         PostgreSQL Connection Pool (Max 5 conn)
                         Execution: 40ms per analytical aggregate
```

* **Premise**: 500 users or automated reporting bots trigger heavy quarterly P&L aggregations.
* **Downstream Constraints**:
  - Target PostgreSQL connection pool is sized to **5 connections** to prevent buffer pool thrashing and OOM kills.
  - Query execution latency: $40\text{ms}$.
* **Without Roviers**:
  - 500 simultaneous connection attempts flood PostgreSQL.
  - Database rejects connections with `FATAL: remaining connection slots are reserved`.
  - Connection pool deadlocks and cascading timeouts crash the reporting service.
* **With Roviers**:
  - All 500 queries are committed to Roviers ledger in **4.2 milliseconds**.
  - A dedicated worker pool leases tasks strictly in batches of 5, matching the available database connections.
  - Peak active DB connections are mathematically capped at 5 at all times.
  - Entire batch completes in $4.24\text{ seconds}$ ($~118\text{ queries/sec}$) with $100\%$ success rate.

---

### Scenario C: Interactive Human-In-The-Loop (HITL) Multi-Turn Sessions

```
[Turn 1] User A ──► Roviers ──► Worker 1 picks task
                                   │
                              Agent parks task: "Upload 2025 W-2"
                                   │
[Parked State] Receipt status = ParkedAwaitingInput (State in Vault)
                                   │
              (10 minutes elapse while human retrieves PDF)
                                   │
[Turn 2] User A ──► Gateway ──► Worker 2 resumes with SignedResumeToken
                                   │
                         Outcome fulfilled in Vault!
```

* **Premise**: 1,000 mortgage loan applications require human underwriting input mid-workflow.
* **Failure Mode in Traditional Brokers**:
  - Worker 1 asks a question and blocks waiting for user response.
  - 1,000 open threads hold memory and socket connections.
  - User submits answer 10 minutes later; load balancer routes to Worker 2.
  - Worker 2 has no memory of User A -> **`404 No Waiter Found` / Broken Session**.
* **With Roviers Session Rendezvous**:
  - Worker 1 calls `POST /api/v1/receipts/{id}/park`, releasing execution threads.
  - Roviers issues a cryptographically signed `ResumeToken` containing `{ session_id, turn_id, checkpoint_uri, schema }`.
  - Receipt status transitions to `ParkedAwaitingInput`; state is persisted in the vault.
  - User submits answer to `POST /api/v1/sessions/{id}/resume`.
  - Any worker (Worker 2, 3, or N) resumes the session statelessly from the checkpoint.
  - Zero lost waiters; zero persistent worker thread consumption during human idle time.

---

### Scenario D: Legacy Mainframe Outage & Dynamic Backpressure Shock Absorption

```
Downstream Mainframe (Times out / 503 Outage)
      │
Worker detects outage ──► POST /services/wire_clearing/backoff
                               │
                Roviers Dynamic Circuit Breaker
      ┌────────────────────────┴────────────────────────┐
      │                                                 │
[Normal Ingestion Continues]              [Dynamic ETA Adjustment]
Ingests requests at wire speed            Receipts stamped with +45s ETA
Persists durably to disk queue            Worker dispatch paused
```

* **Premise**: Core banking wire transfer mainframe experiences a 30-second connection timeout / 503 incident.
* **Without Roviers**:
  - Upstream microservices keep hammering the mainframe.
  - Timeout queues back up, crashing upstream API gateways.
* **With Roviers Adaptive Backpressure**:
  - Worker detects error and trips `POST /api/v1/services/wire_clearing/backoff` (`duration_secs: 45, concurrency_cap: 2`).
  - Roviers engages dynamic circuit-breaker.
  - New incoming requests continue to be accepted at wire speed without dropping sockets.
  - Issued receipts automatically reflect adjusted ETA: $\text{ETA} = \text{Base Latency} + \text{Remaining Backoff}$.
  - When the mainframe recovers, `POST /api/v1/services/wire_clearing/resume` is called, and normal worker dispatch resumes automatically.

---

## 4. Hardware Sizing & Capacity Recommendation Matrix

| Concurrent Active Agents | Target Ingress Throughput | Typical LLM/DB Execution Capacity | Recommended Roviers Setup | Memory Footprint | Disk Storage (SSD) |
|---|---|---|---|---|---|
| **1,000 Agents** | 2,000 req/s | 20 – 50 turns/s | 1 Node (2 vCPU, 4GB RAM) | ~150 MB | 50 GB NVMe |
| **10,000 Agents** | 15,000 req/s | 100 – 300 turns/s | 2 Nodes HA (4 vCPU, 8GB RAM) | ~400 MB | 250 GB NVMe |
| **100,000 Agents** | 100,000+ req/s | 500 – 2,000 turns/s | 3–5 Node Cluster (8 vCPU, 16GB) | ~1.2 GB | 1 TB NVMe |

### Memory & Disk Sizing Rules of Thumb:
1. **Memory**: Roviers requires only **~120 bytes per in-memory indexed receipt**. 1,000,000 active in-flight receipts consume less than **150 MB of RAM**.
2. **Disk I/O**: Append-only commit log writes sequentially at disk hardware saturation (>500 MB/s on modern NVMe drives).
3. **Payload Threshold**: Payloads $< 64\text{ KB}$ are stored inline in the encrypted outcome vault; payloads $> 64\text{ KB}$ (PDFs, images) use the Two-Tier Blob Vault with presigned URLs to prevent disk bloat.

---

## 5. Architectural Comparison: Where Roviers Fits

| Feature | Message Queues (Kafka / RabbitMQ) | Workflow Engines (Temporal / Cadence) | Roviers Broker & Vault |
|---|---|---|---|
| **Ingress Latency** | Sub-millisecond | 20ms – 100ms | **Sub-microsecond (< 150µs)** |
| **Client Interaction** | Fire-and-forget (No receipt) | Stateful RPC | **Cryptographic Ed25519 Claim-Check Receipt** |
| **Real-Time Client Notification** | External WebSocket service needed | Polling or WebSocket gateway | **Native Zero-Polling SSE Fan-Out Streams** |
| **Human-In-The-Loop (HITL)** | Custom state database required | Built-in (heavy DB persistence) | **Native Stateful `SignedResumeToken` Rendezvous** |
| **Service Backpressure** | Consumer rate limiting only | Activity retry backoff | **Dynamic Set/Unset Backoff + Auto-Adjusting ETAs** |
| **Infrastructure Overhead** | ZooKeeper/KRaft, JVM, Storage | PostgreSQL/Cassandra, Elastic | **Zero external dependencies (Single static binary)** |
| **Primary Use Case** | Streaming data pipelines | Long-running multi-day workflows | **Ultra-low-latency agentic shock absorber & claim-check fabric** |

---

## 6. How to Run the Realistic Soak Benchmark Suite

To run the full suite of real-world scenarios on your own hardware:

```bash
# Run the end-to-end multi-scenario soak benchmark:
cargo run --release --example tiered_soak_benchmark -p roviers-core

# Run Criterion micro-benchmarks for token verification, intake, and worker leases:
cargo bench -p roviers-core
```

