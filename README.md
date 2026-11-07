# Roviers 🏛️

> **High-Throughput Asynchronous Service Intake & Claim-Check Vault Engine in Rust**  
> *Stateless Authentication • Memory-Mapped Append-Only Ledger • Claim-Check Receipts • Decoupled Outcome Vault • Zero-Polling Event Intimations*

---

## 💡 Overview

Modern distributed environments—especially those orchestrating **AI agents, LLM pipelines, autonomous workflows, microservices, and web/app clusters**—frequently face critical architectural bottlenecks:
- **Connection Exhaustion:** Holding synchronous HTTP/gRPC sockets open while waiting for long-running AI inference, complex multi-step reasoning, external APIs, or human-in-the-loop decisions causes gateway timeouts and thread starvation.
- **Aggressive Client Polling:** Hundreds or thousands of clients constantly spamming status endpoints degrades database performance and wastes network bandwidth.
- **Tight Coupling & Lost State:** Transient client disconnections during execution often result in lost results or redundant, expensive re-executions.

**Roviers** solves this by decoupling request intake from service execution through a high-performance **Claim-Check Receipt & Vault architecture**:

```
 ┌────────────────────────────────────────────────────────┐
 │                   CONCURRENT CLIENTS                   │
 │   [App Servers]  [Web Clusters]  [AI Orchestrators]    │
 │   [Autonomous AI Agents]         [Background Jobs]     │
 └─────────────────────────┬──────────────────────────────┘
                           │
             1. Submit Request with Stateless ID
             2. Receive Instant Claim-Check Receipt
                           ▼
 ┌────────────────────────────────────────────────────────┐
 │               ROVIERS INGRESS COUNTER                  │
 │   • Sub-microsecond Ed25519 Stateless Verification    │
 │   • Sliding-Window Idempotency Deduplication           │
 │   • Fast Append to MMap Ledger (memmap2 + CRC32 + LZ4) │
 │   • Non-Blocking HTTP 202 with Receipt Token           │
 └─────────────┬────────────────────────────▲─────────────┘
               │                            │
   3. Topic Trie Dispatch          6. Authenticated Outcome Pickup
               │                      (Present ID + Receipt)
               ▼                            │
 ┌───────────────────────────┐   ┌──────────┴─────────────┐
 │       WORKER MESH         │   │     OUTCOME VAULT      │
 │  • App / Web Services     │   │  • Persistent Results  │
 │  • LLM & Vision Agents    │──►│  • Audit Trail         │
 │  • Human-in-the-Loop      │ 4.│  • Secure Isolation    │
 │  • Batch Processors       │ Store                      │
 └─────────────┬─────────────┘ Outcome                    │
               │                                          │
               └─────────► 5. Intimation Bus (SSE) ───────┘
                           "Receipt #UUID ready"
```

---

## 🔑 Core Concepts

### 1. Many Concurrent Clients ➡️ Single or Many Services
- **Clients:** App servers, web applications, autonomous AI agents, multi-agent orchestrators (e.g., LangGraph, AutoGen, CrewAI), and enterprise systems submit service requests concurrently.
- **Services:** Downstream workers can be heterogeneous—a simple database lookup, a distributed map-reduce job, an autonomous research agent, or a slow human approval queue.

### 2. Instant Claim-Check Receipts (Non-Blocking Ingress)
- Clients present an **Ed25519-signed identity credential** alongside their service payload.
- The ingress verifies identity **in sub-microseconds without database roundtrips**, enforces idempotency deduplication, commits the entry to an append-only log, and immediately returns an immutable **Claim-Check Receipt Token** (`UUIDv7`).
- The client connection terminates immediately, completely eliminating socket holding and gateway timeouts.

### 3. Decoupled Outcome Vault & Secure Retrieval
- Once workers complete processing (seconds, hours, or days later), the structured execution result is deposited into the **Outcome Vault**.
- At any later point, the client (or an authorized downstream service) authenticates and presents the **Receipt Token** to fetch the result.
- Outcomes remain tamper-proof and isolated to the authenticated receipt owner.

### 4. Zero-Polling Intimation Bus
- Instead of hammering the server with polling loops, clients or orchestrators subscribe to an ultra-lightweight **Server-Sent Events (SSE)** intimation stream.
- The moment the outcome lands in the vault, an **Intimation Notice** is broadcast: *"Receipt #UUID is ready for pickup at `/api/v1/receipts/{id}/pickup`"*.

### 5. High-Throughput Append-Only Engine
- Built with memory-mapped segments (`memmap2`), hardware-accelerated integrity verification (`crc32fast`), zero-copy serialization, and real-time block compression (`lz4_flex`).

---

## 📦 Workspace Architecture

```
roviers/
├── Cargo.toml                  # Cargo Workspace Definition
├── Dockerfile                  # Multi-stage production container build
├── docker-compose.yml          # Containerized turnkey deployment
├── crates/
│   ├── roviers-proto/          # Cryptographic IDs, Claim-Check Receipts, Outcomes, Envelopes
│   ├── roviers-ledger/         # Memory-mapped append-only storage, Radix Topic Trie, Dedup log
│   ├── roviers-core/           # Engine coordinating Auth, Ingress, Vault, Webhooks, and Intimations
│   ├── roviers-server/         # Axum HTTP/SSE Service Counter & simulated background workers
│   └── roviers-cli/            # CLI for issuing IDs, submitting requests, & real-time pickup
├── clients/
│   ├── python/                 # Python SDK (httpx async client + LangGraph/CrewAI tool wrapper)
│   └── typescript/             # TypeScript SDK (universal fetch-based client for Node & Web)
└── docs/                       # Comprehensive Operations & Integration Guides
    ├── DEPLOYMENT_GUIDE.md     # Production deployment, systemd, Docker, Nginx SSE proxy
    ├── BENCHMARKS_AND_SIZING.md# Empirical throughput & latency sizing models
    └── INTEGRATION_GUIDE.md    # Real-world integration patterns & recipes
```

---

## 🚀 Quick Start

### 1. Build and Run Test Suite
```bash
cargo test
```

### 2. Run Criterion Performance Benchmarks
```bash
cargo bench -p roviers-core
```

### 3. Start the Server (Local or Docker)

**Local Cargo:**
```bash
cargo run -p roviers-server -- --bind 127.0.0.1:8080 --data-dir ./roviers_data
```

**Docker Compose:**
```bash
docker compose up -d
```

### 4. Run the Automated End-to-End Demo
In a separate terminal:
```bash
cargo run -p roviers-cli -- demo
```

**Demo Execution Output:**
```text
1️⃣ [CLIENT AUTH] Issued verified identity token for: Eleanor Vance (CTZ-2026-DEMO-01)
2️⃣ [CLAIM-CHECK] Ingress accepted request!
   Issued Receipt: #019318b2-5f60-7a0e-bc48-bf5b94fefdb9 (Status: Queued)
3️⃣ [ASYNC EXECUTION] Worker pool processing asynchronously in background...
4️⃣ [VAULT RETRIEVAL] Client presented Receipt + Auth and retrieved outcome from Vault:
{
  "document_number": "DOC-A8F21B",
  "seal": "GOV_OFFICIAL_VERIFIED",
  "status": "APPROVED",
  "details": {
    "applicant_notes": "Official certified copy"
  }
}

✨ End-to-end flow verified: non-blocking ingress, zero-polling intimation, and secure vault retrieval!
```

---

## 🛠️ Step-by-Step Walkthrough

### Step 1: Issue a Stateless Client Credential
Generate an Ed25519-signed identity credential for a client (app server, agent, or service account):
```bash
cargo run -p roviers-cli -- issue-citizen \
  --id "CLIENT-AGENT-007" \
  --name "Autonomous Research Agent" \
  --validity-days 365 \
  --out agent_id.json
```

### Step 2: Subscribe to Real-Time Intimations (Optional / Zero-Polling)
Connect an event listener over SSE to get notified the instant tasks complete:
```bash
cargo run -p roviers-cli -- listen --citizen-id "CLIENT-AGENT-007"
```

### Step 3: Dispatch Asynchronous Service Request
Submit a heavy or multi-step service request. Roviers validates the token, appends the request to the ledger, and returns a Receipt Token immediately:
```bash
cargo run -p roviers-cli -- submit \
  --service "ai.inference.document_analysis" \
  --token-file agent_id.json \
  --data '{"model": "deep-reasoning", "document_id": "doc_99182"}'
```
*Response contains the unique Receipt UUID, e.g. `019318b2-5f60-7a0e-bc48-bf5b94fefdb9`.*

### Step 4: Retrieve Results from the Vault
Whenever convenient (immediately upon receiving an intimation or hours later), retrieve the finished artifact from the secure storage vault:
```bash
cargo run -p roviers-cli -- pickup \
  --receipt "019318b2-5f60-7a0e-bc48-bf5b94fefdb9" \
  --token-file agent_id.json
```

---

## 💻 Official Client SDKs

Roviers includes first-party, lightweight SDKs designed for zero-polling async workflows:

### Python Client SDK (`clients/python`)
Designed for **AI Agents, LangGraph, CrewAI, AutoGen, and FastAPI**:
```python
from roviers import RoviersClient, RoviersTool

client = RoviersClient(base_url="http://127.0.0.1:8080", token="agent_token.json")

# 1. Non-blocking dispatch (< 5ms)
receipt = await client.submit("service.ai.deep_analysis", {"query": "Analyze market shift"})

# 2. Wait for zero-polling SSE intimation and automatically redeem from Vault
outcome = await client.wait_for_outcome(receipt.receipt_id)
print("Vault Outcome:", outcome.result)
```

### TypeScript / Node.js Client SDK (`clients/typescript`)
Universal fetch-based client for **Next.js, Express, Fastify, and browser apps**:
```typescript
import { RoviersClient } from 'roviers';

const client = new RoviersClient({ baseUrl: 'http://127.0.0.1:8080' });
await client.issueToken('CLIENT-WEB-01', 'Production Web Cluster');

// Submit asynchronous job and await vault completion via SSE
const receipt = await client.submit('service.reports.monthly_reconcile', { month: '2026-11' });
const outcome = await client.waitForOutcome(receipt.receipt_id);
console.log('Outcome:', outcome.result);
```

---

## 🌐 HTTP REST & SSE API Reference

| Method | Endpoint | Description |
|:-------|:---------|:------------|
| `GET`  | `/health` | Server liveness and readiness health check |
| `POST` | `/api/v1/auth/citizen` | Issue an Ed25519-signed client identity credential |
| `POST` | `/api/v1/services/{service_name}` | Submit service request; returns HTTP 202 + Claim-Check Receipt |
| `GET`  | `/api/v1/receipts/{receipt_id}` | Inspect current receipt status (`Queued`, `Processing`, `ReadyForPickup`) |
| `POST` | `/api/v1/receipts/{receipt_id}/pickup` | Authenticate with client token and retrieve final outcome from Vault |
| `GET`  | `/api/v1/citizens/{citizen_id}/stream` | Stream real-time SSE completion notices filtered by client identity |
| `GET`  | `/api/v1/citizens/{citizen_id}/receipts` | List all active receipts for a client identity |

---

## 📚 In-Depth Guides & Documentation

Explore the comprehensive operational guides:

- 📖 **[Server Deployment & Operations Guide](docs/DEPLOYMENT_GUIDE.md)**: Production setup, systemd units, Docker & Docker Compose, Nginx/Caddy SSE reverse proxying, kernel sysctl tuning, and storage backup strategies.
- 📊 **[Benchmarks & Capacity Sizing Guide](docs/BENCHMARKS_AND_SIZING.md)**: Empirical throughput (150,000+ req/s), latency percentiles (p95 < 750 µs), 100,000-connection SSE streaming capacity on standard cloud VMs, and client socket duration reduction metrics.
- 🧩 **[Client Integration & User Guide](docs/INTEGRATION_GUIDE.md)**: Real-world sample implementations with copy-pasteable code for:
  - **AI Agents & Multi-Agent Orchestrators** (Python async agent loop)
  - **RAG Ingestion Pipelines** (FastAPI / LlamaIndex background vector indexing)
  - **Standard Web Applications** (Node.js / TypeScript Express report generator)

---

## 📄 License
MIT License. Created by Sunil K Sundaram.

