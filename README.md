# Roviers 🏛️

> **High-Throughput Task Broker & Claim-Check Vault Engine in Rust**  
> *Stateless Agent Authentication • Memory-Mapped Append-Only Ledger • Claim-Check Receipts • Persistent Outcome Vault • Zero-Polling Real-Time Signals & Webhooks*

---

## 💡 Overview

Modern distributed environments—especially those orchestrating **AI agents, LLM pipelines, autonomous workflows, microservices, and web clusters**—frequently face critical architectural bottlenecks:
- **Connection Exhaustion:** Holding synchronous HTTP/gRPC sockets open while waiting for long-running AI inference, multi-step agent reasoning, external APIs, or human-in-the-loop decisions causes gateway timeouts and thread starvation.
- **Aggressive Client Polling:** Hundreds or thousands of clients constantly hammering status endpoints degrades database performance and wastes network bandwidth.
- **Tight Coupling & Lost State:** Transient agent disconnections during execution often result in lost results or redundant, expensive re-executions.

**Roviers** solves this by decoupling request intake from service execution through a high-performance **Broker & Agent Claim-Check Architecture**:

```text
 ┌────────────────────────────────────────────────────────┐
 │                   DISTRIBUTED AGENTS                   │
 │   [AI Agents]    [LLM Orchestrators]   [App Servers]   │
 │   [Web Clusters] [Background Workers]  [RAG Pipelines] │
 └─────────────────────────┬──────────────────────────────┘
                           │
             1. Submit Task with Stateless Agent Token
             2. Receive Instant Claim-Check Receipt (~2ms)
                           ▼
 ┌────────────────────────────────────────────────────────┐
 │                 ROVIERS BROKER INGRESS                 │
 │   • Sub-microsecond Ed25519 Stateless Verification    │
 │   • Sliding-Window Idempotency Deduplication           │
 │   • Fast Append to MMap Ledger (memmap2 + CRC32 + LZ4) │
 │   • Non-Blocking HTTP 202 with Receipt Token           │
 └─────────────┬────────────────────────────▲─────────────┘
               │                            │
   3. Topic Trie Dispatch          6. Authenticated Outcome Pickup
               │                      (Present Token + Receipt)
               ▼                            │
 ┌───────────────────────────┐   ┌──────────┴─────────────┐
 │       WORKER MESH         │   │      BROKER VAULT      │
 │  • LLM & Vision Workers   │   │  • Persistent Outcomes │
 │  • App & Web Services     │──►│  • Audit Trail         │
 │  • Human-in-the-Loop      │ 4.│  • Disk-Backed Storage │
 │  • Batch Processors       │ Store                      │
 └─────────────┬─────────────┘ Outcome                    │
               │                                          │
               └─────────► 5. Signal Bus (SSE / Webhook) ─┘
                           "Receipt #UUID ready in Vault"
```

---

## 🔑 Core Concepts

### 1. Broker & Agent Paradigm
- **Broker (`roviers-server`):** The central high-throughput coordination engine, immutable ledger host, and outcome vault.
- **Agent:** Any requesting entity or background processor—autonomous AI agents (LangGraph, CrewAI, AutoGen), web servers, or indexing workers.

### 2. Instant Claim-Check Receipts (Non-Blocking Ingress)
- Agents present an **Ed25519-signed agent credential** alongside their task payload.
- The Broker verifies identity **in sub-microseconds without database roundtrips**, enforces idempotency deduplication, commits the entry to an append-only memory-mapped log, and immediately returns an immutable **Claim-Check Receipt Token** (`UUIDv7`).
- The agent connection closes immediately (~2ms), completely eliminating socket holding and gateway timeouts.

### 3. Persistent Outcome Vault & Secure Retrieval
- Once workers complete processing (seconds, hours, or days later), the structured execution result is deposited into the **Broker Vault**.
- At any later point, the agent authenticates and presents the **Receipt Token** to fetch the verified outcome.
- Outcomes remain tamper-proof, crash-safe, and isolated to the authenticated agent.

### 4. Zero-Polling Signals & Active Webhooks
- Instead of hammering the server with polling loops, agents subscribe to an ultra-lightweight **Server-Sent Events (SSE)** signal stream (`/api/v1/agents/{id}/stream`).
- The moment the outcome lands in the vault, an **AgentSignal** is pushed to the agent.
- Tasks can also specify a `callback_url` for automatic, active webhook delivery.

### 5. High-Throughput Append-Only Engine
- Built with memory-mapped log segments (`memmap2`), hardware-accelerated integrity verification (`crc32fast`), zero-copy serialization, and real-time block compression (`lz4_flex`).

---

## 📦 Workspace Architecture

```
roviers/
├── Cargo.toml                  # Cargo Workspace Definition
├── Dockerfile                  # Multi-stage production container build
├── docker-compose.yml          # Containerized turnkey deployment
├── crates/
│   ├── roviers-proto/          # Cryptographic Agent Tokens, Receipts, Outcomes, Signals
│   ├── roviers-ledger/         # Memory-mapped append-only storage, Radix Topic Trie, Dedup log
│   ├── roviers-core/           # Engine coordinating Broker Ingress, Vault, Webhooks, and Signals
│   ├── roviers-server/         # Axum HTTP/SSE Broker Server & background worker pool
│   └── roviers-cli/            # CLI for issuing tokens, submitting tasks, listening, & pickup
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

### 3. Start the Roviers Broker (Local or Docker)

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
1️⃣ [AGENT TOKEN] Issued verified token for: Autonomous Research Agent (AGENT-2026-DEMO-01)
2️⃣ [RECEIPT TOKEN] Broker accepted request!
   Issued Receipt: #019318b2-5f60-7a0e-bc48-bf5b94fefdb9 (Status: Queued)
3️⃣ [ASYNC WORK] Worker pool processing in background... (Waiting for worker completion)
4️⃣ [OUTCOME PICKUP] Agent presented receipt and collected final outcome from Vault:
{
  "document_number": "DOC-A8F21B",
  "seal": "BROKER_VERIFIED",
  "status": "APPROVED",
  "details": {
    "document_id": "DOC-9912",
    "priority": "high"
  }
}

✨ Full end-to-end flow verified: non-blocking Broker ingress, async execution, and Vault redemption!
```

---

## 🛠️ Step-by-Step CLI Walkthrough

### Step 1: Issue a Stateless Agent Token
Generate an Ed25519-signed identity credential for an agent (AI agent, app server, or service account):
```bash
cargo run -p roviers-cli -- issue-agent \
  --id "AGENT-RESEARCHER-01" \
  --name "Autonomous Research Agent" \
  --validity-days 365 \
  --out agent_token.json
```

### Step 2: Subscribe to Real-Time Signals (Zero-Polling SSE)
Connect an event listener over SSE to get notified the instant tasks complete:
```bash
cargo run -p roviers-cli -- listen --agent-id "AGENT-RESEARCHER-01"
```

### Step 3: Dispatch Asynchronous Service Request
Submit a heavy or multi-step service request. The Broker validates the token, appends the request to the ledger, and returns a Receipt Token immediately (~2ms):
```bash
cargo run -p roviers-cli -- submit \
  --service "ai.inference.document_analysis" \
  --token-file agent_token.json \
  --data '{"model": "deep-reasoning", "document_id": "doc_99182"}'
```
*Response contains the unique Receipt UUID, e.g. `019318b2-5f60-7a0e-bc48-bf5b94fefdb9`.*

### Step 4: Worker Deposits Finished Outcome (Optional / Remote Worker)
External workers or AI execution nodes can complete tasks and deposit results into the Vault:
```bash
cargo run -p roviers-cli -- complete \
  --receipt "019318b2-5f60-7a0e-bc48-bf5b94fefdb9" \
  --data '{"summary": "Document successfully analyzed", "findings": 42}'
```

### Step 5: Retrieve Results from the Broker Vault
Whenever convenient (immediately upon receiving an SSE signal or hours later), retrieve the finished artifact from the secure storage vault:
```bash
cargo run -p roviers-cli -- pickup \
  --receipt "019318b2-5f60-7a0e-bc48-bf5b94fefdb9" \
  --token-file agent_token.json
```

---

## 💻 Official Client SDKs

Roviers includes first-party, lightweight SDKs designed for zero-polling async workflows:

### Python Client SDK (`clients/python`)
Designed for **AI Agents, LangGraph, CrewAI, AutoGen, and FastAPI**:
```python
from roviers import RoviersBrokerClient, RoviersTool

broker = RoviersBrokerClient(base_url="http://127.0.0.1:8080", token="agent_token.json")

# 1. Non-blocking dispatch (< 5ms)
receipt = await broker.submit("service.ai.deep_analysis", {"query": "Analyze market shift"})

# 2. Wait for zero-polling SSE signal and automatically redeem from Vault
outcome = await broker.wait_for_outcome(receipt.receipt_id)
print("Broker Vault Outcome:", outcome.result)
```

### TypeScript / Node.js Client SDK (`clients/typescript`)
Universal fetch-based client for **Next.js, Express, Fastify, and browser apps**:
```typescript
import { RoviersBrokerClient } from 'roviers';

const broker = new RoviersBrokerClient({ baseUrl: 'http://127.0.0.1:8080' });
await broker.issueAgentToken('AGENT-WEB-01', 'Production Web Agent');

// Submit asynchronous job and await vault completion via SSE
const receipt = await broker.submit('service.reports.monthly_reconcile', { month: '2026-11' });
const outcome = await broker.waitForOutcome(receipt.receipt_id);
console.log('Outcome from Vault:', outcome.result);
```

---

## 🌐 HTTP REST & SSE API Reference

| Method | Endpoint | Description |
|:-------|:---------|:------------|
| `GET`  | `/health` | Broker health & readiness check |
| `POST` | `/api/v1/auth/agent` | Issue an Ed25519-signed Agent identity token |
| `POST` | `/api/v1/services/{service_name}` | Agent submits task; returns HTTP 202 + Claim-Check Receipt |
| `GET`  | `/api/v1/receipts/{receipt_id}` | Inspect current receipt status (`Queued`, `Processing`, `ReadyForPickup`) |
| `POST` | `/api/v1/receipts/{receipt_id}/pickup` | Authenticate with agent token and retrieve outcome from Vault |
| `POST` | `/api/v1/receipts/{receipt_id}/complete` | Worker deposits completed execution result into Vault |
| `GET`  | `/api/v1/agents/{agent_id}/stream` | Stream real-time SSE completion signals filtered by agent identity |
| `GET`  | `/api/v1/agents/{agent_id}/receipts` | List all active receipts for an agent identity |

---

## 📚 In-Depth Guides & Documentation

Explore the comprehensive operational guides:

- 📖 **[Server Deployment & Operations Guide](docs/DEPLOYMENT_GUIDE.md)**: Production setup, systemd units, Docker & Docker Compose, Nginx/Caddy SSE reverse proxying, kernel sysctl tuning, and storage backup strategies.
- 📊 **[Benchmarks & Capacity Sizing Guide](docs/BENCHMARKS_AND_SIZING.md)**: Empirical throughput (150,000+ req/s), latency percentiles (p95 < 750 µs), 100,000-connection SSE streaming capacity on standard cloud VMs, and client socket duration reduction metrics.
- 🧩 **[Client Integration & User Guide](docs/INTEGRATION_GUIDE.md)**: Real-world sample implementations with copy-pasteable code for:
  - **AI Agents & Multi-Agent Orchestrators** (Python async agent loop)
  - **RAG Ingestion Pipelines** (FastAPI / LlamaIndex background vector indexing)
  - **Standard Web Applications** (Node.js / TypeScript Express report generator)
- 🗺️ **[Scale, Ops & Multi-Agent Architecture Roadmap (TODO)](docs/SCALE_AND_OPS_TODO.md)**: Production roadmap addressing multi-broker clustering, multi-tenant SSE scoping, interactive Human-In-The-Loop (HITL) session rendezvous, and LLM quota-aware rate limiting.

---

## 📄 License
MIT License. Created by Sunil K Sundaram.
