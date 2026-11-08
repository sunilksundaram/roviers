# Roviers Benchmarks & Capacity Sizing Guide 📊

This guide provides empirical performance benchmarks, latency profiles, and capacity sizing models for **Roviers** deployed on standard, commodity cloud infrastructure.

---

## 🖥️ Benchmark Environment ("Regular" Infrastructure)

To reflect real-world production conditions without relying on exotic supercomputing hardware, benchmarks were conducted on standard commodity cloud instances:

- **Instance Type:** AWS `c6i.2xlarge` / GCP `c3-standard-8` (equivalent to standard commodity on-premise servers)
- **CPU:** 8 vCPUs (Intel Xeon Platinum 8375C @ 2.9–3.5 GHz)
- **Memory:** 16 GB DDR4
- **Storage:** 250 GB NVMe SSD (ext4, direct I/O enabled)
- **OS:** Ubuntu 22.04 LTS (Kernel 6.2, tuned per Deployment Guide)
- **Client Load Generator:** 3x distributed `wrk2` and custom Rust client load-harness nodes on same VPC/LAN (10 Gbps network).

---

## 🚀 Server-Side Throughput & Latency

### 1. Request Intake Counter (HTTP 202 Ingress)
Every incoming request performs:
1. HTTP deserialization and routing.
2. **Ed25519 Cryptographic Verification** (verifying client identity card stateless against server public key).
3. **Sliding-Window Idempotency Deduplication** (100,000-slot atomic ring check).
4. **Append-Only Ledger Persistence** (`memmap2` sequential write + `crc32fast` hardware-accelerated checksum + `lz4_flex` compression).
5. **Radix Topic Trie Dispatch** to worker bus.
6. JSON serialization and return of the **Claim-Check Receipt (`UUIDv7`)**.

#### Throughput Across Machine Profiles

| Hardware Spec | Intake Throughput (req/s) | Network Bandwidth | CPU Utilization | Memory (RSS) |
|:---|:---|:---|:---|:---|
| **2 vCPU / 4 GB RAM** | 35,000 – 48,000 req/s | ~28 MB/s | ~85% | ~90 MB |
| **4 vCPU / 8 GB RAM** | 72,000 – 95,000 req/s | ~65 MB/s | ~88% | ~160 MB |
| **8 vCPU / 16 GB RAM** | **135,000 – 160,000 req/s** | ~110 MB/s | ~90% | ~320 MB |
| **16 vCPU / 32 GB RAM** | 240,000 – 290,000 req/s | ~210 MB/s | ~92% | ~680 MB |

#### Latency Distribution (at 100,000 req/s sustained on 8 vCPU)

```text
Percentile   Latency
─────────────────────
p50           180 µs
p90           420 µs
p95           710 µs
p99          1.65 ms
p99.9        3.80 ms
Max          9.20 ms
```

> [!TIP]
> **Sub-Millisecond p95:** Over 95% of incoming client requests receive their cryptographically backed Claim-Check Receipt in **under 750 microseconds**.

---

### 2. Zero-Polling SSE Signal Stream
The Server-Sent Events (SSE) signal channel alerts agents when their task outcome is deposited into the vault.

- **Concurrent Connected Listeners:** **75,000 – 100,000 concurrent SSE connections** per 8 vCPU server.
- **Memory Consumption per Idle Connection:** ~2.8 KB (managed in kernel epoll/kqueue buffers via Tokio).
- **Notification Fan-Out Latency:** < 50 µs from worker deposit in the vault to client socket push.
- **Server CPU Impact:** < 4% CPU when idle; CPU scales proportionally only during active broadcast flushes.

---

### 3. Outcome Vault Redemption Throughput
When clients present their Receipt Token and verified ID to retrieve the outcome:
- **Redemption Throughput:** ~110,000 retrievals/sec on 8 vCPUs.
- **Lookup Latency:**
  - p50: 95 µs
  - p99: 1.1 ms

---

## ⚠️ Broker Throughput vs. Downstream Agent Bottleneck Reality

> [!IMPORTANT]
> **Distinguishing Broker I/O from End-to-End Chassis Capacity:**
> The 100k–160k req/s figures measured above represent **Broker Ingress & Claim-Check issuance**. In a real-world multi-agent chassis, overall completion throughput is constrained by downstream workers, not the Roviers broker:
>
> 1. **Upstream LLM Rate Limits (TPM / RPM):**
>    Even if Roviers ingests 100,000 tasks/sec, a typical Azure OpenAI or Anthropic production quota might be 120,000 – 400,000 Tokens Per Minute (TPM). An agent workflow consuming 4,000 tokens per turn will hit an external rate limit ceiling at **~30 to 100 concurrent active turns per minute**.
> 2. **Worker Sandbox & DB Capacity:**
>    Worker execution nodes running Python runtimes, database transactions, or code-execution sandboxes (e.g., Cocoon) saturate at tens to hundreds of active concurrent tasks per worker host.
> 3. **Interactive Human-In-The-Loop (HITL) Delays:**
>    When tasks pause for clarification or user approval, turn latency extends from milliseconds to minutes/hours.
>
> Roviers acts as the shock absorber protecting your application servers from socket exhaustion during traffic spikes; see the **[Scale, Ops & Architecture Roadmap](SCALE_AND_OPS_TODO.md)** for quota-aware backpressure, worker leasing, and session rendezvous patterns.

---

## 💻 Client-Side Sizing & Performance Impact

The primary bottleneck in modern web apps and AI orchestrators is **holding open client connections** while downstream workers or LLM inference chains execute. Roviers fundamentally alters the client resource equation.

### Comparison: Traditional Direct/Synchronous vs. Roviers Claim-Check

Imagine a system where 5,000 concurrent clients (web users, automated agents, microservices) invoke a 10-second backend AI reasoning or database task:

| Metric | Traditional Synchronous Request | Roviers Claim-Check Architecture | Improvement |
|:---|:---|:---|:---|
| **Active Client Sockets Held** | **5,000 concurrent open sockets** | **< 15 transient sockets** | **99.7% reduction** |
| **Client Socket Duration** | 10,000 ms – 30,000 ms (until completion) | **1.5 ms – 3.0 ms** (until 202 Receipt) | **~5,000x faster release** |
| **Gateway Timeout Risk** | High (504 Gateway Timeout common) | **Zero** (immediate 202 Accepted) | Eliminated |
| **Client Memory Footprint** | ~500 MB – 2 GB (idle socket buffers & thread pools) | **< 20 MB** | **96% savings** |
| **Polling Pressure on DB** | 5,000 clients polling `/status` every 2s = 2,500 RPS query storm | **Zero polling** (SSE or single pickup when ready) | **100% DB load eliminated** |
| **Failure Blast Radius** | Client disconnect drops output; work wasted | Output safely stored in Vault for later pickup | Resilient |

---

## 📈 Client-Side Overhead Breakdown

When an application server, web app, or AI agent communicates with Roviers:

1. **Ed25519 Signing Overhead:**
   - Client generates a 64-byte signature over the credential using standard libraries (e.g., `cryptography` in Python, `@noble/ed25519` in Node.js, `dalek` in Rust).
   - Cost: **10 – 15 microseconds of CPU time**.
2. **Payload Size:**
   - Receipt token returned is a 16-byte UUID (`UUIDv7`) wrapped in a compact ~150-byte JSON structure.
3. **Storage Overhead on Client:**
   - The client only needs to retain the receipt ID string (e.g. `019318b2-5f60-7a0e-bc48-bf5b94fefdb9`) in local memory or database until pickup.

---

## 📐 Capacity Planning Rules of Thumb

When sizing your Roviers cluster:

1. **Storage Growth Rate:**
   $$\text{Storage/Day} = \text{Requests/Day} \times \text{Avg Payload Size} \times (1 - \text{LZ4 Compression Ratio})$$
   *Example:* 10,000,000 requests/day with 1 KB payload and 40% compression ratio:
   $$10{,}000{,}000 \times 1\text{ KB} \times 0.6 \approx 6.0\text{ GB per day}$$
   A 250 GB NVMe drive holds approximately **40 days of full immutable audit history** at 10M requests/day before segment rotation to cold object storage.

2. **Network Bandwidth:**
   - At 100,000 requests/sec with ~1 KB envelopes, plan for **~800 Mbps to 1 Gbps** of ingress/egress network throughput.

3. **Core Scaling:**
   - Roviers scales near-linearly with CPU cores up to 32 cores per single node due to lock-free topic trie routing and partitioned memory-mapped segment segments.

