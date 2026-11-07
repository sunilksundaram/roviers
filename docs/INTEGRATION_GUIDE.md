# Roviers Broker & Agent Integration Guide 🧩

This guide provides end-to-end integration patterns and copy-pasteable code samples for **Autonomous AI Agents**, **RAG Pipelines**, and **Web Applications** using the **Broker & Agent** architecture.

---

## 📋 Table of Contents
1. [Core Broker & Agent Interaction Pattern](#1-core-broker--agent-interaction-pattern)
2. [Pattern A: AI Agent & Multi-Agent Orchestrators (Python)](#2-pattern-a-ai-agent--multi-agent-orchestrators-python)
3. [Pattern B: RAG Ingestion & Vector Indexing Pipeline (Python)](#3-pattern-b-rag-ingestion--vector-indexing-pipeline-python)
4. [Pattern C: Standard Web Applications & Microservices (Node.js / TypeScript)](#4-pattern-c-standard-web-applications--microservices-nodejs--typescript)
5. [Agent Token Issuance & Management](#5-agent-token-issuance--management)

---

## 1. Core Broker & Agent Interaction Pattern

Every integration with Roviers follows three decoupled steps:

```text
[Step 1: Submit]       Agent   ──POST /api/v1/services/{name}──►  Roviers Broker
                       Agent   ◄── HTTP 202 + Claim-Check Receipt ──  (Connection closes in ~2ms!)

[Step 2: Signal]       Broker  ── SSE AgentSignal / Webhook ───►  Agent Listener
                       ("Receipt #UUID completed and deposited in Broker Vault")

[Step 3: Redeem]       Agent   ──POST /api/v1/receipts/{id}/pickup──► Roviers Vault
                       Agent   ◄── HTTP 200 + Final Verified Outcome ──
```

---

## 2. Pattern A: AI Agent & Multi-Agent Orchestrators (Python)

### Use Case
An autonomous LLM agent or multi-agent supervisor (e.g. LangGraph, CrewAI, AutoGen) needs to delegate an expensive, long-running operation—such as multi-step web browsing, financial analysis, or sandboxed code compilation—without stalling its event loop or blowing past gateway timeouts.

### Python Implementation (`agent_roviers_client.py`)

```python
import asyncio
import json
import httpx
from typing import Any, Dict

ROVIERS_BASE_URL = "http://127.0.0.1:8080"

class RoviersAgentClient:
    def __init__(self, agent_id: str, token_path: str, base_url: str = ROVIERS_BASE_URL):
        self.agent_id = agent_id
        self.base_url = base_url
        with open(token_path, "r") as f:
            self.token_data = json.load(f)

    async def dispatch_heavy_task(self, service_topic: str, payload: Dict[str, Any]) -> str:
        """
        Dispatches a heavy tool or reasoning task.
        Returns the Claim-Check Receipt ID in < 5ms.
        """
        async with httpx.AsyncClient() as client:
            request_body = {
                "request_id": None, # Generated server-side if omitted
                "service_name": service_topic,
                "agent_token": self.token_data,
                "payload": payload,
                "callback_url": None
            }

            resp = await client.post(
                f"{self.base_url}/api/v1/services/{service_topic}",
                json=request_body,
                timeout=5.0
            )
            resp.raise_for_status()
            receipt = resp.json()
            receipt_id = receipt["receipt_id"]
            print(f"🤖 [Agent] Dispatched {service_topic}. Claim-Check Receipt: #{receipt_id}")
            return receipt_id

    async def redeem_outcome(self, receipt_id: str) -> Dict[str, Any]:
        """
        Presents the Receipt ID + Agent Identity to collect the final verified outcome from the Vault.
        """
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/pickup",
                json={"agent_token": self.token_data},
                timeout=10.0
            )
            if resp.status_code == 202:
                raise RuntimeError("Task still in progress in background.")
            resp.raise_for_status()
            outcome = resp.json()
            return outcome["result"]

    async def listen_for_completion(self, expected_receipt_id: str, timeout_secs: int = 60) -> Dict[str, Any]:
        """
        Zero-polling listener over SSE. Wakes up the agent the moment the vault has the outcome.
        """
        print(f"👂 [Agent] Listening for signal on SSE for #{expected_receipt_id}...")
        url = f"{self.base_url}/api/v1/agents/{self.agent_id}/stream"

        async with httpx.AsyncClient(timeout=timeout_secs) as client:
            async with client.stream("GET", url) as stream:
                async for line in stream.aiter_lines():
                    if line.startswith("data:"):
                        event_data = json.loads(line[5:].strip())
                        if event_data.get("receipt_id") == expected_receipt_id:
                            print(f"⚡ [Agent] Signal received! Task ready for pickup.")
                            # Step 3: Redeem verified outcome from the Vault
                            return await self.redeem_outcome(expected_receipt_id)

        raise TimeoutError("Timed out waiting for task signal.")

# -----------------
# Example Execution
# -----------------
async def main():
    agent = RoviersAgentClient(
        agent_id="AGENT-RESEARCHER-01",
        token_path="agent_id.json"
    )

    # 1. Non-blocking dispatch
    receipt_id = await agent.dispatch_heavy_task(
        service_topic="service.llm.deep_analysis",
        payload={
            "query": "Synthesize market impact of quantum encryption on banking",
            "iterations": 5
        }
    )

    # 2. Agent can immediately perform other tasks, coordinate other agents, etc.
    print("🧠 [Agent] Free to perform other reasoning while task runs asynchronously...")

    # 3. Await intimation and redeem outcome from vault
    result = await agent.listen_for_completion(receipt_id)
    print("🏆 [Agent] Final Result Retrieved from Vault:")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 3. Pattern B: RAG Ingestion & Vector Indexing Pipeline (Python)

### Use Case
A Retrieval-Augmented Generation (RAG) system ingests large PDF documents, extracts tables, generates vector embeddings, and builds indexes. In traditional architectures, large uploads either block API servers or fail midway due to network timeouts.

With Roviers, the ingestion job is handed off to an append-only ledger topic. The client receives a receipt immediately, while dedicated GPU embedding workers drain tasks from the topic trie.

### FastAPI Document Ingestion Endpoint (`rag_service.py`)

```python
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
import httpx
import uuid
import json

app = FastAPI(title="RAG Ingestion Gateway")
ROVIERS_URL = "http://127.0.0.1:8080"

# Load server/service account identity
with open("rag_service_token.json") as f:
    RAG_SERVICE_TOKEN = json.load(f)

@app.post("/api/documents/ingest")
async def ingest_document(file: UploadFile = File(...)):
    """
    Accepts file, generates request, returns Claim-Check Receipt to user immediately.
    """
    file_bytes = await file.read()
    request_id = str(uuid.uuid4())

    # Forward ingestion request to Roviers Ledger
    async with httpx.AsyncClient() as client:
        payload = {
            "filename": file.filename,
            "size_bytes": len(file_bytes),
            "storage_uri": f"s3://rag-raw-docs/{request_id}_{file.filename}"
        }

        resp = await client.post(
            f"{ROVIERS_URL}/api/v1/services/service.rag.document_index",
            json={
                "service_name": "service.rag.document_index",
                "agent_token": RAG_SERVICE_TOKEN,
                "payload": payload,
                "callback_url": None
            }
        )
        if resp.status_code != 202:
            raise HTTPException(status_code=500, detail="Failed to enqueue to Roviers")

        receipt = resp.json()

    # Hand 202 Accepted + Receipt Token back to frontend in < 10ms
    return {
        "status": "Accepted",
        "message": "Document queued for chunking and embedding",
        "receipt_id": receipt["receipt_id"],
        "estimated_completion_secs": 15
    }

@app.get("/api/documents/status/{receipt_id}")
async def check_index_status(receipt_id: str):
    """
    Client polls or checks vault when ready.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{ROVIERS_URL}/api/v1/receipts/{receipt_id}/pickup",
            json={"agent_token": RAG_SERVICE_TOKEN}
        )

        if resp.status_code == 202:
            return {"status": "Processing", "receipt_id": receipt_id}
        elif resp.status_code == 200:
            outcome = resp.json()
            return {
                "status": "Ready",
                "receipt_id": receipt_id,
                "index_stats": outcome["result"]
            }
        else:
            raise HTTPException(status_code=404, detail="Receipt not found")
```

---

## 4. Pattern C: Standard Web Applications & Microservices (Node.js / TypeScript)

### Use Case
A Next.js, Express, or Fastify web application generates monthly CSV financial reconciliations or heavy PDF exports. Web hosting providers (e.g. Vercel, Cloudflare, Heroku) strictly enforce 15–30 second request timeouts. Roviers decouples this cleanly.

### TypeScript / Express Implementation (`web_app_integration.ts`)

```typescript
import express, { Request, Response } from 'express';
import axios from 'axios';
import fs from 'fs';
import EventSource from 'eventsource';

const app = express();
app.use(express.json());

const ROVIERS_URL = process.env.ROVIERS_URL || 'http://127.0.0.1:8080';
const WEBAPP_TOKEN = JSON.parse(fs.readFileSync('webapp_token.json', 'utf-8'));

/**
 * 1. User clicks "Generate Monthly Report"
 * Connection to Roviers is non-blocking (takes ~2ms)
 */
app.post('/api/reports/generate', async (req: Request, res: Response) => {
  try {
    const { month, year, accountId } = req.body;

    const roviersResponse = await axios.post(
      `${ROVIERS_URL}/api/v1/services/service.reporting.financial_export`,
      {
        service_name: 'service.reporting.financial_export',
        agent_token: WEBAPP_TOKEN,
        payload: { month, year, accountId },
        callback_url: null
      }
    );

    const receipt = roviersResponse.data;

    // Immediately return HTTP 202 to the browser with Receipt ID
    return res.status(202).json({
      success: true,
      message: 'Report generation started asynchronously.',
      receiptId: receipt.receipt_id,
      pickupUrl: `/api/reports/download/${receipt.receipt_id}`
    });
  } catch (err: any) {
    console.error('Error dispatching to Roviers:', err.message);
    return res.status(500).json({ error: 'Failed to queue report' });
  }
});

/**
 * 2. Real-Time Browser Notification Proxy (SSE)
 * Pipes Roviers signals directly to client browser
 */
app.get('/api/reports/notifications/:agentId', (req: Request, res: Response) => {
  const { agentId } = req.params;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const eventSource = new EventSource(`${ROVIERS_URL}/api/v1/agents/${agentId}/stream`);

  eventSource.onmessage = (event) => {
    res.write(`data: ${event.data}\n\n`);
  };

  req.on('close', () => {
    eventSource.close();
  });
});

/**
 * 3. Outcome Pickup & Download from Vault
 */
app.get('/api/reports/download/:receiptId', async (req: Request, res: Response) => {
  try {
    const { receiptId } = req.params;

    const vaultResponse = await axios.post(
      `${ROVIERS_URL}/api/v1/receipts/${receiptId}/pickup`,
      {
        agent_token: WEBAPP_TOKEN
      }
    );

    const outcome = vaultResponse.data;
    return res.status(200).json({
      status: 'Completed',
      downloadUrl: outcome.result.download_url,
      generatedAt: outcome.completed_at
    });
  } catch (err: any) {
    if (err.response?.status === 202) {
      return res.status(202).json({ status: 'Still Processing' });
    }
    return res.status(404).json({ error: 'Receipt not found or expired' });
  }
});

app.listen(3000, () => {
  console.log('🚀 Web application listening on http://localhost:3000');
});
```

---

## 5. Agent Token Issuance & Management

Agents authenticate using an Ed25519-signed identity credential issued by the Roviers Broker.

### Issuing an Agent Token via CLI
```bash
cargo run -p roviers-cli -- issue-agent \
  --id "AGENT-RESEARCH-01" \
  --name "Autonomous Research Agent" \
  --validity-days 365 \
  --out agent_token.json
```

### Issuing an Agent Token via HTTP API
```bash
curl -X POST http://127.0.0.1:8080/api/v1/auth/agent \
  -H "Content-Type: application/json" \
  -d '{
    "agent_id": "AGENT-WEBAPP-01",
    "agent_name": "Production Web Agent",
    "validity_days": 365
  }' > webapp_token.json
```

The resulting JSON token file can be mounted into container environments or stored securely in secret managers (AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager) and passed with every request.

