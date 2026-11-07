"""
External Worker Node Example for Roviers Broker Vault.

Demonstrates:
1. Connecting to the Broker.
2. Simulating task processing from a queue or external trigger.
3. Depositing the finished outcome into the Broker Vault via `complete()`.
"""

import asyncio
import os
import sys

# Add clients/python to sys.path if running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../clients/python")))

from roviers import RoviersBrokerClient


async def run_worker():
    print("👷 [Worker] Starting Roviers external worker daemon...")
    broker = RoviersBrokerClient(base_url="http://127.0.0.1:8080")

    # Example: Suppose a task was dispatched with receipt_id
    receipt_id = sys.argv[1] if len(sys.argv) > 1 else None

    if not receipt_id:
        print("💡 Usage: python worker_node.py <RECEIPT_UUID>")
        print("   Demonstrating sample task creation and worker completion...")

        # Setup an agent to create a sample receipt
        await broker.issue_agent_token("AGENT-WORKER-TEST", "Task Initiator")
        receipt = await broker.submit(
            service_name="service.rag.index",
            payload={"doc_name": "quarterly_filing.pdf", "chunks": 140},
        )
        receipt_id = receipt.receipt_id
        print(f"   Created test receipt: #{receipt_id}")

    print(f"\n⚙️ [Worker] Processing task for receipt #{receipt_id}...")
    await asyncio.sleep(1.0) # Simulate work

    result_payload = {
        "status": "INDEXED",
        "vector_embeddings": 140,
        "index_id": "idx_9921_vector_db",
        "summary": "Document successfully parsed and indexed in vector space.",
    }

    # Deposit outcome into the Broker Vault
    print(f"📦 [Worker] Depositing completed outcome into Broker Vault...")
    await broker.complete(receipt_id, result_payload)
    print("✅ [Worker] Outcome deposited into Vault! Signals and webhooks dispatched automatically.")


if __name__ == "__main__":
    asyncio.run(run_worker())
