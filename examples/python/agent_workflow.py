"""
Autonomous AI Agent Example using Roviers Broker & Claim-Check Vault.

Demonstrates:
1. Connecting to the Broker.
2. Issuing an Agent Identity Token.
3. Submitting an asynchronous heavy research task (returns in < 5ms with Receipt Token).
4. Waiting for real-time SSE signal without polling.
5. Redeeming the final verified outcome from the Broker Vault.
"""

import asyncio
import json
import os
import sys

# Add clients/python to sys.path if running directly from repo root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../clients/python")))

from roviers import RoviersBrokerClient


async def run_autonomous_agent():
    print("🤖 [Agent] Initializing autonomous research agent...")
    broker = RoviersBrokerClient(base_url="http://127.0.0.1:8080")

    # 1. Issue an Agent Credential from the Broker
    token_path = "sample_agent_token.json"
    print("🔑 [Agent] Requesting identity credential from Broker...")
    await broker.issue_agent_token(
        agent_id="AGENT-AUTONOMOUS-01",
        agent_name="Autonomous Research Agent",
        validity_days=30,
        save_path=token_path,
    )
    print(f"✅ [Agent] Identity token saved to {token_path} (ID: {broker.agent_id})")

    # 2. Submit a heavy reasoning task to the Broker
    print("\n🚀 [Agent] Submitting heavy analysis request to Broker...")
    task_payload = {
        "query": "Synthesize macroeconomic shift in autonomous agent infrastructure",
        "parameters": {"depth": "comprehensive", "max_iterations": 10},
    }

    receipt = await broker.submit(
        service_name="service.ai.deep_analysis",
        payload=task_payload,
    )

    print(f"🎟️ [Agent] Claim-Check Receipt received in ~2ms!")
    print(f"   Receipt ID:   {receipt.receipt_id}")
    print(f"   Status:       {receipt.status}")
    print(f"   Est. Ready:   {receipt.estimated_ready_secs}s")

    # 3. Connection is closed! Agent is free to do other work without blocking
    print("\n🧠 [Agent] Connection closed. Agent free to process other tasks without socket exhaustion...")

    # 4. Zero-polling wait for completion signal over SSE
    print("👂 [Agent] Awaiting real-time completion signal from Broker...")
    outcome = await broker.wait_for_outcome(receipt.receipt_id, timeout_secs=30.0)

    # 5. Outcome collected from Broker Vault
    print("\n🏆 [Agent] Outcome Retrieved from Broker Vault:")
    print(f"   Completed At (Unix): {outcome.completed_at}")
    print("   Result Data:")
    print(json.dumps(outcome.result, indent=2))

    # Clean up token file
    if os.path.exists(token_path):
        os.remove(token_path)
    print("\n✨ Autonomous agent workflow completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_autonomous_agent())

