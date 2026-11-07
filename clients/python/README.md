# Roviers Python SDK 🐍

Asynchronous Service Intake & Claim-Check Vault Client for AI Agents, LLM Orchestrators (LangGraph, CrewAI, AutoGen), and distributed microservices.

## Installation

```bash
pip install ./clients/python
# Or install in editable mode for development:
pip install -e ./clients/python
```

## Quick Start

```python
import asyncio
from roviers import RoviersClient

async def main():
    # 1. Connect to Roviers Service Counter
    client = RoviersClient(base_url="http://127.0.0.1:8080")

    # 2. Issue or load client identity token
    await client.issue_token(
        citizen_id="AGENT-FINANCE-01",
        full_name="Autonomous Financial Analyst",
        save_path="agent_token.json"
    )

    # 3. Submit heavy asynchronous request (returns in ~2ms with Receipt Token)
    receipt = await client.submit(
        service_name="service.finance.risk_model",
        payload={"portfolio_id": "port_9921", "simulations": 10000}
    )
    print(f"Receipt received: #{receipt.receipt_id} (Status: {receipt.status})")

    # 4. Wait for real-time SSE intimation and redeem outcome from Vault
    outcome = await client.wait_for_outcome(receipt.receipt_id)
    print("Vault Outcome:", outcome.result)

if __name__ == "__main__":
    asyncio.run(main())
```

## LangGraph / CrewAI Agent Tool Integration

```python
from roviers import RoviersClient, RoviersTool

client = RoviersClient(base_url="http://127.0.0.1:8080", token="agent_token.json")

# Expose as an agent tool
deep_research_tool = RoviersTool(
    client=client,
    service_name="service.ai.deep_research",
    name="deep_research",
    description="Runs deep multi-source research asynchronously without blocking agent loops"
)
```
