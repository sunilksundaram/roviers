# Roviers Python SDK 🤖

Broker & Agent Asynchronous Task & Claim-Check Vault Client for AI Agents, LLM Orchestrators (LangGraph, CrewAI, AutoGen), and distributed microservices.

## Installation

```bash
pip install ./clients/python
# Or in editable mode:
pip install -e ./clients/python
```

## Quick Start

```python
import asyncio
from roviers import RoviersBrokerClient

async def main():
    # 1. Connect to Roviers Broker
    broker = RoviersBrokerClient(base_url="http://127.0.0.1:8080")

    # 2. Issue or load Agent Token
    await broker.issue_agent_token(
        agent_id="AGENT-RESEARCH-01",
        agent_name="Autonomous Research Agent",
        save_path="agent_token.json"
    )

    # 3. Submit heavy asynchronous request (returns in ~2ms with Receipt Token)
    receipt = await broker.submit(
        service_name="service.finance.risk_model",
        payload={"portfolio_id": "port_9921", "simulations": 10000}
    )
    print(f"Receipt received: #{receipt.receipt_id} (Status: {receipt.status})")

    # 4. Wait for real-time SSE signal and redeem outcome from Broker Vault
    outcome = await broker.wait_for_outcome(receipt.receipt_id)
    print("Vault Outcome:", outcome.result)

if __name__ == "__main__":
    asyncio.run(main())
```

## LangGraph / CrewAI Agent Tool Integration

```python
from roviers import RoviersBrokerClient, RoviersTool

broker = RoviersBrokerClient(base_url="http://127.0.0.1:8080", token="agent_token.json")

# Expose as a non-blocking agent tool
deep_research_tool = RoviersTool(
    broker=broker,
    service_name="service.ai.deep_research",
    name="deep_research",
    description="Runs deep multi-source research asynchronously without blocking agent loops"
)
```
