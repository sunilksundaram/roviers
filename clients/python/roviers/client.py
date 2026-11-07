"""
Roviers Python Client Library
Broker & Agent Paradigm for Asynchronous Service Intake & Claim-Check Vault Engine
"""

from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, Optional, Union
import httpx


@dataclass
class RoviersReceipt:
    receipt_id: str
    agent_id: str
    service_name: str
    issued_at: int
    status: str
    estimated_ready_secs: Optional[int] = None
    callback_url: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RoviersReceipt:
        return cls(
            receipt_id=str(data["receipt_id"]),
            agent_id=data.get("agent_id") or data.get("citizen_id") or data.get("client_id", ""),
            service_name=data["service_name"],
            issued_at=data["issued_at"],
            status=data["status"],
            estimated_ready_secs=data.get("estimated_ready_secs"),
            callback_url=data.get("callback_url"),
        )


@dataclass
class RoviersOutcome:
    receipt_id: str
    agent_id: str
    service_name: str
    completed_at: int
    result: Any

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RoviersOutcome:
        return cls(
            receipt_id=str(data["receipt_id"]),
            agent_id=data.get("agent_id") or data.get("citizen_id") or data.get("client_id", ""),
            service_name=data["service_name"],
            completed_at=data["completed_at"],
            result=data["result"],
        )


class RoviersBrokerClient:
    """
    Client for interacting with Roviers Broker ingress and Outcome Vault.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8080",
        token: Optional[Union[Dict[str, Any], str]] = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.token_data: Optional[Dict[str, Any]] = None

        if isinstance(token, str):
            with open(token, "r", encoding="utf-8") as f:
                self.token_data = json.load(f)
        elif isinstance(token, dict):
            self.token_data = token

    @property
    def agent_id(self) -> str:
        if not self.token_data:
            raise ValueError("Agent token has not been loaded or configured.")
        creds = self.token_data["credentials"]
        return creds.get("agent_id") or creds.get("citizen_id") or creds.get("client_id", "")

    async def issue_agent_token(
        self,
        agent_id: str,
        agent_name: str = "Autonomous Agent",
        validity_days: int = 365,
        save_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Requests an official Ed25519-signed agent credential from the Roviers Broker.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/auth/agent",
                json={
                    "agent_id": agent_id,
                    "agent_name": agent_name,
                    "validity_days": validity_days,
                },
            )
            resp.raise_for_status()
            token = resp.json()

        self.token_data = token
        if save_path:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(token, f, indent=2)

        return token

    # Backwards compatibility alias
    issue_token = issue_agent_token

    async def submit(
        self,
        service_name: str,
        payload: Dict[str, Any],
        callback_url: Optional[str] = None,
    ) -> RoviersReceipt:
        """
        Dispatches a service request to the Broker ingress.
        Returns a Claim-Check Receipt immediately (HTTP 202 Accepted).
        """
        if not self.token_data:
            raise ValueError("Agent token must be provided before submitting requests.")

        body = {
            "service_name": service_name,
            "agent_token": self.token_data,
            "payload": payload,
            "callback_url": callback_url,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/services/{service_name}",
                json=body,
            )
            resp.raise_for_status()
            return RoviersReceipt.from_dict(resp.json())

    async def get_receipt(self, receipt_id: str) -> RoviersReceipt:
        """
        Inspects the current state of a receipt (Queued, Processing, ReadyForPickup, Fulfilled).
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.base_url}/api/v1/receipts/{receipt_id}")
            resp.raise_for_status()
            return RoviersReceipt.from_dict(resp.json())

    async def pickup(self, receipt_id: str) -> Optional[RoviersOutcome]:
        """
        Presents the Receipt ID and cryptographic proof of agent identity to redeem the outcome from the Vault.
        Returns None if outcome is still being processed (HTTP 202).
        """
        if not self.token_data:
            raise ValueError("Token must be loaded before picking up outcomes.")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/pickup",
                json={"agent_token": self.token_data},
            )
            if resp.status_code == 202:
                return None
            resp.raise_for_status()
            return RoviersOutcome.from_dict(resp.json())

    async def complete(self, receipt_id: str, result: Any) -> None:
        """
        Worker capability: deposits a finished execution outcome into the Vault.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/complete",
                json={"result": result},
            )
            resp.raise_for_status()

    async def listen_signals(
        self,
        timeout_secs: Optional[float] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Subscribes to the real-time Server-Sent Events (SSE) signal stream.
        Yields completion signals the instant any task for this agent is ready.
        """
        aid = self.agent_id
        url = f"{self.base_url}/api/v1/agents/{aid}/stream"

        timeout = httpx.Timeout(timeout_secs if timeout_secs else None, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", url) as stream:
                async for line in stream.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        if raw:
                            yield json.loads(raw)

    async def wait_for_outcome(
        self,
        receipt_id: str,
        timeout_secs: float = 60.0,
    ) -> RoviersOutcome:
        """
        High-level helper: waits for the specific receipt outcome using zero-polling SSE signals,
        then automatically redeems and returns the outcome from the Broker Vault.
        """
        # First check if it's already ready
        immediate = await self.pickup(receipt_id)
        if immediate is not None:
            return immediate

        loop = asyncio.get_running_loop()
        end_time = loop.time() + timeout_secs

        async for signal in self.listen_signals(timeout_secs=timeout_secs):
            if signal.get("receipt_id") == receipt_id:
                outcome = await self.pickup(receipt_id)
                if outcome is not None:
                    return outcome

            if loop.time() > end_time:
                break

        raise TimeoutError(f"Timed out waiting for receipt #{receipt_id} outcome.")


# Backwards compatibility alias
RoviersClient = RoviersBrokerClient


class RoviersTool:
    """
    Adapter for AI Agent Frameworks (LangGraph, CrewAI, AutoGen).
    Wraps heavy tasks into non-blocking claim-check operations.
    """

    def __init__(self, broker: RoviersBrokerClient, service_name: str, name: str, description: str):
        self.broker = broker
        self.service_name = service_name
        self.name = name
        self.description = description

    async def run(self, **kwargs) -> Any:
        receipt = await self.broker.submit(self.service_name, kwargs)
        outcome = await self.broker.wait_for_outcome(receipt.receipt_id)
        return outcome.result
