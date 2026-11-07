"""
Roviers Python Client Library
Asynchronous Service Intake & Claim-Check Vault Engine
"""

from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Union
import httpx


@dataclass
class RoviersReceipt:
    receipt_id: str
    citizen_id: str
    service_name: str
    issued_at: int
    status: str
    estimated_ready_secs: Optional[int] = None
    callback_url: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RoviersReceipt:
        return cls(
            receipt_id=str(data["receipt_id"]),
            citizen_id=data["citizen_id"],
            service_name=data["service_name"],
            issued_at=data["issued_at"],
            status=data["status"],
            estimated_ready_secs=data.get("estimated_ready_secs"),
            callback_url=data.get("callback_url"),
        )


@dataclass
class RoviersOutcome:
    receipt_id: str
    citizen_id: str
    service_name: str
    completed_at: int
    result: Any

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RoviersOutcome:
        return cls(
            receipt_id=str(data["receipt_id"]),
            citizen_id=data["citizen_id"],
            service_name=data["service_name"],
            completed_at=data["completed_at"],
            result=data["result"],
        )


class RoviersClient:
    """
    Client for interacting with Roviers service counters and outcome vaults.
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
    def citizen_id(self) -> str:
        if not self.token_data:
            raise ValueError("Client token has not been loaded or configured.")
        return self.token_data["credentials"]["citizen_id"]

    async def issue_token(
        self,
        citizen_id: str,
        full_name: str,
        validity_days: int = 365,
        save_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Requests an official Ed25519-signed client credential from the Roviers authority.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/auth/citizen",
                json={
                    "citizen_id": citizen_id,
                    "full_name": full_name,
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

    async def submit(
        self,
        service_name: str,
        payload: Dict[str, Any],
        callback_url: Optional[str] = None,
    ) -> RoviersReceipt:
        """
        Dispatches a service request to the intake counter.
        Returns a Claim-Check Receipt immediately (HTTP 202 Accepted).
        """
        if not self.token_data:
            raise ValueError("Token must be provided before submitting requests.")

        body = {
            "service_name": service_name,
            "citizen_token": self.token_data,
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
        Presents the Receipt ID and cryptographic proof of identity to redeem the outcome from the Vault.
        Returns None if outcome is still in process (HTTP 202).
        """
        if not self.token_data:
            raise ValueError("Token must be loaded before picking up outcomes.")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/pickup",
                json={"citizen_token": self.token_data},
            )
            if resp.status_code == 202:
                return None
            resp.raise_for_status()
            return RoviersOutcome.from_dict(resp.json())

    async def listen_intimations(
        self,
        timeout_secs: Optional[float] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Subscribes to the real-time Server-Sent Events (SSE) notification stream.
        Yields Intimation notices the instant any outcome for this client is ready.
        """
        cid = self.citizen_id
        url = f"{self.base_url}/api/v1/citizens/{cid}/stream"

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
        High-level helper: waits for the specific receipt outcome using zero-polling SSE intimations,
        then automatically redeems and returns the outcome from the Vault.
        """
        # First check if it's already ready
        immediate = await self.pickup(receipt_id)
        if immediate is not None:
            return immediate

        loop = asyncio.get_running_loop()
        end_time = loop.time() + timeout_secs

        async for notice in self.listen_intimations(timeout_secs=timeout_secs):
            if notice.get("receipt_id") == receipt_id:
                outcome = await self.pickup(receipt_id)
                if outcome is not None:
                    return outcome

            if loop.time() > end_time:
                break

        raise TimeoutError(f"Timed out waiting for receipt #{receipt_id} outcome.")


class RoviersTool:
    """
    Adapter for AI Agent Frameworks (LangGraph, CrewAI, AutoGen).
    Wraps heavy tasks into non-blocking claim-check operations.
    """

    def __init__(self, client: RoviersClient, service_name: str, name: str, description: str):
        self.client = client
        self.service_name = service_name
        self.name = name
        self.description = description

    async def run(self, **kwargs) -> Any:
        receipt = await self.client.submit(self.service_name, kwargs)
        outcome = await self.client.wait_for_outcome(receipt.receipt_id)
        return outcome.result
