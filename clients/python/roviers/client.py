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
    business_key: Optional[str] = None
    session_id: Optional[str] = None
    partition_id: Optional[int] = None

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
            business_key=data.get("business_key"),
            session_id=data.get("session_id"),
            partition_id=data.get("partition_id"),
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
        tenant_id: Optional[str] = None,
        priority: Optional[int] = None,
        business_key: Optional[str] = None,
        traceparent: Optional[str] = None,
        tracestate: Optional[str] = None,
    ) -> RoviersReceipt:
        """
        Dispatches a service request to the Broker ingress.
        Returns a Claim-Check Receipt immediately (HTTP 202 Accepted).
        """
        if not self.token_data:
            raise ValueError("Agent token must be provided before submitting requests.")

        body: Dict[str, Any] = {
            "service_name": service_name,
            "agent_token": self.token_data,
            "payload": payload,
            "callback_url": callback_url,
        }
        if tenant_id:
            body["tenant_id"] = tenant_id
        if priority is not None:
            body["priority"] = priority
        if business_key:
            body["business_key"] = business_key
        if traceparent:
            body["traceparent"] = traceparent
        if tracestate:
            body["tracestate"] = tracestate

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

    async def cancel(self, receipt_id: str, reason: Optional[str] = None) -> RoviersReceipt:
        """
        Cancels an in-flight receipt.
        """
        if not self.token_data:
            raise ValueError("Token required for task cancellation.")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/cancel",
                json={"receipt_id": receipt_id, "agent_token": self.token_data, "reason": reason},
            )
            resp.raise_for_status()
            return RoviersReceipt.from_dict(resp.json())

    async def park_for_input(
        self,
        receipt_id: str,
        question: str,
        schema_for_answer: Optional[Dict[str, Any]] = None,
        checkpoint_uri: Optional[str] = None,
        ttl_secs: Optional[int] = 3600,
    ) -> Dict[str, Any]:
        """
        Parks an active task awaiting human or agent clarification (HITL).
        Returns the SignedResumeToken.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/park",
                json={
                    "receipt_id": receipt_id,
                    "question": question,
                    "schema_for_answer": schema_for_answer,
                    "checkpoint_uri": checkpoint_uri,
                    "ttl_secs": ttl_secs,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def resume_session(
        self,
        session_id: str,
        resume_token: Dict[str, Any],
        answer: Any,
    ) -> RoviersReceipt:
        """
        Resumes a parked session with user input and the cryptographic resume token.
        """
        if not self.token_data:
            raise ValueError("Token required for session resumption.")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/sessions/{session_id}/resume",
                json={
                    "resume_token": resume_token,
                    "answer": answer,
                    "agent_token": self.token_data,
                },
            )
            resp.raise_for_status()
            return RoviersReceipt.from_dict(resp.json())

    async def set_backoff(
        self,
        service_name: str,
        duration_secs: int,
        reason: str,
        concurrency_cap: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Dynamically applies backpressure / backoff to a downstream service.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/services/{service_name}/backoff",
                json={
                    "duration_secs": duration_secs,
                    "reason": reason,
                    "concurrency_cap": concurrency_cap,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def unset_backoff(self, service_name: str) -> None:
        """
        Resumes normal service intake after backoff recovery.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}/api/v1/services/{service_name}/resume")
            resp.raise_for_status()

    async def get_service_status(self, service_name: str) -> Dict[str, Any]:
        """
        Queries downstream circuit and backpressure status for a service.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.base_url}/api/v1/services/{service_name}/status")
            resp.raise_for_status()
            return resp.json()

    async def lease_tasks(
        self,
        worker_id: str,
        service_filter: Optional[str] = None,
        max_tasks: int = 10,
        available_tpm_budget: Optional[int] = None,
        active_inflight: int = 0,
    ) -> Dict[str, Any]:
        """
        Worker capacity lease to safely acquire task batches under rate/TPM limits.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/workers/lease",
                json={
                    "worker_id": worker_id,
                    "service_filter": service_filter,
                    "max_tasks": max_tasks,
                    "available_tpm_budget": available_tpm_budget,
                    "active_inflight": active_inflight,
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def heartbeat(self, worker_id: str, lease_id: str, extend_secs: int = 45) -> None:
        """
        Extends active worker lease expiration.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/workers/heartbeat",
                json={"worker_id": worker_id, "lease_id": lease_id, "extend_secs": extend_secs},
            )
            resp.raise_for_status()

    async def reclaim_expired_leases(self) -> list:
        """
        Reclaims orphaned tasks from crashed workers or diverts them to DLQ.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}/api/v1/workers/reclaim")
            resp.raise_for_status()
            return resp.json()

    async def list_dlq(self) -> list:
        """
        Lists permanently failed tasks diverted to the Dead Letter Queue.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.base_url}/api/v1/dlq")
            resp.raise_for_status()
            return resp.json()

    async def compact_vault(self, max_age_secs: int = 86400) -> Dict[str, Any]:
        """
        Compacts the outcome vault by purging terminal (fulfilled/failed) receipts older than max_age_secs.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/vault/compact",
                json={"max_age_secs": max_age_secs},
            )
            resp.raise_for_status()
            return resp.json()

    async def complete(self, receipt_id: str, result: Any, blob_ref: Optional[Dict[str, Any]] = None) -> None:
        """
        Worker capability: deposits a finished execution outcome into the Vault.
        """
        payload: Dict[str, Any] = {"result": result}
        if blob_ref:
            payload["blob_ref"] = blob_ref

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/receipts/{receipt_id}/complete",
                json=payload,
            )
            resp.raise_for_status()

    async def get_case(self, business_key: str) -> Optional[RoviersReceipt]:
        """
        Inspects active receipt currently holding a business case lock.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.base_url}/api/v1/cases/{business_key}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return RoviersReceipt.from_dict(resp.json())

    async def release_case(self, business_key: str) -> bool:
        """
        Explicitly unlocks/releases a business case lock.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.delete(f"{self.base_url}/api/v1/cases/{business_key}")
            resp.raise_for_status()
            return bool(resp.json().get("released", False))

    async def get_cluster_nodes(self) -> Dict[str, Any]:
        """
        Queries cluster node topology and partition assignments.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.base_url}/api/v1/cluster/nodes")
            resp.raise_for_status()
            return resp.json()

    async def peer_heartbeat(
        self,
        node_id: str,
        listen_addr: str,
        is_leader: bool = False,
        assigned_partitions: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Sends peer node heartbeat to the cluster topology leader.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/v1/cluster/heartbeat",
                json={
                    "node_id": node_id,
                    "listen_addr": listen_addr,
                    "is_leader": is_leader,
                    "assigned_partitions": assigned_partitions or [],
                },
            )
            resp.raise_for_status()
            return resp.json()

    async def listen_tenant_signals(
        self,
        tenant_id: str,
        timeout_secs: Optional[float] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Subscribes to tenant-scoped real-time SSE stream.
        """
        url = f"{self.base_url}/api/v1/tenants/{tenant_id}/stream"
        timeout = httpx.Timeout(timeout_secs if timeout_secs else None, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", url) as stream:
                async for line in stream.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        if raw:
                            yield json.loads(raw)

    async def listen_session_signals(
        self,
        session_id: str,
        timeout_secs: Optional[float] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Subscribes to session-scoped real-time SSE stream for interactive HITL turns.
        """
        url = f"{self.base_url}/api/v1/sessions/{session_id}/stream"
        timeout = httpx.Timeout(timeout_secs if timeout_secs else None, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("GET", url) as stream:
                async for line in stream.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        if raw:
                            yield json.loads(raw)

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
