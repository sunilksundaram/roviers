import {
  SignedAgentToken,
  RoviersReceipt,
  RoviersOutcome,
  AgentSignal,
  BlobReference,
  SignedResumeToken,
  ServiceBackoff,
  WorkerLeaseResponse,
  ClusterTopologyResponse,
  PeerHeartbeatRequest,
} from './types';

export interface RoviersBrokerOptions {
  baseUrl?: string;
  token?: SignedAgentToken;
  timeoutMs?: number;
}

export interface SubmitOptions {
  callbackUrl?: string;
  tenantId?: string;
  priority?: number;
  businessKey?: string;
  traceparent?: string;
  tracestate?: string;
}

export class RoviersBrokerClient {
  public readonly baseUrl: string;
  private token?: SignedAgentToken;
  private timeoutMs: number;

  constructor(options: RoviersBrokerOptions = {}) {
    this.baseUrl = (options.baseUrl || 'http://127.0.0.1:8080').replace(/\/+$/, '');
    this.token = options.token;
    this.timeoutMs = options.timeoutMs || 10000;
  }

  get agentId(): string {
    if (!this.token) {
      throw new Error('Agent identity token has not been set or issued.');
    }
    const creds = this.token.credentials as any;
    return creds.agent_id || creds.citizen_id || creds.client_id;
  }

  setToken(token: SignedAgentToken): void {
    this.token = token;
  }

  /**
   * Request an official signed identity credential from the Broker.
   */
  async issueAgentToken(
    agentId: string,
    agentName: string = 'Autonomous Agent',
    validityDays: number = 365
  ): Promise<SignedAgentToken> {
    const res = await fetch(`${this.baseUrl}/api/v1/auth/agent`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        agent_id: agentId,
        agent_name: agentName,
        validity_days: validityDays,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Failed to issue agent token: ${res.status} ${await res.text()}`);
    }

    const token = (await res.json()) as SignedAgentToken;
    this.token = token;
    return token;
  }

  // Backwards compatibility alias
  async issueToken(agentId: string, name: string = 'Agent', validityDays: number = 365): Promise<SignedAgentToken> {
    return this.issueAgentToken(agentId, name, validityDays);
  }

  /**
   * Submit an asynchronous service request to the Broker ingress.
   * Immediately returns HTTP 202 with Claim-Check Receipt.
   */
  async submit<T = any>(
    serviceName: string,
    payload: T,
    options?: string | SubmitOptions
  ): Promise<RoviersReceipt> {
    if (!this.token) {
      throw new Error('A valid SignedAgentToken is required before submitting requests.');
    }

    const opts: SubmitOptions = typeof options === 'string' ? { callbackUrl: options } : options || {};

    const body: Record<string, any> = {
      service_name: serviceName,
      agent_token: this.token,
      payload,
      callback_url: opts.callbackUrl || null,
    };
    if (opts.tenantId) body.tenant_id = opts.tenantId;
    if (opts.priority !== undefined) body.priority = opts.priority;
    if (opts.businessKey) body.business_key = opts.businessKey;
    if (opts.traceparent) body.traceparent = opts.traceparent;
    if (opts.tracestate) body.tracestate = opts.tracestate;

    const res = await fetch(`${this.baseUrl}/api/v1/services/${serviceName}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (res.status !== 202 && !res.ok) {
      throw new Error(`Submit failed with status ${res.status}: ${await res.text()}`);
    }

    return (await res.json()) as RoviersReceipt;
  }

  /**
   * Retrieve active receipt currently leasing a business key.
   */
  async getCase(businessKey: string): Promise<RoviersReceipt | null> {
    const res = await fetch(`${this.baseUrl}/api/v1/cases/${encodeURIComponent(businessKey)}`, {
      method: 'GET',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (res.status === 404) return null;
    if (!res.ok) {
      throw new Error(`Failed to fetch case ${businessKey}: ${res.status}`);
    }

    return (await res.json()) as RoviersReceipt;
  }

  /**
   * Explicitly release a business case lock.
   */
  async releaseCase(businessKey: string): Promise<boolean> {
    const res = await fetch(`${this.baseUrl}/api/v1/cases/${encodeURIComponent(businessKey)}`, {
      method: 'DELETE',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Failed to release case ${businessKey}: ${res.status}`);
    }

    const data = await res.json();
    return Boolean(data.released);
  }

  /**
   * Retrieve cluster node topology and partition assignments.
   */
  async getClusterNodes(): Promise<ClusterTopologyResponse> {
    const res = await fetch(`${this.baseUrl}/api/v1/cluster/nodes`, {
      method: 'GET',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Failed to fetch cluster topology: ${res.status}`);
    }

    return (await res.json()) as ClusterTopologyResponse;
  }

  /**
   * Send heartbeat to cluster leader.
   */
  async peerHeartbeat(req: PeerHeartbeatRequest): Promise<{ status: string }> {
    const res = await fetch(`${this.baseUrl}/api/v1/cluster/heartbeat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Peer heartbeat failed: ${res.status}`);
    }

    return (await res.json()) as { status: string };
  }

  /**
   * Subscribe to tenant-scoped real-time SSE stream.
   */
  listenTenantSignals(tenantId: string, onSignal: (signal: AgentSignal) => void): () => void {
    const url = `${this.baseUrl}/api/v1/tenants/${encodeURIComponent(tenantId)}/stream`;
    const controller = new AbortController();

    fetch(url, { signal: controller.signal })
      .then(async (response) => {
        if (!response.body) return;
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('data:')) {
              try {
                const signal: AgentSignal = JSON.parse(line.slice(5).trim());
                onSignal(signal);
              } catch {
                // Ignore partial JSON
              }
            }
          }
        }
      })
      .catch(() => {});

    return () => controller.abort();
  }

  /**
   * Subscribe to session-scoped real-time SSE stream (for interactive HITL turns).
   */
  listenSessionSignals(sessionId: string, onSignal: (signal: AgentSignal) => void): () => void {
    const url = `${this.baseUrl}/api/v1/sessions/${encodeURIComponent(sessionId)}/stream`;
    const controller = new AbortController();

    fetch(url, { signal: controller.signal })
      .then(async (response) => {
        if (!response.body) return;
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('data:')) {
              try {
                const signal: AgentSignal = JSON.parse(line.slice(5).trim());
                onSignal(signal);
              } catch {
                // Ignore partial JSON
              }
            }
          }
        }
      })
      .catch(() => {});

    return () => controller.abort();
  }

  /**
   * Check status of an issued receipt.
   */
  async getReceipt(receiptId: string): Promise<RoviersReceipt> {
    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}`, {
      method: 'GET',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Receipt ${receiptId} not found (${res.status})`);
    }

    return (await res.json()) as RoviersReceipt;
  }

  /**
   * Present receipt ID and agent token to collect final outcome from the Broker Vault.
   * Returns null if still being computed (HTTP 202).
   */
  async pickup<T = any>(receiptId: string): Promise<RoviersOutcome<T> | null> {
    if (!this.token) {
      throw new Error('A valid SignedAgentToken is required to pickup outcomes.');
    }

    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}/pickup`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ agent_token: this.token }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (res.status === 202) {
      return null;
    }

    if (!res.ok) {
      throw new Error(`Pickup failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as RoviersOutcome<T>;
  }

  /**
   * Cancel an in-flight receipt.
   */
  async cancel(receiptId: string, reason?: string): Promise<RoviersReceipt> {
    if (!this.token) {
      throw new Error('A valid SignedAgentToken is required to cancel a task.');
    }

    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}/cancel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        receipt_id: receiptId,
        agent_token: this.token,
        reason: reason || null,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Cancel failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as RoviersReceipt;
  }

  /**
   * Park an active task awaiting human or agent input (HITL).
   */
  async parkForInput(
    receiptId: string,
    question: string,
    schemaForAnswer?: any,
    checkpointUri?: string,
    ttlSecs: number = 3600
  ): Promise<SignedResumeToken> {
    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}/park`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        receipt_id: receiptId,
        question,
        schema_for_answer: schemaForAnswer || null,
        checkpoint_uri: checkpointUri || null,
        ttl_secs: ttlSecs,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Park for input failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as SignedResumeToken;
  }

  /**
   * Resume a parked HITL turn with user input and the signed resume token.
   */
  async resumeSession(
    sessionId: string,
    resumeToken: SignedResumeToken,
    answer: any
  ): Promise<RoviersReceipt> {
    if (!this.token) {
      throw new Error('A valid SignedAgentToken is required to resume a session.');
    }

    const res = await fetch(`${this.baseUrl}/api/v1/sessions/${sessionId}/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        resume_token: resumeToken,
        answer,
        agent_token: this.token,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Resume session failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as RoviersReceipt;
  }

  /**
   * Apply dynamic backpressure / backoff to a downstream service.
   */
  async setBackoff(
    serviceName: string,
    durationSecs: number,
    reason: string,
    concurrencyCap?: number
  ): Promise<ServiceBackoff> {
    const res = await fetch(`${this.baseUrl}/api/v1/services/${serviceName}/backoff`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        duration_secs: durationSecs,
        reason,
        concurrency_cap: concurrencyCap || null,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Set backoff failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as ServiceBackoff;
  }

  /**
   * Resume normal service intake after backoff recovery.
   */
  async unsetBackoff(serviceName: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}/api/v1/services/${serviceName}/resume`, {
      method: 'POST',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Unset backoff failed: ${res.status} ${await res.text()}`);
    }
  }

  /**
   * Acquire a capacity lease of tasks for a worker node.
   */
  async leaseTasks(
    workerId: string,
    serviceFilter?: string,
    maxTasks: number = 10,
    availableTpmBudget?: number,
    activeInflight: number = 0
  ): Promise<WorkerLeaseResponse> {
    const res = await fetch(`${this.baseUrl}/api/v1/workers/lease`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        worker_id: workerId,
        service_filter: serviceFilter || null,
        max_tasks: maxTasks,
        available_tpm_budget: availableTpmBudget || null,
        active_inflight: activeInflight,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Worker lease failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as WorkerLeaseResponse;
  }

  /**
   * Send heartbeat to keep an active worker lease alive.
   */
  async heartbeat(workerId: string, leaseId: string, extendSecs: number = 45): Promise<void> {
    const res = await fetch(`${this.baseUrl}/api/v1/workers/heartbeat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        worker_id: workerId,
        lease_id: leaseId,
        extend_secs: extendSecs,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Heartbeat failed: ${res.status} ${await res.text()}`);
    }
  }

  /**
   * Reclaim orphaned tasks from expired worker leases.
   */
  async reclaimExpiredLeases(): Promise<string[]> {
    const res = await fetch(`${this.baseUrl}/api/v1/workers/reclaim`, {
      method: 'POST',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Reclaim leases failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as string[];
  }

  /**
   * List tasks diverted to the Dead Letter Queue.
   */
  async listDlq(): Promise<RoviersReceipt[]> {
    const res = await fetch(`${this.baseUrl}/api/v1/dlq`, {
      method: 'GET',
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`List DLQ failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as RoviersReceipt[];
  }

  /**
   * Worker method: deposit finished task outcome into the Broker Vault.
   */
  async complete<T = any>(receiptId: string, result: T, blobRef?: BlobReference): Promise<void> {
    const payload: any = { result };
    if (blobRef) {
      payload.blob_ref = blobRef;
    }

    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}/complete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Complete failed: ${res.status} ${await res.text()}`);
    }
  }

  /**
   * Waits for a task outcome using non-blocking SSE signals, then claims from the Vault.
   */
  async waitForOutcome<T = any>(
    receiptId: string,
    timeoutMs: number = 60000
  ): Promise<RoviersOutcome<T>> {
    // Check if already completed
    const immediate = await this.pickup<T>(receiptId);
    if (immediate !== null) {
      return immediate;
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Timed out waiting for receipt #${receiptId} after ${timeoutMs}ms`));
      }, timeoutMs);

      const url = `${this.baseUrl}/api/v1/agents/${this.agentId}/stream`;
      const controller = new AbortController();

      fetch(url, { signal: controller.signal })
        .then(async (response) => {
          if (!response.body) {
            clearTimeout(timer);
            return reject(new Error('SSE stream returned no body'));
          }

          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
              if (line.startsWith('data:')) {
                try {
                  const signal: AgentSignal = JSON.parse(line.slice(5).trim());
                  if (signal.receipt_id === receiptId) {
                    controller.abort();
                    clearTimeout(timer);

                    const outcome = await this.pickup<T>(receiptId);
                    if (outcome) {
                      return resolve(outcome);
                    }
                  }
                } catch {
                  // Ignore JSON parse errors on partial stream chunks
                }
              }
            }
          }
        })
        .catch((err) => {
          if (err.name !== 'AbortError') {
            clearTimeout(timer);
            reject(err);
          }
        });
    });
  }
}

// Backwards compatibility alias
export const RoviersClient = RoviersBrokerClient;
