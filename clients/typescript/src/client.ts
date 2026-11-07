import {
  SignedAgentToken,
  RoviersReceipt,
  RoviersOutcome,
  AgentSignal,
} from './types';

export interface RoviersBrokerOptions {
  baseUrl?: string;
  token?: SignedAgentToken;
  timeoutMs?: number;
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
    callbackUrl?: string
  ): Promise<RoviersReceipt> {
    if (!this.token) {
      throw new Error('A valid SignedAgentToken is required before submitting requests.');
    }

    const res = await fetch(`${this.baseUrl}/api/v1/services/${serviceName}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        service_name: serviceName,
        agent_token: this.token,
        payload,
        callback_url: callbackUrl || null,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (res.status !== 202 && !res.ok) {
      throw new Error(`Submit failed with status ${res.status}: ${await res.text()}`);
    }

    return (await res.json()) as RoviersReceipt;
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
   * Worker method: deposit finished task outcome into the Broker Vault.
   */
  async complete<T = any>(receiptId: string, result: T): Promise<void> {
    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}/complete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ result }),
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
