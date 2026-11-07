import {
  SignedCitizenToken,
  RoviersReceipt,
  RoviersOutcome,
  IntimationNotice,
} from './types';

export interface RoviersClientOptions {
  baseUrl?: string;
  token?: SignedCitizenToken;
  timeoutMs?: number;
}

export class RoviersClient {
  public readonly baseUrl: string;
  private token?: SignedCitizenToken;
  private timeoutMs: number;

  constructor(options: RoviersClientOptions = {}) {
    this.baseUrl = (options.baseUrl || 'http://127.0.0.1:8080').replace(/\/+$/, '');
    this.token = options.token;
    this.timeoutMs = options.timeoutMs || 10000;
  }

  get citizenId(): string {
    if (!this.token) {
      throw new Error('Client identity token has not been set or issued.');
    }
    return this.token.credentials.citizen_id;
  }

  setToken(token: SignedCitizenToken): void {
    this.token = token;
  }

  /**
   * Request an official signed identity credential from the authority.
   */
  async issueToken(
    citizenId: string,
    fullName: string,
    validityDays: number = 365
  ): Promise<SignedCitizenToken> {
    const res = await fetch(`${this.baseUrl}/api/v1/auth/citizen`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        citizen_id: citizenId,
        full_name: fullName,
        validity_days: validityDays,
      }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!res.ok) {
      throw new Error(`Failed to issue token: ${res.status} ${await res.text()}`);
    }

    const token = (await res.json()) as SignedCitizenToken;
    this.token = token;
    return token;
  }

  /**
   * Submit an asynchronous service request.
   * Immediately returns HTTP 202 with Claim-Check Receipt.
   */
  async submit<T = any>(
    serviceName: string,
    payload: T,
    callbackUrl?: string
  ): Promise<RoviersReceipt> {
    if (!this.token) {
      throw new Error('A valid SignedCitizenToken is required before submitting requests.');
    }

    const res = await fetch(`${this.baseUrl}/api/v1/services/${serviceName}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        service_name: serviceName,
        citizen_token: this.token,
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
   * Present receipt ID and identity token to collect final outcome from Vault.
   * Returns null if still in process (HTTP 202).
   */
  async pickup<T = any>(receiptId: string): Promise<RoviersOutcome<T> | null> {
    if (!this.token) {
      throw new Error('A valid SignedCitizenToken is required to pickup outcomes.');
    }

    const res = await fetch(`${this.baseUrl}/api/v1/receipts/${receiptId}/pickup`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ citizen_token: this.token }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (res.status === 202) {
      return null; // Outcome still being computed
    }

    if (!res.ok) {
      throw new Error(`Pickup failed: ${res.status} ${await res.text()}`);
    }

    return (await res.json()) as RoviersOutcome<T>;
  }

  /**
   * Waits for a task outcome using non-blocking SSE intimations, then claims from the Vault.
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

      const url = `${this.baseUrl}/api/v1/citizens/${this.citizenId}/stream`;
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
                  const notice: IntimationNotice = JSON.parse(line.slice(5).trim());
                  if (notice.receipt_id === receiptId) {
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
