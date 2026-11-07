# Roviers TypeScript SDK 🌐

Lightweight TypeScript/JavaScript client for **Roviers**: High-Throughput Service Intake & Claim-Check Vault Engine.

## Installation

```bash
npm install ./clients/typescript
```

## Quick Start

```typescript
import { RoviersClient } from 'roviers';

async function main() {
  const client = new RoviersClient({ baseUrl: 'http://127.0.0.1:8080' });

  // 1. Issue or set identity credential
  await client.issueToken('CLIENT-APP-01', 'Production Web App');

  // 2. Submit non-blocking request (returns HTTP 202 Claim-Check Receipt in ~2ms)
  const receipt = await client.submit('service.billing.generate_invoices', {
    period: '2026-Q1',
    accounts: [101, 102, 103],
  });
  console.log(`Receipt issued: #${receipt.receipt_id} (Status: ${receipt.status})`);

  // 3. Wait for real-time SSE intimation and pickup verified outcome from Vault
  const outcome = await client.waitForOutcome(receipt.receipt_id);
  console.log('Outcome from Vault:', outcome.result);
}

main().catch(console.error);
```
