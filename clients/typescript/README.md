# Roviers TypeScript SDK 🌐

Broker & Agent asynchronous client for **Roviers**: High-Throughput Service Intake & Claim-Check Vault Engine.

## Installation

```bash
npm install ./clients/typescript
```

## Quick Start

```typescript
import { RoviersBrokerClient } from 'roviers';

async function main() {
  const broker = new RoviersBrokerClient({ baseUrl: 'http://127.0.0.1:8080' });

  // 1. Issue or set Agent identity token
  await broker.issueAgentToken('AGENT-WEB-01', 'Production Web Agent');

  // 2. Submit non-blocking request (returns HTTP 202 Claim-Check Receipt in ~2ms)
  const receipt = await broker.submit('service.billing.generate_invoices', {
    period: '2026-Q1',
    accounts: [101, 102, 103],
  });
  console.log(`Receipt issued: #${receipt.receipt_id} (Status: ${receipt.status})`);

  // 3. Wait for real-time SSE signal and pickup verified outcome from Broker Vault
  const outcome = await broker.waitForOutcome(receipt.receipt_id);
  console.log('Outcome from Broker Vault:', outcome.result);
}

main().catch(console.error);
```
