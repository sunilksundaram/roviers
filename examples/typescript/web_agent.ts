/**
 * TypeScript Web Agent Example using RoviersBrokerClient.
 *
 * Demonstrates:
 * 1. Issuing Agent identity token.
 * 2. Non-blocking service submission (HTTP 202).
 * 3. Listening for SSE signals.
 * 4. Redeeming outcome from the Broker Vault.
 */

import { RoviersBrokerClient } from '../../clients/typescript/src';

async function main() {
  console.log('🤖 [TS Agent] Initializing TypeScript Agent...');
  const broker = new RoviersBrokerClient({ baseUrl: 'http://127.0.0.1:8080' });

  // 1. Issue an Agent token
  console.log('🔑 [TS Agent] Requesting token from Broker...');
  const token = await broker.issueAgentToken('AGENT-TS-01', 'TypeScript Web Agent', 30);
  console.log(`✅ [TS Agent] Token issued for: ${token.credentials.agent_name} (${token.credentials.agent_id})`);

  // 2. Submit an asynchronous task (returns in ~2ms with Receipt)
  console.log('\n🚀 [TS Agent] Submitting task to Broker ingress...');
  const receipt = await broker.submit('service.reporting.export_csv', {
    report_id: 'rep_2026_11',
    format: 'csv',
    filter: { status: 'active' },
  });

  console.log(`🎟️ [TS Agent] Claim-Check Receipt received:`);
  console.log(`   Receipt ID: ${receipt.receipt_id}`);
  console.log(`   Status:     ${receipt.status}`);

  // 3. Wait for real-time SSE signal and redeem outcome from Vault
  console.log('\n👂 [TS Agent] Listening for SSE signal and collecting from Vault...');
  const outcome = await broker.waitForOutcome(receipt.receipt_id, 30000);

  console.log('\n🏆 [TS Agent] Outcome Retrieved from Broker Vault:');
  console.log(JSON.stringify(outcome, null, 2));
}

main().catch(console.error);

