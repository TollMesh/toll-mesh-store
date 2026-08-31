/**
 * Replay Protection Example - TollMeshCache Node.js SDK
 */

import { Client, ClientConfig, ReplayError } from '../dist';
import { randomUUID } from 'crypto';

async function main() {
  const config: ClientConfig = {
    host: 'localhost',
    port: 8080,
  };

  const client = new Client(config);

  console.log('='.repeat(60));
  console.log('TollMeshCache - Replay Protection Example');
  console.log('='.repeat(60));

  try {
    // Example 1: Basic replay detection
    console.log('\n1. Basic Replay Detection');
    console.log('-'.repeat(60));

    const requestId = randomUUID();
    const ttl = 5 * 60 * 1000; // 5 minutes

    console.log(`Processing request: ${requestId}`);
    console.log(`Tracking for ${ttl / 1000} seconds...`);

    const result1 = await client.seen(requestId, ttl);
    if (!result1.seen) {
      console.log('✓ First request - Processing transaction');
    } else {
      console.log('✗ Replay detected!');
    }

    console.log(`\nAttempting replay with same ID: ${requestId}`);
    const result2 = await client.seen(requestId, ttl);
    if (result2.seen) {
      console.log('✓ Replay correctly detected!');
    } else {
      console.log('✗ Replay not detected (unexpected)');
    }

    // Example 2: Payment transaction protection
    console.log('\n2. Payment Transaction Protection');
    console.log('-'.repeat(60));

    async function processPayment(
      amount: number,
      idempotencyKey: string
    ): Promise<boolean> {
      const result = await client.seen(idempotencyKey, 24 * 60 * 60 * 1000);
      if (result.seen) {
        console.log('  ✗ Replay detected - transaction already processed');
        return false;
      }

      console.log(`  ✓ Processing $${amount.toFixed(2)} payment`);
      return true;
    }

    console.log('First payment attempt:');
    const key1 = 'payment-' + randomUUID();
    await processPayment(99.99, key1);

    console.log('\nRetrying same payment:');
    await processPayment(99.99, key1);

    console.log('\nDifferent payment:');
    const key2 = 'payment-' + randomUUID();
    await processPayment(149.99, key2);

    // Example 3: API request deduplication
    console.log('\n3. API Request Deduplication');
    console.log('-'.repeat(60));

    async function apiCreateUser(
      name: string,
      email: string,
      requestId: string
    ): Promise<{ error?: string; user?: any }> {
      const result = await client.seen(requestId, 60 * 60 * 1000);
      if (result.seen) {
        console.log('  ✗ Duplicate request - skipping');
        return { error: 'duplicate_request' };
      }

      console.log(`  ✓ Creating user: ${name} (${email})`);
      return {
        user: {
          id: randomUUID(),
          name,
          email,
        },
      };
    }

    const reqId = 'req-create-user-' + randomUUID();
    console.log('First request:');
    await apiCreateUser('Alice', 'alice@example.com', reqId);

    console.log('\nDuplicate request (same request_id):');
    await apiCreateUser('Alice', 'alice@example.com', reqId);

    // Example 4: Error handling
    console.log('\n4. Replay Error Handling');
    console.log('-'.repeat(60));

    const nonce = 'error-' + randomUUID();

    try {
      // Mark as seen
      await client.seen(nonce, 5 * 60 * 1000);
      console.log('✓ First request processed');

      // Try again with same nonce
      const result = await client.seen(nonce, 5 * 60 * 1000);
      if (result.seen) {
        throw new ReplayError(nonce);
      }

    } catch (error) {
      if (error instanceof ReplayError) {
        console.log(`✓ Caught replay error: ${error.message}`);
        console.log(`  Nonce: ${error.nonce}`);
      } else {
        console.log(`✗ Unexpected error: ${error}`);
      }
    }

    // Example 5: Distributed tracking
    console.log('\n5. Distributed Request Tracking');
    console.log('-'.repeat(60));

    const requests = [
      { nonce: 'order-' + randomUUID(), desc: 'order-123' },
      { nonce: 'transfer-' + randomUUID(), desc: 'transfer-456' },
      { nonce: 'refund-' + randomUUID(), desc: 'refund-789' },
    ];

    console.log('Tracking requests across cluster:');
    for (const { nonce, desc } of requests) {
      const result = await client.seen(nonce, 6 * 60 * 60 * 1000);
      const status = result.seen ? 'Already seen' : 'New request';
      console.log(`  ${desc.padEnd(20)}: ${status}`);
    }

    console.log('\nRetrying same requests:');
    for (const { nonce, desc } of requests) {
      const result = await client.seen(nonce, 6 * 60 * 60 * 1000);
      const status = result.seen ? 'Already seen' : 'New request';
      console.log(`  ${desc.padEnd(20)}: ${status}`);
    }

  } catch (error) {
    console.error('ERROR:', error);
  } finally {
    client.close();
    console.log('\n' + '='.repeat(60));
    console.log('Example complete!');
    console.log('='.repeat(60));
  }
}

main();
