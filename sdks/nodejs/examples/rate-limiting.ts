/**
 * Rate Limiting Example - TollMeshCache Node.js SDK
 */

import { Client, ClientConfig } from '../dist/client';

async function main() {
  const config: ClientConfig = {
    host: 'localhost',
    port: 8080,
  };

  const client = new Client(config);

  console.log('='.repeat(60));
  console.log('TollMeshCache - Rate Limiting Example');
  console.log('='.repeat(60));

  try {
    // Example 1: Basic rate limiting
    console.log('\n1. Basic Rate Limiting (100 requests per minute)');
    console.log('-'.repeat(60));

    for (let i = 0; i < 3; i++) {
      const result = await client.consume('user-123', 100, 60000);
      console.log(`Request ${i + 1}:`);
      console.log(`  Status: ${result.ok ? 'ALLOWED' : 'RATE LIMITED'}`);
      console.log(`  Remaining: ${result.remaining}`);
      if (result.reset_at) {
        const resetTime = new Date(result.reset_at);
        console.log(`  Resets at: ${resetTime.toISOString()}`);
      }
    }

    // Example 2: API key rate limiting
    console.log('\n2. API Key Rate Limiting (1000 req/hour)');
    console.log('-'.repeat(60));

    for (let i = 0; i < 3; i++) {
      const result = await client.consume('api-key-abc', 1000, 60 * 60 * 1000);
      console.log(`API Call ${i + 1}: ${result.ok ? '✓ OK' : '✗ LIMITED'}`);
      console.log(`  Tokens left: ${result.remaining}`);
    }

    // Example 3: Simulate hitting limit
    console.log('\n3. Simulate Rate Limit (10 req/sec)');
    console.log('-'.repeat(60));

    const key = 'GET /api/search';
    let successful = 0;
    let limited = 0;

    for (let i = 0; i < 12; i++) {
      const result = await client.consume(key, 10, 1000);
      if (result.ok) {
        successful++;
        console.log(`  Request ${i + 1:2d}: ✓ OK (remaining: ${result.remaining})`);
      } else {
        limited++;
        const resetTime = new Date(result.reset_at);
        console.log(`  Request ${i + 1:2d}: ✗ LIMITED (reset: ${resetTime.toISOString()})`);
      }

      // Small delay between requests
      await new Promise(r => setTimeout(r, 50));
    }

    console.log(`\nResults: ${successful} allowed, ${limited} limited`);

    // Example 4: Tier-based rate limiting
    console.log('\n4. Tier-Based Rate Limiting');
    console.log('-'.repeat(60));

    const tiers = {
      free: { limit: 10, window: 60000 },
      pro: { limit: 100, window: 60000 },
      enterprise: { limit: 1000, window: 60000 },
    };

    for (const [tier, config] of Object.entries(tiers)) {
      const result = await client.consume(
        `user-tier-${tier}`,
        config.limit,
        config.window
      );
      const status = result.ok ? '✓ OK' : '✗ LIMITED';
      console.log(`${tier.toUpperCase().padEnd(12)}: ${status} (${String(result.remaining).padStart(4)} remaining)`);
    }

    // Example 5: Health check
    console.log('\n5. Server Health Status');
    console.log('-'.repeat(60));

    const health = await client.health();
    console.log(`Status: ${health.status}`);
    console.log(`Node: ${health.node}`);
    console.log(`Connected Peers: ${health.peers}`);
    if (health.stats) {
      console.log(`Uptime: ${health.stats.uptime_seconds}s`);
      console.log(`Total Requests: ${health.stats.requests_total}`);
      console.log(`P99 Latency: ${health.stats.latency_p99_ms.toFixed(2)}ms`);
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
