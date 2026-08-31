/**
 * Caching Example - TollMeshCache Node.js SDK
 */

import { Client, ClientConfig } from '../dist/client';

async function main() {
  const config: ClientConfig = {
    host: 'localhost',
    port: 8080,
  };

  const client = new Client(config);

  console.log('='.repeat(60));
  console.log('TollMeshCache - Caching Example');
  console.log('='.repeat(60));

  try {
    // Example 1: Simple cache operations
    console.log('\n1. Simple Cache Operations');
    console.log('-'.repeat(60));

    const namespace = 'users';
    const key = 'user-123';
    const value = JSON.stringify({
      id: '123',
      name: 'Alice',
      email: 'alice@example.com',
    });

    console.log(`Setting ${namespace}/${key}...`);
    await client.cacheSet(namespace, key, value, 3600000);
    console.log('✓ Set successful');

    console.log(`Getting ${namespace}/${key}...`);
    const { value: cachedValue, exists } = await client.cacheGet(namespace, key);
    if (exists) {
      console.log(`✓ Found: ${cachedValue}`);
    } else {
      console.log('✗ Not found');
    }

    // Example 2: Cache-aside pattern
    console.log('\n2. Cache-Aside Pattern (Lazy Loading)');
    console.log('-'.repeat(60));

    async function getUser(userId: string) {
      const ns = 'user_profiles';
      console.log(`Checking cache for ${userId}...`);
      const { value, exists } = await client.cacheGet(ns, userId);

      if (exists) {
        console.log('✓ Cache hit!');
        return JSON.parse(value!);
      }

      console.log('✗ Cache miss - fetching from source...');
      const userData = {
        id: userId,
        name: `User ${userId}`,
        email: `user${userId}@example.com`,
      };

      console.log('  Caching result (1 hour TTL)...');
      await client.cacheSet(ns, userId, JSON.stringify(userData), 3600000);

      return userData;
    }

    console.log('\nFirst call to getUser("user-456"):');
    const user1 = await getUser('user-456');
    console.log(`Result: ${user1.name}`);

    console.log('\nSecond call to getUser("user-456"):');
    const user2 = await getUser('user-456');
    console.log(`Result: ${user2.name}`);

    // Example 3: Multiple namespaces
    console.log('\n3. Multiple Cache Namespaces');
    console.log('-'.repeat(60));

    const namespaces = {
      users: { ttl: 3600000, key: 'user-789' },
      posts: { ttl: 86400000, key: 'post-101' },
      comments: { ttl: 21600000, key: 'comment-202' },
      sessions: { ttl: 3600000, key: 'session-abc123' },
    };

    for (const [ns, config] of Object.entries(namespaces)) {
      const data = { namespace: ns, timestamp: new Date().toISOString() };
      await client.cacheSet(ns, config.key, JSON.stringify(data), config.ttl);
      console.log(`✓ Cached ${ns}/${config.key} (TTL: ${config.ttl}ms)`);
    }

    // Example 4: Different data types
    console.log('\n4. Caching Different Data Types');
    console.log('-'.repeat(60));

    const testCases = [
      { ns: 'strings', key: 'greeting', value: 'Hello, World!' },
      { ns: 'numbers', key: 'pi', value: JSON.stringify(3.14159) },
      { ns: 'objects', key: 'person', value: JSON.stringify({ name: 'Bob', age: 30 }) },
      { ns: 'arrays', key: 'scores', value: JSON.stringify([95, 87, 92, 88, 91]) },
    ];

    for (const { ns, key: k, value: v } of testCases) {
      await client.cacheSet(ns, k, v, 3600000);
      const { value: cachedVal } = await client.cacheGet(ns, k);
      console.log(`✓ ${ns.padEnd(10)}/${k.padEnd(10)}: ${cachedVal?.substring(0, 50)}`);
    }

    // Example 5: Cache statistics
    console.log('\n5. Server Statistics');
    console.log('-'.repeat(60));

    const health = await client.health();
    console.log(`Cache Status: ${health.status}`);
    if (health.stats) {
      console.log(`Cache Hit Rate: ${(health.stats.cache_hit_rate * 100).toFixed(1)}%`);
      console.log(`Total Requests: ${health.stats.requests_total}`);
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
