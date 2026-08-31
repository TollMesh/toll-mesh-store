#!/usr/bin/env python3
"""
Async Example - TollMeshCache Python SDK

Demonstrates how to use TollMeshCache with async/await.
"""

import asyncio
import json
from datetime import timedelta
import sys
sys.path.insert(0, '../')

from tollmeshcache import AsyncClient, ClientConfig, TollMeshError


async def example_rate_limiting():
    """Example: Rate limiting with async"""
    config = ClientConfig(host="localhost", port=8080)
    async with AsyncClient(config) as client:
        print("Rate Limiting Example (Async):")
        print("-" * 40)

        for i in range(3):
            try:
                result = await client.consume("async-user", 100, timedelta(minutes=1))
                status = "✓ OK" if result["ok"] else "✗ LIMITED"
                print(f"  Request {i+1}: {status} ({result['remaining']} remaining)")
            except TollMeshError as e:
                print(f"  Request {i+1}: ERROR - {e.message}")


async def example_caching():
    """Example: Caching with async"""
    config = ClientConfig(host="localhost", port=8080)
    async with AsyncClient(config) as client:
        print("\nCaching Example (Async):")
        print("-" * 40)

        # Set cache
        namespace = "async-cache"
        key = "user-data"
        value = json.dumps({"name": "Alice", "email": "alice@example.com"})

        print(f"  Setting {namespace}/{key}...")
        await client.cache_set(namespace, key, value, ttl=timedelta(hours=1))
        print("  ✓ Set successful")

        # Get cache
        print(f"  Getting {namespace}/{key}...")
        cached_value, exists = await client.cache_get(namespace, key)
        if exists:
            print(f"  ✓ Found: {cached_value}")
        else:
            print("  ✗ Not found")


async def example_replay_protection():
    """Example: Replay protection with async"""
    config = ClientConfig(host="localhost", port=8080)
    async with AsyncClient(config) as client:
        print("\nReplay Protection Example (Async):")
        print("-" * 40)

        nonce = "async-nonce-123"
        ttl = timedelta(minutes=5)

        # First check
        result1 = await client.seen(nonce, ttl)
        status1 = "Seen" if result1["seen"] else "New"
        print(f"  First check: {status1}")

        # Second check
        result2 = await client.seen(nonce, ttl)
        status2 = "Seen" if result2["seen"] else "New"
        print(f"  Second check: {status2}")


async def example_concurrent_operations():
    """Example: Concurrent operations with async"""
    config = ClientConfig(host="localhost", port=8080)
    async with AsyncClient(config) as client:
        print("\nConcurrent Operations Example (Async):")
        print("-" * 40)

        async def process_request(req_id):
            """Process a request"""
            result = await client.consume(f"concurrent-user-{req_id}", 100, timedelta(minutes=1))
            return f"Request {req_id}: {'✓' if result['ok'] else '✗'}"

        # Run 5 requests concurrently
        print("  Making 5 concurrent requests...")
        tasks = [process_request(i) for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                print(f"    Error: {result}")
            else:
                print(f"    {result}")


async def example_health_check():
    """Example: Health check with async"""
    config = ClientConfig(host="localhost", port=8080)
    async with AsyncClient(config) as client:
        print("\nHealth Check Example (Async):")
        print("-" * 40)

        try:
            health = await client.health()
            print(f"  Status: {health['status']}")
            print(f"  Node: {health['node']}")
            print(f"  Peers: {health['peers']}")
            if 'stats' in health:
                print(f"  Requests: {health['stats'].get('requests_total', 0)}")
        except TollMeshError as e:
            print(f"  ERROR: {e.message}")


async def main():
    """Run all examples"""
    print("=" * 60)
    print("TollMeshCache - Async Examples")
    print("=" * 60)

    try:
        await example_rate_limiting()
        await example_caching()
        await example_replay_protection()
        await example_concurrent_operations()
        await example_health_check()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n" + "=" * 60)
        print("Examples complete!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
