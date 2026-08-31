#!/usr/bin/env python3
"""
Rate Limiting Example - TollMeshCache Python SDK

Demonstrates how to use TollMeshCache for distributed rate limiting.
"""

from datetime import datetime, timedelta
import time
import sys
sys.path.insert(0, '../')

from tollmeshcache import Client, ClientConfig, TollMeshError


def main():
    # Configure client
    config = ClientConfig(
        host="localhost",
        port=8080,
        timeout=5.0,
    )

    client = Client(config)

    print("=" * 60)
    print("TollMeshCache - Rate Limiting Example")
    print("=" * 60)

    try:
        # Example 1: Basic rate limiting
        print("\n1. Basic Rate Limiting (100 requests per minute)")
        print("-" * 60)

        user_id = "user-12345"
        limit = 100
        window = timedelta(minutes=1)

        for i in range(5):
            try:
                result = client.consume(user_id, limit, window)
                print(f"Request {i+1}:")
                print(f"  Status: {'ALLOWED' if result['ok'] else 'RATE LIMITED'}")
                print(f"  Remaining: {result['remaining']}")
                if result.get('reset_at'):
                    reset_time = datetime.fromtimestamp(result['reset_at'] / 1000)
                    print(f"  Resets at: {reset_time}")
            except TollMeshError as e:
                print(f"Request {i+1}: ERROR - {e.message}")

        # Example 2: API key rate limiting
        print("\n2. API Key Rate Limiting (1000 requests per hour)")
        print("-" * 60)

        api_key = "api-key-abc123"
        for i in range(3):
            try:
                result = client.consume(api_key, 1000, timedelta(hours=1))
                print(f"API Call {i+1}: {'✓ OK' if result['ok'] else '✗ LIMITED'}")
                print(f"  Tokens left: {result['remaining']}")
            except TollMeshError as e:
                print(f"API Call {i+1}: ERROR - {e.message}")

        # Example 3: Simulate hitting the limit
        print("\n3. Simulate Rate Limit (10 requests per second)")
        print("-" * 60)

        api_endpoint = "GET /api/search"
        limit = 10
        window = timedelta(seconds=1)

        print(f"Making 12 requests with {limit} req/sec limit...")
        successful = 0
        limited = 0

        for i in range(12):
            try:
                result = client.consume(api_endpoint, limit, window)
                if result['ok']:
                    successful += 1
                    print(f"  Request {i+1:2d}: ✓ OK (remaining: {result['remaining']})")
                else:
                    limited += 1
                    reset_time = datetime.fromtimestamp(result['reset_at'] / 1000)
                    print(f"  Request {i+1:2d}: ✗ LIMITED (reset: {reset_time.strftime('%H:%M:%S')})")

                # Small delay between requests
                time.sleep(0.05)
            except TollMeshError as e:
                limited += 1
                print(f"  Request {i+1:2d}: ✗ ERROR - {e.message}")

        print(f"\nResults: {successful} allowed, {limited} limited")

        # Example 4: Different rate limits for different tiers
        print("\n4. Tier-Based Rate Limiting")
        print("-" * 60)

        tiers = {
            "free": {"limit": 10, "window": timedelta(minutes=1)},
            "pro": {"limit": 100, "window": timedelta(minutes=1)},
            "enterprise": {"limit": 1000, "window": timedelta(minutes=1)},
        }

        for tier, config_tier in tiers.items():
            try:
                result = client.consume(
                    f"user-tier-{tier}",
                    config_tier["limit"],
                    config_tier["window"]
                )
                status = "✓ OK" if result['ok'] else "✗ LIMITED"
                print(f"{tier.upper():12s}: {status} ({result['remaining']:4d} remaining)")
            except TollMeshError as e:
                print(f"{tier.upper():12s}: ✗ ERROR - {e.message}")

        # Example 5: Health check
        print("\n5. Server Health Status")
        print("-" * 60)

        try:
            health = client.health()
            print(f"Status: {health['status']}")
            print(f"Node: {health['node']}")
            print(f"Connected Peers: {health['peers']}")
            if 'stats' in health:
                stats = health['stats']
                print(f"Uptime: {stats.get('uptime_seconds', 0)}s")
                print(f"Total Requests: {stats.get('requests_total', 0)}")
                print(f"P99 Latency: {stats.get('latency_p99_ms', 0):.2f}ms")
        except TollMeshError as e:
            print(f"ERROR: {e.message}")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n" + "=" * 60)
        print("Example complete!")
        print("=" * 60)


if __name__ == "__main__":
    main()
