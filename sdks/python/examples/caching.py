#!/usr/bin/env python3
"""
Caching Example - TollMeshCache Python SDK

Demonstrates how to use TollMeshCache for distributed caching.
"""

import json
from datetime import timedelta
import sys
sys.path.insert(0, '../')

from tollmeshcache import Client, ClientConfig, TollMeshError


def simulate_database_query(user_id: str) -> dict:
    """Simulate an expensive database query"""
    print(f"    [DB] Fetching user {user_id} from database...")
    return {
        "id": user_id,
        "name": f"User {user_id}",
        "email": f"user{user_id}@example.com",
        "created_at": "2024-01-01T00:00:00Z"
    }


def simulate_api_call(endpoint: str) -> dict:
    """Simulate an expensive API call"""
    print(f"    [API] Calling {endpoint}...")
    return {
        "status": "success",
        "data": [1, 2, 3, 4, 5],
        "timestamp": "2024-01-01T00:00:00Z"
    }


def main():
    config = ClientConfig(
        host="localhost",
        port=8080,
        timeout=5.0,
    )

    client = Client(config)

    print("=" * 60)
    print("TollMeshCache - Distributed Caching Example")
    print("=" * 60)

    try:
        # Example 1: Simple cache get/set
        print("\n1. Simple Cache Operations")
        print("-" * 60)

        namespace = "users"
        key = "user-123"
        value = json.dumps({
            "id": "123",
            "name": "Alice",
            "email": "alice@example.com"
        })

        print(f"Setting {namespace}/{key}...")
        client.cache_set(namespace, key, value, ttl=timedelta(hours=1))
        print("✓ Set successful")

        print(f"Getting {namespace}/{key}...")
        cached_value, exists = client.cache_get(namespace, key)
        if exists:
            print(f"✓ Found: {cached_value}")
        else:
            print("✗ Not found")

        # Example 2: Cache-aside pattern (lazy loading)
        print("\n2. Cache-Aside Pattern (Lazy Loading)")
        print("-" * 60)

        def get_user(user_id: str) -> dict:
            """Get user with caching"""
            namespace = "user_profiles"
            key = user_id

            # Try cache first
            print(f"Checking cache for {user_id}...")
            cached_value, exists = client.cache_get(namespace, key)

            if exists:
                print("✓ Cache hit!")
                return json.loads(cached_value)

            # Cache miss - fetch from database
            print("✗ Cache miss - fetching from source...")
            user_data = simulate_database_query(user_id)

            # Store in cache
            print(f"  Caching result (1 hour TTL)...")
            client.cache_set(
                namespace,
                key,
                json.dumps(user_data),
                ttl=timedelta(hours=1)
            )

            return user_data

        # First call - cache miss
        print("\nFirst call to get_user('user-456'):")
        user1 = get_user("user-456")
        print(f"Result: {user1['name']}")

        # Second call - cache hit
        print("\nSecond call to get_user('user-456'):")
        user2 = get_user("user-456")
        print(f"Result: {user2['name']}")

        # Example 3: Multiple cache namespaces
        print("\n3. Multiple Cache Namespaces")
        print("-" * 60)

        namespaces = {
            "users": {"ttl": timedelta(hours=1), "sample_key": "user-789"},
            "posts": {"ttl": timedelta(days=1), "sample_key": "post-101"},
            "comments": {"ttl": timedelta(hours=6), "sample_key": "comment-202"},
            "sessions": {"ttl": timedelta(hours=1), "sample_key": "session-abc123"},
        }

        for ns, config in namespaces.items():
            data = {"namespace": ns, "timestamp": "2024-01-01"}
            client.cache_set(
                ns,
                config["sample_key"],
                json.dumps(data),
                ttl=config["ttl"]
            )
            print(f"✓ Cached {ns}/{config['sample_key']} (TTL: {config['ttl']})")

        # Example 4: Cache invalidation
        print("\n4. Cache Invalidation Patterns")
        print("-" * 60)

        # Store some data
        namespace = "config"
        key = "app_settings"
        config_data = {"theme": "dark", "language": "en"}

        client.cache_set(namespace, key, json.dumps(config_data), ttl=timedelta(hours=1))
        print(f"✓ Cached {namespace}/{key}")

        # Verify it's there
        value, exists = client.cache_get(namespace, key)
        print(f"✓ Retrieved: {value}")

        # Invalidate by setting with 0 TTL (or deleting)
        print("Invalidating cache (not yet supported - manual cleanup on update)...")

        # Update and recache
        config_data["theme"] = "light"
        client.cache_set(namespace, key, json.dumps(config_data), ttl=timedelta(hours=1))
        print(f"✓ Updated and recached {namespace}/{key}")

        # Example 5: Cache different data types
        print("\n5. Caching Different Data Types")
        print("-" * 60)

        test_cases = [
            ("strings", "greeting", "Hello, World!"),
            ("numbers", "pi", json.dumps(3.14159)),
            ("objects", "person", json.dumps({"name": "Bob", "age": 30})),
            ("arrays", "scores", json.dumps([95, 87, 92, 88, 91])),
        ]

        for namespace, key, value in test_cases:
            client.cache_set(namespace, key, value, ttl=timedelta(hours=1))
            cached_val, exists = client.cache_get(namespace, key)
            print(f"✓ {namespace:10s}/{key:10s}: {cached_val[:50]}")

        # Example 6: Cache statistics
        print("\n6. Server Statistics")
        print("-" * 60)

        try:
            health = client.health()
            print(f"Cache Status: {health['status']}")
            if 'stats' in health:
                stats = health['stats']
                print(f"Cache Hit Rate: {stats.get('cache_hit_rate', 0):.1%}")
                print(f"Total Requests: {stats.get('requests_total', 0)}")
        except TollMeshError as e:
            print(f"Health check error: {e.message}")

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
