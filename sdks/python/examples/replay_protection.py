#!/usr/bin/env python3
"""
Replay Protection Example - TollMeshCache Python SDK

Demonstrates how to use TollMeshCache for preventing replay attacks.
"""

import uuid
from datetime import timedelta
import sys
sys.path.insert(0, '../')

from tollmeshcache import Client, ClientConfig, TollMeshError, ReplayError


def main():
    config = ClientConfig(
        host="localhost",
        port=8080,
        timeout=5.0,
    )

    client = Client(config)

    print("=" * 60)
    print("TollMeshCache - Replay Protection Example")
    print("=" * 60)

    try:
        # Example 1: Basic replay detection
        print("\n1. Basic Replay Detection")
        print("-" * 60)

        # Create a unique request ID
        request_id = str(uuid.uuid4())
        ttl = timedelta(minutes=5)

        print(f"Processing request: {request_id}")
        print(f"Tracking for {ttl.total_seconds():.0f} seconds...")

        # First time - should not be seen
        result = client.seen(request_id, ttl)
        if not result["seen"]:
            print("✓ First request - Processing transaction")
        else:
            print("✗ Replay detected!")

        # Second time - replay detected
        print(f"\nAttempting replay with same ID: {request_id}")
        result = client.seen(request_id, ttl)
        if result["seen"]:
            print("✓ Replay correctly detected!")
        else:
            print("✗ Replay not detected (unexpected)")

        # Example 2: Payment transaction protection
        print("\n2. Payment Transaction Protection")
        print("-" * 60)

        def process_payment(amount: float, idempotency_key: str) -> bool:
            """
            Process a payment with replay protection

            Args:
                amount: Payment amount
                idempotency_key: Unique key for this transaction

            Returns:
                True if processed, False if replay detected
            """
            # Check if this transaction was already processed
            if client.seen(idempotency_key, ttl=timedelta(hours=24))["seen"]:
                print(f"  ✗ Replay detected - transaction already processed")
                return False

            # Process the payment
            print(f"  ✓ Processing ${amount:.2f} payment")
            # In real code: charge credit card, update database, etc.
            return True

        # First payment attempt
        print("First payment attempt:")
        key1 = "payment-" + str(uuid.uuid4())
        success1 = process_payment(99.99, key1)

        # Retry with same key (should be rejected)
        print("\nRetrying same payment:")
        success2 = process_payment(99.99, key1)

        # Different payment (should work)
        print("\nDifferent payment:")
        key2 = "payment-" + str(uuid.uuid4())
        success3 = process_payment(149.99, key2)

        # Example 3: API request deduplication
        print("\n3. API Request Deduplication")
        print("-" * 60)

        def api_create_user(name: str, email: str, request_id: str) -> dict:
            """Create user with request deduplication"""
            # Prevent duplicate API calls within 1 hour
            if client.seen(request_id, ttl=timedelta(hours=1))["seen"]:
                print(f"  ✗ Duplicate request - skipping")
                return {"error": "duplicate_request"}

            # Create user
            print(f"  ✓ Creating user: {name} ({email})")
            user = {
                "id": str(uuid.uuid4()),
                "name": name,
                "email": email,
            }
            return {"success": True, "user": user}

        # First request
        req_id = "req-create-user-" + str(uuid.uuid4())
        print("First request:")
        result1 = api_create_user("Alice", "alice@example.com", req_id)

        # Duplicate request
        print("\nDuplicate request (same request_id):")
        result2 = api_create_user("Alice", "alice@example.com", req_id)

        # Example 4: Distributed request tracking
        print("\n4. Distributed Request Tracking")
        print("-" * 60)

        requests_to_track = [
            ("order-" + str(uuid.uuid4()), "order-123"),
            ("transfer-" + str(uuid.uuid4()), "transfer-456"),
            ("refund-" + str(uuid.uuid4()), "refund-789"),
        ]

        print("Tracking requests across cluster:")
        for nonce, description in requests_to_track:
            result = client.seen(nonce, ttl=timedelta(hours=6))
            status = "Already seen" if result["seen"] else "New request"
            print(f"  {description:20s}: {status}")

        # Try again - should all be marked as seen
        print("\nRetrying same requests:")
        for nonce, description in requests_to_track:
            result = client.seen(nonce, ttl=timedelta(hours=6))
            status = "Already seen" if result["seen"] else "New request"
            print(f"  {description:20s}: {status}")

        # Example 5: Nonce expiration
        print("\n5. Nonce Expiration")
        print("-" * 60)

        short_ttl_nonce = "short-" + str(uuid.uuid4())
        print(f"Tracking nonce for 1 second: {short_ttl_nonce}")

        result1 = client.seen(short_ttl_nonce, ttl=timedelta(seconds=1))
        print(f"First check: {'Seen' if result1['seen'] else 'Not seen'}")

        import time
        print("Waiting 2 seconds...")
        time.sleep(2)

        result2 = client.seen(short_ttl_nonce, ttl=timedelta(seconds=10))
        print(f"Second check (after expiry): {'Seen' if result2['seen'] else 'Not seen'}")

        # Example 6: Error handling
        print("\n6. Replay Error Handling")
        print("-" * 60)

        nonce = "error-" + str(uuid.uuid4())

        try:
            # Mark as seen
            client.seen(nonce, ttl=timedelta(minutes=5))
            print("✓ First request processed")

            # Try again with same nonce
            result = client.seen(nonce, ttl=timedelta(minutes=5))
            if result["seen"]:
                raise ReplayError(nonce)

        except ReplayError as e:
            print(f"✓ Caught replay error: {e.message}")
            print(f"  Nonce: {e.nonce}")
        except TollMeshError as e:
            print(f"✗ Unexpected error: {e.message}")

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
