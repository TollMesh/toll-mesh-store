"""
TollMeshCache HTTP Client implementation
"""

import requests
import json
from typing import Optional, Dict, Any, Tuple
from datetime import timedelta

from .config import ClientConfig
from .errors import TollMeshError, ErrorCode


class Client:
    """TollMeshCache HTTP client for distributed caching and coordination"""

    def __init__(self, config: Optional[ClientConfig] = None):
        """
        Initialize TollMeshCache client

        Args:
            config: ClientConfig instance (uses defaults if None)
        """
        self.config = config or ClientConfig()
        self.session = requests.Session()

        if self.config.api_key:
            self.session.headers.update({"X-API-Key": self.config.api_key})

        self.session.headers.update({"Content-Type": "application/json"})

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict:
        """
        Make HTTP request to TollMeshCache server

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., "/consume")
            data: Request body data (POST)
            params: Query string parameters (GET)

        Returns:
            Response JSON as dictionary

        Raises:
            TollMeshError: If request fails or server returns error
        """
        url = self.config.base_url + endpoint

        try:
            if method == "GET":
                response = self.session.get(
                    url, params=params, timeout=self.config.timeout, verify=self.config.verify_ssl
                )
            elif method == "POST":
                response = self.session.post(
                    url,
                    json=data,
                    timeout=self.config.timeout,
                    verify=self.config.verify_ssl
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.ConnectionError as e:
            raise TollMeshError(
                ErrorCode.UNAVAILABLE,
                f"Failed to connect to {self.config.base_url}: {str(e)}"
            )
        except requests.exceptions.Timeout as e:
            raise TollMeshError(
                ErrorCode.UNAVAILABLE,
                f"Request timeout: {str(e)}"
            )
        except requests.exceptions.HTTPError as e:
            response = e.response
            try:
                error_data = response.json()
                code = ErrorCode(error_data.get("code", ErrorCode.INTERNAL.value))
                # /consume, /seen, /cache/* use "error"; the job queue, sorted
                # set, and stream endpoints use ErrorResponse{"error": ...}
                # from api/http.go -- same key, kept consistent deliberately.
                message = error_data.get("message") or error_data.get("error") or str(e)
            except:
                code = ErrorCode(response.status_code)
                message = response.text or str(e)

            raise TollMeshError(code, message)
        except json.JSONDecodeError as e:
            raise TollMeshError(
                ErrorCode.INTERNAL,
                f"Invalid JSON response: {str(e)}"
            )

    def consume(
        self,
        key: str,
        limit: int,
        window: timedelta
    ) -> Dict[str, Any]:
        """
        Check and consume rate limit tokens

        Args:
            key: Rate limit key (e.g., "user-123", "api-key-abc")
            limit: Maximum tokens allowed per window
            window: Time window duration

        Returns:
            Dictionary with keys:
                - ok (bool): Whether request is allowed
                - remaining (int): Tokens remaining
                - reset_at (int): Unix timestamp when limit resets

        Raises:
            TollMeshError: If operation fails

        Example:
            >>> client = Client()
            >>> result = client.consume("user-123", limit=100, window=timedelta(minutes=1))
            >>> if result["ok"]:
            ...     # Process request
            ... else:
            ...     # Handle rate limit
            ...     print(f"Rate limited. Reset at {result['reset_at']}")
        """
        data = {
            "key": key,
            "limit": limit,
            "window": int(window.total_seconds() * 1000)  # Convert to milliseconds
        }
        return self._request("POST", "/consume", data)

    def seen(
        self,
        key: str,
        ttl: timedelta
    ) -> Dict[str, bool]:
        """
        Check replay protection - check if nonce was already seen

        Args:
            key: Nonce or unique identifier
            ttl: Time-to-live for tracking this nonce

        Returns:
            Dictionary with key:
                - seen (bool): True if already seen (replay detected)

        Raises:
            TollMeshError: If operation fails

        Example:
            >>> result = client.seen("request-id-123", ttl=timedelta(minutes=5))
            >>> if result["seen"]:
            ...     raise Exception("Replay attack detected!")
        """
        data = {
            "key": key,
            "ttl": int(ttl.total_seconds() * 1000)  # Convert to milliseconds
        }
        return self._request("POST", "/seen", data)

    def cache_get(self, namespace: str, key: str) -> Tuple[Optional[str], bool]:
        """
        Get value from distributed cache

        Args:
            namespace: Cache namespace (e.g., "user-profiles")
            key: Cache key within namespace

        Returns:
            Tuple of (value, exists):
                - value (str or None): Cached value or None
                - exists (bool): Whether key exists and is not expired

        Raises:
            TollMeshError: If operation fails

        Example:
            >>> value, exists = client.cache_get("users", "user-123")
            >>> if exists:
            ...     print(f"Cached value: {value}")
            ... else:
            ...     # Fetch from source
            ...     value = fetch_user_data("user-123")
            ...     client.cache_set("users", "user-123", value, ttl=timedelta(hours=1))
        """
        data = {"namespace": namespace, "key": key}
        response = self._request("POST", "/cache/get", data)
        return response.get("value"), response.get("exists", False)

    def cache_set(
        self,
        namespace: str,
        key: str,
        value: str,
        ttl: Optional[timedelta] = None
    ) -> None:
        """
        Set value in distributed cache

        Args:
            namespace: Cache namespace
            key: Cache key within namespace
            value: Value to cache (string or JSON-serializable)
            ttl: Time-to-live (None = no expiration)

        Raises:
            TollMeshError: If operation fails

        Example:
            >>> user_data = json.dumps({"name": "Alice", "email": "alice@example.com"})
            >>> client.cache_set("users", "user-123", user_data, ttl=timedelta(hours=1))
        """
        data = {
            "namespace": namespace,
            "key": key,
            "value": value,
        }
        if ttl:
            data["ttl"] = int(ttl.total_seconds() * 1000)  # Convert to milliseconds

        self._request("POST", "/cache/set", data)

    def health(self) -> Dict[str, Any]:
        """
        Check server health

        Returns:
            Dictionary with keys:
                - status (str): "healthy", "degraded", or "unhealthy"
                - node (str): Node ID/name
                - peers (int): Number of connected peers
                - stats (dict): Operational statistics

        Raises:
            TollMeshError: If operation fails

        Example:
            >>> health = client.health()
            >>> print(f"Status: {health['status']}, Peers: {health['peers']}")
        """
        return self._request("GET", "/health")

    def get_peers(self) -> list:
        """
        Get list of connected cluster peers

        Returns:
            List of peer dictionaries with keys:
                - id (str): Peer ID
                - address (str): Peer address
                - port (int): Peer port
                - latency_ms (int): Latency to peer in milliseconds

        Raises:
            TollMeshError: If operation fails
        """
        response = self._request("GET", "/peers")
        return response.get("peers", [])

    # ===== Job Queues =====

    def enqueue(
        self,
        queue: str,
        payload: str,
        priority: int = 5,
        max_retries: int = 3,
        deadline: Optional[timedelta] = None,
    ) -> Dict[str, Any]:
        """
        Enqueue a job for distributed processing

        Args:
            queue: Queue name
            payload: Job payload (string)
            priority: Job priority, 0-10, higher runs first (default 5)
            max_retries: Maximum retry attempts on failure (default 3)
            deadline: Time until the job expires (default 24 hours)

        Returns:
            The created job as a dictionary, including its "id"

        Example:
            >>> job = client.enqueue("tasks", "process-order-42", priority=8)
            >>> job["id"]
        """
        data = {
            "queue": queue,
            "payload": payload,
            "priority": priority,
            "max_retries": max_retries,
        }
        if deadline is not None:
            data["deadline"] = int(deadline.total_seconds() * 1000)
        return self._request("POST", "/queue/enqueue", data)

    def claim(self, queue: str, worker_id: str) -> Dict[str, Any]:
        """
        Claim the next available job from a queue

        Args:
            queue: Queue name
            worker_id: Identifier for the claiming worker

        Returns:
            The claimed job as a dictionary

        Raises:
            TollMeshError: If no claimable job is available

        Example:
            >>> job = client.claim("tasks", "worker-1")
            >>> # ... process job["payload"] ...
            >>> client.complete("tasks", job["id"], "done")
        """
        return self._request("POST", "/queue/claim", {"queue": queue, "worker_id": worker_id})

    def complete(self, queue: str, job_id: str, result: str = "") -> None:
        """
        Mark a claimed job as completed

        Args:
            queue: Queue name
            job_id: ID of the job to complete
            result: Optional result payload
        """
        self._request("POST", "/queue/complete", {"queue": queue, "job_id": job_id, "result": result})

    def fail(self, queue: str, job_id: str, error: str) -> None:
        """
        Mark a claimed job as failed, triggering retry or dead-lettering

        Args:
            queue: Queue name
            job_id: ID of the job that failed
            error: Error message describing the failure
        """
        self._request("POST", "/queue/fail", {"queue": queue, "job_id": job_id, "error": error})

    def job_status(self, queue: str, job_id: str) -> Dict[str, Any]:
        """
        Get the current status of a job

        Args:
            queue: Queue name
            job_id: ID of the job to look up

        Returns:
            The job as a dictionary, including its current "status"
        """
        return self._request("GET", "/queue/status", params={"queue": queue, "job_id": job_id})

    def queue_stats(self, queue: str) -> Dict[str, Any]:
        """
        Get statistics for a queue

        Args:
            queue: Queue name

        Returns:
            Dictionary with keys such as total_jobs, pending, processing
        """
        return self._request("GET", "/queue/stats", params={"queue": queue})

    # ===== Sorted Sets =====

    def zadd(self, key: str, score: float, member: str) -> None:
        """
        Add or update a member's score in a sorted set

        Args:
            key: Sorted set name
            score: Score for ranking
            member: Member identifier

        Example:
            >>> client.zadd("leaderboard", 100, "alice")
        """
        self._request("POST", "/zset/add", {"key": key, "member": member, "score": score})

    def zrem(self, key: str, member: str) -> None:
        """Remove a member from a sorted set"""
        self._request("POST", "/zset/remove", {"key": key, "member": member})

    def zscore(self, key: str, member: str) -> Tuple[Optional[float], bool]:
        """
        Get a member's score

        Returns:
            Tuple of (score, exists)
        """
        response = self._request("GET", "/zset/score", params={"key": key, "member": member})
        return response.get("score"), response.get("exists", False)

    def zrank(self, key: str, member: str) -> Tuple[Optional[int], bool]:
        """
        Get a member's ascending-order rank (0 = lowest score)

        Returns:
            Tuple of (rank, exists)
        """
        response = self._request("GET", "/zset/rank", params={"key": key, "member": member})
        return response.get("rank"), response.get("exists", False)

    def zrevrank(self, key: str, member: str) -> Tuple[Optional[int], bool]:
        """
        Get a member's descending-order rank (0 = highest score)

        Returns:
            Tuple of (rank, exists)
        """
        response = self._request("GET", "/zset/revrank", params={"key": key, "member": member})
        return response.get("rank"), response.get("exists", False)

    def zrange(self, key: str, min: float = float("-inf"), max: float = float("inf"), limit: int = 100) -> list:
        """
        Get members with scores in [min, max], ascending order

        Returns:
            List of member dictionaries (each with "member" and "score")
        """
        response = self._request(
            "GET", "/zset/range", params={"key": key, "min": min, "max": max, "limit": limit}
        )
        return response.get("members") or []

    def zrevrange(self, key: str, max: float = float("inf"), min: float = float("-inf"), limit: int = 100) -> list:
        """
        Get members with scores in [min, max], descending order (highest first)

        Args:
            key: Sorted set name
            max: Upper score bound (checked first, matching Redis ZREVRANGEBYSCORE)
            min: Lower score bound
            limit: Maximum number of members to return

        Returns:
            List of member dictionaries, highest score first

        Example:
            >>> top_10 = client.zrevrange("leaderboard", limit=10)
        """
        response = self._request(
            "GET", "/zset/revrange", params={"key": key, "max": max, "min": min, "limit": limit}
        )
        return response.get("members") or []

    def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set"""
        response = self._request("GET", "/zset/card", params={"key": key})
        return response.get("card", 0)

    # ===== Streams =====

    def xadd(self, stream: str, fields: Dict[str, str]) -> Dict[str, Any]:
        """
        Append a new entry to a stream

        Args:
            stream: Stream name
            fields: Event data as string key-value pairs

        Returns:
            The created entry as a dictionary, including its "id"

        Example:
            >>> entry = client.xadd("events", {"type": "login", "user": "alice"})
        """
        return self._request("POST", "/stream/add", {"stream": stream, "fields": fields})

    def xrange(self, stream: str, start: str = "0", end: str = "-", limit: int = 100) -> list:
        """
        Get entries from a stream between start and end IDs

        Args:
            stream: Stream name
            start: Starting entry ID ("0" = beginning)
            end: Ending entry ID ("-" = most recent)
            limit: Maximum number of entries to return

        Returns:
            List of entry dictionaries
        """
        response = self._request(
            "GET", "/stream/range", params={"stream": stream, "start": start, "end": end, "limit": limit}
        )
        return response.get("entries") or []

    def xlen(self, stream: str) -> int:
        """Get the number of entries in a stream"""
        response = self._request("GET", "/stream/len", params={"stream": stream})
        return response.get("length", 0)

    def xgroup_create(self, stream: str, group: str) -> None:
        """Create a consumer group for a stream"""
        self._request("POST", "/stream/group/create", {"stream": stream, "group": group})

    def xreadgroup(self, group: str, consumer: str, stream: str, limit: int = 100) -> list:
        """
        Read unacknowledged entries for a consumer in a group

        First call for a given consumer registers it in the group. Entries
        remain re-deliverable until acknowledged with xack.

        Args:
            group: Consumer group name
            consumer: Consumer identifier
            stream: Stream name
            limit: Maximum number of entries to return

        Returns:
            List of entry dictionaries

        Example:
            >>> client.xgroup_create("events", "analytics")
            >>> entries = client.xreadgroup("analytics", "worker-1", "events")
            >>> for entry in entries:
            ...     # ... process entry["fields"] ...
            ...     client.xack("events", "analytics", "worker-1", entry["id"])
        """
        response = self._request(
            "POST",
            "/stream/group/read",
            {"stream": stream, "group": group, "consumer": consumer, "limit": limit},
        )
        return response.get("entries") or []

    def xack(self, stream: str, group: str, consumer: str, entry_id: str) -> None:
        """Acknowledge that a consumer has processed up to entry_id"""
        self._request(
            "POST",
            "/stream/group/ack",
            {"stream": stream, "group": group, "consumer": consumer, "id": entry_id},
        )

    def close(self) -> None:
        """Close the client and cleanup resources"""
        self.session.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Convenience functions for standalone usage
_default_client: Optional[Client] = None


def init(config: Optional[ClientConfig] = None) -> None:
    """Initialize default client"""
    global _default_client
    _default_client = Client(config)


def get_default_client() -> Client:
    """Get default client (initializes if needed)"""
    global _default_client
    if _default_client is None:
        _default_client = Client()
    return _default_client
