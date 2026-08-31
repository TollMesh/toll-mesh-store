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

    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to TollMeshCache server

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., "/consume")
            data: Request body data

        Returns:
            Response JSON as dictionary

        Raises:
            TollMeshError: If request fails or server returns error
        """
        url = self.config.base_url + endpoint

        try:
            if method == "GET":
                response = self.session.get(url, timeout=self.config.timeout, verify=self.config.verify_ssl)
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
                message = error_data.get("message", str(e))
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
