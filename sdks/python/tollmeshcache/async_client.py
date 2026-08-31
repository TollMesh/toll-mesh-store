"""
Asynchronous TollMeshCache Client implementation
"""

import httpx
from typing import Optional, Dict, Any, Tuple
from datetime import timedelta

from .config import ClientConfig
from .errors import TollMeshError, ErrorCode


class AsyncClient:
    """Async TollMeshCache client for async/await applications"""

    def __init__(self, config: Optional[ClientConfig] = None):
        """
        Initialize async TollMeshCache client

        Args:
            config: ClientConfig instance (uses defaults if None)
        """
        self.config = config or ClientConfig()
        self.client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            limits=httpx.Limits(max_connections=self.config.connection_pool_size),
        )

        if self.config.api_key:
            self.client.headers.update({"X-API-Key": self.config.api_key})

        self.client.headers.update({
            "Content-Type": "application/json",
            "User-Agent": self.config.user_agent,
        })

    async def consume(
        self,
        key: str,
        limit: int,
        window: timedelta
    ) -> Dict[str, Any]:
        """
        Check and consume rate limit tokens (async)

        Args:
            key: Rate limit key
            limit: Maximum tokens per window
            window: Time window duration

        Returns:
            Dictionary with ok, remaining, reset_at

        Raises:
            TollMeshError: If operation fails
        """
        data = {
            "key": key,
            "limit": limit,
            "window": int(window.total_seconds() * 1000),
        }
        return await self._post("/consume", data)

    async def seen(
        self,
        key: str,
        ttl: timedelta
    ) -> Dict[str, bool]:
        """
        Check replay protection (async)

        Args:
            key: Nonce or unique identifier
            ttl: Time-to-live for tracking

        Returns:
            Dictionary with seen flag

        Raises:
            TollMeshError: If operation fails
        """
        data = {
            "key": key,
            "ttl": int(ttl.total_seconds() * 1000),
        }
        return await self._post("/seen", data)

    async def cache_get(self, namespace: str, key: str) -> Tuple[Optional[str], bool]:
        """
        Get value from distributed cache (async)

        Args:
            namespace: Cache namespace
            key: Cache key

        Returns:
            Tuple of (value, exists)

        Raises:
            TollMeshError: If operation fails
        """
        data = {"namespace": namespace, "key": key}
        response = await self._post("/cache/get", data)
        return response.get("value"), response.get("exists", False)

    async def cache_set(
        self,
        namespace: str,
        key: str,
        value: str,
        ttl: Optional[timedelta] = None
    ) -> None:
        """
        Set value in distributed cache (async)

        Args:
            namespace: Cache namespace
            key: Cache key
            value: Value to cache
            ttl: Time-to-live (None = no expiration)

        Raises:
            TollMeshError: If operation fails
        """
        data = {
            "namespace": namespace,
            "key": key,
            "value": value,
        }
        if ttl:
            data["ttl"] = int(ttl.total_seconds() * 1000)

        await self._post("/cache/set", data)

    async def health(self) -> Dict[str, Any]:
        """
        Check server health (async)

        Returns:
            Health status and statistics

        Raises:
            TollMeshError: If operation fails
        """
        return await self._get("/health")

    async def get_peers(self) -> list:
        """
        Get connected peers (async)

        Returns:
            List of connected peers

        Raises:
            TollMeshError: If operation fails
        """
        response = await self._get("/peers")
        return response.get("peers", [])

    async def _post(self, endpoint: str, data: Dict) -> Dict:
        """
        Make async POST request

        Args:
            endpoint: API endpoint
            data: Request body

        Returns:
            Response JSON

        Raises:
            TollMeshError: If request fails
        """
        try:
            response = await self.client.post(endpoint, json=data)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_error_response(e)
        except httpx.RequestError as e:
            raise TollMeshError(
                ErrorCode.UNAVAILABLE,
                f"Request failed: {str(e)}"
            )

    async def _get(self, endpoint: str) -> Dict:
        """
        Make async GET request

        Args:
            endpoint: API endpoint

        Returns:
            Response JSON

        Raises:
            TollMeshError: If request fails
        """
        try:
            response = await self.client.get(endpoint)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            await self._handle_error_response(e)
        except httpx.RequestError as e:
            raise TollMeshError(
                ErrorCode.UNAVAILABLE,
                f"Request failed: {str(e)}"
            )

    async def _handle_error_response(self, error: httpx.HTTPStatusError) -> None:
        """Handle error response"""
        try:
            error_data = error.response.json()
            code = ErrorCode(error_data.get("code", error.response.status_code))
            message = error_data.get("message", f"HTTP {error.response.status_code}")
        except Exception:
            code = ErrorCode(error.response.status_code)
            message = f"HTTP {error.response.status_code}"

        raise TollMeshError(code, message)

    async def close(self) -> None:
        """Close the client and cleanup resources"""
        await self.client.aclose()

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
