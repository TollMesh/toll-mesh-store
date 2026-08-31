"""
Async tests for TollMeshCache Python SDK
"""

import pytest
import pytest_asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import json

from tollmeshcache import AsyncClient, ClientConfig
from tollmeshcache.errors import TollMeshError, ErrorCode


@pytest.fixture
def async_client_config():
    """Create a test async client configuration"""
    return ClientConfig(
        host="localhost",
        port=8080,
        timeout=5.0,
    )


@pytest_asyncio.fixture
async def async_client(async_client_config):
    """Create a test async client"""
    client = AsyncClient(async_client_config)
    yield client
    await client.close()


class TestAsyncClient:
    """Tests for AsyncClient"""

    @pytest.mark.asyncio
    async def test_async_client_creation(self, async_client):
        """Test async client creation"""
        assert async_client is not None
        assert async_client.config.host == "localhost"

    @pytest.mark.asyncio
    async def test_async_context_manager(self, async_client_config):
        """Test async client as context manager"""
        async with AsyncClient(async_client_config) as client:
            assert client is not None

    @pytest.mark.asyncio
    async def test_consume_success(self, async_client):
        """Test successful consume"""
        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "ok": True,
                "remaining": 99,
                "reset_at": 1234567890000,
            }
            mock_post.return_value = mock_response

            result = await async_client.consume("user-123", 100, timedelta(minutes=1))

            assert result["ok"] is True
            assert result["remaining"] == 99
            assert result["reset_at"] == 1234567890000

    @pytest.mark.asyncio
    async def test_seen_replay(self, async_client):
        """Test seen when replay"""
        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {"seen": True}
            mock_post.return_value = mock_response

            result = await async_client.seen("nonce-123", timedelta(minutes=5))

            assert result["seen"] is True

    @pytest.mark.asyncio
    async def test_cache_get_hit(self, async_client):
        """Test cache get with hit"""
        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "value": "test-value",
                "exists": True,
            }
            mock_post.return_value = mock_response

            value, exists = await async_client.cache_get("users", "user-123")

            assert value == "test-value"
            assert exists is True

    @pytest.mark.asyncio
    async def test_cache_set(self, async_client):
        """Test cache set"""
        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {}
            mock_post.return_value = mock_response

            await async_client.cache_set("users", "user-123", "test-value", timedelta(hours=1))
            mock_post.assert_called_once()

    @pytest.mark.asyncio
    async def test_health(self, async_client):
        """Test health check"""
        with patch.object(async_client.client, 'get', new_callable=AsyncMock) as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "status": "healthy",
                "node": "node-1",
                "peers": 3,
            }
            mock_get.return_value = mock_response

            health = await async_client.health()

            assert health["status"] == "healthy"
            assert health["peers"] == 3

    @pytest.mark.asyncio
    async def test_get_peers(self, async_client):
        """Test get peers"""
        with patch.object(async_client.client, 'get', new_callable=AsyncMock) as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "peers": [
                    {"id": "node-1", "address": "localhost", "port": 8080, "latency_ms": 5},
                    {"id": "node-2", "address": "localhost", "port": 8081, "latency_ms": 10},
                ]
            }
            mock_get.return_value = mock_response

            peers = await async_client.get_peers()

            assert len(peers) == 2
            assert peers[0]["id"] == "node-1"


class TestAsyncConcurrency:
    """Tests for async concurrency"""

    @pytest.mark.asyncio
    async def test_concurrent_requests(self, async_client):
        """Test multiple concurrent requests"""
        import asyncio

        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "ok": True,
                "remaining": 99,
                "reset_at": 1234567890000,
            }
            mock_post.return_value = mock_response

            # Create 5 concurrent requests
            tasks = [
                async_client.consume(f"user-{i}", 100, timedelta(minutes=1))
                for i in range(5)
            ]

            results = await asyncio.gather(*tasks)

            assert len(results) == 5
            assert all(r["ok"] is True for r in results)
            assert mock_post.call_count >= 5

    @pytest.mark.asyncio
    async def test_concurrent_cache_operations(self, async_client):
        """Test concurrent cache operations"""
        import asyncio

        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            # First call sets cache
            set_response = MagicMock()
            set_response.json.return_value = {}

            # Second call gets cache
            get_response = MagicMock()
            get_response.json.return_value = {
                "value": "test-data",
                "exists": True,
            }

            mock_post.side_effect = [set_response, get_response]

            # Concurrent set and get
            async def set_cache():
                await async_client.cache_set("data", "key-1", "test-data", timedelta(hours=1))

            async def get_cache():
                return await async_client.cache_get("data", "key-1")

            tasks = [set_cache(), get_cache()]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # At least one should complete without exception
            assert any(r is None or not isinstance(r, Exception) for r in results)


class TestAsyncErrorHandling:
    """Tests for async error handling"""

    @pytest.mark.asyncio
    async def test_async_error_handling(self, async_client):
        """Test async error handling"""
        import httpx

        with patch.object(async_client.client, 'post', new_callable=AsyncMock) as mock_post:
            # Simulate HTTP error
            error_response = MagicMock()
            error_response.status_code = 500
            error_response.json.return_value = {
                "code": 500,
                "message": "Internal server error",
            }

            http_error = httpx.HTTPStatusError("Error", request=MagicMock(), response=error_response)
            mock_post.side_effect = http_error

            with pytest.raises(TollMeshError):
                await async_client.consume("key", 100, timedelta(minutes=1))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
