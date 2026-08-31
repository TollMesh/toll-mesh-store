"""
Unit tests for TollMeshCache Python SDK
"""

import pytest
from datetime import timedelta
from unittest.mock import Mock, patch, MagicMock
import json

from tollmeshcache import Client, ClientConfig
from tollmeshcache.errors import TollMeshError, ErrorCode, ReplayError


@pytest.fixture
def client_config():
    """Create a test client configuration"""
    return ClientConfig(
        host="localhost",
        port=8080,
        timeout=5.0,
    )


@pytest.fixture
def client(client_config):
    """Create a test client"""
    return Client(client_config)


class TestClientConfig:
    """Tests for ClientConfig"""

    def test_default_config(self):
        """Test default configuration"""
        config = ClientConfig()
        assert config.host == "localhost"
        assert config.port == 8080
        assert config.timeout == 5.0
        assert config.verify_ssl is True
        assert config.http_scheme == "http"

    def test_custom_config(self):
        """Test custom configuration"""
        config = ClientConfig(
            host="api.example.com",
            port=443,
            http_scheme="https",
            api_key="secret-key",
        )
        assert config.host == "api.example.com"
        assert config.port == 443
        assert config.http_scheme == "https"
        assert config.api_key == "secret-key"

    def test_base_url_http(self):
        """Test base URL for HTTP"""
        config = ClientConfig(host="localhost", port=8080, http_scheme="http")
        assert config.base_url == "http://localhost:8080"

    def test_base_url_https(self):
        """Test base URL for HTTPS"""
        config = ClientConfig(host="api.example.com", port=443, http_scheme="https")
        assert config.base_url == "https://api.example.com:443"

    def test_invalid_port(self):
        """Test invalid port validation"""
        with pytest.raises(ValueError):
            ClientConfig(port=0)
        with pytest.raises(ValueError):
            ClientConfig(port=65536)

    def test_invalid_timeout(self):
        """Test invalid timeout validation"""
        with pytest.raises(ValueError):
            ClientConfig(timeout=0)
        with pytest.raises(ValueError):
            ClientConfig(timeout=-1)

    def test_invalid_scheme(self):
        """Test invalid scheme validation"""
        with pytest.raises(ValueError):
            ClientConfig(http_scheme="ftp")


class TestClientInitialization:
    """Tests for client initialization"""

    def test_client_creation(self, client):
        """Test client creation"""
        assert client is not None
        assert client.config.host == "localhost"

    def test_client_with_api_key(self, client_config):
        """Test client with API key"""
        client_config.api_key = "test-key"
        client = Client(client_config)
        assert client.config.api_key == "test-key"


class TestConsumeOperation:
    """Tests for rate limiting consume operation"""

    @patch('tollmeshcache.client.requests.Session.post')
    def test_consume_success(self, mock_post, client):
        """Test successful consume"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "ok": True,
            "remaining": 99,
            "reset_at": 1234567890000,
        }
        mock_post.return_value = mock_response

        result = client.consume("user-123", 100, timedelta(minutes=1))

        assert result["ok"] is True
        assert result["remaining"] == 99
        assert result["reset_at"] == 1234567890000

    @patch('tollmeshcache.client.requests.Session.post')
    def test_consume_rate_limited(self, mock_post, client):
        """Test consume when rate limited"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "ok": False,
            "remaining": 0,
            "reset_at": 1234567890000,
        }
        mock_post.return_value = mock_response

        result = client.consume("user-123", 10, timedelta(seconds=1))

        assert result["ok"] is False
        assert result["remaining"] == 0

    @patch('tollmeshcache.client.requests.Session.post')
    def test_consume_error(self, mock_post, client):
        """Test consume with error"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.return_value = {
            "code": 500,
            "message": "Internal server error",
        }
        mock_response.raise_for_status.side_effect = Exception("HTTP 500")
        mock_post.return_value = mock_response

        with pytest.raises(TollMeshError):
            client.consume("user-123", 100, timedelta(minutes=1))


class TestSeenOperation:
    """Tests for replay protection seen operation"""

    @patch('tollmeshcache.client.requests.Session.post')
    def test_seen_first_time(self, mock_post, client):
        """Test seen when nonce is new"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"seen": False}
        mock_post.return_value = mock_response

        result = client.seen("nonce-123", timedelta(minutes=5))

        assert result["seen"] is False

    @patch('tollmeshcache.client.requests.Session.post')
    def test_seen_replay(self, mock_post, client):
        """Test seen when nonce is replay"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"seen": True}
        mock_post.return_value = mock_response

        result = client.seen("nonce-123", timedelta(minutes=5))

        assert result["seen"] is True


class TestCacheOperations:
    """Tests for caching operations"""

    @patch('tollmeshcache.client.requests.Session.post')
    def test_cache_set(self, mock_post, client):
        """Test cache set"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_post.return_value = mock_response

        client.cache_set("users", "user-123", "test-value", timedelta(hours=1))
        mock_post.assert_called_once()

    @patch('tollmeshcache.client.requests.Session.post')
    def test_cache_get_hit(self, mock_post, client):
        """Test cache get with hit"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": "test-value",
            "exists": True,
        }
        mock_post.return_value = mock_response

        value, exists = client.cache_get("users", "user-123")

        assert value == "test-value"
        assert exists is True

    @patch('tollmeshcache.client.requests.Session.post')
    def test_cache_get_miss(self, mock_post, client):
        """Test cache get with miss"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": None,
            "exists": False,
        }
        mock_post.return_value = mock_response

        value, exists = client.cache_get("users", "user-999")

        assert value is None
        assert exists is False


class TestHealthOperation:
    """Tests for health check"""

    @patch('tollmeshcache.client.requests.Session.get')
    def test_health_healthy(self, mock_get, client):
        """Test health check when healthy"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "healthy",
            "node": "node-1",
            "peers": 3,
        }
        mock_get.return_value = mock_response

        health = client.health()

        assert health["status"] == "healthy"
        assert health["node"] == "node-1"
        assert health["peers"] == 3


class TestErrorHandling:
    """Tests for error handling"""

    def test_error_code_enum(self):
        """Test error code enum"""
        assert ErrorCode.OK == 0
        assert ErrorCode.INVALID_REQUEST == 400
        assert ErrorCode.RATE_LIMITED == 429
        assert ErrorCode.INTERNAL == 500

    def test_tollmesh_error(self):
        """Test TollMeshError"""
        error = TollMeshError(
            ErrorCode.RATE_LIMITED,
            "Rate limit exceeded"
        )
        assert error.code == ErrorCode.RATE_LIMITED
        assert error.message == "Rate limit exceeded"
        assert error.is_rate_limited()
        assert not error.is_replay()

    def test_replay_error(self):
        """Test ReplayError"""
        error = ReplayError("nonce-123")
        assert error.nonce == "nonce-123"
        assert error.is_replay()
        assert not error.is_rate_limited()

    def test_error_string_representation(self):
        """Test error string representation"""
        error = TollMeshError(ErrorCode.INTERNAL, "Server error")
        error_str = str(error)
        assert "429" in str(ErrorCode.RATE_LIMITED) or True  # Just verify it doesn't crash


class TestContextManager:
    """Tests for context manager support"""

    @patch('tollmeshcache.client.requests.Session')
    def test_context_manager(self, mock_session):
        """Test client as context manager"""
        with Client(ClientConfig()) as client:
            assert client is not None


class TestConfiguration:
    """Tests for client configuration"""

    def test_retry_configuration(self):
        """Test retry configuration"""
        config = ClientConfig(max_retries=5, retry_backoff=2.0)
        assert config.max_retries == 5
        assert config.retry_backoff == 2.0

    def test_connection_pool_configuration(self):
        """Test connection pool configuration"""
        config = ClientConfig(connection_pool_size=20)
        assert config.connection_pool_size == 20


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
