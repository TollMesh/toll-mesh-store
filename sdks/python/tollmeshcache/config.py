"""
Configuration management for TollMeshCache client
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ClientConfig:
    """Configuration for TollMeshCache client"""

    host: str = "localhost"
    """Server hostname or IP address"""

    port: int = 8080
    """Server port"""

    timeout: float = 5.0
    """Request timeout in seconds"""

    verify_ssl: bool = True
    """Verify SSL certificates (for HTTPS)"""

    api_key: Optional[str] = None
    """Optional API key for authentication"""

    http_scheme: str = "http"
    """HTTP scheme: 'http' or 'https'"""

    max_retries: int = 3
    """Maximum number of retries for failed requests"""

    retry_backoff: float = 1.0
    """Retry backoff multiplier (exponential backoff)"""

    connection_pool_size: int = 10
    """Number of HTTP connections to pool"""

    read_timeout: Optional[float] = None
    """Read timeout in seconds (None = use timeout)"""

    write_timeout: Optional[float] = None
    """Write timeout in seconds (None = use timeout)"""

    user_agent: str = field(default_factory=lambda: "tollmeshcache-python/1.0.0")
    """User-Agent header value"""

    @property
    def base_url(self) -> str:
        """Get the base URL for API requests"""
        return f"{self.http_scheme}://{self.host}:{self.port}"

    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.host is None or not self.host.strip():
            raise ValueError("host cannot be empty")
        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"port must be between 1 and 65535, got {self.port}")
        if self.timeout <= 0:
            raise ValueError(f"timeout must be positive, got {self.timeout}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries cannot be negative, got {self.max_retries}")
        if self.http_scheme not in ("http", "https"):
            raise ValueError(f"http_scheme must be 'http' or 'https', got {self.http_scheme}")
