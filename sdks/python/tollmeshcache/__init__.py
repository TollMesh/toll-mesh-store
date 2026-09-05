"""
TollMeshCache Python SDK - Distributed CRDT-based caching and coordination
"""

from .client import Client
from .config import ClientConfig

try:
    from .async_client import AsyncClient
except ImportError:
    # httpx is only required for async support (the "async" extra:
    # `pip install tollmeshcache[async]`). Importing tollmeshcache at all
    # used to require httpx unconditionally -- a plain `pip install
    # tollmeshcache` followed by `from tollmeshcache import Client`
    # raised ModuleNotFoundError, breaking the base install for every
    # sync-only user. AsyncClient is still exported so `from tollmeshcache
    # import AsyncClient` keeps working when httpx is installed; without
    # it, using AsyncClient raises this clear error instead of the import
    # of the whole package failing.
    class AsyncClient:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "AsyncClient requires the 'async' extra: pip install tollmeshcache[async]"
            )
from .errors import (
    TollMeshError,
    ErrorCode,
    RateLimitError,
    ReplayError,
    CacheMissError,
    RATE_LIMITED,
    REPLAY_DETECTED,
    CACHE_MISS,
    INVALID_REQUEST,
    INTERNAL_ERROR,
)
from .retry import RetryConfig, retry, RetryHelper

__version__ = "1.1.0"
__author__ = "TollMesh Team"

__all__ = [
    # Client
    "Client",
    "AsyncClient",
    "ClientConfig",
    # Errors
    "TollMeshError",
    "ErrorCode",
    "RateLimitError",
    "ReplayError",
    "CacheMissError",
    "RATE_LIMITED",
    "REPLAY_DETECTED",
    "CACHE_MISS",
    "INVALID_REQUEST",
    "INTERNAL_ERROR",
    # Retry
    "RetryConfig",
    "retry",
    "RetryHelper",
]
