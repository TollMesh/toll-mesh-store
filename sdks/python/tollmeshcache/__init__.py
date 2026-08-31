"""
TollMeshCache Python SDK - Distributed CRDT-based caching and coordination
"""

from .client import Client
from .async_client import AsyncClient
from .config import ClientConfig
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

__version__ = "1.0.0"
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
