"""
Error types and codes for TollMeshCache SDK
"""

from enum import IntEnum
from typing import Dict, Any, Optional


class ErrorCode(IntEnum):
    """Machine-readable error codes"""
    # Success
    OK = 0

    # Client errors (4xx)
    INVALID_REQUEST = 400
    NOT_FOUND = 404
    CONFLICT = 409

    # Rate limiting
    RATE_LIMITED = 429

    # Server errors (5xx)
    INTERNAL = 500
    UNAVAILABLE = 503
    DEADLINE_EXCEEDED = 504

    # TollMesh-specific errors (1000+)
    REPLAY_DETECTED = 1001
    CACHE_MISS = 1002
    INVALID_NAMESPACE = 1003
    INVALID_KEY = 1004
    INVALID_TTL = 1005
    INVALID_VALUE = 1006
    PEER_UNAVAILABLE = 1007
    GOSSIP_FAILED = 1008
    TRANSACTION_FAILED = 1009
    SCRIPT_ERROR = 1010
    SEARCH_FAILED = 1011
    GRAPH_ERROR = 1012


# Export error codes for convenience
RATE_LIMITED = ErrorCode.RATE_LIMITED
REPLAY_DETECTED = ErrorCode.REPLAY_DETECTED
CACHE_MISS = ErrorCode.CACHE_MISS
INVALID_REQUEST = ErrorCode.INVALID_REQUEST
INTERNAL_ERROR = ErrorCode.INTERNAL


class TollMeshError(Exception):
    """
    Base exception for TollMeshCache SDK

    Attributes:
        code: ErrorCode indicating the type of error
        message: Human-readable error message
        details: Additional context about the error
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize TollMeshError

        Args:
            code: ErrorCode enum value
            message: Human-readable error message
            details: Optional additional context
        """
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format error message with code and context"""
        msg = f"Error {self.code.value}: {self.message}"
        if self.details:
            msg += f" (details: {self.details})"
        return msg

    def __repr__(self) -> str:
        return f"TollMeshError({self.code.name}, {self.message!r})"

    def is_rate_limited(self) -> bool:
        """Check if this is a rate limit error"""
        return self.code == ErrorCode.RATE_LIMITED

    def is_replay(self) -> bool:
        """Check if this is a replay detection error"""
        return self.code == ErrorCode.REPLAY_DETECTED

    def is_not_found(self) -> bool:
        """Check if this is a not found error"""
        return self.code == ErrorCode.NOT_FOUND

    def is_server_error(self) -> bool:
        """Check if this is a server error (5xx)"""
        return 500 <= self.code < 600

    def is_client_error(self) -> bool:
        """Check if this is a client error (4xx)"""
        return 400 <= self.code < 500

    def is_retryable(self) -> bool:
        """Check if the operation should be retried"""
        return self.code in (
            ErrorCode.UNAVAILABLE,
            ErrorCode.DEADLINE_EXCEEDED,
            ErrorCode.GOSSIP_FAILED,
        )


class RateLimitError(TollMeshError):
    """Raised when rate limit is exceeded"""

    def __init__(self, reset_at: int, details: Optional[Dict] = None):
        super().__init__(
            ErrorCode.RATE_LIMITED,
            "Rate limit exceeded",
            details or {}
        )
        self.reset_at = reset_at


class ReplayError(TollMeshError):
    """Raised when replay attack is detected"""

    def __init__(self, nonce: str, details: Optional[Dict] = None):
        details = details or {}
        details["nonce"] = nonce
        super().__init__(
            ErrorCode.REPLAY_DETECTED,
            "Replay attack detected",
            details
        )
        self.nonce = nonce


class CacheMissError(TollMeshError):
    """Raised when cache lookup misses"""

    def __init__(self, namespace: str, key: str):
        super().__init__(
            ErrorCode.CACHE_MISS,
            f"Cache miss for {namespace}/{key}",
            {"namespace": namespace, "key": key}
        )
        self.namespace = namespace
        self.key = key
