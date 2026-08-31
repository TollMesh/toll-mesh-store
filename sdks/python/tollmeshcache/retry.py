"""
Retry logic with exponential backoff for TollMeshCache client
"""

import time
import random
from typing import Callable, TypeVar, Any
from functools import wraps

from .errors import TollMeshError, ErrorCode

T = TypeVar("T")


class RetryConfig:
    """Configuration for retry behavior"""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        backoff_multiplier: float = 2.0,
    ):
        """
        Initialize retry configuration

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            jitter: Whether to add random jitter to delays
            backoff_multiplier: Multiplier for exponential backoff
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.backoff_multiplier = backoff_multiplier

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt"""
        delay = min(
            self.base_delay * (self.backoff_multiplier ** attempt),
            self.max_delay
        )

        if self.jitter:
            # Add random jitter: ±25% of delay
            jitter_amount = delay * 0.25
            delay += random.uniform(-jitter_amount, jitter_amount)

        return max(0, delay)

    def is_retryable(self, error: Exception) -> bool:
        """Check if error is retryable"""
        if isinstance(error, TollMeshError):
            return error.is_retryable()
        # Retry on connection errors, timeouts, etc.
        return isinstance(error, (ConnectionError, TimeoutError))


def retry(
    config: RetryConfig = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying operations with exponential backoff

    Args:
        config: RetryConfig instance (uses defaults if None)

    Example:
        @retry(RetryConfig(max_retries=3))
        def risky_operation():
            ...
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None

            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Don't retry if not retryable
                    if not config.is_retryable(e):
                        raise

                    # Don't retry on last attempt
                    if attempt >= config.max_retries:
                        raise

                    # Calculate and sleep
                    delay = config.calculate_delay(attempt)
                    time.sleep(delay)

            # Should never reach here, but just in case
            raise last_exception

        return wrapper

    return decorator


class RetryHelper:
    """Helper class for manual retry logic"""

    def __init__(self, config: RetryConfig = None):
        """Initialize retry helper"""
        self.config = config or RetryConfig()

    def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """
        Execute function with retries

        Args:
            func: Function to execute
            *args: Positional arguments to pass to func
            **kwargs: Keyword arguments to pass to func

        Returns:
            Result of function

        Raises:
            The last exception if all retries fail
        """
        last_exception = None

        for attempt in range(self.config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if not self.config.is_retryable(e):
                    raise

                if attempt < self.config.max_retries:
                    delay = self.config.calculate_delay(attempt)
                    time.sleep(delay)

        raise last_exception
