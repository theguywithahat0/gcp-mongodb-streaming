"""Circuit breaker implementation for handling failures gracefully."""

import asyncio
import time
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
from functools import wraps
from ..logging.logging_config import get_logger

T = TypeVar('T')

class CircuitBreakerState(Enum):
    """Possible states of the circuit breaker."""
    CLOSED = 'closed'      # Normal operation
    OPEN = 'open'         # Circuit is open, failing fast
    HALF_OPEN = 'half_open'  # Testing if service is back to normal

class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open."""
    pass

class CircuitBreaker:
    """Circuit breaker implementation for handling failures gracefully.
    
    The circuit breaker has three states:
    - CLOSED: Normal operation, all requests go through
    - OPEN: Service is failing, requests fail fast
    - HALF_OPEN: Testing if service is recovered
    
    Attributes:
        failure_threshold: Number of failures before opening the circuit
        reset_timeout: Time in seconds before attempting recovery
        half_open_max_calls: Maximum calls to allow in half-open state
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
        half_open_max_calls: int = 3,
        name: str = "default"
    ):
        """Initialize the circuit breaker.
        
        Args:
            failure_threshold: Number of consecutive failures before opening
            reset_timeout: Seconds to wait before attempting recovery
            half_open_max_calls: Max calls to allow in half-open state
            name: Name of this circuit breaker instance
        """
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls
        self.name = name

        # State
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        
        # Lock for thread safety
        self._state_lock = asyncio.Lock()
        
        # Logging
        self.logger = get_logger(__name__)

    @property
    def state(self) -> CircuitBreakerState:
        """Get the current state of the circuit breaker."""
        return self._state

    async def _update_state(self, new_state: CircuitBreakerState) -> None:
        """Update the circuit breaker state with proper logging.
        
        Args:
            new_state: The new state to transition to
        """
        if new_state != self._state:
            self.logger.info(
                "circuit_breaker_state_change",
                extra={
                    "name": self.name,
                    "old_state": self._state.value,
                    "new_state": new_state.value,
                    "failure_count": self._failure_count
                }
            )
            self._state = new_state

    async def _handle_success(self) -> None:
        """Handle a successful operation."""
        async with self._state_lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._half_open_calls += 1
                # Only close the circuit after enough successful calls
                if self._half_open_calls >= self.half_open_max_calls:
                    await self._update_state(CircuitBreakerState.CLOSED)
                    self._failure_count = 0
                    self._half_open_calls = 0
                    self._last_failure_time = None
            elif self._state == CircuitBreakerState.CLOSED:
                # Reset failure count on success in closed state
                self._failure_count = 0

    async def _handle_failure(self, error: Exception) -> None:
        """Handle a failed operation.
        
        Args:
            error: The exception that caused the failure
        """
        async with self._state_lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitBreakerState.HALF_OPEN:
                # Any failure in half-open state opens the circuit
                await self._update_state(CircuitBreakerState.OPEN)
                self._half_open_calls = 0
            elif (
                self._state == CircuitBreakerState.CLOSED and
                self._failure_count >= self.failure_threshold
            ):
                # Too many failures in closed state
                await self._update_state(CircuitBreakerState.OPEN)

            self.logger.warning(
                "circuit_breaker_failure",
                extra={
                    "name": self.name,
                    "state": self._state.value,
                    "failure_count": self._failure_count,
                    "error": str(error)
                }
            )

    async def _check_state(self) -> None:
        """Check and potentially update the circuit breaker state."""
        async with self._state_lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure_time is None:
                    return

                # Check if it's time to try recovery
                if time.time() - self._last_failure_time >= self.reset_timeout:
                    await self._update_state(CircuitBreakerState.HALF_OPEN)
                    self._half_open_calls = 0
            elif self._state == CircuitBreakerState.HALF_OPEN:
                # Check if we've exceeded max calls in half-open state
                if self._half_open_calls >= self.half_open_max_calls:
                    await self._update_state(CircuitBreakerState.OPEN)

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator for protecting a function with the circuit breaker.
        
        Args:
            func: The function to protect
            
        Returns:
            The wrapped function
        """
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            await self._check_state()

            if self._state == CircuitBreakerState.OPEN:
                raise CircuitBreakerError(
                    f"Circuit breaker '{self.name}' is OPEN"
                )

            try:
                result = await func(*args, **kwargs)
                await self._handle_success()
                return result
            except Exception as e:
                await self._handle_failure(e)
                raise

        return wrapper 