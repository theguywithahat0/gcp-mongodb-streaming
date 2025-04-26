"""Backpressure handling implementation using token bucket algorithm."""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional
import logging
from ..logging.logging_config import get_logger

@dataclass
class BackpressureConfig:
    """Configuration for backpressure handling."""
    # Token bucket settings
    tokens_per_second: float = 1000.0  # Base rate limit
    bucket_size: int = 2000  # Maximum burst size
    min_tokens_per_second: float = 100.0  # Minimum rate during heavy backpressure
    max_tokens_per_second: float = 5000.0  # Maximum rate during light load
    
    # Adaptive rate adjustment
    error_decrease_factor: float = 0.8  # Multiply rate by this on errors
    success_increase_factor: float = 1.1  # Multiply rate by this on success
    rate_update_interval: float = 5.0  # Seconds between rate adjustments
    
    # Monitoring
    metrics_window_size: int = 100  # Number of operations to track for metrics

class BackpressureHandler:
    """Implements backpressure handling using token bucket algorithm with adaptive rate limiting.
    
    Features:
    - Token bucket algorithm for basic rate limiting
    - Adaptive rate adjustment based on downstream pressure
    - Error-based rate reduction
    - Success-based rate increase
    - Monitoring and metrics
    """

    def __init__(self, config: BackpressureConfig):
        """Initialize the backpressure handler.
        
        Args:
            config: Backpressure configuration
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Token bucket state
        self.tokens = config.bucket_size
        self.last_update = time.monotonic()
        self.current_rate = config.tokens_per_second
        
        # Metrics
        self.recent_operations = []  # List of (timestamp, success) tuples
        self.last_rate_update = time.monotonic()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Attempt to acquire tokens for an operation.
        
        Args:
            tokens: Number of tokens needed for the operation
            
        Returns:
            bool: True if tokens were acquired, False if should backpressure
        """
        async with self._lock:
            now = time.monotonic()
            
            # Add new tokens based on time elapsed
            elapsed = now - self.last_update
            new_tokens = elapsed * self.current_rate
            self.tokens = min(
                self.config.bucket_size,
                self.tokens + new_tokens
            )
            self.last_update = now
            
            # Check if we have enough tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            self.logger.warning(
                "Backpressure applied",
                extra={
                    "available_tokens": self.tokens,
                    "requested_tokens": tokens,
                    "current_rate": self.current_rate
                }
            )
            return False

    async def record_operation(self, success: bool) -> None:
        """Record the result of an operation for adaptive rate adjustment.
        
        Args:
            success: Whether the operation succeeded
        """
        async with self._lock:
            now = time.monotonic()
            
            # Add operation to metrics
            self.recent_operations.append((now, success))
            
            # Remove old operations outside the window
            window_start = now - self.config.rate_update_interval
            self.recent_operations = [
                op for op in self.recent_operations
                if op[0] >= window_start
            ]
            
            # Update rate if enough time has passed
            if now - self.last_rate_update >= self.config.rate_update_interval:
                await self._update_rate()
                self.last_rate_update = now

    async def _update_rate(self) -> None:
        """Update the token generation rate based on recent operation results."""
        if not self.recent_operations:
            return
            
        # Calculate success rate
        total_ops = len(self.recent_operations)
        success_ops = sum(1 for _, success in self.recent_operations if success)
        success_rate = success_ops / total_ops if total_ops > 0 else 0
        
        # Adjust rate based on success rate
        if success_rate >= 0.95:  # Very good performance
            new_rate = self.current_rate * self.config.success_increase_factor
        elif success_rate <= 0.8:  # Poor performance
            new_rate = self.current_rate * self.config.error_decrease_factor
        else:  # Maintain current rate
            return
            
        # Clamp rate within configured bounds
        self.current_rate = max(
            self.config.min_tokens_per_second,
            min(self.config.max_tokens_per_second, new_rate)
        )
        
        self.logger.info(
            "Rate adjusted",
            extra={
                "success_rate": success_rate,
                "old_rate": self.current_rate / self.config.success_increase_factor
                if success_rate >= 0.95
                else self.current_rate / self.config.error_decrease_factor,
                "new_rate": self.current_rate,
                "operation_count": total_ops
            }
        )

    async def get_metrics(self) -> dict:
        """Get current backpressure metrics.
        
        Returns:
            dict: Current metrics including rates and token counts
        """
        async with self._lock:
            now = time.monotonic()
            total_ops = len(self.recent_operations)
            success_ops = sum(1 for _, success in self.recent_operations if success)
            
            return {
                "current_rate": self.current_rate,
                "available_tokens": self.tokens,
                "success_rate": success_ops / total_ops if total_ops > 0 else 1.0,
                "operation_count": total_ops,
                "time_since_update": now - self.last_rate_update
            } 