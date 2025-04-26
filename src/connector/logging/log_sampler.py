"""Log sampling implementation for high-volume environments."""

import random
import time
from typing import Dict, Optional, Set
import logging
from dataclasses import dataclass
from enum import Enum

class SamplingStrategy(Enum):
    """Available sampling strategies."""
    PROBABILISTIC = "probabilistic"  # Random sampling based on probability
    RATE_LIMITING = "rate_limiting"  # Rate-limited sampling
    DETERMINISTIC = "deterministic"  # Consistent sampling based on message attributes

@dataclass
class SamplingRule:
    """Configuration for a sampling rule."""
    rate: float  # Sampling rate (0.0 to 1.0)
    strategy: SamplingStrategy = SamplingStrategy.PROBABILISTIC
    ttl: Optional[int] = None  # Time-to-live in seconds for rate limiting
    
    def __post_init__(self):
        """Validate sampling rule configuration."""
        if not 0.0 <= self.rate <= 1.0:
            raise ValueError("Sampling rate must be between 0.0 and 1.0")

class LogSampler:
    """Implements log sampling for high-volume environments.
    
    This class provides configurable log sampling based on:
    - Log level (e.g., DEBUG, INFO, WARNING)
    - Message type or category
    - Sampling strategy (probabilistic, rate-limiting, deterministic)
    
    Attributes:
        default_rate: Default sampling rate if no specific rule matches
        rules: Dictionary of sampling rules by level and message type
    """

    def __init__(
        self,
        default_rate: float = 1.0,
        rules: Optional[Dict[str, Dict[str, SamplingRule]]] = None
    ):
        """Initialize the log sampler.
        
        Args:
            default_rate: Default sampling rate (0.0 to 1.0)
            rules: Dictionary of sampling rules by level and message type
        """
        self.default_rule = SamplingRule(rate=default_rate)
        self.rules = rules or {}
        
        # Cache for rate-limited sampling
        self._last_sampled: Dict[str, float] = {}
        # Cache for deterministic sampling
        self._sampled_messages: Set[str] = set()
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)

    def should_sample(
        self,
        level: int,
        msg: str,
        msg_type: Optional[str] = None,
        attributes: Optional[Dict[str, str]] = None
    ) -> bool:
        """Determine if a log message should be sampled.
        
        Args:
            level: Log level (e.g., logging.INFO)
            msg: Log message
            msg_type: Optional message type or category
            attributes: Optional message attributes for deterministic sampling
            
        Returns:
            bool: True if the message should be sampled
        """
        # Critical and error logs are always sampled
        if level >= logging.ERROR:
            return True

        # Get the appropriate sampling rule
        rule = self._get_rule(level, msg_type)
        
        # Apply sampling strategy
        if rule.strategy == SamplingStrategy.PROBABILISTIC:
            return self._apply_probabilistic_sampling(rule)
        elif rule.strategy == SamplingStrategy.RATE_LIMITING:
            return self._apply_rate_limiting(rule, msg_type or msg)
        else:  # DETERMINISTIC
            return self._apply_deterministic_sampling(rule, msg, attributes)

    def _get_rule(self, level: int, msg_type: Optional[str] = None) -> SamplingRule:
        """Get the sampling rule for a given level and message type.
        
        Args:
            level: Log level
            msg_type: Optional message type
            
        Returns:
            SamplingRule: The matching sampling rule
        """
        level_name = logging.getLevelName(level)
        
        # Check for specific message type rule
        if msg_type and level_name in self.rules:
            if msg_type in self.rules[level_name]:
                return self.rules[level_name][msg_type]
        
        # Check for level-specific rule
        if level_name in self.rules and None in self.rules[level_name]:
            return self.rules[level_name][None]
        
        return self.default_rule

    def _apply_probabilistic_sampling(self, rule: SamplingRule) -> bool:
        """Apply probabilistic sampling.
        
        Args:
            rule: The sampling rule to apply
            
        Returns:
            bool: True if the message should be sampled
        """
        return random.random() < rule.rate

    def _apply_rate_limiting(
        self,
        rule: SamplingRule,
        key: str,
        current_time: Optional[float] = None
    ) -> bool:
        """Apply rate-limited sampling.
        
        Args:
            rule: The sampling rule to apply
            key: Key for rate limiting
            current_time: Optional current time for testing
            
        Returns:
            bool: True if the message should be sampled
        """
        current_time = current_time or time.time()
        
        # Check if we need to clean up old entries
        if rule.ttl:
            self._cleanup_rate_limiting(current_time, rule.ttl)
        
        # Check if enough time has passed since last sample
        last_time = self._last_sampled.get(key, 0)
        if current_time - last_time >= (1.0 / rule.rate):
            self._last_sampled[key] = current_time
            return True
            
        return False

    def _apply_deterministic_sampling(
        self,
        rule: SamplingRule,
        msg: str,
        attributes: Optional[Dict[str, str]] = None
    ) -> bool:
        """Apply deterministic sampling based on message content.
        
        Args:
            rule: The sampling rule to apply
            msg: The log message
            attributes: Optional message attributes
            
        Returns:
            bool: True if the message should be sampled
        """
        # Create a deterministic hash from message and attributes
        sample_key = msg
        if attributes:
            sample_key += str(sorted(attributes.items()))
            
        # Use the hash to make a deterministic decision
        hash_val = hash(sample_key) % 100
        should_sample = hash_val < (rule.rate * 100)
        
        if should_sample:
            self._sampled_messages.add(sample_key)
            
        return should_sample

    def _cleanup_rate_limiting(self, current_time: float, ttl: int) -> None:
        """Clean up expired rate limiting entries.
        
        Args:
            current_time: Current time
            ttl: Time-to-live in seconds
        """
        expired = [
            key for key, last_time in self._last_sampled.items()
            if current_time - last_time > ttl
        ]
        for key in expired:
            del self._last_sampled[key]

class SampledLogger:
    """Logger wrapper that applies sampling before logging messages."""

    def __init__(
        self,
        logger: logging.Logger,
        sampler: LogSampler
    ):
        """Initialize the sampled logger.
        
        Args:
            logger: The logger to wrap
            sampler: The log sampler to use
        """
        self._logger = logger
        self._sampler = sampler

    def log(
        self,
        level: int,
        msg: str,
        msg_type: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        """Log a message if it passes sampling.
        
        Args:
            level: Log level
            msg: Log message
            msg_type: Optional message type
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        if self._sampler.should_sample(level, msg, msg_type):
            self._logger.log(level, msg, *args, **kwargs)

    def debug(self, msg: str, msg_type: Optional[str] = None, *args, **kwargs) -> None:
        """Log a debug message if it passes sampling."""
        self.log(logging.DEBUG, msg, msg_type, *args, **kwargs)

    def info(self, msg: str, msg_type: Optional[str] = None, *args, **kwargs) -> None:
        """Log an info message if it passes sampling."""
        self.log(logging.INFO, msg, msg_type, *args, **kwargs)

    def warning(self, msg: str, msg_type: Optional[str] = None, *args, **kwargs) -> None:
        """Log a warning message if it passes sampling."""
        self.log(logging.WARNING, msg, msg_type, *args, **kwargs)

    def error(self, msg: str, msg_type: Optional[str] = None, *args, **kwargs) -> None:
        """Log an error message if it passes sampling."""
        self.log(logging.ERROR, msg, msg_type, *args, **kwargs)

    def critical(self, msg: str, msg_type: Optional[str] = None, *args, **kwargs) -> None:
        """Log a critical message if it passes sampling."""
        self.log(logging.CRITICAL, msg, msg_type, *args, **kwargs) 