"""Logging configuration for the connector."""

import logging
import logging.handlers
import sys
from typing import Any, Dict, Optional

from ..config.config_manager import LoggingConfig
from .log_sampler import LogSampler, SamplingRule, SamplingStrategy

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
        self._context: Dict[str, Any] = {}

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
            # Add context to extra if it exists
            if self._context:
                kwargs.setdefault('extra', {}).update(self._context)
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

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: The logger instance
    """
    return logging.getLogger(name)

def add_context_to_logger(logger: logging.Logger, context: Dict[str, Any]) -> logging.Logger:
    """Add contextual information to a logger.
    
    This function adds context that will be included with every log message.
    If the logger is a SampledLogger, the context will be added to its internal context.
    Otherwise, a filter will be added to include the context in the log record.
    
    Args:
        logger: The logger to add context to
        context: Dictionary of contextual information
        
    Returns:
        logging.Logger: The logger with added context
    """
    if isinstance(logger, SampledLogger):
        logger._context.update(context)
    else:
        # Create a filter to add context to log records
        class ContextFilter(logging.Filter):
            def filter(self, record):
                for key, value in context.items():
                    setattr(record, key, value)
                return True
        
        # Remove any existing context filters
        for filter_ in logger.filters:
            if isinstance(filter_, ContextFilter):
                logger.removeFilter(filter_)
        
        # Add the new context filter
        logger.addFilter(ContextFilter())
    
    return logger

def configure_logging(config: LoggingConfig) -> None:
    """Configure logging based on configuration.
    
    Args:
        config: Logging configuration
    """
    # Set root logger level
    root_logger = logging.getLogger()
    root_logger.setLevel(config.level)

    # Clear existing handlers
    root_logger.handlers.clear()

    # Configure handlers
    for handler_config in config.handlers:
        handler = _create_handler(handler_config)
        if handler:
            root_logger.addHandler(handler)

    # Configure log sampling if enabled
    if config.sampling.enabled:
        # Convert sampling rules from config
        rules = {}
        for level, level_rules in config.sampling.rules.items():
            rules[level] = {}
            for msg_type, rule_config in level_rules.items():
                rules[level][msg_type] = SamplingRule(
                    rate=rule_config.rate,
                    strategy=SamplingStrategy(rule_config.strategy),
                    ttl=rule_config.ttl
                )

        # Create log sampler
        sampler = LogSampler(
            default_rate=config.sampling.default_rate,
            rules=rules
        )

        # Replace loggers with sampled versions
        _wrap_loggers_with_sampling(sampler)

def _create_handler(config: Dict[str, Any]) -> Optional[logging.Handler]:
    """Create a log handler from configuration.
    
    Args:
        config: Handler configuration
        
    Returns:
        Optional[logging.Handler]: The created handler
    """
    handler_type = config.get("type", "").lower()
    
    if handler_type == "console":
        handler = logging.StreamHandler(sys.stdout)
    elif handler_type == "file":
        handler = logging.handlers.RotatingFileHandler(
            filename=config["filename"],
            maxBytes=config.get("max_bytes", 10485760),  # 10MB default
            backupCount=config.get("backup_count", 5)
        )
    else:
        return None

    # Set formatter
    formatter = logging.Formatter(
        fmt=config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        datefmt=config.get("datefmt", "%Y-%m-%d %H:%M:%S")
    )
    handler.setFormatter(formatter)
    
    return handler

def _wrap_loggers_with_sampling(sampler: LogSampler) -> None:
    """Wrap existing loggers with sampling functionality.
    
    Args:
        sampler: The log sampler to use
    """
    # Get all existing loggers
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    
    # Wrap each logger with sampling
    for logger in loggers:
        if isinstance(logger, logging.Logger):  # Skip PlaceHolder instances
            logger.__class__ = SampledLogger
            logger._sampler = sampler 