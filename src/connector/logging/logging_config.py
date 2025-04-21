"""Structured logging configuration for the connector."""

import logging
import sys
from typing import Any, Dict

import structlog
from structlog.types import Processor

def configure_logging(log_level: str = "INFO") -> None:
    """Configure structured logging for the connector.
    
    Args:
        log_level: The logging level to use. Defaults to "INFO".
    """
    # Set up standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Configure structlog processors
    shared_processors: list[Processor] = [
        # Add timestamp
        structlog.processors.TimeStamper(fmt="iso"),
        # Add log level
        structlog.stdlib.add_log_level,
        # Add logger name
        structlog.stdlib.add_logger_name,
    ]

    # Configure structlog
    structlog.configure(
        processors=[
            *shared_processors,
            # Format exceptions
            structlog.processors.format_exc_info,
            # Convert to JSON format
            structlog.processors.JSONRenderer()
        ],
        # Use standard library's logging for output
        wrapper_class=structlog.stdlib.BoundLogger,
        # Use standard library's logging levels
        context_class=dict,
        # Cache logger
        cache_logger_on_first_use=True,
        # Log level configuration
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

def get_logger(name: str) -> structlog.BoundLogger:
    """Get a configured structured logger.
    
    Args:
        name: The name of the logger, typically __name__.
        
    Returns:
        A configured structured logger.
    """
    return structlog.get_logger(name)

def add_context_to_logger(logger: structlog.BoundLogger, context: Dict[str, Any]) -> structlog.BoundLogger:
    """Add context to a logger that will be included in all subsequent log messages.
    
    Args:
        logger: The logger to add context to.
        context: The context to add.
        
    Returns:
        The logger with added context.
    """
    return logger.bind(**context) 