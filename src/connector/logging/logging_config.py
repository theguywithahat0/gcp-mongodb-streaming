"""Logging configuration for the connector."""

import logging
import logging.handlers
import sys
from typing import Any, Dict, Optional

from ..config.config_manager import LoggingConfig
from .log_sampler import LogSampler, SamplingRule, SamplingStrategy, SampledLogger

def configure_logging(config: LoggingConfig) -> None:
    """Configure logging with sampling support.
    
    Args:
        config: Logging configuration
    """
    # Set up root logger
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
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    return handler

def _wrap_loggers_with_sampling(sampler: LogSampler) -> None:
    """Wrap existing loggers with sampling support.
    
    Args:
        sampler: The log sampler to use
    """
    # Get all existing loggers
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    
    # Wrap each logger with sampling
    for logger in loggers:
        if not isinstance(logger, SampledLogger):
            # Create a sampled version of the logger
            sampled_logger = SampledLogger(logger, sampler)
            
            # Replace the logger in the logging manager
            logging.root.manager.loggerDict[logger.name] = sampled_logger

def get_logger(name: str) -> SampledLogger:
    """Get a sampled logger by name.
    
    Args:
        name: Logger name
        
    Returns:
        SampledLogger: The sampled logger
    """
    logger = logging.getLogger(name)
    if not isinstance(logger, SampledLogger):
        # If the logger hasn't been wrapped yet, return it as is
        # It will be wrapped when sampling is configured
        return logger
    return logger

def add_context_to_logger(logger: logging.Logger, context: Dict[str, Any]) -> logging.Logger:
    """Add context information to a logger's messages.
    
    Args:
        logger: The logger to add context to
        context: Dictionary of context information to add
        
    Returns:
        logging.Logger: The logger with context
    """
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        for key, value in context.items():
            setattr(record, key, value)
        return record
    
    logging.setLogRecordFactory(record_factory)
    return logger 