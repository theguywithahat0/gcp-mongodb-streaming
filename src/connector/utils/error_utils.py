"""Error handling utilities for the connector."""

from enum import Enum
from typing import Optional, Type, Dict, Any
import logging
from functools import wraps
import asyncio

from pymongo.errors import (
    ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError,
    WriteConcernError, WriteError, BulkWriteError
)
from google.api_core.exceptions import RetryError, DeadlineExceeded, ResourceExhausted

from ..logging.logging_config import get_logger

logger = get_logger(__name__)

class ErrorCategory(Enum):
    """Categories of errors for handling and retry decisions."""
    
    # Retryable errors that are likely to succeed on retry
    RETRYABLE_TRANSIENT = "retryable_transient"  # Network issues, timeouts
    RETRYABLE_RESOURCE = "retryable_resource"    # Resource limits, throttling
    
    # Non-retryable errors that won't succeed on retry
    NON_RETRYABLE_CONFIG = "non_retryable_config"    # Configuration issues
    NON_RETRYABLE_DATA = "non_retryable_data"        # Data validation/format issues
    NON_RETRYABLE_AUTH = "non_retryable_auth"        # Authentication/authorization
    
    # Fatal errors that require operator intervention
    FATAL = "fatal"  # Unrecoverable errors

class ConnectorError(Exception):
    """Base class for connector errors with categorization."""
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory,
        original_error: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.category = category
        self.original_error = original_error
        self.context = context or {}
        
    def __str__(self):
        error_str = f"{self.category.value}: {super().__str__()}"
        if self.context:
            error_str += f" (context: {self.context})"
        if self.original_error:
            error_str += f" (original error: {str(self.original_error)})"
        return error_str

# Error classification mappings
ERROR_CLASSIFICATIONS = {
    # MongoDB errors
    ConnectionFailure: ErrorCategory.RETRYABLE_TRANSIENT,
    NetworkTimeout: ErrorCategory.RETRYABLE_TRANSIENT,
    ServerSelectionTimeoutError: ErrorCategory.RETRYABLE_TRANSIENT,
    WriteConcernError: ErrorCategory.RETRYABLE_TRANSIENT,
    WriteError: ErrorCategory.NON_RETRYABLE_DATA,
    BulkWriteError: ErrorCategory.NON_RETRYABLE_DATA,
    
    # Google Cloud errors
    RetryError: ErrorCategory.RETRYABLE_TRANSIENT,
    DeadlineExceeded: ErrorCategory.RETRYABLE_TRANSIENT,
    ResourceExhausted: ErrorCategory.RETRYABLE_RESOURCE,
}

def classify_error(error: Exception) -> ErrorCategory:
    """Classify an error into a category.
    
    Args:
        error: The error to classify
        
    Returns:
        ErrorCategory: The error category
    """
    # Check if it's already a ConnectorError
    if isinstance(error, ConnectorError):
        return error.category
        
    # Look up the error type in our classification mapping
    for error_type, category in ERROR_CLASSIFICATIONS.items():
        if isinstance(error, error_type):
            return category
            
    # Default to FATAL if we don't recognize the error
    return ErrorCategory.FATAL

def wrap_error(
    error: Exception,
    context: Optional[Dict[str, Any]] = None
) -> ConnectorError:
    """Wrap an error with proper categorization and context.
    
    Args:
        error: The original error
        context: Optional context information
        
    Returns:
        ConnectorError: The wrapped error
    """
    category = classify_error(error)
    return ConnectorError(
        message=str(error),
        category=category,
        original_error=error,
        context=context
    )

def is_retryable(error: Exception) -> bool:
    """Check if an error is retryable.
    
    Args:
        error: The error to check
        
    Returns:
        bool: True if the error is retryable
    """
    category = classify_error(error)
    return category in {
        ErrorCategory.RETRYABLE_TRANSIENT,
        ErrorCategory.RETRYABLE_RESOURCE
    }

async def with_retries(
    func,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    context: Optional[Dict[str, Any]] = None
):
    """Execute a function with retries using exponential backoff.
    
    Args:
        func: The function to execute (can be async)
        max_retries: Maximum number of retries
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff
        context: Optional context to include in error wrapping
        
    Returns:
        The result of the function
        
    Raises:
        ConnectorError: If all retries fail
    """
    attempt = 0
    delay = initial_delay
    
    while True:
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func()
            else:
                result = func()
            return result
            
        except Exception as e:
            attempt += 1
            wrapped_error = wrap_error(e, context)
            
            if not is_retryable(wrapped_error) or attempt >= max_retries:
                raise wrapped_error
            
            # Create log context with error info and any additional context
            log_context = {
                "error": str(wrapped_error),
                "category": wrapped_error.category.value,
                "delay": delay
            }
            if context:
                log_context.update(context)
            
            logger.warning(
                f"Retryable error occurred (attempt {attempt}/{max_retries})",
                extra=log_context
            )
            
            await asyncio.sleep(delay)
            delay = min(delay * exponential_base, max_delay) 