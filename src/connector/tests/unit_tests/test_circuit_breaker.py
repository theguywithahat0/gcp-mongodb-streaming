"""Tests for the circuit_breaker module."""

import pytest
from unittest.mock import patch, MagicMock, call
import time
import asyncio

from src.connector.core.circuit_breaker import (
    CircuitBreaker, 
    CircuitBreakerState,
    CircuitBreakerError
)

@pytest.fixture
def mock_logger():
    """Fixture to provide a mock logger."""
    with patch('src.connector.core.circuit_breaker.get_logger') as mock:
        logger = MagicMock()
        mock.return_value = logger
        yield logger

@pytest.fixture(scope="function")
async def circuit_breaker(mock_logger):
    """Fixture to provide a circuit breaker instance."""
    return CircuitBreaker(
        failure_threshold=3,
        reset_timeout=1.0,
        name=f"circuit_breaker_test_{id(mock_logger)}"
    )

@pytest.mark.asyncio
async def test_initial_state(circuit_breaker):
    """Test initial state of the circuit breaker."""
    cb = await circuit_breaker
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb._failure_count == 0
    assert cb._last_failure_time is None

@pytest.mark.asyncio
async def test_successful_call(circuit_breaker):
    """Test successful function call through circuit breaker."""
    cb = await circuit_breaker
    
    # Create an async function to protect
    @cb
    async def protected_func(*args, **kwargs):
        return "success"
        
    # Call the protected function and await it
    result = await protected_func("arg1", kwarg1="value1")
    
    # Verify the result is correct
    assert result == "success"
    
    # Verify the circuit breaker state is still closed
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb._failure_count == 0

@pytest.mark.asyncio
async def test_failed_call(circuit_breaker):
    """Test failed function call through circuit breaker."""
    cb = await circuit_breaker
    
    # Create an async function that fails
    @cb
    async def failing_func():
        raise ValueError("Test error")
        
    # Call the function and expect the exception to be propagated
    with pytest.raises(ValueError):
        await failing_func()
        
    # Verify the failure count increased
    assert cb._failure_count == 1
    
    # Verify the circuit breaker state is still closed
    assert cb.state == CircuitBreakerState.CLOSED

@pytest.mark.asyncio
async def test_circuit_opens_after_threshold(circuit_breaker):
    """Test circuit opens after reaching failure threshold."""
    cb = await circuit_breaker
    
    # Create an async function that fails
    @cb
    async def failing_func():
        raise ValueError("Test error")
        
    # Make multiple failing calls to reach threshold
    for _ in range(cb.failure_threshold):
        with pytest.raises(ValueError):
            await failing_func()
            
    # Verify the circuit breaker state is now open
    assert cb.state == CircuitBreakerState.OPEN
    
    # Verify next call raises CircuitBreakerError
    with pytest.raises(CircuitBreakerError):
        await failing_func()

@pytest.mark.asyncio
async def test_circuit_half_open_after_timeout(circuit_breaker):
    """Test circuit transitions to half-open after timeout."""
    cb = await circuit_breaker
    fail_count = 0
    
    @cb
    async def test_func():
        nonlocal fail_count
        if fail_count < cb.failure_threshold:
            fail_count += 1
            raise ValueError("Test error")
        return "success"
        
    # Make failing calls to open the circuit
    for _ in range(cb.failure_threshold):
        with pytest.raises(ValueError):
            await test_func()
            
    # Verify the circuit is open
    assert cb.state == CircuitBreakerState.OPEN
    
    # Wait for reset timeout to pass
    await asyncio.sleep(cb.reset_timeout * 1.5)
    
    # Next call should transition to half-open and succeed
    result = await test_func()
    
    # Verify the state is half-open
    assert cb.state == CircuitBreakerState.HALF_OPEN
    
    # Verify the result is correct
    assert result == "success"

@pytest.mark.asyncio
async def test_circuit_closes_after_successful_half_open_calls(circuit_breaker):
    """Test circuit closes after successful calls in half-open state."""
    cb = await circuit_breaker
    
    # Force circuit into half-open state
    cb._state = CircuitBreakerState.HALF_OPEN
    cb._last_failure_time = time.time() - cb.reset_timeout * 2
    
    # Create successful function
    @cb
    async def success_func():
        return "success"
        
    # Make successful calls in half-open state
    for _ in range(cb.half_open_max_calls):
        result = await success_func()
        assert result == "success"
        
    # Verify circuit is now closed
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb._failure_count == 0

@pytest.mark.asyncio
async def test_circuit_reopens_on_failure_in_half_open(circuit_breaker):
    """Test circuit reopens on failure in half-open state."""
    cb = await circuit_breaker
    
    # Force circuit into half-open state
    cb._state = CircuitBreakerState.HALF_OPEN
    cb._last_failure_time = time.time() - cb.reset_timeout * 2
    
    # Create failing function
    @cb
    async def failing_func():
        raise ValueError("Test error")
        
    # Call should fail and reopen the circuit
    with pytest.raises(ValueError):
        await failing_func()
        
    # Verify circuit is open again
    assert cb.state == CircuitBreakerState.OPEN

def setUp(self):
    """Set up test environment."""
    # Use a unique logger name for each test instance
    self.logger_name = f"circuit_breaker_test_{id(self)}"
    self.mock_logger = MagicMock()
    with patch('src.connector.logging.logging_config.get_logger') as mock_get_logger:
        mock_get_logger.return_value = self.mock_logger
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            reset_timeout=1.0,
            logger_name=self.logger_name
        ) 