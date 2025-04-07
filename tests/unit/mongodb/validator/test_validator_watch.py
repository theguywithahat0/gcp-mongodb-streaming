"""Tests for MongoDB validator's change stream and cursor handling functionality."""
import pytest
import pytest_asyncio
import asyncio
import time
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.pipeline.mongodb.validator import DocumentValidator

@pytest.fixture
def validator():
    """Create a validator with test schemas."""
    schemas = {
        'test_conn.test_source': {
            'fields': {
                'id': {'type': 'string', 'required': True},
                'name': {'type': 'string', 'required': True},
                'count': {'type': 'integer', 'required': False, 'default': 0},
                'active': {'type': 'boolean', 'required': False, 'default': True}
            }
        }
    }
    return DocumentValidator(schemas)

@pytest.mark.asyncio
async def test_cleanup_cursors():
    """Test cursor cleanup functionality."""
    validator = DocumentValidator({})
    
    # Create a mock database with command method
    mock_db = AsyncMock()
    
    # Setup successful cursor cleanup
    mock_db.command.side_effect = [
        # First call to currentOp
        {
            'inprog': [
                {
                    'cursor': 12345,
                    'ns': 'testdb.testcoll',
                    'microsecs_running': 10000  # > 5000, so should be killed
                },
                {
                    'cursor': 67890,
                    'ns': 'testdb.testcoll',
                    'microsecs_running': 2000  # < 5000, should not be killed
                },
                {
                    # No cursor field, should be skipped
                    'ns': 'testdb.othercoll',
                    'microsecs_running': 10000
                }
            ]
        },
        # Second call to killCursors
        {'ok': 1}
    ]
    
    # Test the cursor cleanup
    await validator.cleanup_cursors(mock_db)
    
    # Verify currentOp was called
    assert mock_db.command.call_count == 2
    assert mock_db.command.call_args_list[0][0][0] == 'currentOp'
    
    # Verify killCursors was called with the correct cursor
    kill_call = mock_db.command.call_args_list[1]
    assert kill_call[0][0] == 'killCursors'
    assert kill_call[0][1] == 'testdb.testcoll'
    assert kill_call[1]['cursors'] == [12345]
    
    # Test error during cursor killing
    mock_db.reset_mock()
    mock_db.command.side_effect = [
        # First call to currentOp
        {
            'inprog': [
                {
                    'cursor': 12345,
                    'ns': 'testdb.testcoll',
                    'microsecs_running': 10000
                }
            ]
        },
        # Second call to killCursors - fails
        Exception("Failed to kill cursor")
    ]
    
    # Should not raise exception, just log warning
    await validator.cleanup_cursors(mock_db)
    
    # Verify both commands were still called
    assert mock_db.command.call_count == 2
    
    # Test error during currentOp
    mock_db.reset_mock()
    mock_db.command.side_effect = Exception("Failed to get current operations")
    
    # Should not raise exception, just log warning
    await validator.cleanup_cursors(mock_db)
    
    # Verify command was called once
    assert mock_db.command.call_count == 1

class AsyncContextManagerMock:
    """A proper async context manager for mocking the change stream."""
    def __init__(self, mock_events):
        self.mock_events = mock_events
        self.current = 0
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False
        
    def __aiter__(self):
        return self
        
    async def __anext__(self):
        if self.current >= len(self.mock_events):
            raise StopAsyncIteration
        
        event = self.mock_events[self.current]
        self.current += 1
        return event

@pytest.mark.asyncio
async def test_watch_collection():
    """Test watch_collection with proper error handling and retry logic."""
    validator = DocumentValidator({})
    
    # Create a mock collection with proper watch method
    mock_collection = MagicMock()
    
    # Setup mock events to be returned by our streams
    mock_events = [
        {'_id': {'_data': 'token1'}, 'operationType': 'insert', 'fullDocument': {'id': '1'}},
        {'_id': {'_data': 'token2'}, 'operationType': 'update', 'fullDocument': {'id': '2'}}
    ]
    
    # Create our context manager for the first successful stream
    ctx_manager1 = AsyncContextManagerMock(mock_events)
    
    # Second call will raise an exception
    ctx_manager2 = Exception("Transient error")
    
    # Third call provides another successful stream
    ctx_manager3 = AsyncContextManagerMock(mock_events)
    
    # Set up call tracking
    call_count = 0
    ctx_managers = [ctx_manager1, ctx_manager2, ctx_manager3]
    
    # Mock the watch method to return our context managers
    def mock_watch(**kwargs):
        nonlocal call_count
        result = ctx_managers[call_count]
        call_count += 1
        if isinstance(result, Exception):
            raise result
        return result
    
    mock_collection.watch = mock_watch
    
    # Set up time values for testing retry logic
    time_values = [0, 0, 1, 1, 2, 2]
    time_index = 0
    
    def mock_time():
        nonlocal time_index
        if time_index < len(time_values):
            value = time_values[time_index]
            time_index += 1
            return value
        return 999  # High value to prevent infinite retries
    
    # Test the watch_collection method
    with patch('time.time', mock_time), \
         patch('asyncio.sleep', return_value=None):
        
        # Collect results from the generator
        results = []
        async for change in validator.watch_collection(mock_collection):
            results.append(change)
            # Only process two events to avoid StopIteration
            if len(results) >= 2:
                break
        
        # Verify we got our expected events
        assert len(results) == 2
        assert results[0]['_id']['_data'] == 'token1'
        assert results[1]['_id']['_data'] == 'token2'
        
        # Check that we made the expected calls
        assert call_count == 1  # One successful call is enough
    
    # Test error handling when max retry time is exceeded
    call_count = 0
    
    # Always raise an exception
    def mock_watch_always_fails(**kwargs):
        raise Exception("Persistent error")
    
    mock_collection.watch = mock_watch_always_fails
    
    # Set up time values that exceed retry limit
    time_values_exceeded = [0, 31]
    time_index = 0
    
    def mock_time_exceeded():
        nonlocal time_index
        value = time_values_exceeded[time_index]
        time_index += 1
        return value
    
    # Test retry limit
    with patch('time.time', mock_time_exceeded), \
         patch('asyncio.sleep', return_value=None):
        
        # Should raise exception after time limit exceeded
        with pytest.raises(Exception, match="Persistent error"):
            async for change in validator.watch_collection(mock_collection):
                pass  # Should not reach here 

@pytest.mark.asyncio
async def test_watch_collection_retry_logic():
    """Test watch_collection retry logic after transient errors."""
    validator = DocumentValidator({})
    
    # Create a mock collection
    mock_collection = MagicMock()
    
    # Create events to be returned by our streams
    mock_events = [
        {'_id': {'_data': 'token1'}, 'operationType': 'insert', 'fullDocument': {'id': '1'}}
    ]
    
    # Setup context managers: fail twice then succeed
    error1 = Exception("First transient error")
    error2 = Exception("Second transient error")
    ctx_manager3 = AsyncContextManagerMock(mock_events)
    
    # Track calls to watch
    call_count = 0
    
    # First two calls fail, third succeeds
    def mock_watch(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise error1
        elif call_count == 2:
            raise error2
        return ctx_manager3
    
    mock_collection.watch = mock_watch
    
    # Set up time control for retry timing
    time_values = [0, 0.5, 1.0, 1.5]  # Start, after error1, after error2, end
    time_index = 0
    
    def mock_time():
        nonlocal time_index
        value = time_values[min(time_index, len(time_values) - 1)]
        time_index += 1
        return value
    
    # Set up a mock logger to verify logging 
    mock_logger = MagicMock()
    
    with patch('time.time', mock_time), \
         patch('asyncio.sleep', AsyncMock()), \
         patch('src.pipeline.mongodb.validator.logger', mock_logger):
        
        # Process changes with retries
        results = []
        async for change in validator.watch_collection(mock_collection):
            results.append(change)
            break  # Just need the first result
        
        # Verify we eventually got data
        assert len(results) == 1
        assert results[0]['_id']['_data'] == 'token1'
        
        # Verify retry attempts 
        assert call_count == 3
        
        # Verify each error was logged
        assert mock_logger.warning.call_count == 2
        assert "First transient error" in str(mock_logger.warning.call_args_list[0])
        assert "Second transient error" in str(mock_logger.warning.call_args_list[1]) 