"""Test error handling and edge cases in the MongoDB connection manager."""
import asyncio
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pymongo.errors import PyMongoError, ConnectionFailure, CursorNotFound
from motor.motor_asyncio import AsyncIOMotorClient

from src.pipeline.mongodb.connection_manager import AsyncMongoDBConnectionManager, StreamStatus

class MockChangeStream:
    """Mock implementation of MongoDB change stream for testing"""
    def __init__(self, events=None, error_after=None, error=None, max_events=None):
        self.events = events or [{'operationType': 'insert', 'fullDocument': {'test': 'data'}}]
        self.current_event = 0
        self.error_after = error_after  # Number of events after which to raise error
        self.error = error  # Error to raise after error_after events
        self.max_events = max_events if max_events is not None else len(self.events)
        self.closed = False
        self._resume_token = None
        
    def __aiter__(self):
        return self
        
    async def __anext__(self):
        # If the stream is closed, stop iteration immediately
        if self.closed:
            raise StopAsyncIteration()
            
        # Check for error condition
        if self.error_after is not None and self.current_event >= self.error_after:
            if isinstance(self.error, Exception):
                raise self.error
            elif self.error is not None:
                raise self.error()
            raise CursorNotFound("Cursor not found")
            
        # Check for max events or end of events list
        if self.current_event >= self.max_events or self.current_event >= len(self.events):
            self.close()
            raise StopAsyncIteration()
            
        # Process the next event
        event = self.events[self.current_event].copy()
        event['_id'] = {'_data': f'token_{self.current_event}'}
        self._resume_token = event['_id']
        self.current_event += 1
        return event
        
    def close(self):
        self.closed = True
        
    @property
    def resume_token(self):
        return self._resume_token
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

@pytest_asyncio.fixture
def test_config():
    return {
        'mongodb': {
            'connections': {
                'test_conn': {
                    'uri': 'mongodb://test:27017',
                    'sources': {
                        'test_source': {
                            'database': 'test_db',
                            'collection': 'test_collection',
                            'batch_size': 100
                        }
                    }
                }
            }
        },
        'monitoring': {
            'connection_timeout': 100  # 100ms timeout for tests
        },
        'error_handling': {
            'max_retries': 2,  # Lower retry count for tests
            'backoff_factor': 1,  # No exponential backoff in tests
            'max_backoff': 1  # Max 1 second backoff in tests
        },
        'testing': {
            'max_changes_per_run': 5  # Limit changes processed in tests
        }
    }

@pytest_asyncio.fixture
def mock_client():
    client = AsyncMock(strict=True)
    client.admin = AsyncMock(strict=True)
    client.admin.command = AsyncMock(return_value={'ok': 1})
    client.close = AsyncMock()
    
    # Mock database and collection access
    mock_db = AsyncMock(strict=True)
    mock_collection = AsyncMock(strict=True)
    mock_stream = MockChangeStream()
    
    # Setup watch method to return the mock stream
    mock_collection.watch = MagicMock(return_value=mock_stream)
    
    # Setup dictionary-style access for db and collection
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    client.__getitem__ = MagicMock(return_value=mock_db)
    
    return client

@pytest_asyncio.fixture(autouse=True)
async def cleanup(connection_manager):
    """Automatically clean up after each test."""
    try:
        yield
    finally:
        # Force stop all streams
        for stream_id in list(connection_manager.status.keys()):
            connection_manager.status[stream_id].active = False
        
        # Clear all state directly
        connection_manager.processing_tasks.clear()
        connection_manager.streams.clear()
        connection_manager.clients.clear()
        connection_manager.status.clear()

@pytest_asyncio.fixture
async def connection_manager(test_config):
    """Create a connection manager with test configuration"""
    manager = AsyncMongoDBConnectionManager(test_config)
    # No need for cleanup here as the autouse cleanup fixture will handle it
    return manager

@pytest.mark.asyncio
async def test_connection_initialization_and_recovery(mock_client, connection_manager):
    """Test connection initialization with error recovery."""
    # Mock client to fail twice then succeed
    mock_client.admin.command.side_effect = [
        ConnectionFailure("First attempt failed"),
        ConnectionFailure("Second attempt failed"),
        {'ok': 1}  # Success on third attempt
    ]
    
    # Setup mock stream that will stop after one event
    mock_stream = MockChangeStream(max_events=1)
    mock_client.test_db.test_collection.watch = MagicMock(return_value=mock_stream)
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        await connection_manager.initialize_connections()
        
        # After max retries (2), the connection should fail and no tasks should be created
        stream_id = 'test_conn.test_source'
        assert stream_id not in connection_manager.processing_tasks
        assert stream_id in connection_manager.status
        status = connection_manager.status[stream_id]
        assert not status.active
        assert status.retry_count == 2  # Should have tried max_retries times
        assert "Second attempt failed" in status.last_error

@pytest.mark.asyncio
async def test_stream_initialization_with_pipeline(mock_client, connection_manager):
    """Test stream initialization with pipeline configuration."""
    # Mock _process_stream to prevent infinite loop
    with patch.object(connection_manager, '_process_stream', return_value=None):
        # Mock db.command to avoid coroutine warning
        mock_db = AsyncMock(strict=True)
        mock_db.command = MagicMock(return_value={'inprog': []})
        mock_client.__getitem__.return_value = mock_db
        
        # Setup mock stream that will stop after one event
        mock_stream = MockChangeStream(
            events=[{'operationType': 'insert', 'fullDocument': {'test': 'data'}}],
            max_events=1
        )
        mock_client['test_db']['test_collection'].watch = MagicMock(return_value=mock_stream)
        
        # Mock successful ping
        mock_client.admin.command = AsyncMock(return_value={'ok': 1})
        
        with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
            await connection_manager.initialize_connections()
            
            # Verify stream was initialized
            stream_id = 'test_conn.test_source'
            assert stream_id in connection_manager.streams
            
            # Verify watch method was called with correct parameters
            mock_client['test_db']['test_collection'].watch.assert_called_with(
                pipeline=[],
                batch_size=100,
                full_document='updateLookup',
                max_await_time_ms=1000
            )
            
            # Verify _process_stream was called (but not executed)
            connection_manager._process_stream.assert_called_once_with(stream_id)

@pytest.mark.asyncio
async def test_error_handling_and_cleanup(mock_client, connection_manager):
    """Test error handling during stream processing and cleanup."""
    # Create a properly mocked error handling context
    stream_id = 'test_conn.test_source'
    
    # Setup test data directly in the manager
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_conn',
            source_name='test_source'
        )
    }
    
    # Directly call the handler method instead of relying on full initialization
    error = PyMongoError("Stream error")
    await connection_manager._handle_stream_error(stream_id, error)
    
    # Verify error handling
    stream_status = connection_manager.status[stream_id]
    assert stream_status.total_errors == 1, "Should have recorded one error"
    assert "Stream error" in stream_status.last_error, "Error message should match"
    assert stream_status.retry_count == 1, "Should have recorded one retry attempt"

@pytest.mark.asyncio
async def test_stream_initialization_error(mock_client, connection_manager):
    """Test error handling during stream initialization."""
    # Mock _process_stream to prevent infinite loops
    with patch.object(connection_manager, '_process_stream', return_value=None):
        # Mock db.command to avoid coroutine warning
        mock_db = AsyncMock(strict=True)
        mock_db.command = MagicMock(return_value={'inprog': []})
        mock_client.__getitem__.return_value = mock_db
        
        # Mock stream initialization to fail
        error = PyMongoError("Stream init failed")
        mock_client['test_db']['test_collection'].watch = MagicMock(side_effect=error)
        
        # Mock successful ping
        mock_client.admin.command = AsyncMock(return_value={'ok': 1})
        
        with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
            await connection_manager.initialize_connections()
            
            # For failed stream initialization, process_stream should not be called
            stream_id = 'test_conn.test_source'
            assert not connection_manager._process_stream.called
            
            # Verify error handling
            stream_status = connection_manager.status[stream_id]
            assert not stream_status.active
            assert "Stream init failed" in stream_status.last_error
            assert stream_status.total_errors > 0, "Should have recorded at least one error"

@pytest.mark.asyncio
async def test_cleanup_with_active_streams(mock_client, connection_manager):
    """Test cleanup of active streams and connections."""
    stream_id = 'test_conn.test_source'
    
    # Create a proper coroutine for the mock close method
    async def mock_close():
        return None
        
    mock_client.close = mock_close
    
    # Create a task that is a real coroutine to avoid issues
    async def dummy_task():
        return None
        
    # Create a real task that can be cancelled
    task = asyncio.create_task(dummy_task())
    
    # Set up necessary state
    connection_manager.clients = {'test_conn': mock_client}
    connection_manager.processing_tasks = {stream_id: task}
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_conn',
            source_name='test_source'
        )
    }
    
    try:
        # Test cleanup directly
        await connection_manager.cleanup()
        
        # Verify cleanup actions
        assert not connection_manager.status[stream_id].active, "Stream should be inactive after cleanup"
        assert len(connection_manager.processing_tasks) == 0, "Tasks should be cleared"
        assert len(connection_manager.clients) == 0, "Clients should be cleared"
    finally:
        # Ensure task is properly cancelled if it's still running
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, 0.1)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

@pytest.mark.asyncio
async def test_connection_state_handling(mock_client, connection_manager):
    """Test connection state handling and recovery."""
    # Create simple test configuration
    stream_id = 'test_conn.test_source'
    
    # Mock client to fail on ping
    mock_client.admin.command = AsyncMock(side_effect=ConnectionFailure("Connection lost"))
    
    # Mock process_stream to avoid execution
    with patch.object(connection_manager, '_process_stream', return_value=None):
        with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
            await connection_manager.initialize_connections()
            
            # Verify stream state after connection failure
            assert stream_id in connection_manager.status
            status = connection_manager.status[stream_id]
            assert not status.active  # Stream should be inactive after failure
            assert "Connection lost" in status.last_error
            assert status.retry_count > 0  # Should have attempted retry

@pytest.mark.asyncio
async def test_batch_processing(connection_manager):
    """Test batch processing of change stream events."""
    # Create simple test data
    stream_id = 'test_conn.test_source'
    
    # Setup direct test of the _process_change method
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_conn',
            source_name='test_source'
        )
    }
    
    connection_manager.streams = {
        stream_id: {
            'config': {},
            'last_processed': datetime.now()
        }
    }
    
    # Process multiple changes
    changes = [
        {'operationType': 'insert', 'fullDocument': {'id': i}}
        for i in range(3)
    ]
    
    # Directly process changes
    for change in changes:
        await connection_manager._process_change(stream_id, change)
    
    # Verify batch processing
    assert connection_manager.status[stream_id].processed_changes == 3
    assert 'last_document' in connection_manager.streams[stream_id] 