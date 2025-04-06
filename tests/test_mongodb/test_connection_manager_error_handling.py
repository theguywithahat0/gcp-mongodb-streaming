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
    def __init__(self, events=None, error_after=None, error=None, max_events=1):
        self.closed = False
        self.events = events or [{'operationType': 'insert', 'fullDocument': {'test': 'data'}}]
        self.current_event = 0
        self.error_after = error_after  # Number of events after which to raise error
        self.error = error  # Error to raise after error_after events
        self.max_events = max_events  # Default to 1 event to prevent infinite loops
        self.ready = True  # Flag to control when to start yielding events - default to True
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.closed = True
        
    async def __anext__(self):
        # Check for error condition
        if self.error_after is not None and self.current_event >= self.error_after:
            if self.error is not None:
                raise self.error
            raise CursorNotFound("Cursor not found")
            
        # Check for max events (prevent infinite loops)
        if self.current_event >= self.max_events:
            raise StopAsyncIteration()
            
        # Check for end of events list
        if self.current_event >= len(self.events):
            # Stop iteration if we've processed all events
            raise StopAsyncIteration()
            
        event = self.events[self.current_event]
        self.current_event += 1
        return event
        
    def close(self):
        self.closed = True

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
    client = AsyncMock()
    client.admin = AsyncMock()
    client.admin.command = AsyncMock(return_value={'ok': 1})
    client.close = AsyncMock()
    
    # Mock database and collection access
    mock_db = AsyncMock()
    mock_collection = AsyncMock()
    mock_stream = MockChangeStream()
    
    # Setup watch method to return the mock stream
    mock_collection.watch = MagicMock(return_value=mock_stream)
    
    # Setup dictionary-style access for db and collection
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    client.__getitem__ = MagicMock(return_value=mock_db)
    
    return client

@pytest_asyncio.fixture
async def connection_manager(test_config):
    """Create a connection manager with test configuration"""
    manager = AsyncMongoDBConnectionManager(test_config)
    yield manager
    # Ensure cleanup after each test
    for stream_id, status in manager.status.items():
        status.active = False
    if manager.processing_tasks:
        # Cancel any running tasks
        tasks_to_cancel = []
        for task in manager.processing_tasks.values():
            if not task.done():
                task.cancel()
                if isinstance(task, asyncio.Task):
                    tasks_to_cancel.append(task)
        
        if tasks_to_cancel:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                pass  # If tasks don't complete in time, move on
    await manager.cleanup()

@pytest_asyncio.fixture(autouse=True)
async def cleanup(connection_manager):
    """Automatically clean up after each test."""
    yield
    await connection_manager.cleanup()

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
        
        # For successful initialization, task should be created
        stream_id = 'test_conn.test_source'
        assert stream_id in connection_manager.processing_tasks
        
        # Verify stream setup
        mock_client['test_db']['test_collection'].watch.assert_called_with(
            pipeline=[],
            batch_size=100
        )

@pytest.mark.asyncio
async def test_error_handling_and_cleanup(mock_client, connection_manager):
    """Test error handling during stream processing and cleanup."""
    # Setup mock stream to fail after one event
    mock_stream = MockChangeStream(
        events=[{'operationType': 'insert', 'fullDocument': {'id': 1}}],
        error_after=1,
        error=PyMongoError("Stream error"),
        max_events=2
    )
    mock_client['test_db']['test_collection'].watch = MagicMock(return_value=mock_stream)
    
    # Mock successful ping
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        await connection_manager.initialize_connections()
        
        # For successful initialization, task should be created
        stream_id = 'test_conn.test_source'
        assert stream_id in connection_manager.processing_tasks
        
        # Let the stream process some changes and hit the error
        await asyncio.sleep(0.5)  # Give more time for error to propagate
        
        # Verify error handling
        stream_status = connection_manager.status[stream_id]
        assert stream_status.total_errors >= 1  # Should have at least one error
        assert "Stream error" in stream_status.last_error  # Error message should match
        assert stream_status.retry_count > 0  # Should have attempted retry

@pytest.mark.asyncio
async def test_stream_initialization_error(mock_client, connection_manager):
    """Test error handling during stream initialization."""
    # Mock stream initialization to fail
    error = PyMongoError("Stream init failed")
    mock_client['test_db']['test_collection'].watch = MagicMock(side_effect=error)
    
    # Mock successful ping
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        await connection_manager.initialize_connections()
        
        # For failed stream initialization, no task should be created
        stream_id = 'test_conn.test_source'
        
        # Let error handling complete
        await asyncio.sleep(0.1)
        
        # Verify error handling
        stream_status = connection_manager.status[stream_id]
        assert not stream_status.active
        assert "Stream init failed" in stream_status.last_error
        assert stream_status.total_errors >= 1

@pytest.mark.asyncio
async def test_cleanup_with_active_streams(mock_client, connection_manager):
    """Test cleanup of active streams and connections."""
    # Setup mock stream that will stop after one event
    mock_stream = MockChangeStream(
        events=[{'operationType': 'insert', 'fullDocument': {'test': 'data'}}],
        max_events=1
    )
    mock_client.test_db.test_collection.watch = MagicMock(return_value=mock_stream)
    
    # Mock successful ping
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        await connection_manager.initialize_connections()
        
        # For successful initialization, task should be created
        stream_id = 'test_conn.test_source'
        assert stream_id in connection_manager.processing_tasks
        
        # Verify stream is active
        assert connection_manager.status[stream_id].active
        
        # Cleanup should cancel all tasks
        await connection_manager.cleanup()
        
        # Verify cleanup
        assert not connection_manager.status[stream_id].active
        assert stream_id not in connection_manager.processing_tasks
        mock_client.close.assert_awaited_once()

@pytest.mark.asyncio
async def test_connection_state_handling(mock_client, connection_manager):
    """Test connection state handling and recovery."""
    # Mock client to fail twice then succeed
    mock_client.admin.command.side_effect = [
        ConnectionFailure("Connection lost"),  # First check fails
        ConnectionFailure("Still lost"),  # Second check fails
        {'ok': 1},  # Third check succeeds
    ]
    
    # Setup mock stream that will stop after one event
    mock_stream = MockChangeStream(max_events=1)
    mock_client.test_db.test_collection.watch = MagicMock(return_value=mock_stream)
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        await connection_manager.initialize_connections()
        
        # After max retries (2), the connection should fail and no tasks should be created
        stream_id = 'test_conn.test_source'
        assert stream_id not in connection_manager.processing_tasks
        
        # Verify connection state
        assert mock_client.admin.command.call_count >= 2  # At least two failures
        status = connection_manager.status[stream_id]
        assert not status.active  # Stream should be inactive after max retries
        assert status.retry_count == 2  # Should have tried max_retries times

@pytest.mark.asyncio
async def test_batch_processing(mock_client, connection_manager):
    """Test batch processing of change stream events."""
    # Setup mock stream with multiple changes but limited max events
    changes = [
        {'operationType': 'insert', 'fullDocument': {'id': i}}
        for i in range(2)  # Only 2 events to prevent long-running tests
    ]
    mock_stream = MockChangeStream(events=changes, max_events=2)
    mock_client.test_db.test_collection.watch = MagicMock(return_value=mock_stream)
    
    # Mock successful ping
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        await connection_manager.initialize_connections()
        
        # For successful initialization, task should be created
        stream_id = 'test_conn.test_source'
        assert stream_id in connection_manager.processing_tasks
        
        # Let the stream process some changes
        await asyncio.sleep(0.1)
        
        # Verify batch processing
        stream_status = connection_manager.status[stream_id]
        assert stream_status.processed_changes >= 1  # Should have processed at least one change
        assert stream_status.active  # Stream should remain active after processing 