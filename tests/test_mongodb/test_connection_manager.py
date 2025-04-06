import pytest
import pytest_asyncio
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure, CursorNotFound, NetworkTimeout, PyMongoError

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

class MockCommand:
    def __init__(self):
        self.call_count = 0
        self.max_failures = 3

    async def __call__(self, *args, **kwargs):
        self.call_count += 1
        if self.call_count <= self.max_failures:
            raise ConnectionFailure("Connection refused")
        return {'ok': 1}

@pytest_asyncio.fixture
def test_config():
    return {
        'mongodb': {
            'connections': {
                'test_client': {
                    'uri': 'mongodb://test:27017',
                    'sources': {
                        'orders': {
                            'database': 'testdb',
                            'collection': 'orders',
                            'batch_size': 100
                        }
                    }
                }
            }
        },
        'monitoring': {
            'connection_timeout': 1000  # 1 second timeout for tests
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
def mock_motor_client():
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
async def test_initialize_connections_success(connection_manager):
    """Test successful initialization of MongoDB connections"""
    # Create a mock client that will be returned by AsyncIOMotorClient
    mock_client = AsyncMock()
    mock_client.admin = AsyncMock()
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    mock_client.close = AsyncMock()
    
    # Mock database and collection access
    mock_db = AsyncMock()
    mock_collection = AsyncMock()
    
    # Create mock stream that will stop after one event
    mock_stream = MockChangeStream(
        events=[{'operationType': 'insert', 'fullDocument': {'test': 'data'}}],
        max_events=1  # Will raise StopAsyncIteration after one event
    )
    
    # Setup watch method to return the mock stream
    mock_collection.watch = MagicMock(return_value=mock_stream)
    
    # Setup dictionary-style access for db and collection
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    
    # Mock the client creation - use the full import path
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        # Initialize connections
        await connection_manager.initialize_connections()
        
        # Wait for all processing tasks to be created
        stream_id = 'test_client.orders'
        assert stream_id in connection_manager.processing_tasks, "Stream processing task not created"
        
        # Verify client was created and pinged
        assert 'test_client' in connection_manager.clients
        mock_client.admin.command.assert_awaited_once_with('ping')
        
        # Verify stream was initialized with correct configuration
        assert stream_id in connection_manager.streams
        stream_info = connection_manager.streams[stream_id]
        assert 'stream' in stream_info
        assert 'config' in stream_info
        assert stream_info['config']['database'] == 'testdb'
        assert stream_info['config']['collection'] == 'orders'
        
        # Verify initial stream status (should be active)
        assert stream_id in connection_manager.status
        status = connection_manager.status[stream_id]
        assert status.active, "Stream should be active after initialization"
        assert status.connection_id == 'test_client'
        assert status.source_name == 'orders'
        assert status.retry_count == 0
        assert status.total_errors == 0
        
        # Now allow the stream to start yielding events
        mock_stream.ready = True
        
        # Wait for task to complete or timeout
        task = connection_manager.processing_tasks[stream_id]
        try:
            # Give the task a chance to process one event
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.TimeoutError:
            # If we timeout, force stop the stream
            status.active = False
            # Wait a bit for cleanup
            await asyncio.sleep(0.1)
        
        # Verify final state
        assert mock_stream.closed, "Stream should be closed"
        assert not status.active, "Stream should be inactive after completion"
        assert status.processed_changes == 1, "Should have processed one event"

@pytest.mark.asyncio
async def test_initialize_connections_failure(connection_manager):
    """Test handling of connection initialization failure"""
    # Create a mock client that will fail to connect
    mock_client = AsyncMock()
    mock_client.admin = AsyncMock()
    mock_client.admin.command = AsyncMock(side_effect=ConnectionFailure("Connection refused"))
    mock_client.close = AsyncMock()
    
    # Mock database and collection access
    mock_db = AsyncMock()
    mock_collection = AsyncMock()
    
    # Create mock stream that will fail
    mock_stream = MockChangeStream(
        events=[{'operationType': 'insert', 'fullDocument': {'test': 'data'}}],
        error_after=0,  # Fail immediately
        error=ConnectionFailure("Stream connection failed"),
        max_events=1
    )
    
    # Setup watch method to return the mock stream
    mock_collection.watch = MagicMock(return_value=mock_stream)
    
    # Setup dictionary-style access for db and collection
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    
    with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
        # Initialize connections
        await connection_manager.initialize_connections()
        
        # Verify error handling
        stream_id = 'test_client.orders'
        assert stream_id in connection_manager.status, "Stream status should be created even on failure"
        status = connection_manager.status[stream_id]
        
        # Verify connection state
        assert not status.active, "Stream should be inactive after connection failure"
        assert status.connection_id == 'test_client'
        assert status.source_name == 'orders'
        
        # Verify error tracking
        assert status.total_errors >= 1, "Should have recorded at least one error"
        assert "Connection refused" in status.last_error, "Error message should be recorded"
        assert isinstance(status.last_error_time, datetime), "Error time should be recorded"
        assert status.retry_count > 0, "Should have attempted retry"
        
        # Verify stream cleanup
        assert stream_id not in connection_manager.streams or connection_manager.streams[stream_id].get('stream') is None, "Failed stream should be cleaned up"
        
        # Verify task cleanup
        if stream_id in connection_manager.processing_tasks:
            task = connection_manager.processing_tasks[stream_id]
            assert task.done(), "Processing task should be completed"
            
        # Verify client cleanup
        mock_client.close.assert_awaited(), "Client should be closed after failure"

@pytest.mark.asyncio
async def test_process_change(connection_manager):
    """Test processing of a change event"""
    stream_id = 'test_client.orders'
    
    # Initialize the manager first
    connection_manager.status = {}
    connection_manager.streams = {}
    
    connection_manager.status[stream_id] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders'
    )
    
    mock_stream = MockChangeStream([
        {'operationType': 'insert', 'fullDocument': {'order_id': '123'}}
    ])
    
    connection_manager.streams[stream_id] = {
        'stream': mock_stream,
        'config': {},
        'last_processed': datetime.now()
    }
    
    test_change = {'operationType': 'insert', 'fullDocument': {'order_id': '123'}}
    await connection_manager._process_change(stream_id, test_change)
    
    # Verify change was processed
    assert connection_manager.status[stream_id].processed_changes == 1
    assert isinstance(connection_manager.streams[stream_id]['last_processed'], datetime)

@pytest.mark.asyncio
async def test_cleanup(connection_manager, mock_motor_client):
    """Test cleanup of connections and streams"""
    # Initialize the manager first
    connection_manager.clients = {'test_client': mock_motor_client}
    
    # Create a mock task that can be properly cancelled
    async def mock_task_coro():
        while True:
            await asyncio.sleep(0.1)
    
    mock_task = asyncio.create_task(mock_task_coro())
    
    connection_manager.processing_tasks = {'test_client.orders': mock_task}
    
    # Setup stream status
    connection_manager.status['test_client.orders'] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders'
    )
    
    # Perform cleanup
    await connection_manager.cleanup()
    
    # Verify cleanup actions
    assert not connection_manager.status['test_client.orders'].active, "Stream should be inactive after cleanup"
    assert mock_task.cancelled(), "Task should be cancelled"
    mock_motor_client.close.assert_awaited_once(), "Client should be closed"
    assert len(connection_manager.processing_tasks) == 0, "Processing tasks should be cleared"

@pytest.mark.asyncio
async def test_get_stream_statistics(connection_manager):
    """Test retrieval of stream statistics"""
    stream_id = 'test_client.orders'
    now = datetime.now()
    
    # Initialize the manager first
    connection_manager.status = {}
    connection_manager.streams = {}
    
    # Setup test data
    connection_manager.status[stream_id] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders',
        processed_changes=10
    )
    connection_manager.streams[stream_id] = {
        'last_processed': now
    }
    
    stats = await connection_manager.get_stream_statistics()
    
    assert stream_id in stats
    assert stats[stream_id]['active']
    assert stats[stream_id]['processed_changes'] == 10
    assert stats[stream_id]['last_processed'] == now

@pytest.mark.asyncio
async def test_retry_logic_exponential_backoff(connection_manager, mock_motor_client):
    """Test exponential backoff behavior during retries"""
    # Mock time.sleep to track delay values
    sleep_times = []

    async def mock_sleep(seconds):
        sleep_times.append(seconds)

    # Set up test configuration
    connection_manager.config = {
        'mongodb': {
            'connections': {
                'test_client': {
                    'uri': 'mongodb://test:27017',
                    'sources': {
                        'orders': {
                            'database': 'test',
                            'collection': 'orders'
                        }
                    }
                }
            }
        },
        'monitoring': {
            'connection_timeout': 1000
        },
        'error_handling': {
            'max_retries': 3,
            'max_backoff': 30
        }
    }

    # Set up the mock client's admin command to fail multiple times
    mock_motor_client.admin = AsyncMock()
    mock_motor_client.admin.command = AsyncMock()
    mock_motor_client.close = AsyncMock()

    # Create a counter to track client creation attempts
    client_creation_count = 0

    # Mock the client factory to fail multiple times
    def mock_client_factory(*args, **kwargs):
        nonlocal client_creation_count
        client_creation_count += 1
        raise PyMongoError("Connection failed")

    with patch('asyncio.sleep', mock_sleep), \
         patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', mock_client_factory):

        # Initialize connections
        await connection_manager.initialize_connections()

        # Verify backoff behavior
        assert len(sleep_times) == 2, "Should have attempted 2 retries with backoff"
        for i in range(1, len(sleep_times)):
            assert sleep_times[i] >= sleep_times[i-1], "Backoff should not decrease"

        # Verify final state
        stream_id = 'test_client.orders'
        assert stream_id in connection_manager.status
        status = connection_manager.status[stream_id]
        assert status.retry_count == 3, "Should have recorded 3 retry attempts"
        assert status.total_errors == 3, "Should have recorded 3 errors"
        assert client_creation_count == 3, "Should have made 3 attempts to create client"

@pytest.mark.asyncio
async def test_connection_state_transitions(connection_manager, mock_motor_client):
    """Test connection state transitions during network issues"""
    stream_id = 'test_client.orders'

    # Setup initial state
    connection_manager.status[stream_id] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders'
    )

    # Simulate series of network events
    events = [
        NetworkTimeout("Connection lost"),  # Initial failure
        NetworkTimeout("Connection lost again"),  # Second failure
        NetworkTimeout("Still down"),  # Third failure
    ]

    for event in events:
        await connection_manager._handle_connection_error('test_client', event)

    # Verify state transitions
    status = connection_manager.status[stream_id]
    assert status.total_errors == 3, "Should have recorded three errors"
    assert status.last_error == "Still down", "Last error message should be recorded"
    assert isinstance(status.last_error_time, datetime), "Error time should be recorded"
    assert status.retry_count == 3, "Should have attempted three retries"

@pytest.mark.asyncio
async def test_cursor_timeout_recovery(connection_manager):
    """Test recovery from cursor timeout with resume token"""
    stream_id = 'test_client.orders'
    
    # Create a mock stream that will raise CursorNotFound after first event
    mock_stream = MockChangeStream(
        events=[{'operationType': 'insert', 'fullDocument': {'order_id': 'ORD123'}}],
        max_events=1,
        error_after=1,  # Raise error after first event
        error=CursorNotFound("Cursor not found or timed out")
    )
    
    # Setup initial stream
    mock_collection = AsyncMock()
    def watch_side_effect(*args, **kwargs):
        # First call returns mock_stream, subsequent calls raise CursorNotFound
        if not hasattr(watch_side_effect, 'called'):
            watch_side_effect.called = True
            return mock_stream
        raise CursorNotFound("Cursor not found or timed out")
    mock_collection.watch = MagicMock(side_effect=watch_side_effect)
    
    mock_db = AsyncMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    
    mock_client = AsyncMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    
    # Setup connection manager state
    connection_manager.clients['test_client'] = mock_client
    connection_manager.streams[stream_id] = {
        'stream': mock_stream,  # Use the mock stream directly
        'config': {
            'database': 'testdb',
            'collection': 'orders'
        },
        'last_processed': datetime.now()
    }
    connection_manager.status[stream_id] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders'
    )
    
    # Create and start the processing task
    process_task = asyncio.create_task(connection_manager._process_stream(stream_id))
    
    try:
        # Wait for first event to be processed (with timeout)
        start_time = datetime.now()
        timeout = 2.0  # 2 seconds timeout
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            if connection_manager.status[stream_id].processed_changes > 0:
                break
            if process_task.done():
                if process_task.exception():
                    raise process_task.exception()
                break
            await asyncio.sleep(0.1)
        
        # Verify first event was processed
        assert connection_manager.status[stream_id].processed_changes >= 1, "First event should be processed"
        
        # Wait for CursorNotFound to be raised and handled
        await asyncio.sleep(0.2)
        
        # Verify retry was attempted
        assert connection_manager.status[stream_id].retry_count > 0, "Should have attempted retry"
        assert connection_manager.status[stream_id].total_errors > 0, "Should have recorded error"
        
        # Stop the stream
        connection_manager.status[stream_id].active = False
        
        # Wait for task to complete
        await asyncio.wait_for(process_task, timeout=1.0)
    except asyncio.TimeoutError:
        process_task.cancel()
    finally:
        # Ensure task is cleaned up
        if not process_task.done():
            process_task.cancel()
            await asyncio.gather(process_task, return_exceptions=True)
        
        # Verify stream behavior
        status = connection_manager.status[stream_id]
        assert status.processed_changes >= 1, "Should have processed at least one event"
        assert mock_stream.closed, "Stream should be closed"
        assert status.retry_count > 0, "Should have attempted retry"
        assert status.total_errors > 0, "Should have recorded error"

@pytest.mark.asyncio
async def test_parallel_stream_recovery(connection_manager, mock_motor_client):
    """Test recovery of multiple streams in parallel"""
    # Setup multiple streams
    streams = {
        'test_client.orders': {'collection': 'orders', 'database': 'test'},
        'test_client.users': {'collection': 'users', 'database': 'test'},
        'test_client.products': {'collection': 'products', 'database': 'test'}
    }

    # Set up the client in the connection manager
    connection_manager.clients['test_client'] = mock_motor_client

    # Simulate network failure for all streams
    for stream_id, config in streams.items():
        connection_manager.status[stream_id] = StreamStatus(
            active=True,
            connection_id='test_client',
            source_name=config['collection']
        )
        mock_stream = MockChangeStream(
            events=[{'operationType': 'insert', 'fullDocument': {'test': 'data'}}],
            error_after=1,  # Fail after first event
            error=NetworkTimeout("Connection lost"),
            max_events=2
        )
        connection_manager.streams[stream_id] = {
            'stream': mock_stream,
            'config': config,
            'last_processed': datetime.now()
        }

    # Process all streams in parallel
    tasks = []
    for stream_id in streams.keys():
        task = asyncio.create_task(connection_manager._process_stream(stream_id))
        tasks.append(task)

    # Wait for a short time to let errors occur and retries happen
    await asyncio.sleep(0.5)  # Increased wait time to allow for retries

    # Stop all streams
    for stream_id in streams:
        connection_manager.status[stream_id].active = False

    # Wait for tasks with timeout
    await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=1.0
    )

    # Verify all streams attempted recovery
    for stream_id in streams:
        status = connection_manager.status[stream_id]
        assert status.retry_count > 0, "Should have attempted retry"
        assert status.total_errors > 0, "Should have recorded errors"
        assert "Connection lost" in status.last_error, "Should have recorded error message"

@pytest.mark.asyncio
async def test_max_retry_limit(connection_manager, mock_motor_client):
    """Test that retry attempts respect the maximum limit"""
    stream_id = 'test_client.orders'
    max_retries = connection_manager.config['error_handling']['max_retries']
    
    # Simulate persistent connection failure
    mock_motor_client.admin.command.side_effect = ConnectionFailure("Connection refused")
    
    with patch('motor.motor_asyncio.AsyncIOMotorClient', return_value=mock_motor_client):
        await connection_manager.initialize_connections()
        
        # Verify retry count doesn't exceed max
        status = connection_manager.status[stream_id]
        assert status.retry_count <= max_retries
        assert not status.active  # Stream should be inactive after max retries 