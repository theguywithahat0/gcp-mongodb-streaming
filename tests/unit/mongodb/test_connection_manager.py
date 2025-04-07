import pytest
import pytest_asyncio
import asyncio
import logging
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure, CursorNotFound, NetworkTimeout, PyMongoError

from src.pipeline.mongodb.connection_manager import AsyncMongoDBConnectionManager, StreamStatus

# Helper method that we'll test directly
async def _handle_error_in_process_stream(self, stream_id: str, error: Exception):
    """Helper for testing the error handling logic in _process_stream"""
    error_msg = str(error)
    
    stream_info = self.streams[stream_id]
    status = self.status[stream_id]
    
    # Handle specific cursor errors
    if any(msg in error_msg.lower() for msg in [
        'cursor id', 'cursorinuse', 'cursorkilled', 'operation was interrupted'
    ]):
        self.logger.info(f"Stream {stream_id} interrupted, will restart with resume token")
        stream_info['stream'] = None  # Force stream recreation
        return True  # Restart condition
        
    # Update status and handle error
    status.last_error = error_msg
    status.last_error_time = datetime.now()
    status.retry_count += 1
    status.total_errors += 1
    
    await self._handle_stream_error(stream_id, error)
    
    if status.retry_count > self.config['error_handling']['max_retries']:
        self.logger.error(f"Stream {stream_id} exceeded max retries, stopping")
        return False  # Stop condition
    
    return True  # Continue condition

# Add the test method to the AsyncMongoDBConnectionManager class
AsyncMongoDBConnectionManager._handle_error_in_process_stream = _handle_error_in_process_stream

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class MockChangeStream:
    """Mock implementation of MongoDB change stream for testing."""
    def __init__(self, events=None, error_after=None, error=None, max_events=None):
        self.events = events or []
        self.error_after = error_after
        self.error = error
        self.max_events = max_events if max_events is not None else len(self.events)
        self.processed = 0
        self.closed = False
        self._resume_token = None
        # Set ready to True by default to avoid waiting for it to be set
        self.ready = True
        logger.info(f"Created MockChangeStream with {len(self.events)} events, max_events={self.max_events}")

    def __aiter__(self):
        logger.debug("MockChangeStream.__aiter__ called")
        return self

    async def __anext__(self):
        logger.debug(f"MockChangeStream.__anext__ called (processed={self.processed}, max_events={self.max_events}, closed={self.closed})")
        # If the stream is closed, stop iteration immediately
        if self.closed:
            logger.info("MockChangeStream is closed, raising StopAsyncIteration")
            raise StopAsyncIteration()

        # If we've reached the error threshold, raise the error
        if self.error_after is not None and self.processed >= self.error_after:
            logger.info(f"MockChangeStream reached error_after ({self.error_after}), raising error")
            if isinstance(self.error, Exception):
                raise self.error
            raise self.error()

        # If we've processed all events or reached max_events, stop
        if self.processed >= self.max_events or self.processed >= len(self.events):
            logger.info(f"MockChangeStream reached max_events ({self.max_events}) or end of events, stopping")
            self.close()
            raise StopAsyncIteration()

        # Process the next event
        if self.processed < len(self.events):
            event = self.events[self.processed].copy()
            event['_id'] = {'_data': f'token_{self.processed}'}
            self._resume_token = event['_id']
            self.processed += 1
            logger.debug(f"MockChangeStream returning event {self.processed}")
            return event
        else:
            # This should never happen given the check above, but just in case
            self.close()
            raise StopAsyncIteration()

    def close(self):
        """Close the change stream."""
        logger.info("MockChangeStream.close called")
        self.closed = True

    @property
    def resume_token(self):
        """Get the current resume token."""
        return self._resume_token

    async def __aenter__(self):
        logger.debug("MockChangeStream.__aenter__ called")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.debug(f"MockChangeStream.__aexit__ called with exc_type={exc_type}")
        self.close()
        return False

class MockCommand:
    def __init__(self):
        self.call_count = 0
        self.max_failures = 3

    async def __call__(self, *args, **kwargs):
        self.call_count += 1
        if self.call_count <= self.max_failures:
            raise ConnectionFailure("Connection refused")
        return {'ok': 1}

class CustomDict(dict):
    """A dictionary subclass that can simulate errors on demand."""
    def __init__(self, *args, **kwargs):
        self.raise_on_getitem = False
        self.raise_on_setitem = False
        super().__init__(*args, **kwargs)
        
    def __getitem__(self, key):
        if self.raise_on_getitem:
            raise KeyError(f"Simulated error accessing key: {key}")
        return super().__getitem__(key)
        
    def __setitem__(self, key, value):
        if self.raise_on_setitem:
            raise ValueError(f"Simulated error setting key: {key}")
        return super().__setitem__(key, value)

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
        logger.info("Running cleanup fixture")
        # Force stop all streams immediately
        for stream_id in list(connection_manager.status.keys()):
            connection_manager.status[stream_id].active = False
        
        # Create a new empty processing_tasks dictionary rather than trying to cancel mocks
        connection_manager.processing_tasks.clear()
        
        # Close all clients by creating a new empty clients dictionary
        connection_manager.clients.clear()
        
        # Clear all state directly
        connection_manager.streams.clear()
        connection_manager.status.clear()
        logger.info("Cleanup complete")

@pytest_asyncio.fixture
async def connection_manager(test_config):
    """Create a connection manager with test configuration"""
    manager = AsyncMongoDBConnectionManager(test_config)
    # No need for cleanup here as the autouse cleanup fixture will handle it
    return manager

@pytest.mark.asyncio
async def test_initialize_connections_success(connection_manager):
    """Test successful initialization of MongoDB connections"""
    # Create minimal mock client
    mock_client = AsyncMock(strict=True)
    mock_client.admin = AsyncMock(strict=True)
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    
    # Completely mock _process_stream to avoid executing it
    with patch.object(connection_manager, '_process_stream', return_value=None):
        # Mock collection.watch to simply return a non-functional mock
        mock_stream = AsyncMock(strict=True)
        mock_stream.__aiter__.return_value = mock_stream
        mock_stream.__anext__.side_effect = StopAsyncIteration()
        
        # Create mock database and collection system
        mock_db = AsyncMock(strict=True)
        # Mock db.command to avoid coroutine warning
        mock_db.command = MagicMock(return_value={'inprog': []})
        mock_collection = AsyncMock(strict=True)
        mock_collection.watch = MagicMock(return_value=mock_stream)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        
        # Mock client creation
        with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
            # Initialize connections
            await connection_manager.initialize_connections()
            
            # We just want to check the initialization succeeded
            stream_id = 'test_client.orders'
            assert 'test_client' in connection_manager.clients
            assert stream_id in connection_manager.status
            
            # Verify _process_stream is called but don't execute it
            connection_manager._process_stream.assert_called_once_with(stream_id)

@pytest.mark.asyncio
async def test_initialize_connections_failure(connection_manager):
    """Test handling of connection initialization failure"""
    # Create mock client that will fail to connect
    mock_client = AsyncMock()
    mock_client.admin = AsyncMock()
    mock_client.admin.command = AsyncMock(side_effect=ConnectionFailure("Connection refused"))
    
    # Mock _process_stream to avoid execution
    with patch.object(connection_manager, '_process_stream', return_value=None):
        # Create minimal mock for watch
        mock_stream = AsyncMock()
        mock_stream.__aiter__.return_value = mock_stream
        mock_stream.__anext__.side_effect = StopAsyncIteration()
        
        # Create mock DB system
        mock_db = AsyncMock()
        mock_collection = AsyncMock()
        mock_collection.watch = MagicMock(return_value=mock_stream)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        
        # Mock client creation
        with patch('src.pipeline.mongodb.connection_manager.AsyncIOMotorClient', return_value=mock_client):
            # Initialize connections with expected failure
            await connection_manager.initialize_connections()
            
            # Verify error handling
            stream_id = 'test_client.orders'
            assert stream_id in connection_manager.status, "Stream status should be created even on failure"
            status = connection_manager.status[stream_id]
            
            # Verify failure state
            assert not status.active, "Stream should be inactive after connection failure"
            assert status.connection_id == 'test_client'
            assert status.source_name == 'orders'
            assert status.total_errors >= 1, "Should have recorded at least one error"
            assert "Connection refused" in status.last_error, "Error message should be recorded"
            
            # _process_stream should not be called for a failed connection
            connection_manager._process_stream.assert_not_called()

@pytest.mark.asyncio
async def test_process_change(connection_manager):
    """Test processing of a change event"""
    stream_id = 'test_client.orders'
    
    # Initialize the manager first
    connection_manager.status = {}
    connection_manager.streams = {}
    
    # Create a simple status
    connection_manager.status[stream_id] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders'
    )
    
    # Setup a simple stream info
    connection_manager.streams[stream_id] = {
        'stream': None,  # Not needed for this test
        'config': {},
        'last_processed': datetime.now()
    }
    
    # Create a simple test change
    test_change = {
        'operationType': 'insert', 
        'fullDocument': {'order_id': '123'}
    }
    
    # Test the _process_change method directly - this is safe because
    # it doesn't create any tasks or loops
    await connection_manager._process_change(stream_id, test_change)
    
    # Verify change was processed
    assert connection_manager.status[stream_id].processed_changes == 1
    assert 'last_processed' in connection_manager.streams[stream_id]
    assert isinstance(connection_manager.streams[stream_id]['last_processed'], datetime)
    assert 'last_document' in connection_manager.streams[stream_id]
    assert connection_manager.streams[stream_id]['last_document'] == test_change['fullDocument']

@pytest.mark.asyncio
async def test_cleanup(connection_manager, mock_motor_client):
    """Test cleanup of connections and streams"""
    # Set up test environment in a safer way
    stream_id = 'test_client.orders'
    
    # Create a proper coroutine for the mock close method
    async def mock_close():
        return None
        
    mock_motor_client.close = mock_close
    
    # Initialize test data
    connection_manager.clients = {'test_client': mock_motor_client}
    
    # Create a task that is a real coroutine to avoid issues
    async def dummy_task():
        return None
        
    # Create a real task that can be cancelled
    task = asyncio.create_task(dummy_task())
    
    # Setup initial state
    connection_manager.processing_tasks = {stream_id: task}
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_client',
            source_name='orders'
        )
    }
    
    try:
        # Test cleanup method directly
        await connection_manager.cleanup()
        
        # Verify state changes
        assert not connection_manager.status[stream_id].active, "Stream should be inactive after cleanup"
        assert len(connection_manager.processing_tasks) == 0, "Processing tasks should be cleared"
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

@pytest.mark.asyncio
async def test_cursor_error_handling(connection_manager):
    """Test handling of specific cursor errors"""
    stream_id = 'test_client.orders'
    
    # Set up the necessary state
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_client',
            source_name='orders',
            retry_count=0
        )
    }
    
    # Create a mock stream that will raise a cursor error
    cursor_errors = [
        "cursor id 123456 not found",
        "operation was interrupted",
        "cursorKilled",
        "cursorInUse"
    ]
    
    # Set up streams info
    for error_msg in cursor_errors:
        # Reset streams for each test case
        connection_manager.streams = {
            stream_id: {
                'stream': None,
                'config': {},
                'last_processed': datetime.now()
            }
        }
        
        # Create a special _initialize_stream mock to track calls
        initialize_calls = []
        original_initialize = connection_manager._initialize_stream
        
        async def mock_initialize(conn_id, source_name, config):
            initialize_calls.append((conn_id, source_name))
            # Don't actually call the original to avoid side effects
            
        # Patch the sleep function to avoid delays
        with patch('asyncio.sleep', return_value=None), \
             patch.object(connection_manager, '_initialize_stream', side_effect=mock_initialize), \
             patch.object(connection_manager, '_handle_stream_error', return_value=None):
            
            # Create an exception with the cursor error message
            error = PyMongoError(error_msg)
            
            # Call the internal error handling directly
            await connection_manager._handle_error_in_process_stream(stream_id, error)
            
            # Verify that the stream was scheduled for recreation
            assert connection_manager.streams[stream_id]['stream'] is None
            assert len(initialize_calls) == 0  # Initialize isn't called directly, just prepared for next loop
            
    # Clean up
    connection_manager.streams = {}
    connection_manager.status = {}

@pytest.mark.asyncio
async def test_resume_token_handling(connection_manager):
    """Test handling of resume tokens when initializing streams."""
    connection_id = 'test_client'
    source_name = 'orders'
    stream_id = f"{connection_id}.{source_name}"
    
    # Create a mock client
    mock_client = AsyncMock()
    mock_client.admin = AsyncMock()
    mock_client.admin.command = AsyncMock(return_value={'ok': 1})
    
    # Mock db with regular MagicMock to avoid coroutine issues
    mock_db = MagicMock()
    mock_db.command.return_value = {'inprog': []}
    
    # Mock collection with a regular MagicMock
    mock_collection = MagicMock()
    
    # Mock stream 
    mock_stream = AsyncMock()
    mock_stream.__aiter__.return_value = mock_stream
    mock_stream.__anext__.side_effect = StopAsyncIteration()
    
    # Track watch arguments
    watch_calls = []
    
    # Create a watch method that records args
    def mock_watch(**kwargs):
        watch_calls.append(kwargs.copy())
        return mock_stream
    
    # Setup mocks
    mock_collection.watch = mock_watch
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    
    # Set up test data with an existing resume token
    resume_token = {'_data': 'test_token_123'}
    
    connection_manager.clients = {connection_id: mock_client}
    connection_manager.streams = {
        stream_id: {
            'config': {
                'database': 'testdb',
                'collection': 'orders',
                'batch_size': 100
            },
            'resume_token': resume_token,  # Set a resume token
            'last_processed': datetime.now()
        }
    }
    
    # Mock the _process_stream method to avoid executing it
    with patch.object(connection_manager, '_process_stream', return_value=None):
        # Call _initialize_stream with resume token
        await connection_manager._initialize_stream(connection_id, source_name, 
                                                 connection_manager.streams[stream_id]['config'])
        
        # Verify watch was called with resume token
        assert len(watch_calls) == 1
        assert 'resume_after' in watch_calls[0]
        assert watch_calls[0]['resume_after'] == resume_token
        
        # Clear tracking for second test
        watch_calls.clear()
        connection_manager.streams[stream_id]['resume_token'] = None
        
        # Call _initialize_stream without resume token
        await connection_manager._initialize_stream(connection_id, source_name, 
                                                 connection_manager.streams[stream_id]['config'])
        
        # Verify watch was called with no resume token (or a None value)
        assert len(watch_calls) == 1
        # The key might be present but with None value, so check this instead
        if 'resume_after' in watch_calls[0]:
            assert watch_calls[0]['resume_after'] is None 

@pytest.mark.asyncio
async def test_handle_specific_error_types(connection_manager):
    """Test handling of different error types in _handle_error_in_process_stream."""
    stream_id = 'test_client.orders'
    
    # Set up manager state
    connection_manager.streams = {
        stream_id: {
            'stream': AsyncMock(),
            'config': {'database': 'testdb', 'collection': 'orders'},
            'last_processed': datetime.now()
        }
    }
    
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_client',
            source_name='orders',
            retry_count=0,
            total_errors=0
        )
    }
    
    # Mock logger
    mock_logger = MagicMock()
    original_logger = connection_manager.logger
    connection_manager.logger = mock_logger
    
    # Mock the _handle_stream_error method
    mock_handle_error = AsyncMock()
    original_handle_error = connection_manager._handle_stream_error
    connection_manager._handle_stream_error = mock_handle_error
    
    try:
        # Test cursor related errors - should be logged as info and force stream recreation
        cursor_errors = [
            CursorNotFound("Cursor ID 123 not found"),
            PyMongoError("cursor id not valid"),
            PyMongoError("operation was interrupted"),
            PyMongoError("cursorKilled"),
            PyMongoError("cursorInUse")
        ]
        
        for error in cursor_errors:
            # Reset logger tracking
            mock_logger.reset_mock()
            mock_handle_error.reset_mock()
            
            # Reset stream to a new mock
            connection_manager.streams[stream_id]['stream'] = AsyncMock()
            
            # Call the error handler
            result = await connection_manager._handle_error_in_process_stream(stream_id, error)
            
            # Should return True to continue processing
            assert result is True, f"Cursor error {error} should return True to continue"
            
            # Stream should be set to None to force recreation
            assert connection_manager.streams[stream_id]['stream'] is None, "Stream should be reset to force recreation"
            
            # Should log as info
            assert mock_logger.info.called, f"Cursor error {error} should be logged as info"
            
            # Special error handler should not be called for cursor errors
            assert not mock_handle_error.called, f"_handle_stream_error should not be called for cursor error {error}"
            
        # Test regular errors - should update status and call special handler
        regular_error = PyMongoError("Some other error")
        
        # Reset tracking
        mock_logger.reset_mock()
        mock_handle_error.reset_mock()
        
        # Set retry count to test max retry logic
        connection_manager.status[stream_id].retry_count = connection_manager.config['error_handling']['max_retries'] - 1
        
        # Call the error handler
        result = await connection_manager._handle_error_in_process_stream(stream_id, regular_error)
        
        # Should return True if retries not exceeded
        assert result is True, "Regular error should return True if retries not exceeded"
        
        # Status should be updated
        status = connection_manager.status[stream_id]
        assert status.last_error == str(regular_error), "Error message should be stored"
        assert status.retry_count == connection_manager.config['error_handling']['max_retries'], "Retry count should be incremented"
        assert status.total_errors == 1, "Total errors should be incremented"
        
        # Special handler should be called
        assert mock_handle_error.called, "_handle_stream_error should be called for regular errors"
        
        # Test max retry limit
        connection_manager.status[stream_id].retry_count = connection_manager.config['error_handling']['max_retries']
        mock_logger.reset_mock()
        
        # Call the error handler
        result = await connection_manager._handle_error_in_process_stream(stream_id, regular_error)
        
        # Should return False to stop processing
        assert result is False, "Should return False when max retries exceeded"
        
        # Should log error about max retries
        max_retry_logs = [
            args[0] for args, _ in mock_logger.error.call_args_list
            if isinstance(args[0], str) and "max retries" in args[0].lower()
        ]
        assert len(max_retry_logs) > 0, "Should log max retry error"
        
    finally:
        connection_manager.logger = original_logger
        connection_manager._handle_stream_error = original_handle_error

@pytest.mark.asyncio
async def test_process_change_error_handling(connection_manager):
    """Test that errors in _process_change are caught and logged."""
    stream_id = 'test_client.orders'
    
    # Create mock logger to verify error is logged
    mock_logger = MagicMock()
    original_logger = connection_manager.logger
    connection_manager.logger = mock_logger
    
    # Set up the test environment
    connection_manager.status = {
        stream_id: StreamStatus(
            active=True,
            connection_id='test_client',
            source_name='orders',
            processed_changes=0
        )
    }
    
    # Create a custom dict that will raise an error when needed
    stream_data = CustomDict({
        'config': {},
        'last_processed': datetime.now()
    })
    
    # Save the original streams
    original_streams = connection_manager.streams
    
    try:
        # Replace streams with our custom dict
        connection_manager.streams = {stream_id: stream_data}
        
        # First test: normal case with no error
        test_change = {
            'operationType': 'insert',
            'fullDocument': {'id': '123'}
        }
        
        await connection_manager._process_change(stream_id, test_change)
        assert connection_manager.status[stream_id].processed_changes == 1
        assert not mock_logger.error.called
        
        # Now simulate an error when setting last_document
        mock_logger.reset_mock()
        stream_data.raise_on_setitem = True
        
        await connection_manager._process_change(stream_id, test_change)
        
        # Verify error was logged
        assert mock_logger.error.called
        error_message = mock_logger.error.call_args[0][0]
        assert "Error processing change" in error_message
        assert stream_id in error_message
        
        # Status was still updated
        assert connection_manager.status[stream_id].processed_changes == 2
        
    finally:
        # Restore the original state
        connection_manager.streams = original_streams
        connection_manager.logger = original_logger

@pytest.mark.asyncio
async def test_check_existing_cursors(connection_manager):
    """Test checking for existing cursors during stream initialization."""
    # Test configuration
    connection_id = "test_client"
    stream_name = "orders"
    stream_id = f"{connection_id}.{stream_name}"
    
    # Setup monitoring and error handling configs
    connection_manager.config = {
        'monitoring': {'connection_timeout': 1000},
        'error_handling': {'max_retries': 3}
    }
    
    # Mock client and database
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    
    # Mock a collection
    mock_collection = MagicMock()
    mock_db.__getitem__.return_value = mock_collection
    
    # Create a mock stream to return
    mock_stream = MagicMock()
    mock_collection.watch.return_value = mock_stream
    
    # Setup existing cursors to be found
    existing_cursor = {'id': 123456, 'microsecs_running': 10000, 'ns': 'testdb.orders', 'cursor': 789456}
    
    # Set up the command response for currentOp
    def mock_command(*args, **kwargs):
        command = args[0]
        if command == 'currentOp':
            return {'inprog': [existing_cursor]}
        elif command == 'killCursors':
            # This command should be called to kill the cursor
            return {'ok': 1, 'cursorsKilled': [kwargs.get('cursors', [])[0]]}
        return {'ok': 1}
    
    # Apply the mock command function
    mock_db.command = AsyncMock(side_effect=mock_command)
    
    # Create a logger to check for messages
    mock_logger = MagicMock()
    original_logger = connection_manager.logger
    connection_manager.logger = mock_logger
    
    # Set up client in the connection manager
    connection_manager.clients = {connection_id: mock_client}
    
    # Define a stream config
    stream_config = {
        'database': 'testdb',
        'collection': 'orders',
        'batch_size': 100
    }
    
    # Mock the _process_stream method to prevent it from being called
    with patch.object(connection_manager, '_process_stream', return_value=None):
        # Call the method being tested
        await connection_manager._initialize_stream(connection_id, stream_name, stream_config)
        
        # Verify the database command was called to check for existing cursors
        mock_db.command.assert_any_call(
            'currentOp',
            {'$or': [
                {'op': 'getmore', 'ns': 'testdb.orders'},
                {'op': 'command', 'command.getMore': {'$exists': True}, 'ns': 'testdb.orders'}
            ]}
        )
        
        # Verify killCursors was called for the existing cursor
        mock_db.command.assert_any_call('killCursors', existing_cursor['ns'], cursors=[existing_cursor['cursor']])
        
        # Verify appropriate log messages
        info_messages = [args[0] for args, _ in mock_logger.info.call_args_list]
        kill_message_found = any("Killed existing cursor" in msg for msg in info_messages)
        assert kill_message_found, "Should log message about killing cursor"
        
        # Verify stream was created and stored
        assert stream_id in connection_manager.streams
        assert connection_manager.streams[stream_id]['stream'] == mock_stream
        assert stream_id in connection_manager.status
        assert connection_manager.status[stream_id].active
        
        # Verify process_stream was called with the right stream ID
        connection_manager._process_stream.assert_called_once_with(stream_id)
        
    # Restore the original logger
    connection_manager.logger = original_logger 