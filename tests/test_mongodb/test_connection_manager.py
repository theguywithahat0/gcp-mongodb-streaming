import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure, OperationFailure

from src.pipeline.mongodb.connection_manager import AsyncMongoDBConnectionManager, StreamStatus

@pytest.fixture
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
            'connection_timeout': 5000
        },
        'error_handling': {
            'max_retries': 3,
            'backoff_factor': 2
        }
    }

@pytest.fixture
def mock_motor_client():
    client = AsyncMock(spec=AsyncIOMotorClient)
    client.admin.command = AsyncMock()
    client.admin.command.return_value = {'ok': 1}  # Successful ping
    return client

@pytest.fixture
async def connection_manager(test_config):
    manager = AsyncMongoDBConnectionManager(test_config)
    yield manager
    await manager.cleanup()

@pytest.mark.asyncio
async def test_initialize_connections_success(connection_manager, mock_motor_client):
    """Test successful initialization of MongoDB connections"""
    with patch('motor.motor_asyncio.AsyncIOMotorClient', return_value=mock_motor_client):
        await connection_manager.initialize_connections()
        
        # Verify client was created
        assert 'test_client' in connection_manager.clients
        
        # Verify ping was called
        mock_motor_client.admin.command.assert_called_once_with('ping')
        
        # Verify stream was initialized
        stream_id = 'test_client.orders'
        assert stream_id in connection_manager.streams
        assert stream_id in connection_manager.status
        
        # Verify stream status
        status = connection_manager.status[stream_id]
        assert status.active
        assert status.connection_id == 'test_client'
        assert status.source_name == 'orders'
        assert status.retry_count == 0
        assert status.total_errors == 0
        assert isinstance(status.last_healthy, datetime)

@pytest.mark.asyncio
async def test_initialize_connections_failure(connection_manager, mock_motor_client):
    """Test handling of connection initialization failure"""
    mock_motor_client.admin.command.side_effect = ConnectionFailure("Connection refused")
    
    with patch('motor.motor_asyncio.AsyncIOMotorClient', return_value=mock_motor_client):
        await connection_manager.initialize_connections()
        
        # Verify error was handled
        stream_id = 'test_client.orders'
        assert stream_id in connection_manager.status
        status = connection_manager.status[stream_id]
        assert not status.active
        assert status.total_errors == 1
        assert "Connection refused" in status.last_error
        assert isinstance(status.last_error_time, datetime)

@pytest.mark.asyncio
async def test_process_change(connection_manager):
    """Test processing of a change event"""
    stream_id = 'test_client.orders'
    connection_manager.status[stream_id] = StreamStatus(
        active=True,
        connection_id='test_client',
        source_name='orders'
    )
    connection_manager.streams[stream_id] = {
        'stream': AsyncMock(),
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
    # Setup mock client and processing task
    connection_manager.clients['test_client'] = mock_motor_client
    mock_task = AsyncMock()
    connection_manager.processing_tasks['test_client.orders'] = mock_task
    
    await connection_manager.cleanup()
    
    # Verify task was cancelled
    mock_task.cancel.assert_called_once()
    
    # Verify client was closed
    mock_motor_client.close.assert_called_once()

@pytest.mark.asyncio
async def test_get_stream_statistics(connection_manager):
    """Test retrieval of stream statistics"""
    stream_id = 'test_client.orders'
    now = datetime.now()
    
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