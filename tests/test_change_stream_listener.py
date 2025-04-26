import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pymongo.errors import ConnectionFailure, NetworkTimeout
from ..src.connector.core.change_stream_listener import ChangeStreamListener
from ..src.connector.config.config_manager import ConnectorConfig, MongoDBCollectionConfig, RetryConfig

@pytest.fixture
def retry_config():
    return RetryConfig(
        initial_delay=0.1,  # Small delay for faster tests
        max_delay=1.0,
        multiplier=2.0,
        jitter=0.1,
        max_retries=3
    )

@pytest.fixture
def connector_config(retry_config):
    config = MagicMock(spec=ConnectorConfig)
    config.retry = retry_config
    config.mongodb.database = "test_db"
    config.pubsub.project_id = "test-project"
    config.pubsub.status_topic = "status-topic"
    config.health.heartbeat.interval = 0.1
    return config

@pytest.fixture
def collection_config():
    config = MagicMock(spec=MongoDBCollectionConfig)
    config.name = "test_collection"
    config.topic = "test-topic"
    config.deduplication.enabled = False
    return config

@pytest.fixture
def mock_mongo_client():
    client = MagicMock()
    collection = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = collection
    return client, collection

@pytest.mark.asyncio
async def test_retry_on_connection_failure(connector_config, collection_config, mock_mongo_client):
    """Test that the listener retries on connection failures."""
    client, collection = mock_mongo_client
    
    # Configure the collection to raise errors then succeed
    collection.watch.side_effect = [
        ConnectionFailure("Connection lost"),
        NetworkTimeout("Timeout"),
        MagicMock()  # Success on third try
    ]

    listener = ChangeStreamListener(
        config=connector_config,
        collection_config=collection_config,
        mongo_client=client
    )

    # Mock sleep to speed up tests
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        # Start the listener
        start_task = asyncio.create_task(listener.start())
        
        # Let it run for a bit
        await asyncio.sleep(0.1)
        
        # Stop the listener
        await listener.stop()
        await start_task

        # Verify retry behavior
        assert collection.watch.call_count == 3
        assert mock_sleep.call_count >= 2  # At least 2 retries

        # Verify exponential backoff
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert delays[0] < delays[1]  # Second delay should be longer

@pytest.mark.asyncio
async def test_max_retries_exceeded(connector_config, collection_config, mock_mongo_client):
    """Test that the listener stops after max retries."""
    client, collection = mock_mongo_client
    
    # Configure the collection to always fail
    collection.watch.side_effect = ConnectionFailure("Connection lost")

    listener = ChangeStreamListener(
        config=connector_config,
        collection_config=collection_config,
        mongo_client=client
    )

    # Mock sleep to speed up tests
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        # Start the listener
        start_task = asyncio.create_task(listener.start())
        
        # Let it run for a bit
        await asyncio.sleep(0.1)
        
        # Stop the listener
        await listener.stop()
        await start_task

        # Verify retry attempts
        assert collection.watch.call_count <= connector_config.retry.max_retries + 1
        assert not listener.is_running  # Listener should stop after max retries

@pytest.mark.asyncio
async def test_retry_success_recovery(connector_config, collection_config, mock_mongo_client):
    """Test that the listener continues processing after successful retry."""
    client, collection = mock_mongo_client
    
    # Mock change stream
    mock_change_stream = MagicMock()
    mock_change_stream.__next__.side_effect = [
        {"operationType": "insert", "documentKey": {"_id": "test1"}},
        None  # End the stream
    ]
    
    # Configure collection to fail once then succeed
    collection.watch.side_effect = [
        ConnectionFailure("Connection lost"),
        mock_change_stream
    ]

    listener = ChangeStreamListener(
        config=connector_config,
        collection_config=collection_config,
        mongo_client=client
    )

    # Mock sleep to speed up tests
    with patch('asyncio.sleep', new_callable=AsyncMock):
        # Start the listener
        start_task = asyncio.create_task(listener.start())
        
        # Let it run for a bit
        await asyncio.sleep(0.1)
        
        # Stop the listener
        await listener.stop()
        await start_task

        # Verify recovery
        assert collection.watch.call_count == 2
        assert mock_change_stream.__next__.call_count == 2  # Processed one change and None 