"""Basic end-to-end test for MongoDB Change Stream to Pub/Sub connector."""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, UTC

# Set up environment variables for emulators
os.environ['PUBSUB_EMULATOR_HOST'] = 'localhost:8085'
os.environ['FIRESTORE_EMULATOR_HOST'] = 'localhost:8086'
os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project'

from google.cloud import firestore, pubsub_v1
from google.api_core import retry
from pymongo import MongoClient

from connector.config.config_manager import (
    ConnectorConfig,
    MongoDBConfig,
    PubSubConfig,
    FirestoreConfig,
    MongoDBCollectionConfig,
    PublisherConfig,
    BatchSettings,
    HealthConfig,
    HealthEndpointsConfig,
    ReadinessConfig,
    HeartbeatConfig,
    BackpressureConfig,
    CircuitBreakerSettings
)
from connector.core.change_stream_listener import ChangeStreamListener
from connector.utils.serialization import deserialize_message, SerializationFormat

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def wait_for_operation(received_messages, expected_operation, timeout=10):
    """Wait for a specific operation to be received."""
    start_time = datetime.now()
    while (datetime.now() - start_time).total_seconds() < timeout:
        for msg in received_messages:
            if msg['operation'] == expected_operation:
                return True
        await asyncio.sleep(0.1)
    return False

async def run_test():
    """Run a simple end-to-end test."""
    listener = None
    listener_task = None
    future = None
    try:
        # Connect to MongoDB
        mongo_client = MongoClient('mongodb://localhost:27017/?replicaSet=rs0')
        db = mongo_client.test
        collection = db.test_collection
        
        # Clear existing data
        collection.delete_many({})
        logger.info("Connected to MongoDB and cleared test collection")

        # Initialize Pub/Sub (using emulator)
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('test-project', 'test-topic')
        status_topic_path = publisher.topic_path('test-project', 'status-topic')
        
        # Create topics with retry
        def create_topic_with_retry(topic_path):
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    publisher.create_topic(request={"name": topic_path})
                    logger.info(f"Created topic: {topic_path}")
                    return
                except Exception as e:
                    if "Topic already exists" in str(e):
                        logger.info(f"Topic already exists: {topic_path}")
                        return
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"Failed to create topic (attempt {attempt + 1}): {e}")
                    time.sleep(1)

        create_topic_with_retry(topic_path)
        create_topic_with_retry(status_topic_path)

        # Initialize subscriber with retry
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path('test-project', 'test-sub')
        
        def create_subscription_with_retry():
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    subscriber.create_subscription(
                        request={"name": subscription_path, "topic": topic_path}
                    )
                    logger.info(f"Created subscription: {subscription_path}")
                    return
                except Exception as e:
                    if "Subscription already exists" in str(e):
                        logger.info(f"Subscription already exists: {subscription_path}")
                        return
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"Failed to create subscription (attempt {attempt + 1}): {e}")
                    time.sleep(1)

        create_subscription_with_retry()

        # Verify topics exist
        try:
            publisher.get_topic(request={"topic": topic_path})
            publisher.get_topic(request={"topic": status_topic_path})
            logger.info("Verified topics exist")
        except Exception as e:
            logger.error(f"Failed to verify topics: {e}")
            raise

        # Initialize Firestore (using emulator)
        firestore_client = firestore.Client(
            project='test-project',
            client_options={'api_endpoint': 'localhost:8086'}
        )

        # Store received messages and test start time
        received_messages = []
        test_start_time = datetime.now(UTC)

        def message_callback(message):
            try:
                logger.info(f"Received raw message with attributes: {message.attributes}")
                
                # Get serialization format from attributes
                format_str = message.attributes.get('serialization_format', 'msgpack')
                format = SerializationFormat(format_str)
                logger.info(f"Using serialization format: {format}")
                
                # Deserialize message data
                data = deserialize_message(message.data, format=format)
                logger.info(f"Deserialized message data: {data}")
                
                # Only process messages from after our test started
                if 'data' in data and 'wallTime' in data['data']:
                    try:
                        wall_time = datetime.fromisoformat(data['data']['wallTime'])
                        # Ensure wall_time is timezone-aware
                        if wall_time.tzinfo is None:
                            wall_time = wall_time.replace(tzinfo=UTC)
                        if wall_time >= test_start_time:
                            logger.info(f"Processing message with wall_time: {wall_time}")
                            received_messages.append(data)
                        else:
                            logger.info(f"Skipping message with old wall_time: {wall_time}")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error processing message timestamp: {e}")
                else:
                    logger.warning("Message missing data or wallTime fields")
                message.ack()
            except Exception as e:
                logger.error(f"Error in message callback: {e}")
                message.nack()  # Negative acknowledge to retry

        # Start subscriber
        future = subscriber.subscribe(subscription_path, message_callback)
        logger.info("Started Pub/Sub subscriber")

        # Wait a moment for the subscriber to be ready
        await asyncio.sleep(2)

        # Basic connector config
        config = ConnectorConfig(
            mongodb=MongoDBConfig(
                uri="mongodb://localhost:27017/?replicaSet=rs0",
                database="test",
                warehouse_id="TEST_WH",
                collections=[
                    MongoDBCollectionConfig(
                        name="test_collection",
                        topic="test-topic",
                        watch_full_document=True
                    )
                ]
            ),
            pubsub=PubSubConfig(
                project_id='test-project',
                status_topic='status-topic',
                publisher=PublisherConfig(
                    batch_settings=BatchSettings(
                        max_messages=100,
                        max_bytes=1024 * 1024,
                        max_latency=2.0
                    ),
                    backpressure=BackpressureConfig(
                        tokens_per_second=1000.0,
                        bucket_size=2000,
                        min_tokens_per_second=100.0,
                        max_tokens_per_second=5000.0,
                        error_decrease_factor=0.8,
                        success_increase_factor=1.1,
                        rate_update_interval=5.0,
                        metrics_window_size=100
                    ),
                    circuit_breaker=CircuitBreakerSettings(
                        failure_threshold=5,
                        reset_timeout=60.0,
                        half_open_max_calls=3
                    )
                )
            ),
            firestore=FirestoreConfig(
                collection='message_dedup',
                ttl=86400  # 24 hours
            ),
            health=HealthConfig(
                endpoints=HealthEndpointsConfig(
                    health="/health",
                    readiness="/readiness",
                    metrics="/metrics"
                ),
                readiness=ReadinessConfig(timeout=5),
                heartbeat=HeartbeatConfig(enabled=False)
            )
        )

        # Initialize and start change stream listener
        try:
            listener = ChangeStreamListener(
                config=config,
                collection_config=config.mongodb.collections[0],
                mongo_client=mongo_client,
                publisher=publisher,
                firestore_client=firestore_client,
                serialization_format=SerializationFormat.MSGPACK,
                backpressure_config=config.pubsub.publisher.backpressure
            )
            logger.info("Change stream listener initialized")

            # Start the listener
            listener_task = asyncio.create_task(listener.start())
            await asyncio.sleep(2)  # Wait for listener to be ready
            
            # Verify listener is running
            if not listener_task.done():
                logger.info("Change stream listener started successfully")
            else:
                exc = listener_task.exception()
                if exc:
                    logger.error(f"Change stream listener failed to start: {exc}")
                    raise exc
                else:
                    logger.error("Change stream listener stopped unexpectedly")
                    raise RuntimeError("Change stream listener stopped unexpectedly")

        except Exception as e:
            logger.error(f"Failed to initialize or start change stream listener: {e}")
            raise

        # Test 1: Insert document
        test_doc = {
            "_id": "test1",
            "_schema_version": "v1",
            "transaction_id": "T123",
            "product_id": "P123",
            "warehouse_id": "W1",
            "quantity": 10,
            "transaction_type": "sale",
            "timestamp": datetime.now(UTC).isoformat(),
            "order_id": "O123",
            "customer_id": "C123",
            "notes": "Test transaction"
        }
        collection.insert_one(test_doc)
        logger.info("Inserted test document")
        
        # Wait for insert operation to be confirmed
        assert await wait_for_operation(received_messages, 'insert'), "Insert operation not received"
        logger.info("Insert operation confirmed")

        # Test 2: Update document
        collection.update_one(
            {"_id": "test1"},
            {"$set": {
                "quantity": 15,
                "notes": "Updated test transaction",
                "timestamp": datetime.now(UTC).isoformat()
            }}
        )
        logger.info("Updated test document")
        
        # Wait for update operation to be confirmed
        assert await wait_for_operation(received_messages, 'update'), "Update operation not received"
        logger.info("Update operation confirmed")

        # Test 3: Delete document
        collection.delete_one({"_id": "test1"})
        logger.info("Deleted test document")
        
        # Wait for delete operation to be confirmed
        assert await wait_for_operation(received_messages, 'delete'), "Delete operation not received"
        logger.info("Delete operation confirmed")

        # Verify results
        logger.info(f"Received {len(received_messages)} messages:")
        for msg in received_messages:
            logger.info(f"Operation: {msg['operation']}")

        # Basic assertions
        assert len(received_messages) == 3, f"Expected 3 messages, got {len(received_messages)}"
        
        logger.info("All test assertions passed!")

    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

    finally:
        # Cleanup
        try:
            # Stop the change stream listener
            if listener:
                await listener.stop()
                
            # Cancel the listener task
            if listener_task:
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass

            # Cancel the subscriber
            if future:
                future.cancel()
                
            # Clean up the collection
            collection.delete_many({})

            # Clean up Pub/Sub resources
            try:
                subscriber.delete_subscription(request={"subscription": subscription_path})
                publisher.delete_topic(request={"topic": topic_path})
                publisher.delete_topic(request={"topic": status_topic_path})
            except Exception as e:
                logger.warning(f"Error during Pub/Sub cleanup: {e}")

            # Close clients
            mongo_client.close()
            subscriber.close()
            publisher.close()

        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")

if __name__ == "__main__":
    asyncio.run(run_test()) 