#!/usr/bin/env python3
"""
PubSub Client for Stock Monitoring Integration Tests

This module provides a simplified interface for working with Google Pub/Sub
during integration testing.
"""

import os
import json
import logging
import asyncio
from typing import Dict, List, Any, Optional, Callable, Union
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PubSubClient:
    """Client for interacting with Google Cloud Pub/Sub during tests."""
    
    def __init__(self, project_id: str):
        """
        Initialize the PubSub client.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        
        # Use GCP authentication
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        
        # Keep track of active subscriptions
        self.active_subscriptions = {}
    
    def create_topic(self, topic_id: str) -> str:
        """
        Create a topic if it doesn't exist.
        
        Args:
            topic_id: ID of the topic to create
            
        Returns:
            The full topic path
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        
        try:
            self.publisher.get_topic(request={"topic": topic_path})
            logger.info(f"Topic {topic_id} already exists")
        except NotFound:
            self.publisher.create_topic(request={"name": topic_path})
            logger.info(f"Created topic {topic_id}")
        
        return topic_path
    
    def create_subscription(self, topic_id: str, subscription_id: str) -> str:
        """
        Create a subscription to a topic if it doesn't exist.
        
        Args:
            topic_id: ID of the topic to subscribe to
            subscription_id: ID for the subscription
            
        Returns:
            The full subscription path
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        subscription_path = self.subscriber.subscription_path(
            self.project_id, subscription_id
        )
        
        try:
            self.subscriber.get_subscription(
                request={"subscription": subscription_path}
            )
            logger.info(f"Subscription {subscription_id} already exists")
        except NotFound:
            self.subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "ack_deadline_seconds": 10,
                    "expiration_policy": {"ttl": {"seconds": 86400}}  # 1 day
                }
            )
            logger.info(f"Created subscription {subscription_id} to topic {topic_id}")
        
        return subscription_path
    
    async def publish_message(
        self, 
        topic_id: str, 
        data: Union[Dict[str, Any], str],
        attributes: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Publish a message to a topic.
        
        Args:
            topic_id: ID of the topic to publish to
            data: Message data (dictionary will be converted to JSON)
            attributes: Optional message attributes
            
        Returns:
            Message ID if successful
        """
        # Create the topic path
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        
        # Convert data to bytes
        if isinstance(data, dict):
            data_bytes = json.dumps(data).encode("utf-8")
        else:
            data_bytes = str(data).encode("utf-8")
        
        # Set default attributes if none provided
        attributes = attributes or {}
        
        # Publish in async way
        future = self.publisher.publish(topic_path, data=data_bytes, **attributes)
        
        # Create an asyncio future to wait for the result
        loop = asyncio.get_running_loop()
        asyncio_future = loop.create_future()
        
        def callback(pubsub_future):
            try:
                message_id = pubsub_future.result()
                loop.call_soon_threadsafe(
                    lambda: asyncio_future.set_result(message_id)
                )
            except Exception as e:
                loop.call_soon_threadsafe(
                    lambda: asyncio_future.set_exception(e)
                )
        
        future.add_done_callback(callback)
        
        # Wait for the result
        try:
            message_id = await asyncio_future
            logger.debug(f"Published message with ID: {message_id}")
            return message_id
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            raise
    
    def subscribe(
        self, 
        subscription_id: str, 
        callback: Callable
    ) -> pubsub_v1.subscriber.futures.StreamingPullFuture:
        """
        Subscribe to messages on a subscription.
        
        Args:
            subscription_id: ID of the subscription
            callback: Callback function to process messages
            
        Returns:
            Streaming pull future for the subscription
        """
        subscription_path = self.subscriber.subscription_path(
            self.project_id, subscription_id
        )
        
        streaming_pull_future = self.subscriber.subscribe(
            subscription_path, callback=callback
        )
        
        self.active_subscriptions[subscription_id] = streaming_pull_future
        logger.info(f"Started subscription to {subscription_id}")
        
        return streaming_pull_future
    
    def stop_subscription(self, subscription_id: str) -> bool:
        """
        Stop a subscription.
        
        Args:
            subscription_id: ID of the subscription to stop
            
        Returns:
            True if successful
        """
        if subscription_id in self.active_subscriptions:
            streaming_pull_future = self.active_subscriptions.pop(subscription_id)
            streaming_pull_future.cancel()
            logger.info(f"Stopped subscription to {subscription_id}")
            return True
        
        logger.warning(f"No active subscription found for {subscription_id}")
        return False
    
    def stop_all_subscriptions(self):
        """Stop all active subscriptions."""
        for subscription_id, future in list(self.active_subscriptions.items()):
            future.cancel()
            logger.info(f"Stopped subscription to {subscription_id}")
        
        self.active_subscriptions.clear()
    
    def delete_subscription(self, subscription_id: str) -> bool:
        """
        Delete a subscription.
        
        Args:
            subscription_id: ID of the subscription to delete
            
        Returns:
            True if successful
        """
        subscription_path = self.subscriber.subscription_path(
            self.project_id, subscription_id
        )
        
        # Stop the subscription first if it's active
        self.stop_subscription(subscription_id)
        
        try:
            self.subscriber.delete_subscription(
                request={"subscription": subscription_path}
            )
            logger.info(f"Deleted subscription {subscription_id}")
            return True
        except NotFound:
            logger.warning(f"Subscription {subscription_id} not found")
            return False
        except Exception as e:
            logger.error(f"Error deleting subscription {subscription_id}: {e}")
            return False
    
    def delete_topic(self, topic_id: str) -> bool:
        """
        Delete a topic.
        
        Args:
            topic_id: ID of the topic to delete
            
        Returns:
            True if successful
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        
        try:
            self.publisher.delete_topic(request={"topic": topic_path})
            logger.info(f"Deleted topic {topic_id}")
            return True
        except NotFound:
            logger.warning(f"Topic {topic_id} not found")
            return False
        except Exception as e:
            logger.error(f"Error deleting topic {topic_id}: {e}")
            return False
    
    async def close(self):
        """Close all connections and cleanup resources."""
        try:
            # Stop all active subscriptions
            self.stop_all_subscriptions()
            
            # Close clients
            self.subscriber.close()
            await asyncio.sleep(0.5)  # Brief delay to allow shutdown
            
            logger.info("Closed PubSub client connections")
        except Exception as e:
            logger.error(f"Error closing PubSub client: {e}")

async def main():
    """Example usage of the PubSub client."""
    # Set up the client (using emulator for testing)
    client = PubSubClient(
        project_id="test-project"
    )
    
    try:
        # Create topic and subscription
        client.create_topic("test-topic")
        client.create_subscription("test-topic", "test-sub")
        
        # Define message callback
        def callback(message):
            print(f"Received message: {message.data.decode('utf-8')}")
            message.ack()
        
        # Subscribe to messages
        client.subscribe("test-sub", callback)
        
        # Publish a test message
        await client.publish_message(
            "test-topic",
            {"message": "Hello, PubSub!"},
            {"source": "test"}
        )
        
        # Wait a bit to receive the message
        await asyncio.sleep(5)
        
    finally:
        # Clean up resources
        await client.close()

if __name__ == "__main__":
    asyncio.run(main()) 