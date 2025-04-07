"""
Google Cloud Pub/Sub sink for the MongoDB change stream pipeline.

This module handles:
1. Converting MongoDB documents to Pub/Sub messages
2. Publishing messages to Pub/Sub topics with proper attributes
3. Batching for efficiency
4. Error handling and retries
"""
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPIError, NotFound, PermissionDenied, InvalidArgument
from google.api_core.retry import Retry, exponential_sleep_generator

logger = logging.getLogger(__name__)

class PubSubSinkError(Exception):
    """Base class for PubSubSink errors."""
    pass

class TopicNotFoundError(PubSubSinkError):
    """Raised when a topic doesn't exist."""
    pass

class PublishError(PubSubSinkError):
    """Raised when publishing messages fails."""
    def __init__(self, message, topic_name, original_error=None):
        super().__init__(message)
        self.topic_name = topic_name
        self.original_error = original_error

class PubSubSink:
    """
    Handles publishing documents to Google Cloud Pub/Sub.
    Supports batching, retries, and error handling.
    """
    
    def __init__(self, project_id: str, topic_config: Dict[str, Dict[str, Any]], validate_topics: bool = True):
        """
        Initialize the Pub/Sub sink.
        
        Args:
            project_id: GCP project ID
            topic_config: Configuration for topics
                Format: {
                    "topic_name": {
                        "batch_size": int,
                        "attributes": Dict[str, str],
                        "ordering_key": str
                    }
                }
            validate_topics: Whether to validate that topics exist during initialization
            
        Raises:
            TopicNotFoundError: If validate_topics is True and a topic doesn't exist
            PermissionDenied: If the application doesn't have permission to access topics
        """
        self.project_id = project_id
        self.topic_config = topic_config
        self.publisher = pubsub_v1.PublisherClient()
        self.topics = {}
        
        # Validate topic configuration
        if not topic_config:
            raise ValueError("Topic configuration cannot be empty")
            
        # Initialize and validate topics
        for topic_name in topic_config:
            topic_path = self.publisher.topic_path(project_id, topic_name)
            self.topics[topic_name] = topic_path
            logger.info(f"Configured Pub/Sub topic: {topic_path}")
            
        # Validate that topics exist if requested
        if validate_topics:
            self._validate_topics()
            
    def _validate_topics(self) -> None:
        """
        Validate that all configured topics exist.
        
        Raises:
            TopicNotFoundError: If a topic doesn't exist
            PermissionDenied: If the application doesn't have permission to access topics
        """
        for topic_name, topic_path in self.topics.items():
            try:
                self.publisher.get_topic(request={"topic": topic_path})
                logger.info(f"Successfully validated topic exists: {topic_name}")
            except NotFound:
                error_msg = f"Topic does not exist: {topic_name} ({topic_path})"
                logger.error(error_msg)
                raise TopicNotFoundError(error_msg)
            except PermissionDenied:
                error_msg = f"Permission denied accessing topic: {topic_name} ({topic_path})"
                logger.error(error_msg)
                raise
            except Exception as e:
                logger.warning(f"Unexpected error validating topic {topic_name}: {str(e)}")
                # Don't raise here - this is not critical for operation, just a warning
            
    def _create_message(self, data: Dict[str, Any], topic_name: str) -> pubsub_v1.types.PubsubMessage:
        """
        Convert document to Pub/Sub message.
        
        Args:
            data: Document data to publish
            topic_name: Target topic name
            
        Returns:
            Pub/Sub message object
            
        Raises:
            InvalidArgument: If data cannot be serialized
        """
        # Get topic-specific configuration
        config = self.topic_config[topic_name]
        
        # Create message attributes
        attributes = config.get("attributes", {}).copy()
        
        # Add metadata from document as attributes if available
        if "_metadata" in data:
            metadata = data.get("_metadata", {})
            for key in ["source", "connection_id", "source_name", "version"]:
                if key in metadata:
                    attributes[key] = str(metadata[key])
        
        # JSON-encode the data
        try:
            message_data = json.dumps(data).encode("utf-8")
        except (TypeError, ValueError) as e:
            error_msg = f"Failed to serialize message: {str(e)}"
            logger.error(error_msg)
            raise InvalidArgument(error_msg)
        
        # Create message with attributes and optional ordering key
        message = pubsub_v1.types.PubsubMessage(
            data=message_data,
            attributes=attributes
        )
        
        # Add ordering key if configured
        if "ordering_key" in config:
            ordering_field = config["ordering_key"]
            if ordering_field in data:
                message.ordering_key = str(data[ordering_field])
        
        return message
    
    def publish_message(self, data: Dict[str, Any], topic_name: str, 
                        timeout: float = 60.0) -> str:
        """
        Publish a single message to Pub/Sub.
        
        Args:
            data: Document data to publish
            topic_name: Target topic name
            timeout: Maximum time to wait for publish confirmation
            
        Returns:
            Message ID if successful
            
        Raises:
            TopicNotFoundError: If topic_name not configured
            PublishError: If publishing fails after retries
            TimeoutError: If publishing confirmation times out
        """
        if topic_name not in self.topics:
            raise TopicNotFoundError(f"Topic not configured: {topic_name}")
        
        topic_path = self.topics[topic_name]
        
        try:
            message = self._create_message(data, topic_name)
            
            # Configure retry with exponential backoff
            retry = Retry(
                initial=0.1,  # Initial backoff in seconds
                maximum=60.0,  # Maximum backoff
                multiplier=1.5,  # Multiplier for backoff
                predicate=lambda e: isinstance(e, GoogleAPIError),
                deadline=timeout  # Maximum time to retry
            )
            
            # Publish with retry
            future = self.publisher.publish(
                topic_path, 
                data=message.data,
                attributes=message.attributes,
                retry=retry
            )
            
            # Wait for message to be published with timeout
            message_id = future.result(timeout=timeout)
            logger.debug(f"Published message to {topic_name}: {message_id}")
            return message_id
            
        except TimeoutError:
            error_msg = f"Timed out waiting for confirmation on topic {topic_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        except NotFound:
            error_msg = f"Topic not found: {topic_name}"
            logger.error(error_msg)
            raise TopicNotFoundError(error_msg)
        except Exception as e:
            error_msg = f"Failed to publish to {topic_name}: {str(e)}"
            logger.error(error_msg)
            raise PublishError(error_msg, topic_name, e)
    
    def publish_batch(self, data_batch: List[Dict[str, Any]], topic_name: str, 
                      fail_fast: bool = False) -> Tuple[List[str], List[Dict[str, Any]]]:
        """
        Publish a batch of messages to Pub/Sub.
        
        Args:
            data_batch: List of documents to publish
            topic_name: Target topic name
            fail_fast: If True, fail on first error; if False, continue with other messages
            
        Returns:
            Tuple of (successful message IDs, failed documents)
            
        Raises:
            TopicNotFoundError: If topic_name not configured
            PublishError: If fail_fast is True and any message fails
        """
        if not data_batch:
            return [], []
            
        if topic_name not in self.topics:
            raise TopicNotFoundError(f"Topic not configured: {topic_name}")
            
        message_ids = []
        failed_documents = []
        batch_size = self.topic_config[topic_name].get("batch_size", 1000)
        
        # Process in configured batch sizes
        for i in range(0, len(data_batch), batch_size):
            batch_chunk = data_batch[i:i+batch_size]
            batch_futures = []
            batch_data = []
            
            # Publish all messages in batch
            for data in batch_chunk:
                try:
                    message = self._create_message(data, topic_name)
                    future = self.publisher.publish(
                        self.topics[topic_name],
                        data=message.data,
                        attributes=message.attributes
                    )
                    batch_futures.append(future)
                    batch_data.append(data)
                except Exception as e:
                    logger.error(f"Failed to publish message in batch: {str(e)}")
                    failed_documents.append(data)
                    if fail_fast:
                        raise PublishError(f"Failed to publish message: {str(e)}", topic_name, e)
            
            # Wait for all messages to be published
            for idx, future in enumerate(batch_futures):
                try:
                    message_id = future.result(timeout=30.0)  # 30 second timeout per message
                    message_ids.append(message_id)
                except Exception as e:
                    logger.error(f"Failed to get message ID from future: {str(e)}")
                    failed_documents.append(batch_data[idx])
                    if fail_fast:
                        raise PublishError(f"Failed to confirm message publish: {str(e)}", topic_name, e)
        
        success_count = len(message_ids)
        failure_count = len(failed_documents)
        total_count = len(data_batch)
        
        logger.info(f"Published {success_count}/{total_count} messages to {topic_name} "
                   f"({failure_count} failures)")
        
        return message_ids, failed_documents
    
    def publish_to_multiple_topics(self, data: Dict[str, Any], topic_names: List[str]) -> Dict[str, str]:
        """
        Publish a single message to multiple topics.
        
        Args:
            data: Document data to publish
            topic_names: List of target topic names
            
        Returns:
            Dictionary mapping topic names to message IDs for successful publishes
        """
        results = {}
        
        for topic_name in topic_names:
            try:
                message_id = self.publish_message(data, topic_name)
                results[topic_name] = message_id
            except Exception as e:
                logger.error(f"Failed to publish to {topic_name}: {str(e)}")
                # Continue with other topics
                
        return results
    
    async def close(self):
        """
        Clean up resources and ensure all pending messages are published.
        
        Returns:
            None
        """
        try:
            # Make sure all pending messages are delivered by shutting down
            self.publisher.stop()
            logger.info("Successfully closed Pub/Sub publisher")
        except Exception as e:
            logger.error(f"Error shutting down Pub/Sub publisher: {str(e)}")
