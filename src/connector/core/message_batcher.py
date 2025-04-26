"""Message batching for Pub/Sub publishing."""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from bson import Timestamp

from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings

from ..config.config_manager import PublisherConfig
from ..logging.logging_config import get_logger
from ..utils.json_utils import MongoDBJSONEncoder
from ..utils.error_utils import with_retries, wrap_error, ErrorCategory, ConnectorError

logger = get_logger(__name__)

@dataclass
class Message:
    """A message to be published to Pub/Sub."""
    data: Dict[str, Any]
    attributes: Optional[Dict[str, str]] = None


class MessageBatcher:
    """Batches messages for efficient publishing to Pub/Sub."""

    def __init__(
        self,
        project_id: str,
        topic: str,
        batch_settings: Optional[BatchSettings] = None,
        retry_settings: Optional[retry.Retry] = None,
        max_batch_size: int = 100,
        max_latency: float = 0.1
    ):
        """Initialize the message batcher.
        
        Args:
            project_id: Google Cloud project ID
            topic: Pub/Sub topic name
            batch_settings: Optional batch settings for the publisher
            retry_settings: Optional retry settings for publishing
            max_batch_size: Maximum number of messages in a batch
            max_latency: Maximum time to wait before publishing a batch
        """
        self.project_id = project_id
        self.topic = topic
        self.max_batch_size = max_batch_size
        self.max_latency = max_latency
        
        # Initialize publisher
        self.publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings,
            publisher_options=pubsub_v1.types.PublisherOptions(
                retry=retry_settings or retry.Retry(deadline=30)
            )
        )
        
        self.topic_path = self.publisher.topic_path(project_id, topic)
        
        # Initialize batching state
        self.batch: List[Message] = []
        self.batch_lock = asyncio.Lock()
        self.last_publish_time = datetime.now()
        self.publish_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the background publishing task."""
        self.publish_task = asyncio.create_task(self._publish_loop())
        logger.info(
            "Started message batcher",
            extra={
                "project_id": self.project_id,
                "topic": self.topic,
                "max_batch_size": self.max_batch_size,
                "max_latency": self.max_latency
            }
        )

    async def stop(self):
        """Stop the batcher and publish any remaining messages."""
        if self.publish_task:
            self.publish_task.cancel()
            try:
                await self.publish_task
            except asyncio.CancelledError:
                pass
        
        # Publish any remaining messages
        await self._publish_batch()
        logger.info("Stopped message batcher")

    async def add_message(self, message: Message):
        """Add a message to the batch.
        
        Args:
            message: The message to add
        """
        async with self.batch_lock:
            self.batch.append(message)
            
            if len(self.batch) >= self.max_batch_size:
                await self._publish_batch()

    async def _publish_loop(self):
        """Background task that publishes messages based on latency."""
        while True:
            try:
                await asyncio.sleep(self.max_latency)
                now = datetime.now()
                
                if (now - self.last_publish_time).total_seconds() >= self.max_latency:
                    await self._publish_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                wrapped_error = wrap_error(e, {
                    "component": "message_batcher",
                    "operation": "publish_loop"
                })
                logger.error(
                    "Error in publish loop",
                    extra={
                        "error": str(wrapped_error),
                        "category": wrapped_error.category.value
                    },
                    exc_info=True
                )

    async def _publish_single_message(self, message: Message, context: Dict[str, Any]):
        """Publish a single message with retries.
        
        Args:
            message: The message to publish
            context: Context information for error handling
        
        Returns:
            str: The message ID
        """
        async def publish():
            data = json.dumps(message.data, cls=MongoDBJSONEncoder).encode("utf-8")
            future = self.publisher.publish(
                self.topic_path,
                data=data,
                **message.attributes if message.attributes else {}
            )
            return await asyncio.to_thread(future.result)
        
        return await with_retries(
            publish,
            max_retries=3,
            initial_delay=1.0,
            max_delay=10.0,
            context=context
        )

    async def _publish_batch(self):
        """Publish the current batch of messages."""
        async with self.batch_lock:
            if not self.batch:
                return
            
            messages_to_publish = self.batch
            self.batch = []
        
        try:
            # Publish messages in parallel with retries
            publish_tasks = []
            
            for i, message in enumerate(messages_to_publish):
                context = {
                    "component": "message_batcher",
                    "operation": "publish_message",
                    "batch_index": i,
                    "batch_size": len(messages_to_publish),
                    "topic": self.topic
                }
                task = self._publish_single_message(message, context)
                publish_tasks.append(task)
            
            # Wait for all publishes to complete
            results = await asyncio.gather(*publish_tasks, return_exceptions=True)
            
            # Check for errors
            failed_messages = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    if isinstance(result, ConnectorError):
                        error = result
                    else:
                        error = wrap_error(result, {
                            "component": "message_batcher",
                            "operation": "publish_message",
                            "batch_index": i,
                            "batch_size": len(messages_to_publish),
                            "topic": self.topic
                        })
                    logger.error(
                        "Failed to publish message",
                        extra={
                            "error": str(error),
                            "category": error.category.value,
                            "message": messages_to_publish[i].data
                        }
                    )
                    failed_messages.append(messages_to_publish[i])
            
            # Add failed messages back to the batch if they're retryable
            if failed_messages:
                async with self.batch_lock:
                    retryable_messages = [
                        msg for msg, result in zip(failed_messages, results)
                        if isinstance(result, Exception) and 
                        (isinstance(result, ConnectorError) and result.category in {
                            ErrorCategory.RETRYABLE_TRANSIENT,
                            ErrorCategory.RETRYABLE_RESOURCE
                        })
                    ]
                    if retryable_messages:
                        self.batch = retryable_messages + self.batch
            
            self.last_publish_time = datetime.now()
            logger.debug(
                "Published messages",
                extra={
                    "batch_size": len(messages_to_publish),
                    "failed": len(failed_messages),
                    "topic": self.topic
                }
            )
        except Exception as e:
            wrapped_error = wrap_error(e, {
                "component": "message_batcher",
                "operation": "publish_batch",
                "batch_size": len(messages_to_publish),
                "topic": self.topic
            })
            logger.error(
                "Error publishing batch",
                extra={
                    "error": str(wrapped_error),
                    "category": wrapped_error.category.value
                },
                exc_info=True
            )
            # Add messages back to the batch if the error is retryable
            if wrapped_error.category in {
                ErrorCategory.RETRYABLE_TRANSIENT,
                ErrorCategory.RETRYABLE_RESOURCE
            }:
                async with self.batch_lock:
                    self.batch = messages_to_publish + self.batch 