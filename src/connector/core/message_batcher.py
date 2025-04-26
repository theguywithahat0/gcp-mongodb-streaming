"""Message batching for Pub/Sub publishing."""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from bson import Timestamp
import time

from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings

from ..config.config_manager import PublisherConfig
from ..logging.logging_config import get_logger
from ..utils.serialization import serialize_message, SerializationFormat
from ..utils.error_utils import with_retries, wrap_error, ErrorCategory, ConnectorError

logger = get_logger(__name__)

@dataclass
class Message:
    """A message to be published to Pub/Sub."""
    data: Dict[str, Any]
    attributes: Optional[Dict[str, str]] = None
    serialization_format: SerializationFormat = SerializationFormat.MSGPACK

class MessageBatcher:
    """Batches messages for efficient Pub/Sub publishing."""

    def __init__(
        self,
        project_id: str,
        topic: str,
        batch_settings: BatchSettings,
        retry_settings: Dict,
        max_batch_size: int = 100,
        max_latency: float = 0.05,
        serialization_format: SerializationFormat = SerializationFormat.MSGPACK
    ):
        """Initialize the message batcher.
        
        Args:
            project_id: Google Cloud project ID
            topic: Pub/Sub topic name
            batch_settings: Pub/Sub batch settings
            retry_settings: Pub/Sub retry settings
            max_batch_size: Maximum number of messages per batch
            max_latency: Maximum latency before publishing a batch
            serialization_format: Format to use for message serialization
        """
        self.project_id = project_id
        self.topic = topic
        self.batch_settings = batch_settings
        self.retry_settings = retry_settings
        self.max_batch_size = max_batch_size
        self.max_latency = max_latency
        self.serialization_format = serialization_format
        
        # Initialize logger
        self.logger = get_logger(__name__)

        # Initialize publisher client
        self.publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings,
            publisher_options=pubsub_v1.types.PublisherOptions(
                retry=retry_settings
            )
        )
        self.topic_path = self.publisher.topic_path(project_id, topic)

        # Initialize message queue and batch state
        self.messages: List[Message] = []
        self.last_publish_time = time.time()
        self.publish_task: Optional[asyncio.Task] = None

    async def add_message(self, message: Message) -> None:
        """Add a message to the batch.
        
        Args:
            message: The message to add
        """
        self.messages.append(message)

        # Start delayed publish task if not already running
        if not self.publish_task or self.publish_task.done():
            self.publish_task = asyncio.create_task(self._delayed_publish())

        # Publish immediately if batch is full
        if len(self.messages) >= self.max_batch_size:
            if self.publish_task:
                self.publish_task.cancel()
            await self._publish_batch()

    async def _delayed_publish(self) -> None:
        """Publish batch after max latency."""
        try:
            await asyncio.sleep(self.max_latency)
            await self._publish_batch()
        except asyncio.CancelledError:
            pass  # Task was cancelled for immediate publish
        except Exception as e:
            self.logger.error(
                "Delayed publish failed",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )

    async def _publish_batch(self) -> None:
        """Publish current batch of messages."""
        if not self.messages:
            return

        try:
            # Prepare messages for publishing
            publish_futures = []
            
            for message in self.messages:
                # Add serialization format to attributes
                message.attributes['serialization_format'] = message.serialization_format.value
                
                # Serialize message data
                data = serialize_message(message.data, format=message.serialization_format)
                
                # Publish message
                future = self.publisher.publish(
                    self.topic_path,
                    data=data,
                    **message.attributes
                )
                publish_futures.append(future)

            # Wait for all messages to be published
            for future in publish_futures:
                try:
                    # Run the future in an executor to avoid blocking
                    await asyncio.get_event_loop().run_in_executor(None, future.result)
                except Exception as e:
                    self.logger.error(
                        "Failed to publish message",
                        extra={
                            "error": str(e),
                            "error_type": type(e).__name__
                        }
                    )

            # Update state
            self.last_publish_time = time.time()
            self.messages.clear()

            self.logger.info(
                "Published message batch",
                extra={
                    "batch_size": len(publish_futures),
                    "topic": self.topic
                }
            )

        except Exception as e:
            self.logger.error(
                "Batch publish failed",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "batch_size": len(self.messages)
                }
            )
            raise

    async def flush(self) -> None:
        """Publish any remaining messages."""
        if self.publish_task:
            self.publish_task.cancel()
        await self._publish_batch()

    async def close(self) -> None:
        """Close the batcher and clean up resources."""
        await self.flush()
        if self.publisher:
            await asyncio.get_event_loop().run_in_executor(None, self.publisher.close) 