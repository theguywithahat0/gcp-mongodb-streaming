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

logger = get_logger(__name__)

class MongoJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles MongoDB types."""
    def default(self, obj):
        if isinstance(obj, Timestamp):
            return obj.time
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

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
                logger.error(f"Error in publish loop: {e}", exc_info=True)

    async def _publish_batch(self):
        """Publish the current batch of messages."""
        async with self.batch_lock:
            if not self.batch:
                return
            
            messages_to_publish = self.batch
            self.batch = []
        
        try:
            # Publish messages in parallel
            publish_futures = []
            
            for message in messages_to_publish:
                data = json.dumps(message.data, cls=MongoJSONEncoder).encode("utf-8")
                future = self.publisher.publish(
                    self.topic_path,
                    data=data,
                    **message.attributes if message.attributes else {}
                )
                publish_futures.append(future)
            
            # Wait for all publishes to complete
            results = await asyncio.gather(
                *[asyncio.to_thread(future.result) for future in publish_futures],
                return_exceptions=True
            )
            
            # Check for errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"Failed to publish message: {result}",
                        extra={"message": messages_to_publish[i].data},
                        exc_info=True
                    )
            
            self.last_publish_time = datetime.now()
            logger.debug(
                f"Published {len(messages_to_publish)} messages",
                extra={
                    "batch_size": len(messages_to_publish),
                    "topic": self.topic
                }
            )
        except Exception as e:
            logger.error(f"Error publishing batch: {e}", exc_info=True)
            # Add messages back to the batch
            async with self.batch_lock:
                self.batch = messages_to_publish + self.batch 