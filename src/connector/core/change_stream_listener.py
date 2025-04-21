"""MongoDB Change Stream Listener implementation."""

import asyncio
import json
import time
from typing import Any, Dict, Optional
from bson import ObjectId

import structlog
from google.cloud import firestore, pubsub_v1
from pymongo import MongoClient
from pymongo.change_stream import ChangeStream
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from ..config.config_manager import (ConnectorConfig, MongoDBCollectionConfig,
                                   RetryConfig)
from .schema_validator import SchemaValidator

logger = structlog.get_logger(__name__)

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles MongoDB ObjectId."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

class ChangeStreamListener:
    """Listens to MongoDB change streams and publishes events to Pub/Sub."""

    def __init__(
        self,
        config: ConnectorConfig,
        collection_config: MongoDBCollectionConfig,
        mongo_client: Optional[MongoClient] = None,
        publisher: Optional[pubsub_v1.PublisherClient] = None,
        firestore_client: Optional[firestore.Client] = None
    ):
        """Initialize the change stream listener.
        
        Args:
            config: The connector configuration.
            collection_config: Configuration for the collection to watch.
            mongo_client: Optional MongoDB client instance.
            publisher: Optional Pub/Sub publisher instance.
            firestore_client: Optional Firestore client instance.
        """
        self.config = config
        self.collection_config = collection_config
        self.retry_config = config.retry

        # Initialize clients
        self.mongo_client = mongo_client or MongoClient(
            config.mongodb.uri,
            **config.mongodb.options
        )
        self.publisher = publisher or pubsub_v1.PublisherClient()
        self.firestore_client = firestore_client or firestore.Client()

        # Get the collection to watch
        self.collection = self.mongo_client[config.mongodb.database][collection_config.name]
        
        # Prepare Pub/Sub topic paths
        self.topic_path = self.publisher.topic_path(
            config.pubsub.project_id,
            collection_config.topic
        )
        self.status_topic_path = self.publisher.topic_path(
            config.pubsub.project_id,
            config.pubsub.status_topic
        )

        # Initialize state
        self.change_stream: Optional[ChangeStream] = None
        self.is_running = False
        self.last_resume_token = self._load_resume_token()
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.last_change_time = time.time()

    def _load_resume_token(self) -> Optional[Dict[str, Any]]:
        """Load the last resume token from Firestore.
        
        Returns:
            Optional[Dict[str, Any]]: The resume token if found, None otherwise.
        """
        try:
            doc_ref = self._get_resume_token_ref()
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict().get("token")
        except Exception as e:
            logger.error("Failed to load resume token", error=str(e))
        return None

    def _save_resume_token(self, token: Dict[str, Any]) -> None:
        """Save the resume token to Firestore.
        
        Args:
            token: The resume token to save.
        """
        try:
            doc_ref = self._get_resume_token_ref()
            doc_ref.set({
                "token": token,
                "updated_at": firestore.SERVER_TIMESTAMP,
                "collection": self.collection_config.name
            })
        except Exception as e:
            logger.error("Failed to save resume token", error=str(e))

    def _get_resume_token_ref(self) -> firestore.DocumentReference:
        """Get the Firestore document reference for the resume token.
        
        Returns:
            firestore.DocumentReference: The document reference.
        """
        return self.firestore_client.collection(
            self.config.firestore.collection
        ).document(f"{self.collection_config.name}_token")

    async def _publish_heartbeat(self) -> None:
        """Publish heartbeat messages periodically."""
        while self.is_running:
            try:
                # Prepare heartbeat message
                message = {
                    "type": "heartbeat",
                    "collection": self.collection_config.name,
                    "timestamp": time.time(),
                    "status": {
                        "is_running": self.is_running,
                        "last_change_time": self.last_change_time,
                        "time_since_last_change": time.time() - self.last_change_time,
                        "has_resume_token": self.last_resume_token is not None
                    }
                }

                # Publish to status topic
                future = self.publisher.publish(
                    self.status_topic_path,
                    json.dumps(message, cls=JSONEncoder).encode("utf-8"),
                    message_type="heartbeat",
                    collection=self.collection_config.name
                )

                # Wait for the publish to complete
                await asyncio.get_event_loop().run_in_executor(
                    None, future.result
                )

                logger.debug(
                    "Published heartbeat",
                    collection=self.collection_config.name
                )

            except Exception as e:
                logger.error(
                    "Failed to publish heartbeat",
                    error=str(e),
                    collection=self.collection_config.name
                )

            # Wait for next interval
            await asyncio.sleep(self.config.health.heartbeat.interval)

    async def _handle_change(self, change: Dict[str, Any]) -> None:
        """Handle a single change event.
        
        Args:
            change: The change event from MongoDB.
        """
        try:
            # Update last change time
            self.last_change_time = time.time()

            # Extract the resume token
            if token := change.get("_id"):
                self._save_resume_token(token)

            # Skip validation for delete operations as they don't contain full documents
            if change["operationType"] != "delete":
                # Get the full document for validation
                document = change.get("fullDocument")
                if not document:
                    logger.warning(
                        "No full document available for validation",
                        collection=self.collection_config.name,
                        operation=change["operationType"]
                    )
                    return

                # Determine document type and validate
                doc_type = "inventory" if self.collection_config.name == "inventory" else "transaction"
                validation_error = SchemaValidator.validate_document(document, doc_type)
                
                if validation_error:
                    logger.error(
                        "Document failed schema validation",
                        error=validation_error,
                        collection=self.collection_config.name,
                        operation=change["operationType"]
                    )
                    return  # Skip publishing invalid documents

            # Prepare the message
            message = {
                "collection": self.collection_config.name,
                "operation": change["operationType"],
                "timestamp": time.time(),
                "data": change
            }

            # Publish to Pub/Sub using custom JSON encoder
            future = self.publisher.publish(
                self.topic_path,
                json.dumps(message, cls=JSONEncoder).encode("utf-8"),
                collection=self.collection_config.name,
                operation=change["operationType"]
            )
            
            # Wait for the publish to complete
            await asyncio.get_event_loop().run_in_executor(
                None, future.result
            )

            logger.info(
                "Published change event",
                collection=self.collection_config.name,
                operation=change["operationType"]
            )

        except Exception as e:
            logger.error(
                "Failed to handle change event",
                error=str(e),
                collection=self.collection_config.name
            )
            raise

    async def _watch_collection(self) -> None:
        """Watch the collection for changes."""
        while self.is_running:
            try:
                # Start change stream
                self.change_stream = self.collection.watch(
                    pipeline=self.collection_config.watch_filter or [],
                    resume_after=self.last_resume_token
                )

                # Process changes
                async for change in self._aiter_change_stream():
                    await self._handle_change(change)

            except PyMongoError as e:
                if not self.is_running:
                    break

                logger.error(
                    "Error in change stream",
                    error=str(e),
                    collection=self.collection_config.name
                )
                
                # Wait before retrying
                await self._wait_with_backoff()

    async def _aiter_change_stream(self):
        """Async iterator for the change stream."""
        while self.is_running:
            if change := await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: next(self.change_stream, None)
            ):
                yield change
            else:
                break

    async def _wait_with_backoff(self) -> None:
        """Wait with exponential backoff before retrying."""
        delay = self.retry_config.initial_delay
        retries = 0

        while retries < self.retry_config.max_retries:
            await asyncio.sleep(delay)
            
            # Add jitter to avoid thundering herd
            jitter = delay * self.retry_config.jitter
            delay = min(
                delay * self.retry_config.multiplier + jitter,
                self.retry_config.max_delay
            )
            retries += 1

    async def start(self) -> None:
        """Start watching the collection."""
        if self.is_running:
            return

        self.is_running = True
        logger.info(
            "Starting change stream listener",
            collection=self.collection_config.name
        )

        # Start heartbeat task if enabled
        if self.config.health.heartbeat.enabled:
            self.heartbeat_task = asyncio.create_task(self._publish_heartbeat())
        
        await self._watch_collection()

    async def stop(self) -> None:
        """Stop watching the collection."""
        self.is_running = False
        
        if self.change_stream:
            self.change_stream.close()

        # Stop heartbeat task if running
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            
        logger.info(
            "Stopped change stream listener",
            collection=self.collection_config.name
        )

    async def __aenter__(self) -> 'ChangeStreamListener':
        """Context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        await self.stop() 