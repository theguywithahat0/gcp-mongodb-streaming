"""MongoDB Change Stream Listener implementation."""

import asyncio
import json
import time
from typing import Any, Dict, Optional
from bson import ObjectId
from datetime import datetime, UTC

from google.cloud import firestore, pubsub_v1
from pymongo import MongoClient
from pymongo.change_stream import ChangeStream
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from ..config.config_manager import (ConnectorConfig, MongoDBCollectionConfig,
                                   RetryConfig, BackpressureConfig)
from ..logging.logging_config import get_logger, add_context_to_logger
from .schema_validator import SchemaValidator
from .schema_registry import DocumentType
from .schema_migrator import SchemaMigrator, SchemaMigrationError
from .document_transformer import (
    DocumentTransformer, TransformStage, TransformationError,
    add_processing_metadata, add_field_prefix, remove_sensitive_fields
)
from .message_batcher import MessageBatcher, Message
from .message_deduplication import MessageDeduplication, DeduplicationConfig
from ..utils.json_utils import MongoDBJSONEncoder
from ..utils.error_utils import with_retries, wrap_error, ErrorCategory, ConnectorError
from ..utils.serialization import SerializationFormat

class ChangeStreamListener:
    """Listens to MongoDB change streams and publishes events to Pub/Sub."""

    def __init__(
        self,
        config: ConnectorConfig,
        collection_config: MongoDBCollectionConfig,
        mongo_client: Optional[MongoClient] = None,
        publisher: Optional[pubsub_v1.PublisherClient] = None,
        firestore_client: Optional[firestore.Client] = None,
        serialization_format: SerializationFormat = SerializationFormat.MSGPACK,
        backpressure_config: Optional[BackpressureConfig] = None
    ):
        """Initialize the change stream listener.
        
        Args:
            config: The connector configuration.
            collection_config: Configuration for the collection to watch.
            mongo_client: Optional MongoDB client instance.
            publisher: Optional Pub/Sub publisher instance.
            firestore_client: Optional Firestore client instance.
            serialization_format: Format to use for message serialization
            backpressure_config: Optional backpressure configuration
        """
        self.config = config
        self.collection_config = collection_config
        self.retry_config = config.retry
        self.serialization_format = serialization_format
        self.backpressure_config = backpressure_config or config.pubsub.publisher.backpressure

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

        # Initialize logger with context
        self.logger = get_logger(__name__)
        self.logger = add_context_to_logger(
            self.logger,
            {
                "collection": collection_config.name,
                "database": config.mongodb.database,
                "connector_id": f"connector-{collection_config.name}",
                "pubsub_topic": collection_config.topic,
                "status_topic": config.pubsub.status_topic
            }
        )

        # Initialize state
        self.change_stream: Optional[ChangeStream] = None
        self.is_running = False
        self.last_resume_token = self._load_resume_token()
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.last_change_time = time.time()

        # Initialize document transformer
        self.transformer = DocumentTransformer()
        
        # Register security transformations based on document type
        if collection_config.name == "inventory":
            # For inventory documents
            self.transformer.register_transform(
                TransformStage.PRE_PUBLISH,
                DocumentType.INVENTORY,
                add_field_prefix(f"wh_{config.mongodb.warehouse_id}_")
            )
        else:
            # For transaction documents
            # Remove sensitive customer information before publishing
            self.transformer.register_transform(
                TransformStage.PRE_PUBLISH,
                DocumentType.TRANSACTION,
                remove_sensitive_fields([
                    "customer_email",
                    "customer_phone",
                    "customer.address",
                    "payment.card_number",
                    "payment.cvv",
                    "shipping.address"
                ])
            )
            # Add warehouse prefix for data isolation
            self.transformer.register_transform(
                TransformStage.PRE_PUBLISH,
                DocumentType.TRANSACTION,
                add_field_prefix(f"wh_{config.mongodb.warehouse_id}_", [
                    "product_id",
                    "warehouse_id",
                    "location",
                    "shelf"
                ])
            )

        # Register default transformations
        self.transformer.register_transform(
            TransformStage.PRE_PUBLISH,
            DocumentType.INVENTORY,
            add_processing_metadata
        )
        self.transformer.register_transform(
            TransformStage.PRE_PUBLISH,
            DocumentType.TRANSACTION,
            add_processing_metadata
        )

        # Initialize message batcher with serialization format
        self.message_batcher = MessageBatcher(
            project_id=config.pubsub.project_id,
            topic=collection_config.topic,
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=config.pubsub.publisher.batch_settings.max_messages,
                max_bytes=config.pubsub.publisher.batch_settings.max_bytes,
                max_latency=config.pubsub.publisher.batch_settings.max_latency
            ),
            retry_settings=config.pubsub.publisher.retry,
            max_batch_size=config.pubsub.publisher.batch_settings.max_messages,
            max_latency=config.pubsub.publisher.batch_settings.max_latency,
            serialization_format=serialization_format,
            backpressure_config=self.backpressure_config
        )

        # Initialize message deduplication if enabled
        self.deduplication = None
        if collection_config.deduplication.enabled:
            self.deduplication = MessageDeduplication(
                config=collection_config.deduplication,
                firestore_client=self.firestore_client,
                collection_name=collection_config.name
            )

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
            wrapped_error = wrap_error(e, {
                "component": "change_stream_listener",
                "operation": "load_resume_token"
            })
            self.logger.error(
                "Failed to load resume token",
                extra={
                    "error": str(wrapped_error),
                    "category": wrapped_error.category.value
                }
            )
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
            wrapped_error = wrap_error(e, {
                "component": "change_stream_listener",
                "operation": "save_resume_token"
            })
            self.logger.error(
                "Failed to save resume token",
                extra={
                    "error": str(wrapped_error),
                    "category": wrapped_error.category.value
                }
            )

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
                    json.dumps(message, cls=MongoDBJSONEncoder).encode("utf-8"),
                    message_type="heartbeat",
                    collection=self.collection_config.name
                )

                # Wait for the publish to complete
                await asyncio.get_event_loop().run_in_executor(
                    None, future.result
                )

                self.logger.debug(
                    "heartbeat_published",
                    extra={
                        "time_since_last_change": time.time() - self.last_change_time,
                        "has_resume_token": self.last_resume_token is not None
                    }
                )

            except Exception as e:
                self.logger.error(
                    "heartbeat_publish_failed",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__
                    },
                    exc_info=True
                )

            # Wait for next interval
            await asyncio.sleep(self.config.health.heartbeat.interval)

    async def _handle_change(self, change: Dict[str, Any]) -> None:
        """Handle a single change event."""
        try:
            # Update last change time
            self.last_change_time = time.time()

            # Extract the resume token
            if token := change.get("_id"):
                self._save_resume_token(token)

            operation_type = change["operationType"]
            document_key = change.get("documentKey", {}).get("_id")
            timestamp = change.get("clusterTime", time.time())

            # Check for duplicates if deduplication is enabled
            if self.deduplication and document_key:
                try:
                    is_duplicate = self.deduplication.is_duplicate(
                        str(document_key),
                        operation_type,
                        timestamp
                    )
                    if is_duplicate:
                        self.logger.info(
                            "Skipping duplicate message",
                            extra={
                                "operation_type": operation_type,
                                "doc_id": str(document_key),
                                "timestamp": timestamp
                            }
                        )
                        return
                except Exception as e:
                    wrapped_error = wrap_error(e, {
                        "component": "change_stream_listener",
                        "operation": "check_duplicate",
                        "doc_id": str(document_key)
                    })
                    self.logger.error(
                        "Failed to check for duplicate message",
                        extra={
                            "error": str(wrapped_error),
                            "category": wrapped_error.category.value
                        }
                    )
                    # Continue processing if deduplication fails
            
            # Get document for validation (if applicable)
            document = None
            if operation_type == "insert":
                document = change.get("fullDocument")
            elif operation_type == "update":
                document = change.get("fullDocument")  # From updateLookup option
            # Delete operations don't have documents to validate

            # Validate document if we have one
            if document:
                # Determine document type
                doc_type = (
                    DocumentType.INVENTORY if self.collection_config.name == "inventory"
                    else DocumentType.TRANSACTION
                )

                try:
                    # Apply pre-validation transformations
                    document = self.transformer.apply_transforms(
                        document,
                        doc_type,
                        TransformStage.PRE_VALIDATION
                    )

                    # Attempt to migrate document if needed
                    document = SchemaMigrator.migrate_document(document, doc_type)

                    # Apply post-validation transformations
                    document = self.transformer.apply_transforms(
                        document,
                        doc_type,
                        TransformStage.POST_VALIDATION
                    )

                except (TransformationError, SchemaMigrationError) as e:
                    wrapped_error = wrap_error(e, {
                        "component": "change_stream_listener",
                        "operation": "transform_document",
                        "doc_id": str(document.get("_id")),
                        "operation_type": operation_type
                    })
                    self.logger.error(
                        "Document processing failed",
                        extra={
                            "error": str(wrapped_error),
                            "category": wrapped_error.category.value
                        }
                    )
                    return

                # Validate migrated document
                is_valid = (
                    SchemaValidator.is_valid_inventory(document)
                    if doc_type == DocumentType.INVENTORY
                    else SchemaValidator.is_valid_transaction(document)
                )
                
                if not is_valid:
                    wrapped_error = ConnectorError(
                        message="Document failed schema validation",
                        category=ErrorCategory.NON_RETRYABLE_DATA,
                        context={
                            "component": "change_stream_listener",
                            "operation": "validate_document",
                            "doc_id": str(document.get("_id")),
                            "doc_type": doc_type.value,
                            "operation_type": operation_type
                        }
                    )
                    self.logger.error(
                        "Document validation failed",
                        extra={
                            "error": str(wrapped_error),
                            "category": wrapped_error.category.value
                        }
                    )
                    return  # Skip publishing invalid documents

                try:
                    # Apply pre-publish transformations
                    document = self.transformer.apply_transforms(
                        document,
                        doc_type,
                        TransformStage.PRE_PUBLISH
                    )
                except TransformationError as e:
                    wrapped_error = wrap_error(e, {
                        "component": "change_stream_listener",
                        "operation": "pre_publish_transform",
                        "doc_id": str(document.get("_id")),
                        "operation_type": operation_type
                    })
                    self.logger.error(
                        "Pre-publish transform failed",
                        extra={
                            "error": str(wrapped_error),
                            "category": wrapped_error.category.value
                        }
                    )
                    return

            # Prepare the message
            message = {
                "collection": self.collection_config.name,
                "operation": operation_type,
                "timestamp": timestamp,
                "data": {
                    **change,
                    "wallTime": datetime.fromtimestamp(timestamp.time, UTC).isoformat()
                }
            }

            # Add message to batch with serialization format
            await self.message_batcher.add_message(
                Message(
                    data=message,
                    attributes={
                        "collection": self.collection_config.name,
                        "operation": operation_type,
                        "schema_version": (
                            document.get("_schema_version", "v1")
                            if document
                            else "v1"
                        )
                    },
                    serialization_format=self.serialization_format
                )
            )

            self.logger.info(
                "Change event processed",
                extra={
                    "operation_type": operation_type,
                    "doc_id": str(document_key),
                    "processing_time": time.time() - self.last_change_time,
                    "schema_version": document.get("_schema_version", "v1") if document else "v1"
                }
            )

        except Exception as e:
            wrapped_error = wrap_error(e, {
                "component": "change_stream_listener",
                "operation": "handle_change",
                "doc_id": str(change.get("documentKey", {}).get("_id")),
                "operation_type": change.get("operationType")
            })
            self.logger.error(
                "Change event processing failed",
                extra={
                    "error": str(wrapped_error),
                    "category": wrapped_error.category.value
                }
            )
            raise wrapped_error

    async def _watch_collection(self) -> None:
        """Watch the collection for changes."""
        while self.is_running:
            try:
                # Start change stream
                self.change_stream = self.collection.watch(
                    pipeline=self.collection_config.watch_filter or [],
                    resume_after=self.last_resume_token,
                    full_document='updateLookup'  # Always get the full document for updates
                )

                self.logger.info(
                    "change_stream_started",
                    extra={
                        "resume_token": str(self.last_resume_token) if self.last_resume_token else None,
                        "watch_filter": self.collection_config.watch_filter
                    }
                )

                # Process changes
                async for change in self._aiter_change_stream():
                    if change:
                        self.logger.info(
                            "change_detected",
                            extra={
                                "operation_type": change.get("operationType"),
                                "doc_id": str(change.get("documentKey", {}).get("_id"))
                            }
                        )
                        await self._handle_change(change)
                    else:
                        self.logger.debug("No change detected in stream")

            except PyMongoError as e:
                if not self.is_running:
                    break

                wrapped_error = wrap_error(e, {
                    "component": "change_stream_listener",
                    "operation": "watch_collection"
                })
                self.logger.error(
                    "change_stream_error",
                    extra={
                        "error": str(wrapped_error),
                        "category": wrapped_error.category.value
                    }
                )
                
                # Wait before retrying if error is retryable
                if wrapped_error.category in {
                    ErrorCategory.RETRYABLE_TRANSIENT,
                    ErrorCategory.RETRYABLE_RESOURCE
                }:
                    await self._wait_with_backoff()
                else:
                    # Non-retryable error, stop the listener
                    self.is_running = False
                    break

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
            self.logger.info(
                "retry_backoff",
                attempt=retries + 1,
                delay=delay,
                max_retries=self.retry_config.max_retries
            )
            
            await asyncio.sleep(delay)
            
            # Add jitter to avoid thundering herd
            jitter = delay * self.retry_config.jitter
            delay = min(
                delay * self.retry_config.multiplier + jitter,
                self.retry_config.max_delay
            )
            retries += 1

    async def start(self) -> None:
        """Start listening to the change stream."""
        if self.is_running:
            return

        self.is_running = True
        self.logger.info("Starting change stream listener")

        # Start heartbeat task
        self.heartbeat_task = asyncio.create_task(self._publish_heartbeat())

        # Start watching collection
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

        # Clean up deduplication cache if enabled
        if self.deduplication:
            self.deduplication._cleanup_memory()
            if self.deduplication.config.persistent_enabled:
                self.deduplication._cleanup_persistent()
            
        self.logger.info("change_stream_listener_stopped")

    async def __aenter__(self) -> 'ChangeStreamListener':
        """Context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        await self.stop() 