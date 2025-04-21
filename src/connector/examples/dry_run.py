"""Dry run test for the MongoDB Change Stream Connector."""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, Iterator, Optional
from bson import ObjectId

import mongomock
import structlog
from google.cloud import pubsub_v1
from unittest.mock import MagicMock, AsyncMock

from ..core.connector import MongoDBConnector
from ..config.config_manager import (
    ConnectorConfig, 
    MongoDBConfig, 
    MongoDBCollectionConfig,
    PubSubConfig,
    PubSubPublisherConfig,
    FirestoreConfig,
    MonitoringConfig,
    HealthConfig,
    RetryConfig
)
from ..logging.logging_config import configure_logging, get_logger

# Configure logging for the test
configure_logging("DEBUG")
logger = get_logger(__name__)

class CaptureProcessor:
    """Processor that captures log messages for validation."""
    def __init__(self):
        self.messages = []

    def __call__(self, logger, method_name, event_dict):
        """Process a log event by storing it and passing it through."""
        # Store the event dict
        self.messages.append(event_dict)
        # Print to console for visibility
        print(json.dumps(event_dict), file=sys.stderr)
        # Pass through the event dict unchanged
        return event_dict

# Add our capture processor to structlog
capture_processor = CaptureProcessor()
processors = structlog.get_config()["processors"]
# Insert our processor before the JSON renderer
processors.insert(-1, capture_processor)
structlog.configure(processors=processors)

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles MongoDB ObjectId."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

class MockChangeStream:
    """Mock implementation of MongoDB change stream."""
    
    def __init__(self):
        self.changes = []
        self.closed = False
        self._processed_count = 0
        self._total_expected = 0
        
    def add_change(self, change: Dict[str, Any]):
        """Add a change to the stream."""
        self.changes.append(change)
        self._total_expected += 1
    
    def __iter__(self):
        return self
        
    def __next__(self):
        if self.closed:
            raise StopIteration
        
        if not self.changes:
            # If we've processed all expected changes, stop
            if self._processed_count >= self._total_expected:
                self.closed = True
                raise StopIteration
            # Otherwise, simulate waiting for changes
            return None
            
        self._processed_count += 1
        return self.changes.pop(0)
    
    def close(self):
        """Close the change stream."""
        self.closed = True
        self.changes.clear()

class MockCollection:
    """Mock MongoDB collection with change stream support."""
    
    def __init__(self, collection):
        self._collection = collection
        self.change_stream = MockChangeStream()
        
    def watch(self, pipeline=None, resume_after=None):
        """Mock watch method that returns our change stream."""
        return self.change_stream
        
    def insert_one(self, document):
        """Insert a document and record the change."""
        result = self._collection.insert_one(document)
        self.change_stream.add_change({
            "_id": {"_data": str(result.inserted_id)},
            "operationType": "insert",
            "fullDocument": document,
            "ns": {"db": "test_db", "coll": "inventory"},
            "documentKey": {"_id": result.inserted_id}
        })
        return result
        
    def update_one(self, filter, update):
        """Update a document and record the change."""
        result = self._collection.update_one(filter, update)
        updated_doc = self._collection.find_one(filter)
        if updated_doc:
            self.change_stream.add_change({
                "_id": {"_data": str(updated_doc["_id"])},
                "operationType": "update",
                "fullDocument": updated_doc,
                "ns": {"db": "test_db", "coll": "inventory"},
                "documentKey": {"_id": updated_doc["_id"]},
                "updateDescription": {
                    "updatedFields": {k.replace("$set.", ""): v for k, v in update["$set"].items()},
                    "removedFields": []
                }
            })
        return result
        
    def delete_one(self, filter):
        """Delete a document and record the change."""
        doc_to_delete = self._collection.find_one(filter)
        result = self._collection.delete_one(filter)
        if doc_to_delete:
            self.change_stream.add_change({
                "_id": {"_data": str(doc_to_delete["_id"])},
                "operationType": "delete",
                "ns": {"db": "test_db", "coll": "inventory"},
                "documentKey": {"_id": doc_to_delete["_id"]}
            })
        return result
        
    def find_one(self, *args, **kwargs):
        """Pass through to underlying collection."""
        return self._collection.find_one(*args, **kwargs)

class MockDatabase:
    """Mock MongoDB database that returns mock collections."""
    
    def __init__(self, database):
        self._database = database
        self._collections = {}
        
    def __getitem__(self, name):
        return self.__getattr__(name)
        
    def __getattr__(self, name):
        if name not in self._collections:
            base_collection = self._database[name]
            self._collections[name] = MockCollection(base_collection)
        return self._collections[name]

class MockMongoClient:
    """Mock MongoDB client that returns mock databases."""
    
    def __init__(self):
        self._client = mongomock.MongoClient()
        self._databases = {}
        
    def __getitem__(self, name):
        return self.__getattr__(name)
        
    def __getattr__(self, name):
        if name not in self._databases:
            base_database = self._client[name]
            self._databases[name] = MockDatabase(base_database)
        return self._databases[name]
        
    def close(self):
        """Mock close method."""
        pass

class MockPubSubPublisher:
    """Mock Pub/Sub publisher that logs messages instead of publishing."""
    
    def __init__(self):
        self.published_messages = []
        
    def topic_path(self, project_id: str, topic_id: str) -> str:
        return f"projects/{project_id}/topics/{topic_id}"
        
    def publish(self, topic: str, data: bytes, **attrs):
        """Log the message instead of publishing."""
        message = {
            'topic': topic,
            'data': json.loads(data.decode('utf-8')),
            'attributes': attrs
        }
        self.published_messages.append(message)
        logger.info(
            "Message would be published",
            message=json.dumps(message, cls=JSONEncoder)
        )
        
        # Return a mock future
        future = MagicMock()
        future.result = lambda: None
        return future

    def close(self):
        pass

async def run_dry_test():
    """Run a dry test of the connector with logging validation."""
    
    logger.info("starting_dry_run_test")
    
    # Get the path to the test config
    current_dir = Path(__file__).parent
    config_path = current_dir / "test_config.yaml"
    
    # Create mock clients
    mongo_client = MockMongoClient()
    publisher = MockPubSubPublisher()
    firestore_client = MagicMock()
    doc_mock = MagicMock()
    doc_mock.exists = False
    doc_mock.to_dict = MagicMock(return_value={})
    doc_ref_mock = MagicMock()
    doc_ref_mock.get = MagicMock(return_value=doc_mock)
    doc_ref_mock.set = MagicMock()
    coll_mock = MagicMock()
    coll_mock.document = MagicMock(return_value=doc_ref_mock)
    firestore_client.collection = MagicMock(return_value=coll_mock)
    
    # Create a test document
    test_doc = {
        "_id": ObjectId(),
        "product_id": "TEST001",
        "name": "Test Product",
        "quantity": 100
    }
    
    # Get the test database and collection
    db = mongo_client.test_db
    collection = db.inventory
    
    logger.debug("inserting_test_document", doc_id=str(test_doc["_id"]))
    
    # Insert the test document
    collection.insert_one(test_doc)
    
    # Update the document
    logger.debug("updating_test_document", doc_id=str(test_doc["_id"]))
    collection.update_one(
        {"_id": test_doc["_id"]},
        {"$set": {"quantity": 50}}
    )
    
    # Delete the document
    logger.debug("deleting_test_document", doc_id=str(test_doc["_id"]))
    collection.delete_one({"_id": test_doc["_id"]})
    
    # Start the connector
    connector = MongoDBConnector(str(config_path))
    connector.mongo_client = mongo_client
    connector.publisher = publisher
    connector.firestore_client = firestore_client
    
    # Run for a short time to process the changes
    try:
        async with connector:
            # Wait for a maximum of 5 seconds or until all changes are processed
            for _ in range(5):
                if all(listener.change_stream.closed for listener in connector.listeners.values()):
                    break
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(
            "connector_error",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        raise
    
    # Validate logging
    logger.info("validating_logs")
    
    # Check that we have structured logs with required fields
    validation_errors = []
    required_fields = {"timestamp", "level", "event"}
    
    for msg in capture_processor.messages:
        # Check for required fields
        missing_fields = required_fields - set(msg.keys())
        if missing_fields:
            validation_errors.append(f"Missing required fields: {missing_fields}")
    
    if validation_errors:
        logger.error(
            "log_validation_failed",
            errors=validation_errors
        )
        raise ValueError("Log validation failed")
    
    logger.info(
        "dry_run_completed",
        total_logs=len(capture_processor.messages),
        published_messages=len(publisher.published_messages)
    )

def main():
    """Main entry point for the dry run test."""
    try:
        asyncio.run(run_dry_test())
    except Exception as e:
        logger.error(
            "dry_run_failed",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        sys.exit(1)

if __name__ == "__main__":
    main() 