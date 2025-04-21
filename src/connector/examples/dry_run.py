"""Dry run test for the MongoDB Change Stream Connector."""

import asyncio
import json
import os
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

logger = structlog.get_logger(__name__)

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
        if self.closed or not self.changes:
            raise StopIteration
        self._processed_count += 1
        # Stop after processing all expected changes
        if self._processed_count >= self._total_expected:
            self.closed = True
        return self.changes.pop(0)
    
    def close(self):
        """Close the change stream."""
        self.closed = True

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
    """Run a dry test of the connector."""
    
    # Get the path to the test config
    current_dir = Path(__file__).parent
    config_path = current_dir / "test_config.yaml"
    
    # Create mock MongoDB client
    mongo_client = MockMongoClient()
    
    # Get the collection through the mock client
    collection = mongo_client.test_db.inventory
    
    # Start the connector first
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
    
    # Initialize connector with test config
    connector = MongoDBConnector(str(config_path))
    connector.mongo_client = mongo_client
    connector.publisher = publisher
    connector.firestore_client = firestore_client
    
    try:
        # Start the connector
        logger.info("Starting connector dry run")
        start_task = asyncio.create_task(connector.start())
        
        # Wait a bit for the connector to start
        await asyncio.sleep(1)
        
        # Simulate some changes
        logger.info("Simulating inventory changes")
        
        # Create test document
        test_doc = {
            "product_id": "PROD-456",
            "warehouse_id": "WH-1",
            "quantity": 100,
            "last_updated": "2024-03-20T10:00:00Z",
            "sku": "TEST-123",
            "category": "Electronics",
            "brand": "TestBrand",
            "threshold_min": 10,
            "threshold_max": 200
        }
        collection.insert_one(test_doc)
        
        # Update document
        collection.update_one(
            {"product_id": "PROD-456"},
            {"$set": {"quantity": 90, "last_updated": "2024-03-20T10:15:00Z"}}
        )
        
        # Insert new document
        collection.insert_one({
            "product_id": "PROD-789",
            "warehouse_id": "WH-2",
            "quantity": 50,
            "last_updated": "2024-03-20T10:30:00Z",
            "sku": "TEST-456",
            "category": "Electronics",
            "brand": "TestBrand",
            "threshold_min": 5,
            "threshold_max": 100
        })
        
        # Delete document
        collection.delete_one({"sku": "TEST-123"})
        
        # Wait for changes to be processed (4 changes: insert, update, insert, delete)
        await asyncio.sleep(2)
        
        # Print published messages
        logger.info(
            "Messages that would have been published",
            count=len(publisher.published_messages)
        )
        for msg in publisher.published_messages:
            logger.info("Published message", operation=msg["data"]["operation"])
            
    finally:
        # Stop the connector
        await connector.stop()
        logger.info("Connector stopped")

def main():
    """Main entry point for dry run."""
    try:
        asyncio.run(run_dry_test())
    except KeyboardInterrupt:
        logger.info("Dry run interrupted")
    except Exception as e:
        logger.error("Error during dry run", error=str(e))
        raise  # Add this to see full traceback

if __name__ == "__main__":
    main() 