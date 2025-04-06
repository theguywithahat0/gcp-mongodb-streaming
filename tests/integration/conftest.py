"""Integration test configuration and fixtures."""
import pytest
from pymongo import MongoClient
from pymongo.errors import ConnectionError

@pytest.fixture(scope="session")
def mongodb_client(mongodb_uri):
    """Create a MongoDB client for integration tests."""
    client = MongoClient(mongodb_uri)
    try:
        # Verify connection
        client.admin.command('ping')
        yield client
    except ConnectionError:
        pytest.skip("MongoDB server is not available")
    finally:
        client.close()

@pytest.fixture(scope="function")
def test_collection(mongodb_client, mongodb_database, mongodb_collection):
    """Create a test collection and clean it up after the test."""
    collection = mongodb_client[mongodb_database][mongodb_collection]
    
    # Clean up any existing data
    collection.delete_many({})
    
    yield collection
    
    # Clean up after test
    collection.delete_many({})

@pytest.fixture(scope="function")
def populated_collection(test_collection):
    """Create a collection with some test documents."""
    documents = [
        {"_id": f"test_{i}", "value": i, "status": "pending"}
        for i in range(10)
    ]
    test_collection.insert_many(documents)
    yield test_collection
    test_collection.delete_many({}) 