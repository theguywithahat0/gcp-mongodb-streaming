"""Tests for MongoDB document validation module."""
import pytest
from datetime import datetime, UTC
from src.pipeline.mongodb.validator import DocumentValidator

@pytest.fixture
def sample_schemas():
    """Sample schemas for testing."""
    return {
        "client1.users": {
            "user_id": {
                "type": "string",
                "required": True,
                "nullable": False
            },
            "age": {
                "type": "number",
                "required": True,
                "nullable": False,
                "validation": lambda x: x >= 0
            },
            "email": {
                "type": "string",
                "required": True,
                "nullable": False,
                "validation": lambda x: "@" in x
            },
            "created_at": {
                "type": "date",
                "required": True,
                "nullable": False
            }
        },
        "client1.orders": {
            "order_id": {
                "type": "string",
                "required": True,
                "nullable": False
            },
            "user_id": {
                "type": "string",
                "required": True,
                "nullable": False
            },
            "total": {
                "type": "number",
                "required": True,
                "nullable": False,
                "validation": lambda x: x > 0
            },
            "items": {
                "type": "array",
                "required": True,
                "nullable": False
            }
        }
    }

@pytest.fixture
def valid_user_document():
    """Valid user document for testing."""
    return {
        "user_id": "user123",
        "age": 25,
        "email": "test@example.com",
        "created_at": datetime.now(UTC),
        "tags": ["test", "user"],
        "metadata": {"source": "registration"}
    }

@pytest.fixture
def valid_order_document():
    """Valid order document for testing."""
    return {
        "order_id": "order123",
        "user_id": "user123",
        "total": 99.99,
        "items": [
            {"id": "item1", "quantity": 2},
            {"id": "item2", "quantity": 1}
        ]
    }

def test_schema_validation(sample_schemas):
    """Test schema validation during initialization."""
    # Test valid schemas
    transformer = DocumentValidator(sample_schemas)
    assert transformer.schemas == sample_schemas
    
    # Test invalid stream_id format
    with pytest.raises(ValueError, match="Invalid stream_id format"):
        DocumentValidator({"invalid_id": {}})
    
    # Test invalid field type
    invalid_schemas = {
        "client1.test": {
            "field": {"type": "invalid"}
        }
    }
    with pytest.raises(ValueError, match="has invalid type in client1.test: invalid"):
        DocumentValidator(invalid_schemas)
    
    # Test missing type
    invalid_schemas = {
        "client1.test": {
            "field": {"required": True}
        }
    }
    with pytest.raises(ValueError, match="missing required 'type' key in client1.test"):
        DocumentValidator(invalid_schemas)

def test_document_validation(sample_schemas, valid_user_document, valid_order_document):
    """Test document validation."""
    transformer = DocumentValidator(sample_schemas)
    
    # Test valid user document
    transformed = transformer.transform_document("client1.users", valid_user_document)
    assert transformed is not None
    assert "_metadata" in transformed
    assert transformed["_metadata"]["connection_id"] == "client1"
    assert transformed["_metadata"]["source_name"] == "users"
    
    # Test valid order document
    transformed = transformer.transform_document("client1.orders", valid_order_document)
    assert transformed is not None
    assert "_metadata" in transformed
    assert transformed["_metadata"]["connection_id"] == "client1"
    assert transformed["_metadata"]["source_name"] == "orders"
    
    # Test missing required field
    invalid_doc = valid_user_document.copy()
    del invalid_doc["user_id"]
    with pytest.raises(ValueError, match="Required field missing in client1.users: user_id"):
        transformer.transform_document("client1.users", invalid_doc)
    
    # Test null in non-nullable field
    invalid_doc = valid_user_document.copy()
    invalid_doc["user_id"] = None
    with pytest.raises(ValueError, match="Required field cannot be null in client1.users: user_id"):
        transformer.transform_document("client1.users", invalid_doc)
    
    # Test invalid field type
    invalid_doc = valid_user_document.copy()
    invalid_doc["age"] = "25"  # string instead of number
    with pytest.raises(ValueError, match="Invalid type for field age in client1.users"):
        transformer.transform_document("client1.users", invalid_doc)
    
    # Test custom validation failure
    invalid_doc = valid_user_document.copy()
    invalid_doc["age"] = -1
    with pytest.raises(ValueError, match="Validation failed for field age in client1.users"):
        transformer.transform_document("client1.users", invalid_doc)
    
    invalid_doc = valid_user_document.copy()
    invalid_doc["email"] = "invalid-email"
    with pytest.raises(ValueError, match="Validation failed for field email in client1.users"):
        transformer.transform_document("client1.users", invalid_doc)
    
    # Test unknown stream
    with pytest.raises(ValueError, match="No schema defined for stream"):
        transformer.transform_document("unknown.collection", valid_user_document)

def test_field_formatting(sample_schemas, valid_user_document):
    """Test field formatting."""
    transformer = DocumentValidator(sample_schemas)
    transformed = transformer.transform_document("client1.users", valid_user_document)
    
    # Check datetime formatting
    assert isinstance(transformed["created_at"], str)
    
    # Check metadata
    assert "_metadata" in transformed
    assert "processed_at" in transformed["_metadata"]
    assert "source" in transformed["_metadata"]
    assert "version" in transformed["_metadata"]
    assert transformed["_metadata"]["connection_id"] == "client1"
    assert transformed["_metadata"]["source_name"] == "users"
    
    # Check nested structures
    assert isinstance(transformed.get("tags"), list)
    assert isinstance(transformed.get("metadata"), dict)

def test_pubsub_preparation(sample_schemas, valid_user_document):
    """Test document preparation for Pub/Sub."""
    transformer = DocumentValidator(sample_schemas)
    transformed = transformer.transform_document("client1.users", valid_user_document)
    pubsub_message = transformer.prepare_for_pubsub(transformed, "insert")
    
    # Check message structure matches the actual implementation
    assert isinstance(pubsub_message, dict)
    assert "data" in pubsub_message
    assert "attributes" in pubsub_message
    assert pubsub_message["data"] == transformed
    assert "operation" in pubsub_message["attributes"]
    assert pubsub_message["attributes"]["operation"] == "insert"
    assert "timestamp" in pubsub_message["attributes"]
