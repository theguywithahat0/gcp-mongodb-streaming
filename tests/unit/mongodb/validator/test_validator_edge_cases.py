"""Test edge cases and schema validation in the document validator."""
import pytest
from datetime import datetime, UTC
from typing import Any, Dict

from src.pipeline.mongodb.validator import DocumentValidator

@pytest.fixture
def test_schema():
    """Create a test schema with various validation cases."""
    return {
        "test.collection": {
            "field_string": {
                "type": "string",
                "required": True,
                "nullable": False
            },
            "field_number": {
                "type": "number",
                "required": True,
                "nullable": True
            },
            "field_boolean": {
                "type": "boolean",
                "required": False,
                "nullable": True
            },
            "field_date": {
                "type": "date",
                "required": True,
                "nullable": False
            },
            "field_object": {
                "type": "object",
                "required": False,
                "nullable": True
            },
            "field_array": {
                "type": "array",
                "required": False,
                "nullable": True
            },
            "field_custom": {
                "type": "string",
                "required": True,
                "nullable": False,
                "validation": lambda x: len(x) >= 3 and x.isalpha()
            }
        }
    }

def test_invalid_schema_field_name():
    """Test schema validation with invalid field name type."""
    invalid_schema = {
        "test.collection": {
            123: {  # Invalid field name (not a string)
                "type": "string",
                "required": True
            }
        }
    }
    with pytest.raises(ValueError, match="Field name must be string"):
        DocumentValidator(invalid_schema)

def test_invalid_schema_field_config():
    """Test schema validation with invalid field configuration."""
    invalid_schema = {
        "test.collection": {
            "field": "string"  # Invalid config (not a dict)
        }
    }
    with pytest.raises(ValueError, match="Field config must be dict"):
        DocumentValidator(invalid_schema)

def test_missing_type_in_schema():
    """Test schema validation with missing type field."""
    invalid_schema = {
        "test.collection": {
            "field": {
                "required": True  # Missing type
            }
        }
    }
    with pytest.raises(ValueError, match="missing required 'type' key"):
        DocumentValidator(invalid_schema)

def test_invalid_type_in_schema():
    """Test schema validation with invalid type."""
    invalid_schema = {
        "test.collection": {
            "field": {
                "type": "invalid_type",  # Invalid type
                "required": True
            }
        }
    }
    with pytest.raises(ValueError, match="has invalid type"):
        DocumentValidator(invalid_schema)

def test_invalid_stream_id_format():
    """Test schema validation with invalid stream ID format."""
    invalid_schema = {
        "invalid_stream_id": {  # Missing dot separator
            "field": {
                "type": "string",
                "required": True
            }
        }
    }
    with pytest.raises(ValueError, match="Invalid stream_id format"):
        DocumentValidator(invalid_schema)

def test_custom_validation_error(test_schema):
    """Test custom validation function error handling."""
    validator = DocumentValidator(test_schema)
    document = {
        "field_string": "test",
        "field_number": 42,
        "field_date": datetime.now(UTC),
        "field_custom": "12"  # Invalid: contains numbers
    }
    
    with pytest.raises(ValueError, match="Validation failed for field field_custom"):
        validator.transform_document("test.collection", document)

def test_nullable_required_field(test_schema):
    """Test validation of nullable required fields."""
    validator = DocumentValidator(test_schema)
    document = {
        "field_string": "test",
        "field_number": None,  # Nullable required field
        "field_date": datetime.now(UTC),
        "field_custom": "valid"
    }
    
    # Should not raise an error
    transformed = validator.transform_document("test.collection", document)
    assert transformed["field_number"] is None

def test_complex_object_validation(test_schema):
    """Test validation of complex nested objects."""
    validator = DocumentValidator(test_schema)
    document = {
        "field_string": "test",
        "field_number": 42,
        "field_date": datetime.now(UTC),
        "field_custom": "valid",
        "field_object": {
            "nested": {
                "deep": [1, 2, 3]
            }
        },
        "field_array": [
            {"item": 1},
            {"item": 2}
        ]
    }
    
    transformed = validator.transform_document("test.collection", document)
    assert isinstance(transformed["field_object"], dict)
    assert isinstance(transformed["field_array"], list)
    assert len(transformed["field_array"]) == 2

def test_custom_validation_exception_handling(test_schema):
    """Test handling of exceptions in custom validation functions."""
    def failing_validation(x: Any) -> bool:
        raise RuntimeError("Validation function failed")
    
    schema_with_failing_validation = test_schema.copy()
    schema_with_failing_validation["test.collection"]["field_custom"]["validation"] = failing_validation
    
    validator = DocumentValidator(schema_with_failing_validation)
    document = {
        "field_string": "test",
        "field_number": 42,
        "field_date": datetime.now(UTC),
        "field_custom": "valid"
    }
    
    with pytest.raises(ValueError, match=f"Validation failed for field field_custom in test.collection: Validation function failed"):
        validator.transform_document("test.collection", document)

def test_metadata_consistency(test_schema):
    """Test consistency of metadata fields."""
    validator = DocumentValidator(test_schema)
    document = {
        "field_string": "test",
        "field_number": 42,
        "field_date": datetime.now(UTC),
        "field_custom": "valid"
    }
    
    transformed1 = validator.transform_document("test.collection", document)
    transformed2 = validator.transform_document("test.collection", document)
    
    # Verify metadata structure
    assert "_metadata" in transformed1
    assert "_metadata" in transformed2
    assert transformed1["_metadata"]["source"] == "mongodb_change_stream"
    assert transformed1["_metadata"]["version"] == "1.0"
    
    # Verify connection parsing
    assert transformed1["_metadata"]["connection_id"] == "test"
    assert transformed1["_metadata"]["source_name"] == "collection" 