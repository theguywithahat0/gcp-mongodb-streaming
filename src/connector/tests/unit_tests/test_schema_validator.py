"""Tests for the schema_validator module."""

import unittest
from unittest.mock import patch, MagicMock

from src.connector.core.schema_validator import SchemaValidator
from src.connector.core.schema_registry import DocumentType

class TestSchemaValidator(unittest.TestCase):
    """Test cases for the SchemaValidator class."""

    def setUp(self):
        """Set up test environment."""
        # Mock schema registry with actual schema structure
        self.mock_schemas = {
            DocumentType.INVENTORY: {
                "v1": {
                    "type": "object",
                    "required": [
                        "_schema_version",
                        "product_id",
                        "warehouse_id",
                        "quantity",
                        "last_updated"
                    ],
                    "properties": {
                        "_schema_version": {"type": "string", "enum": ["v1"]},
                        "product_id": {"type": "string"},
                        "warehouse_id": {"type": "string"},
                        "quantity": {"type": "integer", "minimum": 0},
                        "last_updated": {"type": "string", "format": "date-time"},
                        "category": {"type": "string"},
                        "brand": {"type": "string"},
                        "sku": {"type": "string"},
                        "threshold_min": {"type": "integer", "minimum": 0},
                        "threshold_max": {"type": "integer", "minimum": 0}
                    },
                    "additionalProperties": True
                }
            },
            DocumentType.TRANSACTION: {
                "v1": {
                    "type": "object",
                    "required": [
                        "_schema_version",
                        "transaction_id",
                        "product_id",
                        "warehouse_id",
                        "quantity",
                        "transaction_type",
                        "timestamp"
                    ],
                    "properties": {
                        "_schema_version": {"type": "string", "enum": ["v1"]},
                        "transaction_id": {"type": "string"},
                        "product_id": {"type": "string"},
                        "warehouse_id": {"type": "string"},
                        "quantity": {"type": "integer"},
                        "transaction_type": {
                            "type": "string",
                            "enum": ["sale", "restock", "return", "adjustment"]
                        },
                        "timestamp": {"type": "string", "format": "date-time"},
                        "order_id": {"type": "string"},
                        "customer_id": {"type": "string"},
                        "notes": {"type": "string"}
                    },
                    "additionalProperties": True
                }
            }
        }
        
        # Patch SchemaRegistry.get_schema
        patcher = patch('src.connector.core.schema_registry.SchemaRegistry.get_schema')
        self.mock_get_schema = patcher.start()
        self.addCleanup(patcher.stop)
        
        # Configure mock to return appropriate schemas
        def get_schema_side_effect(doc_type, version=None):
            if version:
                return self.mock_schemas.get(doc_type, {}).get(version)
            # If no version specified, return v1 (latest)
            return self.mock_schemas.get(doc_type, {}).get("v1")
            
        self.mock_get_schema.side_effect = get_schema_side_effect

        # Patch SchemaRegistry.get_latest_version
        patcher = patch('src.connector.core.schema_registry.SchemaRegistry.get_latest_version')
        self.mock_get_latest = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_get_latest.return_value = "v1"

    def test_valid_document_validation(self):
        """Test validation of a valid document."""
        # Valid document with schema version
        doc = {
            "_schema_version": "v1",
            "product_id": "P123",
            "warehouse_id": "W1",
            "quantity": 10,
            "last_updated": "2024-03-14T12:00:00Z"
        }
        
        # Validate document
        result = SchemaValidator.validate_document(doc, "inventory")
        
        # Check that validation passed
        self.assertIsNone(result)
        
    def test_invalid_document_validation(self):
        """Test validation of an invalid document."""
        # Invalid document (negative quantity)
        doc = {
            "_schema_version": "v1",
            "product_id": "P123",
            "warehouse_id": "W1",
            "quantity": -5,  # Invalid: negative quantity
            "last_updated": "2024-03-14T12:00:00Z"
        }
        
        # Validate document
        result = SchemaValidator.validate_document(doc, "inventory")
        
        # Check that validation failed
        self.assertIsNotNone(result)
        self.assertIn("Schema validation failed", result)
        
    def test_missing_required_field(self):
        """Test validation when a required field is missing."""
        # Document missing required field (quantity)
        doc = {
            "_schema_version": "v1",
            "product_id": "P123",
            "warehouse_id": "W1",
            "last_updated": "2024-03-14T12:00:00Z"
        }
        
        # Validate document
        result = SchemaValidator.validate_document(doc, "inventory")
        
        # Check that validation failed
        self.assertIsNotNone(result)
        self.assertIn("quantity", result)
        
    def test_version_specific_validation(self):
        """Test validation against a specific schema version."""
        # Document with v1 schema
        doc = {
            "_schema_version": "v1",
            "product_id": "P123",
            "warehouse_id": "W1",
            "quantity": 10,
            "last_updated": "2024-03-14T12:00:00Z"
        }
        
        # Validate document with v1 schema
        result = SchemaValidator.validate_document(doc, "inventory")
        
        # Check that validation passed for v1
        self.assertIsNone(result)
        
    def test_auto_version_detection(self):
        """Test automatic schema version detection."""
        # Document without schema version
        doc = {
            "product_id": "P123",
            "warehouse_id": "W1",
            "quantity": 10,
            "last_updated": "2024-03-14T12:00:00Z"
        }
        
        # Validate document (should use latest version v1)
        result = SchemaValidator.validate_document(doc, "inventory")
        
        # Check that validation passed
        self.assertIsNone(result)
        self.assertEqual(doc["_schema_version"], "v1")
        
    def test_invalid_schema_version(self):
        """Test validation with an invalid schema version."""
        # Document with non-existent schema version
        doc = {
            "_schema_version": "v999",  # Non-existent version
            "product_id": "P123",
            "warehouse_id": "W1",
            "quantity": 10,
            "last_updated": "2024-03-14T12:00:00Z"
        }
        
        # Configure mock to raise ValueError for non-existent version
        def raise_value_error(*args):
            raise ValueError("Invalid schema version")
        self.mock_get_schema.side_effect = raise_value_error
        
        # Validate document
        result = SchemaValidator.validate_document(doc, "inventory")
        
        # Check that validation failed
        self.assertIsNotNone(result)
        self.assertIn("Invalid schema version", result)

if __name__ == '__main__':
    unittest.main() 