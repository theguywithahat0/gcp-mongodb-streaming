import unittest
import json
from datetime import datetime
import jsonschema
from src.connector.core.schema_registry import SchemaRegistry, DocumentType


class TestSchemaValidation(unittest.TestCase):
    """Integration tests for schema validation against real documents."""

    def test_valid_inventory_document(self):
        """Test validation of a valid inventory document."""
        inventory_doc = {
            "_schema_version": "v1",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 100,
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "category": "electronics",
            "brand": "TechCo",
            "sku": "SKU123",
            "threshold_min": 10,
            "threshold_max": 1000
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        try:
            jsonschema.validate(instance=inventory_doc, schema=schema)
            validation_succeeded = True
        except jsonschema.exceptions.ValidationError:
            validation_succeeded = False
            
        self.assertTrue(validation_succeeded)

    def test_invalid_inventory_document_missing_required(self):
        """Test validation of an inventory document missing required fields."""
        # Missing warehouse_id and quantity
        inventory_doc = {
            "_schema_version": "v1",
            "product_id": "P12345",
            "last_updated": datetime.utcnow().isoformat() + "Z"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=inventory_doc, schema=schema)

    def test_invalid_inventory_document_wrong_types(self):
        """Test validation of an inventory document with wrong data types."""
        inventory_doc = {
            "_schema_version": "v1",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": "not_an_integer",  # Should be an integer
            "last_updated": datetime.utcnow().isoformat() + "Z"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=inventory_doc, schema=schema)

    def test_valid_transaction_document(self):
        """Test validation of a valid transaction document."""
        transaction_doc = {
            "_schema_version": "v1",
            "transaction_id": "T98765",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 5,
            "transaction_type": "sale",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "order_id": "O456",
            "customer_id": "C456",
            "notes": "Test transaction"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        try:
            jsonschema.validate(instance=transaction_doc, schema=schema)
            validation_succeeded = True
        except jsonschema.exceptions.ValidationError:
            validation_succeeded = False
            
        self.assertTrue(validation_succeeded)

    def test_invalid_transaction_document_enum_value(self):
        """Test validation of a transaction document with invalid enum value."""
        transaction_doc = {
            "_schema_version": "v1",
            "transaction_id": "T98765",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 5,
            "transaction_type": "invalid_type",  # Not in enum
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=transaction_doc, schema=schema)

    def test_invalid_transaction_document_wrong_timestamp(self):
        """Test validation of a transaction document with invalid timestamp format."""
        transaction_doc = {
            "_schema_version": "v1",
            "transaction_id": "T98765",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 5,
            "transaction_type": "sale",
            "timestamp": "not-a-valid-timestamp"  # Invalid format
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=transaction_doc, schema=schema)

    def test_invalid_schema_version(self):
        """Test validation of a document with invalid schema version."""
        inventory_doc = {
            "_schema_version": "v999",  # Doesn't exist
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 100,
            "last_updated": datetime.utcnow().isoformat() + "Z"
        }
        
        # This will fail at schema retrieval level
        with self.assertRaises(ValueError):
            schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v999")
            jsonschema.validate(instance=inventory_doc, schema=schema)


if __name__ == "__main__":
    unittest.main() 