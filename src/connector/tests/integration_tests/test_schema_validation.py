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
            "timestamp": "2023-13-32T25:61:99Z"  # Invalid month, day, hour, minute and second
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

    def test_invalid_inventory_document_wrong_timestamp(self):
        """Test validation of an inventory document with invalid timestamp format."""
        inventory_doc = {
            "_schema_version": "v1",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 100,
            "last_updated": "not-a-timestamp",  # Invalid timestamp format
            "category": "electronics"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=inventory_doc, schema=schema)

    def test_valid_timestamp_formats(self):
        """Test various valid ISO 8601 timestamp formats."""
        valid_timestamps = [
            "2024-03-20T15:30:00Z",  # Basic UTC format
            "2024-03-20T15:30:00.123Z",  # With milliseconds
            "2024-03-20T15:30:00+00:00",  # With timezone offset
            "2024-03-20T15:30:00.123+00:00",  # With milliseconds and offset
            "2024-03-20T15:30:00-05:00",  # With negative timezone offset
            "2024-03-20T15:30:00.123456Z"  # With microseconds
        ]
        
        for timestamp in valid_timestamps:
            # Test inventory document
            inventory_doc = {
                "_schema_version": "v1",
                "product_id": "prod123",
                "warehouse_id": "wh456",
                "quantity": 100,
                "last_updated": timestamp,
                "category": "electronics",
                "brand": "TestBrand",
                "sku": "SKU123",
                "threshold_min": 10,
                "threshold_max": 1000
            }
            try:
                jsonschema.validate(instance=inventory_doc, schema=SchemaRegistry.get_schema(DocumentType.INVENTORY))
            except Exception as e:
                self.fail(f"Valid timestamp {timestamp} failed validation for inventory: {str(e)}")
            
            # Test transaction document
            transaction_doc = {
                "_schema_version": "v1",
                "transaction_id": "t123",
                "product_id": "prod123",
                "warehouse_id": "wh456",
                "quantity": 5,
                "transaction_type": "sale",
                "timestamp": timestamp,
                "order_id": "ord789",
                "customer_id": "cust123",
                "notes": "Test transaction"
            }
            try:
                jsonschema.validate(instance=transaction_doc, schema=SchemaRegistry.get_schema(DocumentType.TRANSACTION))
            except Exception as e:
                self.fail(f"Valid timestamp {timestamp} failed validation for transaction: {str(e)}")

    def test_invalid_timestamp_formats(self):
        """Test various invalid timestamp formats."""
        invalid_timestamps = [
            "2024-03-20",  # Date only
            "15:30:00",  # Time only
            "2024-03-20 15:30:00",  # No T separator
            "2024-03-20T15:30:00",  # No timezone
            "2024-03-20T15:30:00GMT",  # Invalid timezone format
            "2024-03-20T25:30:00Z",  # Invalid hour
            "2024-03-20T15:60:00Z",  # Invalid minute
            "2024-03-20T15:30:61Z",  # Invalid second
            "2024-13-20T15:30:00Z",  # Invalid month
            "2024-03-32T15:30:00Z",  # Invalid day
            "not-a-timestamp",  # Invalid format
            "2024/03/20T15:30:00Z",  # Wrong date separator
            "2024-03-20T15.30.00Z",  # Wrong time separator
        ]
        
        base_inventory_doc = {
            "_schema_version": "v1",
            "product_id": "prod123",
            "warehouse_id": "wh456",
            "quantity": 100,
            "category": "electronics",
            "brand": "TestBrand",
            "sku": "SKU123",
            "threshold_min": 10,
            "threshold_max": 1000
        }
        
        base_transaction_doc = {
            "_schema_version": "v1",
            "transaction_id": "t123",
            "product_id": "prod123",
            "warehouse_id": "wh456",
            "quantity": 5,
            "transaction_type": "sale",
            "order_id": "ord789",
            "customer_id": "cust123",
            "notes": "Test transaction"
        }
        
        for timestamp in invalid_timestamps:
            # Test inventory document
            inventory_doc = base_inventory_doc.copy()
            inventory_doc["last_updated"] = timestamp
            with self.assertRaises(
                (jsonschema.exceptions.ValidationError, jsonschema.exceptions.FormatError),
                msg=f"Invalid timestamp {timestamp} should fail validation for inventory"
            ):
                jsonschema.validate(instance=inventory_doc, schema=SchemaRegistry.get_schema(DocumentType.INVENTORY))
            
            # Test transaction document
            transaction_doc = base_transaction_doc.copy()
            transaction_doc["timestamp"] = timestamp
            with self.assertRaises(
                (jsonschema.exceptions.ValidationError, jsonschema.exceptions.FormatError),
                msg=f"Invalid timestamp {timestamp} should fail validation for transaction"
            ):
                jsonschema.validate(instance=transaction_doc, schema=SchemaRegistry.get_schema(DocumentType.TRANSACTION))


if __name__ == "__main__":
    unittest.main() 