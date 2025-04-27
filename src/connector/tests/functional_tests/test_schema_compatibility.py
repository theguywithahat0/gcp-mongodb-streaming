import unittest
import copy
import jsonschema
from src.connector.core.schema_registry import SchemaRegistry, DocumentType


class TestSchemaCompatibility(unittest.TestCase):
    """Functional tests for schema compatibility between different versions."""
    
    def test_backward_compatibility_inventory(self):
        """
        Test that documents validated against older schema versions
        are still valid under newer schema versions (backward compatibility).
        """
        # Get v1 schema
        v1_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v1")
        
        # Create a valid document for v1
        inventory_doc_v1 = {
            "_schema_version": "v1",
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 100,
            "timestamp": "2023-01-01T12:00:00Z"
        }
        
        # Validate against v1 schema
        jsonschema.validate(instance=inventory_doc_v1, schema=v1_schema)
        
        # Get the latest schema (which should be >= v1)
        latest_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        
        # Document created for v1 should still be valid under the latest schema
        jsonschema.validate(instance=inventory_doc_v1, schema=latest_schema)
    
    def test_backward_compatibility_transaction(self):
        """
        Test that transaction documents validated against older schema versions
        are still valid under newer schema versions.
        """
        # Get v1 schema
        v1_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION, "v1")
        
        # Create a valid document for v1
        transaction_doc_v1 = {
            "_schema_version": "v1",
            "transaction_id": "T98765",
            "product_id": "P12345", 
            "quantity": 5,
            "transaction_type": "sale",
            "timestamp": "2023-01-01T12:00:00Z"
        }
        
        # Validate against v1 schema
        jsonschema.validate(instance=transaction_doc_v1, schema=v1_schema)
        
        # Get the latest schema (which should be >= v1)
        latest_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        
        # Document created for v1 should still be valid under the latest schema
        jsonschema.validate(instance=transaction_doc_v1, schema=latest_schema)
    
    def test_optional_fields_forward_compatibility_inventory(self):
        """
        Test that new optional fields added in newer schema versions
        do not break validation for documents created with newer schemas
        when validated against older schemas (forward compatibility).
        """
        latest_version = SchemaRegistry.get_latest_version(DocumentType.INVENTORY)
        latest_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, latest_version)
        
        # Check if we have multiple versions to test
        if latest_version != "v1":
            # Create a document with the latest schema version
            inventory_doc_latest = {
                "_schema_version": latest_version,
                "product_id": "P12345",
                "warehouse_id": "W789",
                "quantity": 100,
                "timestamp": "2023-01-01T12:00:00Z"
            }
            
            # Add any new fields that might be in the latest schema but not in v1
            # For now, we'll use a generic approach - add a field that might be optional
            inventory_doc_latest["last_audit_date"] = "2023-02-01"
            
            # Should validate against latest schema
            jsonschema.validate(instance=inventory_doc_latest, schema=latest_schema)
            
            # Now create a copy without the schema version field
            v1_compatible_doc = copy.deepcopy(inventory_doc_latest)
            v1_compatible_doc["_schema_version"] = "v1"
            
            # Get v1 schema
            v1_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v1")
            
            # Try validation - this tests if new optional fields in latest schema
            # would break v1 validation
            try:
                jsonschema.validate(instance=v1_compatible_doc, schema=v1_schema)
                forward_compatible = True
            except jsonschema.exceptions.ValidationError:
                # If it fails, we need to make sure it's not because of the 
                # new fields (which v1 schema might not know about)
                stripped_doc = {
                    k: v for k, v in v1_compatible_doc.items() 
                    if k in ["_schema_version", "product_id", "warehouse_id", "quantity", "timestamp"]
                }
                try:
                    jsonschema.validate(instance=stripped_doc, schema=v1_schema)
                    forward_compatible = False  # Failed because of new fields
                except:
                    forward_compatible = False  # Failed for other reasons
                    
            self.assertTrue(forward_compatible, 
                           "The latest schema should be forward compatible with v1")

    def test_schema_version_field_required(self):
        """Test that _schema_version field is required in all documents."""
        # Create a document without schema version
        inventory_doc_no_version = {
            "product_id": "P12345",
            "warehouse_id": "W789",
            "quantity": 100,
            "timestamp": "2023-01-01T12:00:00Z"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=inventory_doc_no_version, schema=schema)
            
        # Create a transaction document without schema version
        transaction_doc_no_version = {
            "transaction_id": "T98765",
            "product_id": "P12345",
            "quantity": 5,
            "transaction_type": "sale",
            "timestamp": "2023-01-01T12:00:00Z"
        }
        
        schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(instance=transaction_doc_no_version, schema=schema)


if __name__ == "__main__":
    unittest.main() 