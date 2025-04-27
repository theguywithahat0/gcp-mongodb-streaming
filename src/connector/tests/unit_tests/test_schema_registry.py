import unittest
from src.connector.core.schema_registry import SchemaRegistry, DocumentType


class TestSchemaRegistry(unittest.TestCase):
    """Test cases for SchemaRegistry class."""

    def test_get_latest_version(self):
        """Test retrieval of latest schema version for each document type."""
        # Test for inventory documents
        latest_inventory_version = SchemaRegistry.get_latest_version(DocumentType.INVENTORY)
        self.assertEqual(latest_inventory_version, "v1")

        # Test for transaction documents
        latest_transaction_version = SchemaRegistry.get_latest_version(DocumentType.TRANSACTION)
        self.assertEqual(latest_transaction_version, "v1")

    def test_get_schema_with_explicit_version(self):
        """Test retrieval of schema with explicitly specified version."""
        # Test for inventory document schema with explicit version
        inventory_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v1")
        self.assertEqual(inventory_schema["type"], "object")
        self.assertIn("product_id", inventory_schema["required"])
        self.assertIn("warehouse_id", inventory_schema["required"])
        self.assertIn("quantity", inventory_schema["required"])

        # Test for transaction document schema with explicit version
        transaction_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION, "v1")
        self.assertEqual(transaction_schema["type"], "object")
        self.assertIn("transaction_id", transaction_schema["required"])
        self.assertIn("product_id", transaction_schema["required"])
        self.assertIn("transaction_type", transaction_schema["required"])

    def test_get_schema_with_default_version(self):
        """Test retrieval of schema with default version (latest)."""
        # Test for inventory document schema with default version
        inventory_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        latest_inventory_version = SchemaRegistry.get_latest_version(DocumentType.INVENTORY)
        self.assertIn("_schema_version", inventory_schema["properties"])
        self.assertEqual(inventory_schema["properties"]["_schema_version"]["enum"], [latest_inventory_version])

        # Test for transaction document schema with default version
        transaction_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        latest_transaction_version = SchemaRegistry.get_latest_version(DocumentType.TRANSACTION)
        self.assertIn("_schema_version", transaction_schema["properties"])
        self.assertEqual(transaction_schema["properties"]["_schema_version"]["enum"], [latest_transaction_version])

    def test_get_schema_with_invalid_version(self):
        """Test retrieval of schema with invalid version raises ValueError."""
        # Test for inventory document schema with invalid version
        with self.assertRaises(ValueError):
            SchemaRegistry.get_schema(DocumentType.INVENTORY, "v999")

        # Test for transaction document schema with invalid version
        with self.assertRaises(ValueError):
            SchemaRegistry.get_schema(DocumentType.TRANSACTION, "v999")

    def test_is_valid_version(self):
        """Test validation of schema versions."""
        # Test valid versions
        self.assertTrue(SchemaRegistry.is_valid_version(DocumentType.INVENTORY, "v1"))
        self.assertTrue(SchemaRegistry.is_valid_version(DocumentType.TRANSACTION, "v1"))

        # Test invalid versions
        self.assertFalse(SchemaRegistry.is_valid_version(DocumentType.INVENTORY, "v2"))
        self.assertFalse(SchemaRegistry.is_valid_version(DocumentType.TRANSACTION, "v999"))
        self.assertFalse(SchemaRegistry.is_valid_version(DocumentType.INVENTORY, "invalid"))

    def test_schema_content(self):
        """Test specific content of schemas."""
        # Test inventory schema content
        inventory_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v1")
        self.assertEqual(inventory_schema["properties"]["quantity"]["type"], "integer")
        self.assertEqual(inventory_schema["properties"]["quantity"]["minimum"], 0)
        self.assertEqual(inventory_schema["properties"]["transaction_type"] if "transaction_type" in inventory_schema["properties"] else None, None)

        # Test transaction schema content
        transaction_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION, "v1")
        self.assertEqual(transaction_schema["properties"]["transaction_type"]["type"], "string")
        self.assertListEqual(transaction_schema["properties"]["transaction_type"]["enum"], ["sale", "restock", "return", "adjustment"])
        self.assertEqual(transaction_schema["properties"]["timestamp"]["format"], "date-time")


if __name__ == "__main__":
    unittest.main() 