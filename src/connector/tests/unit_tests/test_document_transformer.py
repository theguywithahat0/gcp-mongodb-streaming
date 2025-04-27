"""Tests for the document_transformer module."""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.connector.core.document_transformer import (
    DocumentTransformer, 
    TransformStage, 
    TransformationError,
    add_processing_metadata,
    add_field_prefix,
    remove_sensitive_fields
)
from src.connector.core.schema_registry import DocumentType

class TestDocumentTransformer(unittest.TestCase):
    """Test cases for the DocumentTransformer class."""

    def setUp(self):
        """Set up test environment."""
        self.transformer = DocumentTransformer()

    def test_register_transform(self):
        """Test registering a transformation function."""
        # Mock transform function
        transform_func = lambda doc: doc
        
        # Register transform
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            transform_func
        )
        
        # Verify transform was registered
        self.assertIn(
            TransformStage.PRE_VALIDATION,
            self.transformer._transforms
        )
        self.assertIn(
            DocumentType.INVENTORY,
            self.transformer._transforms[TransformStage.PRE_VALIDATION]
        )
        self.assertEqual(
            self.transformer._transforms[TransformStage.PRE_VALIDATION][DocumentType.INVENTORY][0],
            transform_func
        )
        
    def test_apply_transforms_single_stage(self):
        """Test applying transforms for a single stage."""
        # Mock transform function that adds a field
        def add_test_field(doc):
            doc["test_field"] = "added"
            return doc
            
        # Register transform
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            add_test_field
        )
        
        # Test document
        doc = {
            "product_id": "P123",
            "quantity": 10
        }
        
        # Apply transforms
        result = self.transformer.apply_transforms(
            doc,
            DocumentType.INVENTORY,
            TransformStage.PRE_VALIDATION
        )
        
        # Verify field was added
        self.assertIn("test_field", result)
        self.assertEqual(result["test_field"], "added")
        
    def test_apply_transforms_multiple_stages(self):
        """Test applying transforms across multiple stages."""
        # Mock transform functions that add fields
        def add_field_1(doc):
            doc["field1"] = "stage1"
            return doc
            
        def add_field_2(doc):
            doc["field2"] = "stage2"
            return doc
        
        # Register transforms for different stages
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            add_field_1
        )
        self.transformer.register_transform(
            TransformStage.POST_VALIDATION,
            DocumentType.INVENTORY,
            add_field_2
        )
        
        # Test document
        doc = {
            "product_id": "P123",
            "quantity": 10
        }
        
        # Apply transforms for stage 1
        result = self.transformer.apply_transforms(
            doc,
            DocumentType.INVENTORY,
            TransformStage.PRE_VALIDATION
        )
        
        # Verify first field was added
        self.assertIn("field1", result)
        self.assertEqual(result["field1"], "stage1")
        self.assertNotIn("field2", result)
        
        # Apply transforms for stage 2
        result = self.transformer.apply_transforms(
            result,
            DocumentType.INVENTORY,
            TransformStage.POST_VALIDATION
        )
        
        # Verify both fields are present
        self.assertIn("field1", result)
        self.assertEqual(result["field1"], "stage1")
        self.assertIn("field2", result)
        self.assertEqual(result["field2"], "stage2")
        
    def test_transform_error_handling(self):
        """Test error handling during transformation."""
        # Mock transform function that raises an error
        def failing_transform(doc):
            raise ValueError("Test error")
            
        # Register transform
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            failing_transform
        )
        
        # Test document
        doc = {
            "product_id": "P123",
            "quantity": 10
        }
        
        # Apply transforms and verify error is raised
        with self.assertRaises(TransformationError):
            self.transformer.apply_transforms(
                doc,
                DocumentType.INVENTORY,
                TransformStage.PRE_VALIDATION
            )
            
    def test_multiple_transforms_same_stage(self):
        """Test applying multiple transforms at the same stage."""
        # Mock transform functions that modify different fields
        def add_field_a(doc):
            doc["field_a"] = "value_a"
            return doc
            
        def add_field_b(doc):
            doc["field_b"] = "value_b"
            return doc
        
        # Register both transforms for the same stage/type
        self.transformer.register_transform(
            TransformStage.PRE_PUBLISH,
            DocumentType.INVENTORY,
            add_field_a
        )
        self.transformer.register_transform(
            TransformStage.PRE_PUBLISH,
            DocumentType.INVENTORY,
            add_field_b
        )
        
        # Test document
        doc = {
            "product_id": "P123",
            "quantity": 10
        }
        
        # Apply transforms
        result = self.transformer.apply_transforms(
            doc,
            DocumentType.INVENTORY,
            TransformStage.PRE_PUBLISH
        )
        
        # Verify both fields were added
        self.assertIn("field_a", result)
        self.assertEqual(result["field_a"], "value_a")
        self.assertIn("field_b", result)
        self.assertEqual(result["field_b"], "value_b")

    def test_register_transform_with_position(self):
        """Test registering a transformation function at a specific position."""
        # Mock transform functions
        transform_1 = lambda doc: {**doc, "field1": "value1"}
        transform_2 = lambda doc: {**doc, "field2": "value2"}
        transform_3 = lambda doc: {**doc, "field3": "value3"}
        
        # Register transforms
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            transform_1
        )
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            transform_2
        )
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            transform_3,
            position=1  # Insert between transform_1 and transform_2
        )
        
        # Test document
        doc = {"test": "value"}
        
        # Apply transforms
        result = self.transformer.apply_transforms(
            doc,
            DocumentType.INVENTORY,
            TransformStage.PRE_VALIDATION
        )
        
        # Verify order of transformations
        self.assertEqual(result["field1"], "value1")  # First transform
        self.assertEqual(result["field3"], "value3")  # Second transform (inserted at position 1)
        self.assertEqual(result["field2"], "value2")  # Third transform

    def test_clear_transforms(self):
        """Test clearing transformations."""
        # Mock transform function
        transform_func = lambda doc: doc
        
        # Register transforms for different stages and types
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            transform_func
        )
        self.transformer.register_transform(
            TransformStage.POST_VALIDATION,
            DocumentType.TRANSACTION,
            transform_func
        )
        
        # Test clearing specific stage
        self.transformer.clear_transforms(stage=TransformStage.PRE_VALIDATION)
        self.assertEqual(len(self.transformer._transforms[TransformStage.PRE_VALIDATION][DocumentType.INVENTORY]), 0)
        self.assertEqual(len(self.transformer._transforms[TransformStage.POST_VALIDATION][DocumentType.TRANSACTION]), 1)
        
        # Test clearing specific document type
        self.transformer.clear_transforms(doc_type=DocumentType.TRANSACTION)
        self.assertEqual(len(self.transformer._transforms[TransformStage.POST_VALIDATION][DocumentType.TRANSACTION]), 0)
        
        # Register new transforms
        self.transformer.register_transform(
            TransformStage.PRE_VALIDATION,
            DocumentType.INVENTORY,
            transform_func
        )
        
        # Test clearing all transforms
        self.transformer.clear_transforms()
        for stage in TransformStage:
            for doc_type in DocumentType:
                self.assertEqual(len(self.transformer._transforms[stage][doc_type]), 0)

    def test_compose_transforms(self):
        """Test composing multiple transformation functions."""
        # Define transform functions
        def add_field_1(doc):
            return {**doc, "field1": "value1"}
            
        def add_field_2(doc):
            return {**doc, "field2": "value2"}
            
        def add_timestamp(doc):
            return {**doc, "timestamp": "2024-01-01"}
        
        # Compose transforms
        composed = DocumentTransformer.compose_transforms(
            add_field_1,
            add_field_2,
            add_timestamp
        )
        
        # Verify composed function name
        self.assertEqual(
            composed.__name__,
            "composed_add_field_1_add_field_2_add_timestamp"
        )
        
        # Test composed transformation
        doc = {"original": "value"}
        result = composed(doc)
        
        # Verify all transformations were applied in order
        self.assertEqual(result["field1"], "value1")
        self.assertEqual(result["field2"], "value2")
        self.assertEqual(result["timestamp"], "2024-01-01")
        self.assertEqual(result["original"], "value")

    def test_remove_sensitive_fields_error_handling(self):
        """Test error handling in remove_sensitive_fields."""
        # Test with invalid nested path
        doc = {
            "customer": "not_a_dict",  # This will cause the nested path to fail
            "other_field": "value"
        }
        
        transform = remove_sensitive_fields(["customer.email"])
        result = transform(doc)
        
        # Verify document wasn't modified (except for metadata)
        self.assertEqual(result["customer"], "not_a_dict")
        self.assertEqual(result["other_field"], "value")
        self.assertIn("_security_metadata", result)

    def test_add_field_prefix_edge_cases(self):
        """Test edge cases in add_field_prefix."""
        # Test with non-dict value in nested structure
        doc = {
            "field1": {
                "nested": "value",
                "other": ["not_a_dict"]
            }
        }
        
        transformer = add_field_prefix("test", ["field1.nested", "field1.other"])
        result = transformer(doc)
        
        # Verify prefixing works at the nested level
        self.assertIn("field1", result)
        self.assertIn("test_nested", result["field1"])
        self.assertIn("test_other", result["field1"])
        self.assertEqual(result["field1"]["test_nested"], "value")
        self.assertEqual(result["field1"]["test_other"], ["not_a_dict"])
        
        # Verify metadata was added
        self.assertIn("_security_metadata", result)
        self.assertEqual(set(result["_security_metadata"]["prefixed_fields"]), {"field1.nested", "field1.other"})

class TestTransformationFunctions(unittest.TestCase):
    """Test cases for the built-in transformation functions."""
    
    def test_add_processing_metadata(self):
        """Test adding processing metadata to documents."""
        # Test document
        doc = {"field": "value"}
        
        # Transform document
        result = add_processing_metadata(doc)
        
        # Verify metadata was added
        self.assertIn("_processing_metadata", result)
        self.assertIn("processed_at", result["_processing_metadata"])
        self.assertIn("processor_version", result["_processing_metadata"])
        self.assertEqual(result["_processing_metadata"]["processor_version"], "1.0")
        self.assertIsInstance(result["_processing_metadata"]["processed_at"], str)
        
        # Verify original document was not modified
        self.assertEqual(doc, {"field": "value"})

    def test_add_field_prefix(self):
        """Test adding prefix to specified fields."""
        # Create transformer
        transformer = add_field_prefix("test", ["field1", "nested.field2"])
        
        # Test document
        doc = {
            "field1": "value1",
            "field2": "value2",
            "nested": {
                "field1": "nested1",
                "field2": "nested2"
            },
            "_metadata": "unchanged"
        }
        
        # Transform document
        result = transformer(doc)
        
        # Verify prefixes were added correctly
        self.assertIn("test_field1", result)
        self.assertIn("field2", result)  # Not in fields list, shouldn't be prefixed
        self.assertIn("test_field2", result["nested"])  # In fields list as nested.field2
        self.assertIn("field1", result["nested"])  # Not in fields list
        self.assertIn("_metadata", result)  # Metadata fields shouldn't be prefixed
        
        # Verify metadata was added
        self.assertIn("_security_metadata", result)
        self.assertIn("prefixed_at", result["_security_metadata"])
        self.assertIsInstance(result["_security_metadata"]["prefixed_at"], str)
        self.assertIn("prefixed_fields", result["_security_metadata"])
        self.assertEqual(set(result["_security_metadata"]["prefixed_fields"]), {"field1", "nested.field2"})
        
        # Verify original document was not modified
        self.assertEqual(doc["field1"], "value1")
        self.assertEqual(doc["nested"]["field2"], "nested2")

    def test_remove_sensitive_fields(self):
        """Test removing sensitive fields."""
        doc = {
            "product_id": "P123",
            "quantity": 10,
            "customer": {
                "name": "John Doe",
                "email": "john@example.com",
                "card": {
                    "number": "4111111111111111",
                    "cvv": "123"
                }
            }
        }
        
        # Apply transformation to remove sensitive fields
        sensitive_fields = [
            "customer.email",
            "customer.card.cvv"
        ]
        result = remove_sensitive_fields(sensitive_fields)(doc)
        
        # Verify sensitive fields were removed
        self.assertIn("customer", result)
        self.assertIn("name", result["customer"])
        self.assertNotIn("email", result["customer"])
        self.assertIn("card", result["customer"])
        self.assertIn("number", result["customer"]["card"])
        self.assertNotIn("cvv", result["customer"]["card"])
        
        # Verify non-sensitive fields were not modified
        self.assertEqual(result["product_id"], "P123")
        self.assertEqual(result["quantity"], 10)
        self.assertEqual(result["customer"]["name"], "John Doe")
        self.assertEqual(result["customer"]["card"]["number"], "4111111111111111")

if __name__ == '__main__':
    unittest.main() 