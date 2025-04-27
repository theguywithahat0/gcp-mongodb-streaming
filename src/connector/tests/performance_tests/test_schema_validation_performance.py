import unittest
import time
import json
import random
import string
from datetime import datetime, timedelta
import jsonschema
from src.connector.core.schema_registry import SchemaRegistry, DocumentType


class TestSchemaValidationPerformance(unittest.TestCase):
    """Performance tests for schema validation."""
    
    def setUp(self):
        """Set up test data and schemas."""
        self.inventory_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
        self.transaction_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
        
        # Generate test data
        self.inventory_docs = self._generate_inventory_docs(1000)
        self.transaction_docs = self._generate_transaction_docs(1000)
    
    def _generate_inventory_docs(self, count):
        """Generate a specified number of valid inventory documents."""
        docs = []
        for i in range(count):
            timestamp = datetime.utcnow() - timedelta(days=random.randint(0, 30))
            docs.append({
                "_schema_version": "v1",
                "product_id": f"P{random.randint(10000, 99999)}",
                "warehouse_id": f"W{random.randint(100, 999)}",
                "quantity": random.randint(1, 1000),
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            })
        return docs
    
    def _generate_transaction_docs(self, count):
        """Generate a specified number of valid transaction documents."""
        transaction_types = ["sale", "purchase", "return", "adjustment"]
        docs = []
        for i in range(count):
            timestamp = datetime.utcnow() - timedelta(days=random.randint(0, 30))
            docs.append({
                "_schema_version": "v1",
                "transaction_id": f"T{random.randint(10000, 99999)}",
                "product_id": f"P{random.randint(10000, 99999)}",
                "quantity": random.randint(1, 50),
                "transaction_type": random.choice(transaction_types),
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            })
        return docs
    
    def test_inventory_schema_validation_performance(self):
        """Test the performance of validating many inventory documents."""
        start_time = time.time()
        
        for doc in self.inventory_docs:
            jsonschema.validate(instance=doc, schema=self.inventory_schema)
            
        elapsed_time = time.time() - start_time
        avg_time_per_doc = elapsed_time / len(self.inventory_docs) * 1000  # in milliseconds
        
        print(f"\nInventory validation: {len(self.inventory_docs)} documents in {elapsed_time:.2f}s")
        print(f"Average time per document: {avg_time_per_doc:.2f}ms")
        
        # Assert that validation speed is acceptable (adjust threshold as needed)
        self.assertLess(avg_time_per_doc, 5.0, 
                        f"Inventory document validation too slow: {avg_time_per_doc:.2f}ms per document")
    
    def test_transaction_schema_validation_performance(self):
        """Test the performance of validating many transaction documents."""
        start_time = time.time()
        
        for doc in self.transaction_docs:
            jsonschema.validate(instance=doc, schema=self.transaction_schema)
            
        elapsed_time = time.time() - start_time
        avg_time_per_doc = elapsed_time / len(self.transaction_docs) * 1000  # in milliseconds
        
        print(f"\nTransaction validation: {len(self.transaction_docs)} documents in {elapsed_time:.2f}s")
        print(f"Average time per document: {avg_time_per_doc:.2f}ms")
        
        # Assert that validation speed is acceptable (adjust threshold as needed)
        self.assertLess(avg_time_per_doc, 5.0, 
                        f"Transaction document validation too slow: {avg_time_per_doc:.2f}ms per document")
    
    def test_batch_validation_performance(self):
        """Test the performance of validating a batch of mixed documents."""
        # Mix inventory and transaction documents
        mixed_docs = []
        for i in range(500):
            mixed_docs.append({"type": "inventory", "doc": self.inventory_docs[i]})
            mixed_docs.append({"type": "transaction", "doc": self.transaction_docs[i]})
        
        start_time = time.time()
        
        for item in mixed_docs:
            if item["type"] == "inventory":
                jsonschema.validate(instance=item["doc"], schema=self.inventory_schema)
            else:
                jsonschema.validate(instance=item["doc"], schema=self.transaction_schema)
            
        elapsed_time = time.time() - start_time
        avg_time_per_doc = elapsed_time / len(mixed_docs) * 1000  # in milliseconds
        
        print(f"\nBatch validation: {len(mixed_docs)} mixed documents in {elapsed_time:.2f}s")
        print(f"Average time per document: {avg_time_per_doc:.2f}ms")
        
        # Assert that validation speed is acceptable (adjust threshold as needed)
        self.assertLess(avg_time_per_doc, 5.0, 
                        f"Batch document validation too slow: {avg_time_per_doc:.2f}ms per document")
    
    def test_schema_lookup_performance(self):
        """Test the performance of schema registry lookups."""
        iterations = 1000
        
        start_time = time.time()
        
        for _ in range(iterations):
            # Randomly choose document type and version
            doc_type = random.choice([DocumentType.INVENTORY, DocumentType.TRANSACTION])
            version = "v1"  # Can be expanded if multiple versions exist
            
            SchemaRegistry.get_schema(doc_type, version)
            
        elapsed_time = time.time() - start_time
        avg_time_per_lookup = elapsed_time / iterations * 1000  # in milliseconds
        
        print(f"\nSchema lookup: {iterations} lookups in {elapsed_time:.2f}s")
        print(f"Average time per lookup: {avg_time_per_lookup:.2f}ms")
        
        # Assert that lookup speed is acceptable (adjust threshold as needed)
        self.assertLess(avg_time_per_lookup, 1.0, 
                        f"Schema lookup too slow: {avg_time_per_lookup:.2f}ms per lookup")


if __name__ == "__main__":
    unittest.main() 