import random
from datetime import datetime, timedelta
import jsonschema
import pytest
from src.connector.core.schema_registry import SchemaRegistry, DocumentType


def generate_inventory_docs(count):
    """Generate a specified number of valid inventory documents."""
    docs = []
    for i in range(count):
        timestamp = datetime.utcnow() - timedelta(days=random.randint(0, 30))
        docs.append({
            "_schema_version": "v1",
            "product_id": f"P{random.randint(10000, 99999)}",
            "warehouse_id": f"W{random.randint(100, 999)}",
            "quantity": random.randint(1, 1000),
            "last_updated": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "category": f"Category-{random.randint(1, 10)}",
            "brand": f"Brand-{random.randint(1, 20)}",
            "sku": f"SKU-{random.randint(1000, 9999)}"
        })
    return docs


def generate_transaction_docs(count):
    """Generate a specified number of valid transaction documents."""
    transaction_types = ["sale", "restock", "return", "adjustment"]
    docs = []
    for i in range(count):
        timestamp = datetime.utcnow() - timedelta(days=random.randint(0, 30))
        docs.append({
            "_schema_version": "v1",
            "transaction_id": f"T{random.randint(10000, 99999)}",
            "product_id": f"P{random.randint(10000, 99999)}",
            "warehouse_id": f"W{random.randint(100, 999)}",
            "quantity": random.randint(1, 50),
            "transaction_type": random.choice(transaction_types),
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "order_id": f"O{random.randint(10000, 99999)}",
            "customer_id": f"C{random.randint(1000, 9999)}"
        })
    return docs


@pytest.fixture
def inventory_docs():
    """Fixture to provide inventory documents for benchmarks."""
    return generate_inventory_docs(1000)


@pytest.fixture
def transaction_docs():
    """Fixture to provide transaction documents for benchmarks."""
    return generate_transaction_docs(1000)


@pytest.fixture
def inventory_schema():
    """Fixture to provide inventory schema."""
    return SchemaRegistry.get_schema(DocumentType.INVENTORY)


@pytest.fixture
def transaction_schema():
    """Fixture to provide transaction schema."""
    return SchemaRegistry.get_schema(DocumentType.TRANSACTION)


def test_inventory_validation_benchmark(benchmark, inventory_docs, inventory_schema):
    """Benchmark validation of inventory documents."""
    def validate_docs():
        for doc in inventory_docs:
            jsonschema.validate(instance=doc, schema=inventory_schema)
    
    result = benchmark(validate_docs)
    print(f"\nInventory validation benchmark: {result}")


def test_transaction_validation_benchmark(benchmark, transaction_docs, transaction_schema):
    """Benchmark validation of transaction documents."""
    def validate_docs():
        for doc in transaction_docs:
            jsonschema.validate(instance=doc, schema=transaction_schema)
    
    result = benchmark(validate_docs)
    print(f"\nTransaction validation benchmark: {result}")


def test_single_inventory_validation_benchmark(benchmark, inventory_docs, inventory_schema):
    """Benchmark validation of a single inventory document."""
    doc = inventory_docs[0]
    result = benchmark(lambda: jsonschema.validate(instance=doc, schema=inventory_schema))
    print(f"\nSingle inventory document validation benchmark: {result}")


def test_single_transaction_validation_benchmark(benchmark, transaction_docs, transaction_schema):
    """Benchmark validation of a single transaction document."""
    doc = transaction_docs[0]
    result = benchmark(lambda: jsonschema.validate(instance=doc, schema=transaction_schema))
    print(f"\nSingle transaction document validation benchmark: {result}")


def test_schema_lookup_benchmark(benchmark):
    """Benchmark schema registry lookups."""
    def lookup_schemas():
        for _ in range(100):
            doc_type = random.choice([DocumentType.INVENTORY, DocumentType.TRANSACTION])
            version = "v1"
            SchemaRegistry.get_schema(doc_type, version)
    
    result = benchmark(lookup_schemas)
    print(f"\nSchema lookup benchmark (100 lookups): {result}")


def test_mixed_batch_validation_benchmark(benchmark, inventory_docs, transaction_docs, 
                                        inventory_schema, transaction_schema):
    """Benchmark validation of a mixed batch of documents."""
    def validate_mixed_batch():
        batch_size = 100
        inv_batch = inventory_docs[:batch_size]
        trans_batch = transaction_docs[:batch_size]
        
        for i in range(batch_size):
            jsonschema.validate(instance=inv_batch[i], schema=inventory_schema)
            jsonschema.validate(instance=trans_batch[i], schema=transaction_schema)
    
    result = benchmark(validate_mixed_batch)
    print(f"\nMixed batch validation benchmark (200 docs total): {result}")


def test_schema_version_comparison_benchmark(benchmark, inventory_docs):
    """Benchmark comparison of different schema versions."""
    def compare_versions():
        v1_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v1")
        # Assuming v2 exists, if not available, use v1 again for comparison
        try:
            v2_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY, "v2")
        except:
            v2_schema = v1_schema
        
        for doc in inventory_docs[:50]:  # Use smaller batch for this test
            jsonschema.validate(instance=doc, schema=v1_schema)
            jsonschema.validate(instance=doc, schema=v2_schema)
    
    result = benchmark(compare_versions)
    print(f"\nSchema version comparison benchmark: {result}")


if __name__ == "__main__":
    print("To run these benchmarks, use: pytest -xvs test_schema_validation_benchmark.py") 