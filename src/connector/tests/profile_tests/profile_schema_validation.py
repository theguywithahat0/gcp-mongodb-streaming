import cProfile
import pstats
import io
import random
import time
from datetime import datetime, timedelta
import jsonschema
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
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
    return docs


def generate_transaction_docs(count):
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


def profile_inventory_validation(docs, schema):
    """Profile the validation of inventory documents."""
    for doc in docs:
        jsonschema.validate(instance=doc, schema=schema)


def profile_transaction_validation(docs, schema):
    """Profile the validation of transaction documents."""
    for doc in docs:
        jsonschema.validate(instance=doc, schema=schema)


def profile_batch_validation(inventory_docs, transaction_docs, inventory_schema, transaction_schema):
    """Profile the validation of a batch of mixed documents."""
    mixed_docs = []
    for i in range(min(len(inventory_docs), len(transaction_docs))):
        mixed_docs.append({"type": "inventory", "doc": inventory_docs[i]})
        mixed_docs.append({"type": "transaction", "doc": transaction_docs[i]})
    
    for item in mixed_docs:
        if item["type"] == "inventory":
            jsonschema.validate(instance=item["doc"], schema=inventory_schema)
        else:
            jsonschema.validate(instance=item["doc"], schema=transaction_schema)


def profile_schema_lookup():
    """Profile the schema registry lookups."""
    for _ in range(1000):
        doc_type = random.choice([DocumentType.INVENTORY, DocumentType.TRANSACTION])
        version = "v1"
        SchemaRegistry.get_schema(doc_type, version)


def run_profiling(num_docs=1000):
    """Run profiling on various schema validation operations."""
    # Get schemas
    inventory_schema = SchemaRegistry.get_schema(DocumentType.INVENTORY)
    transaction_schema = SchemaRegistry.get_schema(DocumentType.TRANSACTION)
    
    # Generate test data
    inventory_docs = generate_inventory_docs(num_docs)
    transaction_docs = generate_transaction_docs(num_docs)
    
    # Profile inventory validation
    pr = cProfile.Profile()
    pr.enable()
    profile_inventory_validation(inventory_docs, inventory_schema)
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(20)
    print("Inventory Validation Profiling Results:")
    print(s.getvalue())
    
    # Profile transaction validation
    pr = cProfile.Profile()
    pr.enable()
    profile_transaction_validation(transaction_docs, transaction_schema)
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(20)
    print("\nTransaction Validation Profiling Results:")
    print(s.getvalue())
    
    # Profile batch validation
    pr = cProfile.Profile()
    pr.enable()
    profile_batch_validation(inventory_docs[:500], transaction_docs[:500], 
                            inventory_schema, transaction_schema)
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(20)
    print("\nBatch Validation Profiling Results:")
    print(s.getvalue())
    
    # Profile schema lookup
    pr = cProfile.Profile()
    pr.enable()
    profile_schema_lookup()
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(20)
    print("\nSchema Lookup Profiling Results:")
    print(s.getvalue())
    
    # Print simple timing stats
    print("\nSimple Timing Statistics:")
    
    start_time = time.time()
    profile_inventory_validation(inventory_docs, inventory_schema)
    elapsed = time.time() - start_time
    print(f"Inventory validation ({num_docs} docs): {elapsed:.4f}s, {elapsed/num_docs*1000:.4f}ms per doc")
    
    start_time = time.time()
    profile_transaction_validation(transaction_docs, transaction_schema)
    elapsed = time.time() - start_time
    print(f"Transaction validation ({num_docs} docs): {elapsed:.4f}s, {elapsed/num_docs*1000:.4f}ms per doc")
    
    start_time = time.time()
    profile_schema_lookup()
    elapsed = time.time() - start_time
    print(f"Schema lookup (1000 lookups): {elapsed:.4f}s, {elapsed/1000*1000:.4f}ms per lookup")


if __name__ == "__main__":
    print("Running schema validation profiling...")
    run_profiling(num_docs=1000)
    print("\nProfiling complete.") 