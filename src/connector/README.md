# MongoDB Change Stream Connector

This connector streams real-time changes from MongoDB collections to Google Cloud Pub/Sub. It's designed to be reliable, scalable, and production-ready with features like resume token management, error handling, and monitoring.

## Features

- Real-time MongoDB change stream monitoring
- Reliable message delivery to Pub/Sub
- Document schema validation and versioning
- Resume token management for fault tolerance
- Structured logging for operational visibility
- Comprehensive error handling
- Configurable connection retry with backoff
- Health check endpoints
- Message batching for improved throughput
- Circuit breaker pattern for external dependencies
- Message deduplication to prevent duplicate processing
- Log sampling for high-volume environments
- Document transformation hooks for custom processing
- Graceful shutdown handling
- Security transformations for sensitive data handling

## Prerequisites

- Python 3.9+
- MongoDB 4.0+ with replica set enabled
- Google Cloud project with Pub/Sub and Firestore enabled
- Service account with necessary permissions

## Setup

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your settings in `config.yaml`:
```bash
cp config/config.yaml.example config/config.yaml
# Edit config.yaml with your settings
```

## Configuration

The connector is configured through a YAML configuration file. See `config/config.yaml.example` for available options.

Key configuration options:
- MongoDB connection settings
- Collection watch filters
- Pub/Sub topic configurations
- Resume token storage settings
- Logging configuration
- Message batching settings
- Circuit breaker configuration
- Message deduplication options
- Log sampling rules

### Message Batching

The connector supports batching messages to improve throughput and reduce Pub/Sub costs:

```yaml
pubsub:
  publisher:
    batch_settings:
      max_messages: 100
      max_bytes: 1048576  # 1MB
      max_latency: 0.1    # seconds
```

### Circuit Breaker

The connector uses the circuit breaker pattern to handle external service failures gracefully:

```yaml
pubsub:
  publisher:
    circuit_breaker:
      failure_threshold: 5
      reset_timeout: 60.0
      half_open_max_calls: 3
```

### Message Deduplication

To prevent duplicate message processing, the connector offers two-tier deduplication:

```yaml
mongodb:
  collections:
    - name: inventory
      topic: inventory-updates
      deduplication:
        enabled: true
        memory_ttl: 3600        # 1 hour
        memory_max_size: 10000
        persistent_enabled: true
        persistent_ttl: 86400   # 24 hours
        cleanup_interval: 3600  # 1 hour
```

### Log Sampling

For high-volume environments, log sampling reduces logging overhead:

```yaml
monitoring:
  logging:
    sampling:
      enabled: true
      default_rate: 1.0      # Sample all by default
      rules:
        DEBUG:
          "change_event_processed":
            rate: 0.1        # Sample 10% of events
            strategy: "probabilistic"
```

### Schema Versioning

The connector includes a robust schema versioning system that ensures data consistency and backward compatibility:

#### Schema Registry
- Centralized management of document schemas
- Support for multiple versions per document type
- Automatic version detection and validation
- Easy addition of new schema versions

```python
# Example schema types in schema_registry.py
class DocumentType(str, Enum):
    """Document types for schema validation."""
    INVENTORY = "inventory"
    TRANSACTION = "transaction"

# Schema registry with validation schemas
class SchemaRegistry:
    """Registry for document schemas with version support."""
    
    # Initialize registry with schemas for each document type
    def __init__(self):
        """Initialize schema registry."""
        self.schemas = {
            DocumentType.INVENTORY: {
                "v1": {
                    "type": "object",
                    "required": ["_id", "product_id", "quantity", "last_updated"],
                    "properties": {
                        "_id": {"type": "string"},
                        "product_id": {"type": "string"},
                        "quantity": {"type": "integer", "minimum": 0},
                        "last_updated": {"type": "string", "format": "date-time"}
                    }
                }
            },
            DocumentType.TRANSACTION: {
                "v1": {
                    "type": "object",
                    "required": ["_id", "product_id", "quantity_change", "timestamp"],
                    "properties": {
                        "_id": {"type": "string"},
                        "product_id": {"type": "string"},
                        "quantity_change": {"type": "integer"},
                        "timestamp": {"type": "string", "format": "date-time"}
                    }
                }
            }
        }
```

#### Schema Migration
- Automatic migration of documents to latest schema version
- Support for custom migration functions
- Path finding for multi-step migrations
- Error handling and logging during migration

```python
# Example migration registration
def migrate_inventory_v1_to_v2(doc):
    """Migrate inventory document from v1 to v2."""
    migrated = doc.copy()
    migrated["_schema_version"] = "v2"
    # Add new fields with default values
    migrated["reorder_point"] = 0
    return migrated

# Register the migration
schema_migrator.register_migration(
    DocumentType.INVENTORY,
    "v1",
    "v2",
    migrate_inventory_v1_to_v2
)
```

### Document Transformation

The connector features a powerful document transformation system through the `DocumentTransformer` class, supporting multiple stages:

```python
# Available transformation stages
class TransformStage(Enum):
    PRE_VALIDATION = "pre_validation"  # Before document validation
    POST_VALIDATION = "post_validation"  # After document validation
    PRE_PUBLISH = "pre_publish"  # Before publishing to Pub/Sub

# Register a transformation
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.INVENTORY,
    transform_function
)
```

#### Built-in Security Transformations

The connector includes several built-in transformations for handling sensitive data:

1. **Field Removal**: Completely removes sensitive fields
```python
# Remove sensitive fields like PII or payment information 
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.TRANSACTION,
    remove_sensitive_fields([
        "customer_email",
        "customer_phone",
        "payment.card_number",
        "payment.cvv"
    ])
)
```

2. **Field Prefixing**: Adds prefixes for data isolation
```python
# Add warehouse prefix for data isolation
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.INVENTORY,
    add_field_prefix(f"wh_{config.mongodb.warehouse_id}_")
)
```

3. **Metadata Addition**: Adds processing metadata
```python
# Add processing metadata to documents
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.INVENTORY,
    add_processing_metadata
)
```

4. **Transformation Composition**: Combine multiple transformations
```python
# Compose multiple transformations
from functools import partial

# Create partial functions with specific configurations
remove_pii = remove_sensitive_fields([
    "customer_email", 
    "customer_phone"
])
add_warehouse_prefix = add_field_prefix(f"wh_{warehouse_id}_")

# Compose transformations
def compose_transforms(doc, *transforms):
    """Apply multiple transformations in sequence."""
    result = doc.copy()
    for transform in transforms:
        result = transform(result)
    return result

# Register the composed transformation
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.TRANSACTION,
    partial(compose_transforms, 
            remove_pii, 
            add_warehouse_prefix, 
            add_processing_metadata)
)
```

## Health Checks

The connector provides two health check endpoints:

- `/health`: Basic health check that returns 200 if the connector is running
- `/readiness`: Detailed health check that verifies connectivity to MongoDB, Pub/Sub, and Firestore

## Resource Monitoring

The connector includes comprehensive resource monitoring to prevent memory issues and optimize performance:

```python
# Resource monitor callbacks for different memory thresholds
resource_monitor.register_callback(
    'memory_warning',   # 65% memory usage
    self._handle_memory_warning
)

resource_monitor.register_callback(
    'memory_critical',  # 80% memory usage
    self._handle_memory_critical
)

resource_monitor.register_callback(
    'memory_emergency', # 90% memory usage
    self._handle_memory_emergency
)
```

The resource monitor provides automated responses to memory pressure:
- Warning level: Log detailed memory usage
- Critical level: Trigger garbage collection and reduce buffers
- Emergency level: Recreate change streams and reduce message batch sizes

## Running Tests

### Unit Tests

```bash
pytest src/connector/tests/unit_tests
```

### End-to-End Tests

See the detailed instructions in `src/connector/tests/e2e_tests/README.md` to set up the test environment and run end-to-end tests.

## Running the Connector

```bash
# Run with default config
python -m src.connector

# Run with specific config
python -m src.connector --config /path/to/config.yaml
```

## Monitoring

The connector exposes the following endpoints:
- `/health`: Basic health check
- `/readiness`: Readiness probe with component status
- `/metrics`: Prometheus metrics

Each endpoint provides different levels of health information:
- Health: Simple up/down status
- Readiness: Detailed status of MongoDB, Pub/Sub, and Firestore connections
- Metrics: Operational metrics for monitoring systems

## Fault Tolerance

The connector implements multiple fault tolerance mechanisms:

1. **Resume Token Management**: Stores processing position in Firestore
2. **Connection Retry**: Exponential backoff for reconnection attempts
3. **Circuit Breaker**: Prevents cascading failures during outages
4. **Message Deduplication**: Ensures exactly-once processing
5. **Graceful Shutdown**: Handles termination signals properly

## Security Features

The connector implements several security features:

1. **Sensitive Data Handling**: 
   - Automatic removal of sensitive fields like customer email, phone, payment information
   - Support for field masking and data anonymization

2. **Data Isolation**:
   - Warehouse prefixing for multi-tenant isolation
   - Environment-specific configuration

3. **Access Control**:
   - Service account-based authentication
   - Least privilege principle for cloud resources

For more details, see the `SECURITY.md` document in the project root.

## Architecture

The connector follows a modular architecture:
- `core/`: Core connector logic and schema validation
- `config/`: Configuration management
- `logging/`: Structured logging and sampling
- `health/`: Health check endpoints
- `utils/`: Utility functions
- `tests/`: Test suite

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT
