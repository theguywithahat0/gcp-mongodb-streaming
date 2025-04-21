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

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

The connector can be configured through environment variables or a YAML configuration file. See `config/config.yaml.example` for available options.

Key configuration options:
- MongoDB connection settings
- Collection watch filters
- Pub/Sub topic configurations
- Resume token storage settings
- Logging configuration

### Schema Versioning

The connector includes a robust schema versioning system that ensures data consistency and backward compatibility:

#### Schema Registry
- Centralized management of document schemas
- Support for multiple versions per document type
- Automatic version detection and validation
- Easy addition of new schema versions

Example schema version:
```python
INVENTORY_SCHEMAS = {
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
            "last_updated": {"type": "string", "format": "date-time"}
        }
    }
}
```

#### Schema Migration
- Automatic migration of documents to latest schema version
- Support for custom migration functions
- Path finding for multi-step migrations
- Error handling and logging during migration

Example migration registration:
```python
def migrate_inventory_v1_to_v2(doc):
    return {
        **doc,
        "_schema_version": "v2",
        "new_field": "default_value"
    }

SchemaMigrator.register_migration(
    DocumentType.INVENTORY,
    "v1",
    "v2",
    migrate_inventory_v1_to_v2
)
```

#### Version Tracking
- Schema version included in Pub/Sub messages
- Logging of schema versions and migrations
- Validation against specific schema versions
- Automatic handling of unversioned documents

### Structured Logging

The connector uses structured JSON logging for better operational visibility and monitoring:

```json
{
    "timestamp": "2024-03-20T10:00:00Z",
    "level": "INFO",
    "event": "change_stream_event",
    "collection": "inventory",
    "operation": "insert",
    "warehouse_id": "WH-EAST-1",
    "document_id": "123456",
    "correlation_id": "abc-xyz-789",
    "schema_version": "v1"
}
```

Standard log fields:
- `timestamp`: ISO format timestamp
- `level`: Log severity (DEBUG, INFO, WARNING, ERROR)
- `event`: Event type identifier
- `correlation_id`: Unique identifier for tracking related events
- `schema_version`: Document schema version

Event-specific fields are included based on the operation type and context.

## Schema Validation

The connector includes built-in schema validation for inventory and transaction documents:

### Inventory Documents
Required fields:
- `product_id` (string): Unique identifier for the product
- `warehouse_id` (string): Identifier for the warehouse
- `quantity` (integer): Current stock quantity (minimum: 0)
- `last_updated` (string): Timestamp in ISO format (e.g., "2024-03-20T10:00:00Z")

Optional fields:
- `category` (string): Product category
- `brand` (string): Product brand
- `sku` (string): Stock keeping unit
- `threshold_min` (integer): Minimum stock threshold
- `threshold_max` (integer): Maximum stock threshold

### Transaction Documents
Required fields:
- `transaction_id` (string): Unique identifier for the transaction
- `product_id` (string): Product identifier
- `warehouse_id` (string): Warehouse identifier
- `quantity` (integer): Transaction quantity
- `transaction_type` (string): One of: "sale", "restock", "return", "adjustment"
- `timestamp` (string): Transaction timestamp in ISO format

Optional fields:
- `order_id` (string): Associated order identifier
- `customer_id` (string): Customer identifier
- `notes` (string): Additional transaction notes

Invalid documents are logged and not published to Pub/Sub. This ensures data quality and consistency throughout the pipeline.

## Running the Connector

Development:
```bash
python -m connector.main
```

Production (using Cloud Run):
```bash
# Build container
docker build -t gcr.io/[PROJECT_ID]/mongo-connector .

# Push to Container Registry
docker push gcr.io/[PROJECT_ID]/mongo-connector

# Deploy to Cloud Run
gcloud run deploy mongo-connector \
  --image gcr.io/[PROJECT_ID]/mongo-connector \
  --platform managed
```

## Monitoring

The connector exposes the following endpoints:
- `/health`: Basic health check
- `/readiness`: Readiness probe
- `/metrics`: Prometheus metrics

## Architecture

The connector follows a modular architecture:
- `core/`: Core connector logic and schema validation
- `config/`: Configuration management
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
