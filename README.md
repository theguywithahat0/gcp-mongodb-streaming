# MongoDB to GCP Streaming Pipeline

This project implements a streaming pipeline that reads data from MongoDB and streams it to Google Cloud Platform (GCP) services using Apache Beam and Dataflow.

## Architecture

```
MongoDB Change Streams -> Apache Beam/Dataflow -> Cloud Pub/Sub
```

The pipeline monitors MongoDB change streams for real-time data changes and publishes them to Cloud Pub/Sub topics, enabling real-time data processing and analytics.

## Project Structure

```
gcp-mongodb-streaming/
├── config/
│   └── config.yaml           # Main configuration file
├── src/
│   ├── pipeline/
│   │   ├── beam/          # Apache Beam components
│   │   │   ├── pipeline.py  # Main pipeline definition
│   │   │   └── sources/   # Beam source components
│   │   │       └── mongodb.py  # MongoDB source
│   │   ├── mongodb/       # MongoDB connectivity
│   │   │   ├── connection_manager.py  # MongoDB connection handling
│   │   │   └── validator.py # Document validation and enrichment
│   │   ├── pubsub/        # Pub/Sub integration
│   │   │   └── sink.py     # Publishing logic
│   │   └── utils/         # Shared utilities
│   │       ├── config.py   # Configuration management
│   │       └── logging.py  # Logging setup
│   └── scripts/
│       ├── setup_pubsub.py  # GCP resource setup
│       └── run_pipeline.py  # Pipeline entry point
├── tests/                   # Test suite
│   ├── test_mongodb/       # MongoDB tests
│   ├── test_pubsub/        # Pub/Sub tests
│   └── test_pipeline/      # Pipeline tests
├── cheat_sheets/           # Documentation and examples
│   └── connection_manager_explained.py  # Connection manager guide
└── [Configuration Files]
```

## Prerequisites

- Python 3.8+
- MongoDB Atlas account (free tier)
- GCP account with enabled services:
  - Cloud Pub/Sub
  - Dataflow
  - Cloud Storage

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in your configuration:
   ```bash
   cp .env.example .env
   ```
5. Edit the `.env` file with your MongoDB and GCP credentials

## Configuration

The project uses two main configuration mechanisms:
1. **Environment Variables**
   - Production Environment (`.env`)
     - Copy `.env.example` to `.env` for production settings
     - Contains sensitive information like API keys and credentials
     - Connection strings for production MongoDB and GCP services
   
   - Test Environment (`tests/.env.test`)
     - Copy `tests/.env.test.example` to `tests/.env.test` for test settings
     - Contains test-specific configuration
     - Uses separate MongoDB instance for testing
     - Never mix with production credentials

2. **Application Config** (config/config.yaml)
   - Pipeline settings
   - Batch sizes and windows
   - Topic/subscription names
   - Non-sensitive configuration
   - Supports dot notation for nested access (e.g., `config.get("mongodb.connections.main.uri")`)
   - Schema validation for required fields
   - Default value support

### Configuration Example
```yaml
mongodb:
  connections:
    main:
      uri: ${MONGODB_URI}
      database: mydb
      sources:
        orders:
          collection: orders
          batch_size: 100
```

Access via:
```python
config.get("mongodb.connections.main.uri")  # Returns URI
config.get("mongodb.connections.main.sources.orders.batch_size", default=50)  # Returns 100
```

## MongoDB Connection Management

The project uses an asynchronous MongoDB connection manager (`AsyncMongoDBConnectionManager`) that:
- Handles multiple MongoDB connections concurrently using `motor_asyncio`
- Processes change streams in parallel using asyncio tasks
- Provides automatic retry with exponential backoff
- Monitors connection and stream health with detailed statistics
- Implements graceful error handling and recovery
- Supports batch processing with configurable batch sizes
- Maintains connection status tracking per stream

Example configuration:
```yaml
mongodb:
  connections:
    client1:
      uri: ${MONGODB_URI}
      sources:
        orders:
          database: mydb
          collection: orders
          batch_size: 100
          pipeline: []  # Optional aggregation pipeline
```

For detailed usage examples and documentation, see `cheat_sheets/connection_manager_explained.py`.

## Development

### Running the Pipeline

1. Set up GCP resources:
   ```bash
   python src/scripts/setup_pubsub.py
   ```

2. Run the pipeline:
   ```bash
   python src/scripts/run_pipeline.py
   ```

### Running Tests

```bash
pytest tests/
```

### Testing and Error Handling

The project includes comprehensive test coverage for all components:

### Unit Tests
- Connection initialization and cleanup
- Change stream processing
- Error handling and retry logic
- Stream recovery and parallel processing
- State transitions and monitoring
- Resource cleanup and task management

### Integration Tests
The project includes live integration tests to validate MongoDB change streams:
- Real-time change stream monitoring
- Test data generation and streaming
- Connection handling and error recovery
- See [tests/README.md](tests/README.md) for detailed documentation

Key test scenarios covered:
- Exponential backoff during connection failures
- Parallel stream recovery after disconnections
- Maximum retry limit handling
- Resource cleanup during shutdown
- Task cancellation and state management
- Connection pool management

For detailed test documentation and examples, see the [Test Suite Documentation](tests/README.md).

### Documentation

The project includes detailed cheat sheets and examples in the `cheat_sheets/` directory:
- Connection Manager API documentation and examples
- Configuration usage patterns
- Best practices for stream handling

## Cost Management

This project is designed to work within free tiers where possible:
- MongoDB Atlas Free Tier
- GCP Free Tier
- Minimal Dataflow usage

For cost optimization:
- Uses small instance types
- Implements batching
- Configurable scaling

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 