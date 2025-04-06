# Tests

This directory contains tests for the GCP MongoDB Streaming project.

## Directory Structure

```
tests/
├── integration/              # Integration tests
│   ├── change_streams/      # Change stream monitoring tests
│   │   ├── test_stream_monitor.py
│   │   └── test_data_generator.py
│   └── validator/           # Document validation tests
│       ├── test_validator_integration.py
│       └── test_validator_data_generator.py
├── test_pipeline/           # Unit tests for pipeline components
└── test_mongodb/           # Unit tests for MongoDB components
```

## Test Coverage

The test suite maintains 90% code coverage across all components. Key areas covered:

### MongoDB Connection Manager Tests
Located in `test_mongodb/test_connection_manager.py` and `test_connection_manager_error_handling.py`:
- Basic connection management and stream processing
- Error handling and recovery scenarios
- Mock change streams for reliable testing
- Connection failure simulation
- Stream initialization errors
- Batch processing with configurable sizes
- Task management and cleanup

### Error Handling Tests
The `test_connection_manager_error_handling.py` file contains specialized tests for:
- Connection failures with exponential backoff
- Stream initialization errors
- Maximum retry limit enforcement
- Error count tracking
- Status monitoring
- Resource cleanup
- Task cancellation

## Test Environment

Tests use environment variables defined in `.env.test`. Copy `.env.test.example` to `.env.test` and configure:

```bash
cp .env.test.example .env.test
```

Required variables:
- `MONGODB_TEST_URI`: MongoDB connection string
- `MONGODB_TEST_DB`: Test database name
- `MONGODB_TEST_COLLECTION`: Test collection name

## Running Tests

### Unit Tests
```bash
# Run all tests with coverage report
python -m pytest --cov=src --cov-report=term-missing tests/

# Run specific test files
python -m pytest tests/test_mongodb/test_connection_manager.py
python -m pytest tests/test_mongodb/test_connection_manager_error_handling.py
```

### Integration Tests

Change Streams:
```bash
# Terminal 1: Start data generator
./tests/integration/change_streams/test_data_generator.py

# Terminal 2: Run stream monitor
./tests/integration/change_streams/test_stream_monitor.py
```

Validator:
```bash
# Terminal 1: Start validator data generator
./tests/integration/validator/test_validator_data_generator.py

# Terminal 2: Run validator test
./tests/integration/validator/test_validator_integration.py
```

## Test Configuration

- `TEST_WRITE_INTERVAL`: Time between document writes (default: 2s)
- `TEST_DURATION`: Test duration
  - Change streams tests: 60s
  - Validator tests: 30s

Test behavior can be configured through `test_config` fixtures:
- `max_retries`: Number of retry attempts (default: 2)
- `connection_timeout`: Connection timeout in ms (default: 100)
- `backoff_factor`: Retry backoff multiplier (default: 1)
- `max_backoff`: Maximum backoff time in seconds (default: 1)
- `max_changes_per_run`: Limit changes processed in tests (default: 5)

### Mock Components

The test suite includes mock implementations for reliable testing:
- `MockChangeStream`: Simulates MongoDB change streams
  - Configurable event generation
  - Error injection capabilities
  - Controlled iteration behavior
  - Automatic cleanup 