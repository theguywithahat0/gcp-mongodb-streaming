# Tests

This directory contains tests for the GCP MongoDB Streaming project.

## Directory Structure

```
tests/
├── unit/                     # Unit tests
│   ├── mongodb/              # MongoDB component tests
│   │   ├── test_connection_manager.py          # Connection manager tests
│   │   ├── test_connection_manager_error_handling.py  # Error handling tests
│   │   └── validator/        # Validator tests
│   │       ├── test_validator.py                # Basic validation tests
│   │       ├── test_validator_edge_cases.py     # Edge case tests
│   │       └── test_validator_watch.py          # Change stream tests
│   └── config/               # Configuration tests
│       └── test_config.py    # Configuration parsing tests
├── integration/              # Integration tests
│   └── mongodb/              # MongoDB integration tests
│       ├── change_streams/   # Change stream integration tests
│       │   ├── run_test.py          # Change stream test runner
│   │   ├── data_generator.py    # Test data generator
│   │   └── stream_monitor.py    # Stream monitoring utility
│   └── validator/         # Validator integration tests
│       ├── run_test.py           # Validator test runner
│       ├── data_generator.py     # Test data generator
│       └── validator_monitor.py  # Validation monitoring utility
└── conftest.py              # Test fixtures and configuration
```

## Test Coverage

The test suite maintains 90% code coverage across all components. Key areas covered:

- Overall test coverage: **90%**
- `connection_manager.py`: **87%** coverage (208 statements, 28 missed)
- `validator.py`: **98%** coverage (102 statements, 2 missed)

### MongoDB Connection Manager Tests
Located in `unit/mongodb/test_connection_manager.py` and `test_connection_manager_error_handling.py`:
- Connection initialization and cleanup
- Change stream processing
- Error handling and retry logic
- Stream recovery with exponential backoff
- State transitions and monitoring
- Resource cleanup and task management
- Resume token handling
- Cursor timeout recovery
- Parallel stream recovery

### Validator Tests
Located in `unit/mongodb/validator/`:
- Schema validation
- Document validation
- Field formatting
- PubSub message preparation
- Edge case handling
- Error recovery
- Custom validation functions
- Change stream watching

### Error Handling Tests
The `test_connection_manager_error_handling.py` file contains specialized tests for:
- Connection failures with exponential backoff
- Stream initialization errors
- Maximum retry limit enforcement
- Error count tracking
- Status monitoring
- Resource cleanup
- Task cancellation
- Cursor error detection and remediation
- Network timeout recovery

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
# Run all unit tests with coverage report
python -m pytest tests/unit/ --cov=src.pipeline.mongodb --cov-report=term-missing:skip-covered

# Run specific test modules
python -m pytest tests/unit/mongodb/test_connection_manager.py
python -m pytest tests/unit/mongodb/validator/

# Run specific test cases
python -m pytest tests/unit/mongodb/test_connection_manager.py::test_resume_token_handling
```

### Integration Tests

Change Streams:
```bash
# Run the change streams integration test
cd tests/integration/mongodb/change_streams
python -m run_test
```

Validator:
```bash
# Run the validator integration test
cd tests/integration/mongodb/validator
python -m run_test
```

## Test Configuration

- `TEST_WRITE_INTERVAL`: Time between document writes (default: 2s)
- `TEST_DURATION`: Test duration
  - Change streams tests: 60s
  - Validator tests: 30s

Test behavior can be configured through `test_config` fixtures:
- `max_retries`: Number of retry attempts (default: 3)
- `connection_timeout`: Connection timeout in ms (default: 1000)
- `backoff_factor`: Retry backoff multiplier (default: 1)
- `max_backoff`: Maximum backoff time in seconds (default: 5)
- `max_changes_per_run`: Limit changes processed in tests (default: 10)
- `max_inactivity_seconds`: Maximum allowed inactivity before restart (default: 30)

### Mock Components

The test suite includes mock implementations for reliable testing:
- `MockChangeStream`: Simulates MongoDB change streams
  - Configurable event generation
  - Error injection capabilities
  - Controlled iteration behavior
  - Automatic cleanup 
- `CustomDict`: Dictionary with configurable errors for testing error handling
- `ErrorSequenceStream`: Stream that raises specific error sequences for testing

## Integration Test Reports

Both integration tests generate detailed reports in the `tests/integration/mongodb/reports/` directory, containing:
- Document processing statistics
- Valid/invalid document counts
- Processing times
- Error logs
- Stream events 