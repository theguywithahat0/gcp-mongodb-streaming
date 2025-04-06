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
python -m pytest tests/test_pipeline tests/test_mongodb
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