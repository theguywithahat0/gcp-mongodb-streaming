# Change Streams Integration Tests

This directory contains integration tests for MongoDB change streams functionality.

## Files

- `test_stream_monitor.py`: Tests the basic change stream monitoring functionality
- `test_data_generator.py`: Generates sample order data for testing change streams

## Usage

1. Start the data generator:
```bash
./test_data_generator.py
```

2. In another terminal, run the stream monitor:
```bash
./test_stream_monitor.py
```

## Configuration

Tests use environment variables from `tests/.env.test`:
- `MONGODB_TEST_URI`: MongoDB connection string
- `MONGODB_TEST_DB`: Test database name (default: source_db_test)
- `MONGODB_TEST_COLLECTION`: Test collection name (default: source_collection_test)
- `TEST_WRITE_INTERVAL`: Interval between writes in seconds (default: 2)
- `TEST_DURATION`: Test duration in seconds (default: 60) 