# Validator Integration Tests

This directory contains integration tests for the MongoDB document validator functionality.

## Files

- `test_validator_integration.py`: Tests the document validation and transformation pipeline
- `test_validator_data_generator.py`: Generates both valid and invalid test documents

## Usage

1. Start the validator data generator:
```bash
./test_validator_data_generator.py
```

2. In another terminal, run the validator test:
```bash
./test_validator_integration.py
```

## Test Cases

The data generator creates various test cases:
- Valid documents following all schema rules
- Invalid documents with:
  - Invalid product names
  - Invalid quantities (exceeding limits)
  - Invalid status values
  - Negative prices
  - Wrong order_id format
  - Wrong customer_id format

## Configuration

Tests use environment variables from `tests/.env.test`:
- `MONGODB_TEST_URI`: MongoDB connection string
- `MONGODB_TEST_DB`: Test database name (default: source_db_test)
- `MONGODB_TEST_COLLECTION`: Test collection name (default: source_collection_test)
- `TEST_WRITE_INTERVAL`: Interval between writes in seconds (default: 2)
- `TEST_DURATION`: Test duration in seconds (default: 30) 