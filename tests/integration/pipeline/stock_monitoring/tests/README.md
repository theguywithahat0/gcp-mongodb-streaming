# Test Scripts

This directory contains test scripts for the stock monitoring pipeline.

## Available Tests

### test_mock.py

A simplified test that focuses only on Pub/Sub without requiring MongoDB. It:
- Sets up Pub/Sub topics and subscriptions
- Monitors Pub/Sub for messages
- Verifies message delivery
- Generates test reports

```bash
python test_mock.py --duration 30
```

### test_pubsub.py

A specialized test for Pub/Sub integration that:
- Tests publishing and subscribing
- Verifies message formats
- Tests error handling
- Checks subscription management

```bash
python test_pubsub.py
```

### test_stock_monitoring.py

The full integration test for the stock monitoring pipeline that:
- Sets up MongoDB with test data
- Configures Pub/Sub topics and subscriptions
- Runs the pipeline
- Monitors for expected outputs
- Validates position calculations and alerts

```bash
python test_stock_monitoring.py --duration 60
```

### run_integration_test.py

A script to run the integration test with various configurations:
- Can test with multiple MongoDB instances
- Supports different tenant configurations
- Provides detailed logs and reports

```bash
python run_integration_test.py
```

## Running the Tests

To run all tests in sequence:

```bash
python run_integration_test.py
```

To run a specific test:

```bash
python test_mock.py  # For Pub/Sub only test
python test_stock_monitoring.py  # For full integration test
```

## Test Reports

Test reports are generated in the `../reports/` directory and include:
- Message counts and statistics
- Alert information
- Position calculations
- Performance metrics 