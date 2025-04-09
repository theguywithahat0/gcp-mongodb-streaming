# Utility Classes and Scripts

This directory contains utility classes and scripts for working with the stock monitoring pipeline.

## PubSub Utilities

### pubsub_client.py

A client for interacting with Google Cloud Pub/Sub during tests. It provides methods for:
- Creating topics and subscriptions
- Publishing messages
- Subscribing to topics
- Deleting topics and subscriptions

### pubsub_monitor.py

A monitor that listens for messages on Pub/Sub subscriptions. It's used in tests to:
- Monitor multiple subscriptions simultaneously
- Capture and analyze messages
- Track metrics about message delivery
- Generate reports on messages received

## Data Generation

### data_generator.py

A generator for creating test data for MongoDB. It creates:
- Random stock trade data
- Data with configurable parameters (quantity, price range, etc.)
- Data for specific trading sites

### stock_trade_generator.py

A more specialized generator for stock trades, with features for:
- Generating realistic stock trade patterns
- Creating time-sequenced data
- Simulating various trading scenarios

## Scripts

### publish_test_messages.py

A utility script to publish test messages to Pub/Sub topics. Use it to:
```bash
python publish_test_messages.py --project YOUR_PROJECT_ID --messages 10
```

### cloud_pubsub_setup.sh

Shell script to set up all necessary Pub/Sub topics and subscriptions:
```bash
./cloud_pubsub_setup.sh YOUR_PROJECT_ID
```

### run_online_test.sh

Script to run tests with live Google Cloud resources:
```bash
./run_online_test.sh YOUR_PROJECT_ID
```

### run_tests.sh

Script to run all tests in sequence:
```bash
./run_tests.sh
``` 