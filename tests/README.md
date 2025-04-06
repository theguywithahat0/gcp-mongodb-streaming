# Test Suite

This directory contains the test suite for the MongoDB to GCP Streaming Pipeline project.

## Directory Structure

```
tests/
├── integration/           # Integration tests
│   ├── test_stream_monitor.py    # Change stream monitoring tests
│   └── test_data_generator.py    # Test data generation
├── test_mongodb/         # MongoDB unit tests
├── test_pubsub/         # Pub/Sub unit tests
└── test_pipeline/       # Pipeline unit tests
```

## Environment Configuration

Test configuration is kept separate from production configuration to avoid mixing test and production settings.

### Test Environment Setup

1. Copy the test environment example file:
   ```bash
   cp .env.test.example .env.test
   ```

2. Edit `.env.test` with your test MongoDB credentials and settings:
   ```
   MONGODB_TEST_URI=your_test_mongodb_uri
   MONGODB_TEST_DB=your_test_db
   MONGODB_TEST_COLLECTION=your_test_collection

   # Test Runtime Configuration
   TEST_WRITE_INTERVAL=2  # seconds between writes
   TEST_DURATION=60      # total test duration in seconds
   ```

   **Important**: Never use production credentials in test environment files.

## Integration Tests

The integration tests in `tests/integration/` validate the MongoDB change stream functionality:

### Change Stream Testing

Two scripts work together to test change stream functionality:

1. **Data Generator** (`test_data_generator.py`):
   - Generates random order data
   - Writes to the test MongoDB collection at regular intervals
   - Creates proper indexes for efficient streaming
   - Handles duplicate order IDs
   - Sample data includes:
     - Order ID
     - Product details
     - Status
     - Timestamps
     - Customer information

2. **Stream Monitor** (`test_stream_monitor.py`):
   - Monitors the test collection for changes
   - Uses MongoDB change streams
   - Displays real-time updates
   - Tracks operation types (insert, update, etc.)
   - Shows full document content for each change

### Running Integration Tests

1. Ensure your test environment is configured:
   ```bash
   cp .env.test.example .env.test
   # Edit .env.test with your MongoDB details
   ```

2. Run the monitor and generator in separate terminals:
   ```bash
   # Terminal 1: Start the change stream monitor
   python tests/integration/test_stream_monitor.py

   # Terminal 2: Start generating test data
   python tests/integration/test_data_generator.py
   ```

3. Monitor Output:
   - Watch real-time changes as they occur
   - See full document contents
   - View operation types (insert, update, etc.)
   - Check error handling and reconnection

4. Test Control:
   - Tests run for 60 seconds by default (configurable via TEST_DURATION)
   - Data is generated every 2 seconds (configurable via TEST_WRITE_INTERVAL)
   - Can be stopped at any time with Ctrl+C
   - Proper cleanup on exit

### Test Data Schema

The generator creates order documents with the following structure:
```json
{
    "order_id": "ORD-XXXXX",
    "product": "product_name",
    "quantity": 1-5,
    "status": ["pending", "processing", "shipped", "delivered"],
    "price": 100.00-1000.00,
    "created_at": "timestamp",
    "customer_id": "CUST-XXXX"
}
```

### Unit Tests

Unit tests use pytest and can be run with:
```bash
pytest tests/test_mongodb/ tests/test_pubsub/ tests/test_pipeline/
``` 