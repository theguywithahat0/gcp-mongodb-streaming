# Stock Monitoring Test Suite

This directory contains the integration tests and utilities for the stock position monitoring pipeline. The code has been organized into the following directories:

## Directory Structure

- **`core/`**: Core functionality and scripts for end users
  - `pull_messages.py`: Utility to view messages from Pub/Sub subscriptions

- **`utils/`**: Utility classes and helper scripts
  - `pubsub_client.py`: Client for interacting with Google Cloud Pub/Sub
  - `pubsub_monitor.py`: Monitor for Pub/Sub messages
  - `data_generator.py`: Generator for test data
  - `stock_trade_generator.py`: Generator for stock trade test data
  - `publish_test_messages.py`: Utility to publish test messages to Pub/Sub
  - `cloud_pubsub_setup.sh`: Script to set up Pub/Sub topics and subscriptions
  - `run_online_test.sh`: Script to run tests with live Google Cloud resources
  - `run_tests.sh`: Script to run all tests

- **`tests/`**: Test implementations
  - `test_mock.py`: Test that focuses only on Pub/Sub without MongoDB
  - `test_pubsub.py`: Test for Pub/Sub integration
  - `test_stock_monitoring.py`: Full integration test for stock monitoring
  - `run_integration_test.py`: Script to run the integration test

## Quick Start

To get started with testing the stock monitoring pipeline:

1. **Setup GCP resources**:
   ```bash
   ./utils/cloud_pubsub_setup.sh YOUR_PROJECT_ID
   ```

2. **View messages in Pub/Sub**:
   ```bash
   python core/pull_messages.py --project-id YOUR_PROJECT_ID
   ```

3. **Publish test messages**:
   ```bash
   python utils/publish_test_messages.py --project YOUR_PROJECT_ID
   ```

4. **Run mock test** (Pub/Sub only):
   ```bash
   python tests/test_mock.py
   ```

For more detailed information, refer to the README files in each directory.

# Stock Position Monitoring Integration Test

This directory contains integration tests for the stock position monitoring pipeline. The tests verify end-to-end functionality by:

1. Generating and loading test stock trading data into MongoDB
2. Running the pipeline to process the data
3. Monitoring Pub/Sub for expected outputs

## Configuration

The test is configured using the `.env.test` file in the `tests` directory. The configuration is organized by tenant (trading site) for better clarity.

### Key Configuration Areas:

- **Common Settings**: GCP project, main Pub/Sub topic/subscription
- **Tenant-specific Settings**: MongoDB URIs and Pub/Sub topics for each trading site
- **Test Parameters**: Duration, window size, number of documents, etc.
- **Stock Configurations**: Symbols, quantities, and threshold values

## Prerequisites

- Python 3.8+
- MongoDB running locally or accessible URI
- Access to Google Cloud Pub/Sub (or running the emulator locally)
- Required Python packages installed (see `requirements.txt` in the project root)

## Running the Integration Test

### 1. Set up the environment

```bash
# Make sure you're in the project root directory
cd gcp-mongodb-streaming

# Create a Python virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### 2. Configure the test

Edit the `.env.test` file in the `tests` directory to match your environment:

```bash
# Copy the example file if needed
cp tests/.env.test.example tests/.env.test

# Edit the file with your specific configuration
nano tests/.env.test
```

### 3. Run the test

```bash
# From the project root
python -m tests.integration.pipeline.stock_monitoring.test_stock_monitoring

# Specify a custom duration (in seconds)
python -m tests.integration.pipeline.stock_monitoring.test_stock_monitoring --duration 300
```

### 4. Using the Pub/Sub Emulator (Optional)

For local testing without a GCP project, you can use the Pub/Sub emulator:

```bash
# Start the emulator
gcloud beta emulators pubsub start --project=stock-monitoring-test

# In another terminal, set the emulator environment variable
$(gcloud beta emulators pubsub env-init)

# Uncomment PUBSUB_EMULATOR_HOST in .env.test
```

## Test Results

After the test completes:

- Results are saved to `tests/integration/reports/test_results.json`
- Detailed logs are printed to the console
- The exit code will be 0 for success, 1 for failure

## Test Components

- `test_stock_monitoring.py`: Main test orchestration
- `data_generator.py`: Generates realistic stock trading data
- `pubsub_client.py`: Manages Pub/Sub resources
- `pubsub_monitor.py`: Monitors and analyzes Pub/Sub messages

## Troubleshooting

- **MongoDB Connection Issues**: Verify MongoDB is running and URIs are correct
- **Missing Messages**: Increase `TEST_DURATION` to allow more processing time
- **Pub/Sub Errors**: Check project permissions or try using the emulator
- **No Alerts**: Adjust threshold values in `TEST_POSITION_THRESHOLDS` to be lower

# Stock Monitoring Pub/Sub Testing

This directory contains scripts for testing stock monitoring functionality using Google Cloud Pub/Sub.

## Prerequisites

1. Ensure you've authenticated with Google Cloud:
   ```
   gcloud auth login
   gcloud auth application-default login
   ```

2. Set your project ID:
   ```
   gcloud config set project YOUR_PROJECT_ID
   ```

3. Make sure the Pub/Sub API is enabled in your project:
   ```
   gcloud services enable pubsub.googleapis.com
   ```

4. Ensure your .env.test file has the following variables:
   ```
   GCP_PROJECT_ID=your-project-id
   TEST_TRADING_SITES=fidelity,etrade,robinhood
   PUBSUB_TOPIC_FIDELITY=fidelity-stock-trades
   PUBSUB_TOPIC_ETRADE=etrade-stock-trades
   PUBSUB_TOPIC_ROBINHOOD=robinhood-stock-trades
   ```

## Testing Scripts

### Basic Pub/Sub Setup Test

Verify that your Pub/Sub topics and subscriptions are set up correctly:

```
./run_tests.sh your-project-id pubsub
```

This will:
1. Check your authentication and project
2. List existing topics and subscriptions
3. Create topics and subscriptions if they don't exist
4. Report success or failure

### Pub/Sub Monitoring Test

This test monitors for messages on your Pub/Sub subscriptions:

```
./run_tests.sh your-project-id mock
```

By default, it will monitor for 10 seconds. You can also run this directly with custom options:

```
python test_mock.py --duration 30
```

### Publishing Test Messages

To publish test messages to your Pub/Sub topics:

```
python publish_test_messages.py --messages 5
```

Optional arguments:
- `--site SITE`: Publish only to a specific site (fidelity, etrade, robinhood)
- `--messages N`: Number of messages to publish per topic (default: 10)
- `--delay SECONDS`: Delay between messages in seconds (default: 0.1)

## For Local Development

If you need to use the Pub/Sub emulator for local development, you can:

1. Start the emulator:
   ```
   gcloud beta emulators pubsub start --project=your-project-id
   ```

2. Use the `--emulator` flag with any of the scripts:
   ```
   python test_mock.py --emulator
   python publish_test_messages.py --emulator
   ```

The emulator mode is useful for development and testing without incurring costs from the Google Cloud Pub/Sub service. 