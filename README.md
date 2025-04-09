# MongoDB to GCP Streaming Pipeline

This project implements a streaming pipeline that reads data from MongoDB and streams it to Google Cloud Platform (GCP) services using Apache Beam and Dataflow.

## Quick Start

1. Clone the repository
2. Set up your environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Create the required Pub/Sub topics and subscriptions:
   ```bash
   gcloud pubsub topics create fidelity-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub topics create etrade-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub topics create robinhood-stock-trades --project=YOUR_PROJECT_ID
   
   gcloud pubsub subscriptions create fidelity-stock-trades-sub --topic=fidelity-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub subscriptions create etrade-stock-trades-sub --topic=etrade-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub subscriptions create robinhood-stock-trades-sub --topic=robinhood-stock-trades --project=YOUR_PROJECT_ID
   ```
4. Publish test messages:
   ```bash
   gcloud pubsub topics publish etrade-stock-trades --project=YOUR_PROJECT_ID \
     --message='{"stock_symbol":"AAPL", "trade_type":"BUY", "quantity":100, "price":185.50}'
   ```
5. Pull and view messages:
   ```bash
   python tests/integration/pipeline/stock_monitoring/pull_messages.py --project-id YOUR_PROJECT_ID
   ```

## Architecture

```
MongoDB Change Streams -> Apache Beam/Dataflow -> Cloud Pub/Sub
```

The pipeline monitors MongoDB change streams for real-time data changes and publishes them to Cloud Pub/Sub topics, enabling real-time data processing and analytics.

## Pipeline Architecture

The streaming pipeline is built on Apache Beam with a modular architecture:

```
MongoDB → Source Connector → Transform System → Pub/Sub Sink
                                     ↑
                          Pluggable Transform Modules
```

### Key Components

- **Source Connector**: Reads from MongoDB change streams with configurable filters
- **Transform System**: Extensible framework for data processing and enrichment
- **Pub/Sub Sink**: Publishes processed messages to configured Pub/Sub topics

### Transform Framework

The pipeline includes a flexible transform system that:

- Supports pluggable data processing modules without modifying core code
- Provides built-in metrics and monitoring
- Enables content-based routing to different Pub/Sub topics
- Handles schema validation and document enrichment

### Stock Position Monitoring

The pipeline includes a stock position monitoring transform that:

- Monitors stock trading activity across multiple trading sites
- Calculates positions based on buy/sell transactions
- Generates alerts when position thresholds are exceeded
- Routes alerts to site-specific Pub/Sub topics

For more details, see the [pipeline documentation](src/pipeline/README.md) and [transform system](src/pipeline/transforms/README.md).

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
│   ├── unit/               # Unit tests
│   │   ├── mongodb/        # MongoDB component tests
│   │   │   ├── test_connection_manager.py   # Connection manager tests
│   │   │   ├── test_connection_manager_error_handling.py   # Error handling tests
│   │   │   └── validator/  # Validator tests
│   │   │       ├── test_validator.py         # Basic validation tests
│   │   │       ├── test_validator_edge_cases.py  # Edge case tests
│   │   │       └── test_validator_watch.py   # Change stream tests
│   │   └── config/         # Configuration tests
│   │       └── test_config.py  # Configuration parsing tests
│   ├── integration/        # Integration tests
│   │   └── mongodb/        # MongoDB integration tests
│   │       ├── change_streams/  # Change stream integration tests
│   │       │   ├── run_test.py  # Change stream test runner
│   │       │   ├── data_generator.py  # Test data generator
│   │       │   └── stream_monitor.py  # Stream monitoring utility
│   │       └── validator/   # Validator integration tests
│   │           ├── run_test.py  # Validator test runner
│   │           ├── data_generator.py  # Test data generator
│   │           └── validator_monitor.py  # Validation monitoring utility
│   └── conftest.py         # Test fixtures and configuration
├── cheat_sheets/           # Documentation and examples
│   └── connection_manager_explained.py  # Connection manager guide
└── [Configuration Files]
```

## Prerequisites

- Python 3.8+
- MongoDB Atlas account
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
   
   - Test Environment (`tests/config/.env.integration`)
     - Copy `tests/config/.env.integration.example` to `tests/config/.env.integration` for test settings
     - Contains test-specific configuration
     - Uses separate MongoDB instance for testing

2. **Application Config** (config/config.yaml)
   - Pipeline settings
   - Batch sizes and windows
   - Topic/subscription names
   - Non-sensitive configuration

## Development

### Running the Pipeline

1. Run the pipeline:
   ```bash
   python -m src.pipeline.beam.pipeline --project YOUR_PROJECT_ID --config_file config/config.yaml
   ```

### Viewing Pub/Sub Messages

Use the included utility script to view messages in Pub/Sub subscriptions:

```bash
python tests/integration/pipeline/stock_monitoring/pull_messages.py --project-id YOUR_PROJECT_ID
```

Options:
- `--project-id`: Your GCP project ID
- `--sites`: Comma-separated list of sites to pull messages from (default: all)
- `--max-messages`: Maximum number of messages to pull per subscription (default: 10)
- `--acknowledge`: Whether to acknowledge and remove messages from the subscription

### Testing

Run all unit tests:
```bash
pytest tests/unit/
```

Run integration tests:
```bash
python tests/integration/pipeline/stock_monitoring/test_stock_monitoring.py
```

For a quick test of Pub/Sub connectivity without MongoDB:
```bash
python tests/integration/pipeline/stock_monitoring/test_mock.py --duration 30
```

## Monitoring

The pipeline includes built-in monitoring capabilities:

- **Logging**: Structured logs to stdout/stderr or Cloud Logging
- **Metrics**: Counter, gauge, and distribution metrics
- **Health Checks**: Status endpoints for pipeline health

## Troubleshooting

### Common Issues

1. **Connection Failures**: Check MongoDB connectivity and credentials
2. **Pub/Sub Authorization**: Verify service account permissions
3. **Transform Errors**: Review logs for specific transform errors

### Debugging

Set the log level to DEBUG for more detailed information:

```bash
export LOG_LEVEL=DEBUG
python -m src.pipeline.beam.pipeline --config_file=config/local.yaml
```

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

## Testing Without MongoDB

To test the Pub/Sub messaging capabilities without setting up MongoDB, you can:

1. Run the mock test that focuses only on Pub/Sub:
   ```bash
   python tests/integration/pipeline/stock_monitoring/test_mock.py --duration 30
   ```

2. Publish test messages directly to Pub/Sub topics:
   ```bash
   gcloud pubsub topics publish etrade-stock-trades --project=YOUR_PROJECT_ID \
     --message='{"stock_symbol":"AAPL", "trade_type":"BUY", "quantity":100, "price":185.50}'
   ```

3. Use the pull_messages.py script to view messages in the subscriptions:
   ```bash
   python tests/integration/pipeline/stock_monitoring/pull_messages.py --project-id YOUR_PROJECT_ID
   ```

This allows you to verify that your Pub/Sub topics and subscriptions are properly configured and that you can publish and retrieve messages before integrating with MongoDB.

## Stock Monitoring Examples

The project includes a stock position monitoring example that demonstrates how to:
- Process real-time trading data
- Calculate positions based on buy/sell transactions
- Generate alerts when thresholds are exceeded

### Testing Stock Monitoring

The stock monitoring tests have been organized into a clear structure:

```
tests/integration/pipeline/stock_monitoring/
├── core/      # Core utilities for end users
├── utils/     # Utility classes and helper scripts
└── tests/     # Test implementations
```

To get started:
1. View messages from Pub/Sub:
   ```bash
   python tests/integration/pipeline/stock_monitoring/core/pull_messages.py --project-id YOUR_PROJECT_ID
   ```

2. Publish test messages:
   ```bash
   python tests/integration/pipeline/stock_monitoring/utils/publish_test_messages.py --project YOUR_PROJECT_ID
   ```

3. Run Pub/Sub-only tests:
   ```bash
   python tests/integration/pipeline/stock_monitoring/tests/test_mock.py --duration 30
   ```

For more information, see the [stock monitoring README](tests/integration/pipeline/stock_monitoring/README.md). 