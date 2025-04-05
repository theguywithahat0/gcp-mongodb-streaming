# MongoDB to GCP Streaming Pipeline

This project implements a streaming pipeline that reads data from MongoDB and streams it to Google Cloud Platform (GCP) services using Apache Beam and Dataflow.

## Architecture

```
MongoDB Change Streams -> Apache Beam/Dataflow -> Cloud Pub/Sub
```

The pipeline monitors MongoDB change streams for real-time data changes and publishes them to Cloud Pub/Sub topics, enabling real-time data processing and analytics.

## Project Structure

```
gcp-mongodb-streaming/
├── config/
│   └── config.yaml           # Main configuration file
├── src/
│   ├── pipeline/
│   │   ├── beam_pipeline.py  # Main Apache Beam pipeline
│   │   ├── mongodb/         # MongoDB connectivity
│   │   │   ├── source.py    # Change stream source
│   │   │   └── transforms.py # Document transformations
│   │   ├── pubsub/         # Pub/Sub integration
│   │   │   └── sink.py      # Publishing logic
│   │   └── utils/          # Shared utilities
│   │       ├── config.py    # Configuration management
│   │       └── logging.py   # Logging setup
│   └── scripts/
│       ├── setup_pubsub.py  # GCP resource setup
│       └── run_pipeline.py  # Pipeline entry point
├── tests/                   # Test suite
│   ├── test_mongodb/       # MongoDB tests
│   ├── test_pubsub/        # Pub/Sub tests
│   └── test_pipeline/      # Pipeline tests
└── [Configuration Files]
```

## Prerequisites

- Python 3.8+
- MongoDB Atlas account (free tier)
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
1. **Environment Variables** (.env file)
   - Sensitive information
   - API keys and credentials
   - Connection strings

2. **Application Config** (config/config.yaml)
   - Pipeline settings
   - Batch sizes and windows
   - Topic/subscription names
   - Non-sensitive configuration

## Development

### Running the Pipeline

1. Set up GCP resources:
   ```bash
   python src/scripts/setup_pubsub.py
   ```

2. Run the pipeline:
   ```bash
   python src/scripts/run_pipeline.py
   ```

### Running Tests

```bash
pytest tests/
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