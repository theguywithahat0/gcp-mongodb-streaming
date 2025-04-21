# MongoDB Change Stream Connector

This connector streams real-time changes from MongoDB collections to Google Cloud Pub/Sub. It's designed to be reliable, scalable, and production-ready with features like resume token management, error handling, and monitoring.

## Features

- Real-time MongoDB change stream monitoring
- Reliable message delivery to Pub/Sub
- Resume token management for fault tolerance
- Structured logging and monitoring
- Health check endpoints
- Comprehensive error handling
- Configurable connection retry with backoff

## Prerequisites

- Python 3.9+
- MongoDB 4.0+ with replica set enabled
- Google Cloud project with Pub/Sub and Firestore enabled
- Service account with necessary permissions

## Setup

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Run tests:
```bash
pytest
```

## Configuration

The connector can be configured through environment variables or a YAML configuration file. See `config/config.yaml.example` for available options.

Key configuration options:
- MongoDB connection settings
- Collection watch filters
- Pub/Sub topic configurations
- Resume token storage settings
- Logging and monitoring options

## Running the Connector

Development:
```bash
python -m connector.main
```

Production (using Cloud Run):
```bash
# Build container
docker build -t gcr.io/[PROJECT_ID]/mongo-connector .

# Push to Container Registry
docker push gcr.io/[PROJECT_ID]/mongo-connector

# Deploy to Cloud Run
gcloud run deploy mongo-connector \
  --image gcr.io/[PROJECT_ID]/mongo-connector \
  --platform managed
```

## Monitoring

The connector exposes the following endpoints:
- `/health`: Basic health check
- `/readiness`: Readiness probe
- `/metrics`: Prometheus metrics

## Architecture

The connector follows a modular architecture:
- `core/`: Core connector logic
- `config/`: Configuration management
- `utils/`: Utility functions
- `tests/`: Test suite

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT
