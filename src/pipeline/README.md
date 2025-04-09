# MongoDB to Pub/Sub Streaming Pipeline

A scalable, flexible streaming pipeline for processing MongoDB change streams and publishing to Google Cloud Pub/Sub.

## Table of Contents

- [Architecture](#architecture)
- [Components](#components)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Extending the Pipeline](#extending-the-pipeline)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Architecture

This pipeline uses Apache Beam to process data streaming from MongoDB change streams and route them to Google Cloud Pub/Sub topics. The architecture consists of:

```
MongoDB Change Streams → Apache Beam Pipeline → Google Cloud Pub/Sub
                                  ↑
                            Transforms System
```

Key features:
- **Streaming**: Real-time processing of MongoDB change events
- **Transform System**: Modular, pluggable transforms for data processing
- **Flexible Routing**: Content-based routing to different Pub/Sub topics
- **Metrics Collection**: Built-in performance and health monitoring
- **Error Handling**: Robust error capture and recovery mechanisms

## Components

The pipeline is organized into several key modules:

### beam/

Contains the main Apache Beam pipeline definition and execution logic:
- `pipeline.py`: Core pipeline implementation that connects all components
- `sources/`: Custom Beam I/O connectors for reading from MongoDB

### mongodb/

MongoDB integration components:
- `connection.py`: MongoDB client management
- `reader.py`: Change stream reader implementation
- `validator.py`: Document validation logic

### pubsub/

Google Cloud Pub/Sub integration:
- `publisher.py`: Pub/Sub message publishing
- `formatter.py`: Message formatting utilities

### transforms/

Pluggable data processing components:
- `base.py`: Base classes and interfaces for custom transforms
- `loader.py`: Dynamic transform loading and configuration
- Various transform implementations (see `transforms/` directory)

### utils/

Shared utilities and helpers:
- `config.py`: Configuration loading and parsing
- `logging.py`: Logging setup and management
- `metrics.py`: Metrics collection and reporting

## Configuration

The pipeline is configured using YAML files located in the `config/` directory:

```yaml
# Example of main configuration structure
mongodb:
  uri: "mongodb://username:password@hostname:port"
  db: "mydatabase"
  collection: "mycollection"
  
pubsub:
  project_id: "my-gcp-project"
  default_topic: "mongodb-events"
  
pipeline:
  window_interval_sec: 60
  runner: "DirectRunner"  # or DataflowRunner for GCP
  
transforms:
  - name: "TransformName"
    module: "pipeline.transforms.module_name"
    enabled: true
    config:
      # Transform-specific configuration
```

### Environment Variables

You can override configuration settings using environment variables:
- `MONGODB_URI`: MongoDB connection string
- `PUBSUB_PROJECT`: GCP project ID
- `PUBSUB_TOPIC`: Default Pub/Sub topic

## Running the Pipeline

### Prerequisites

1. Python 3.8 or higher
2. MongoDB instance with change streams enabled (requires replica set)
3. Google Cloud project with Pub/Sub API enabled
4. Apache Beam dependencies

### Local Execution

```bash
# Set up environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run with local configuration
python -m src.scripts.run_pipeline --config_file=config/local.yaml
```

### Cloud Execution (Dataflow)

```bash
python -m src.scripts.run_pipeline \
  --config_file=config/production.yaml \
  --runner=DataflowRunner \
  --project=your-gcp-project \
  --region=us-central1 \
  --temp_location=gs://your-bucket/temp \
  --staging_location=gs://your-bucket/staging
```

## Extending the Pipeline

### Adding Custom Transforms

1. Create a new Python module in the `transforms/` directory
2. Implement a class that inherits from `MessageTransform`
3. Register your transform in the configuration file

See the `transforms/README.md` for detailed instructions.

### Adding New Sources or Sinks

The pipeline can be extended to support additional data sources or destinations:

1. Create a custom Beam I/O connector
2. Add connection logic to the pipeline builder
3. Update configuration schema to support new options

## Monitoring

The pipeline includes built-in monitoring capabilities:

- **Logging**: Structured logs to stdout/stderr or Cloud Logging
- **Metrics**: Counter, gauge, and distribution metrics
- **Health Checks**: Status endpoints for pipeline health

### Available Metrics

- `documents_processed`: Total number of documents processed
- `processing_time_ms`: Distribution of document processing times
- `transform_errors`: Count of errors by transform
- `pubsub_publish_latency`: Pub/Sub publishing latency

## Troubleshooting

### Common Issues

1. **Connection Failures**: Check MongoDB connectivity and credentials
2. **Pub/Sub Authorization**: Verify service account permissions
3. **Transform Errors**: Review logs for specific transform errors

### Debugging

Set the log level to DEBUG for more detailed information:

```bash
export LOG_LEVEL=DEBUG
python -m src.scripts.run_pipeline --config_file=config/local.yaml
```

For more information, consult the troubleshooting guide in the documentation. 