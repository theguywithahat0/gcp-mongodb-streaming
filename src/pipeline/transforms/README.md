# Transforms System for MongoDB to Pub/Sub Pipeline

This document explains the transforms system for the MongoDB to Pub/Sub streaming pipeline. The transforms system allows you to plug in custom data processing logic without modifying the core pipeline code.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Available Transforms](#available-transforms)
4. [Creating Custom Transforms](#creating-custom-transforms)
5. [Configuring Transforms](#configuring-transforms)
6. [Monitoring and Metrics](#monitoring-and-metrics)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

## Overview

The transforms system allows you to insert custom processing logic into the streaming pipeline between the MongoDB source and the Pub/Sub sink. This enables you to:

- Process and transform documents as they flow through the pipeline
- Calculate aggregations and statistics
- Generate alerts based on document content
- Enrich documents with additional data
- Filter or route documents based on custom logic

All without modifying the core pipeline code.

## Architecture

The transform system architecture consists of:

1. **Base Classes and Interfaces**:
   - `MessageTransform`: Base class for all transforms
   - `MetricsProvider`: Interface for transforms that provide metrics
   - `SchemaAware`: Interface for transforms that need schema information
   - `ConfigurableTransform`: Interface for transforms that can be configured dynamically

2. **Transform Loader**:
   - Dynamically loads and configures transforms based on configuration
   - Handles errors and provides validation

3. **Pipeline Integration**:
   - Applies transforms sequentially to the document stream
   - Handles the flow of data between transforms

```
MongoDB → Source → [Transform 1] → [Transform 2] → ... → [Transform N] → Sink → Pub/Sub
```

## Available Transforms

### Stock Position Monitor

The `StockPositionMonitor` transform included as an example tracks stock transactions and monitors position thresholds:

```python
from pipeline.transforms.examples.stock_monitoring import StockPositionMonitor

# Create with default parameters
monitor = StockPositionMonitor(
    thresholds=[
        {'site': 'fidelity', 'stock': 'AAPL', 'limit': 100},
        {'site': 'etrade', 'stock': 'GOOGL', 'limit': 50}
    ],
    window_size=60  # seconds
)
```

This transform:
1. Groups stock transactions by site and symbol
2. Calculates net positions (buys - sells)
3. Checks positions against configured thresholds
4. Adds threshold and alert information to messages

## Creating Custom Transforms

### Basic Transform

To create a custom transform, inherit from `MessageTransform` and implement the `expand` method:

```python
from pipeline.transforms.base import MessageTransform
import apache_beam as beam

class MyCustomTransform(MessageTransform):
    """Custom transform description."""
    
    def __init__(self, param1=None, param2=None):
        self.param1 = param1
        self.param2 = param2
    
    def expand(self, pcoll):
        return (
            pcoll
            | "Step1" >> beam.Map(self._transform_step1)
            | "Step2" >> beam.Map(self._transform_step2)
        )
    
    def _transform_step1(self, document):
        # First transformation step
        result = document.copy()
        result['field1'] = self._process_field1(document)
        return result
    
    def _transform_step2(self, document):
        # Second transformation step
        result = document.copy()
        result['field2'] = self._process_field2(document)
        return result
        
    def _process_field1(self, document):
        # Process field logic
        return f"Processed: {document.get('original_field', '')}"
        
    def _process_field2(self, document):
        # Process field logic
        return document.get('field1', '').upper()
```

### Adding Metrics

To add metrics to your transform, implement the `MetricsProvider` interface:

```python
from pipeline.transforms.base import MessageTransform, MetricsProvider

class MetricsEnabledTransform(MessageTransform, MetricsProvider):
    def __init__(self):
        self._processed_count = 0
        self._error_count = 0
        
    def expand(self, pcoll):
        return pcoll | beam.Map(self._process_with_metrics)
        
    def _process_with_metrics(self, document):
        try:
            self._processed_count += 1
            # Process document
            return document
        except Exception:
            self._error_count += 1
            # Handle error
            return document
            
    def get_metrics(self):
        return {
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "success_rate": (
                (self._processed_count - self._error_count) / self._processed_count 
                if self._processed_count > 0 else 0
            )
        }
```

### Using Schema Information

To access document schemas, implement the `SchemaAware` interface:

```python
from pipeline.transforms.base import MessageTransform, SchemaAware

class SchemaAwareTransform(MessageTransform, SchemaAware):
    def __init__(self):
        self.schemas = {}
        
    def set_schemas(self, schemas):
        self.schemas = schemas
        
    def expand(self, pcoll):
        return pcoll | beam.Map(self._validate_with_schema)
        
    def _validate_with_schema(self, document):
        # Get appropriate schema based on document metadata
        metadata = document.get('_metadata', {})
        stream_id = f"{metadata.get('connection_id')}.{metadata.get('source_name')}"
        
        # Apply schema validation or transformation
        if stream_id in self.schemas:
            schema = self.schemas[stream_id]
            # Use schema to validate or transform document
            
        return document
```

### Making Transforms Configurable

To make your transform configurable at runtime, implement the `ConfigurableTransform` interface:

```python
from pipeline.transforms.base import MessageTransform, ConfigurableTransform

class ConfigurableFilterTransform(MessageTransform, ConfigurableTransform):
    def __init__(self):
        self.field_name = None
        self.field_value = None
        
    def configure(self, config):
        self.field_name = config.get('field_name')
        self.field_value = config.get('field_value')
        
    def expand(self, pcoll):
        return pcoll | beam.Filter(self._filter_documents)
        
    def _filter_documents(self, document):
        # Skip filtering if not configured
        if not self.field_name:
            return True
            
        # Filter based on configuration
        return document.get(self.field_name) == self.field_value
```

## Configuring Transforms

Transforms are configured in the `config.yaml` file under the `transforms` section:

```yaml
transforms:
  transforms:
    - name: "StockPositionMonitor"
      module: "pipeline.transforms.examples.stock_monitoring"
      enabled: true
      config:
        thresholds:
          - site: "fidelity"
            stock: "AAPL"
            limit: 100
          - site: "etrade"
            stock: "GOOGL"
            limit: 50
        window_size: 60
```

You can also provide transform configuration via a separate JSON file:

```bash
python src/scripts/run_pipeline.py --project my-gcp-project \
  --transform_config my_transforms.json
```

Where `my_transforms.json` contains:

```json
{
  "transforms": [
    {
      "name": "MyCustomTransform",
      "module": "my_package.transforms.my_module",
      "enabled": true,
      "config": {
        "param1": "value1",
        "param2": "value2"
      }
    }
  ]
}
```

## Monitoring and Metrics

Transforms that implement the `MetricsProvider` interface can provide metrics for monitoring. These metrics are collected and exported according to the monitoring configuration:

```yaml
monitoring:
  metrics:
    enable: true
    report_interval: 60  # seconds
    namespace: "mongodb_streaming"
    exporters:
      stackdriver: true
      prometheus: false
```

When running on Google Cloud Dataflow, metrics are automatically exported to Stackdriver. You can also enable the `--enable_monitoring` flag for additional monitoring:

```bash
python src/scripts/run_pipeline.py --project my-gcp-project \
  --enable_monitoring
```

## Best Practices

1. **Keep transforms focused**: Each transform should do one thing well.
2. **Optimize for streaming**: Be mindful of memory usage and latency.
3. **Handle errors gracefully**: Don't let one document failure affect the entire pipeline.
4. **Provide metrics**: Implement `MetricsProvider` for operational visibility.
5. **Document your transforms**: Include docstrings and README files.
6. **Test thoroughly**: Write unit tests for your transforms using the Apache Beam testing utilities.
7. **Make transforms configurable**: Implement `ConfigurableTransform` when appropriate.

## Troubleshooting

### Transform Not Found

If your transform module cannot be found:

1. Ensure the module is in the Python path
2. Check the module name and class name in the configuration
3. Verify the package is installed if it's a third-party package

### Performance Issues

If your transform is causing performance issues:

1. Optimize memory usage by avoiding large in-memory collections
2. Use windowing appropriately for aggregations
3. Implement batch processing where possible
4. Monitor transform metrics to identify bottlenecks

### Configuration Problems

If your transform is not receiving the correct configuration:

1. Validate your configuration syntax
2. Check the logs for configuration parsing errors
3. Use the `--validation_only` flag to validate configuration without running the pipeline 