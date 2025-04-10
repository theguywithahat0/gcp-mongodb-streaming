# Creating New Use Cases for the MongoDB Streaming Pipeline

This guide explains how to create custom use cases that leverage the core pipeline framework for streaming data from MongoDB to Google Cloud Pub/Sub. It provides step-by-step instructions for creating domain-specific implementations and integrating them into the pipeline.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Use Case Structure](#use-case-structure)
4. [Step 1: Define Your Domain Model](#step-1-define-your-domain-model)
5. [Step 2: Create a New Use Case Package](#step-2-create-a-new-use-case-package)
6. [Step 3: Implement Your Transform](#step-3-implement-your-transform)
7. [Step 4: Create Configuration Files](#step-4-create-configuration-files)
8. [Step 5: Write Unit Tests](#step-5-write-unit-tests)
9. [Step 6: Integrate with the Pipeline](#step-6-integrate-with-the-pipeline)
10. [Step 7: Create Integration Tests](#step-7-create-integration-tests)
11. [Example: Inventory Monitoring Use Case](#example-inventory-monitoring-use-case)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)

## Overview

The MongoDB Streaming Pipeline provides a flexible framework for processing data from MongoDB and publishing it to Google Cloud Pub/Sub. By creating custom use cases, you can implement domain-specific logic for various business needs while leveraging the core infrastructure.

Key components of a use case:
- Custom transform implementations for domain-specific processing
- Configuration files for customizing behavior
- Unit and integration tests to verify functionality
- Documentation to guide users

## Prerequisites

Before creating a new use case, ensure you have:

1. Set up the gcp-mongodb-streaming project
2. Installed all dependencies (run `pip install -e .` from the project root)
3. Configured GCP credentials
4. Basic understanding of Apache Beam and PTransforms

## Use Case Structure

Each use case is organized in its own package under the `use_cases` directory with the following structure:

```
use_cases/
└── your_use_case/
    ├── __init__.py             # Package initialization with imports
    ├── your_transform.py       # Main transform implementation
    ├── your_transform_config.yaml  # Example configuration
    └── README.md               # Documentation for the use case
```

Unit tests for use cases are located in a dedicated test directory:

```
tests/
└── unit/
    └── use_cases/
        └── test_your_use_case.py  # Unit tests for your use case
```

Integration tests have their own directory structure:

```
tests/
└── integration/
    └── pipeline/
        └── your_use_case/
            ├── tests/          # Test scripts
            ├── utils/          # Testing utilities
            └── core/           # Core test functionality
```

## Step 1: Define Your Domain Model

Begin by clearly defining your domain model and the specific business problem you're addressing:

1. What data are you processing? (source, format, fields)
2. What transformations or analysis do you need to perform?
3. What outputs do you need to generate?
4. What metrics or monitoring are required?

## Step 2: Create a New Use Case Package

Create the directory structure and basic files for your new use case:

```bash
# From the project root
mkdir -p use_cases/your_use_case
touch use_cases/your_use_case/__init__.py
touch use_cases/your_use_case/your_transform.py
touch use_cases/your_use_case/your_transform_config.yaml
touch use_cases/your_use_case/README.md

# Create the unit test directory
mkdir -p tests/unit/use_cases
touch tests/unit/use_cases/test_your_use_case.py
```

In the `__init__.py` file, make your transform class available:

```python
"""
Your Use Case Description

This module contains transforms and utilities for [your domain-specific functionality].
"""

from use_cases.your_use_case.your_transform import YourTransform

__all__ = ['YourTransform']
```

## Step 3: Implement Your Transform

Implement your custom transform by extending `beam.PTransform`:

```python
import logging
import apache_beam as beam
from apache_beam.metrics import Metrics

class YourTransform(beam.PTransform):
    """
    A beam transform that processes [your domain] data.
    
    The transform performs the following operations:
    1. [First step description]
    2. [Second step description]
    ...
    
    Args:
        param1: Description of first parameter
        param2: Description of second parameter
    """
    
    def __init__(self, param1, param2=default_value):
        """Initialize the transform with configuration."""
        super().__init__()
        self.param1 = param1
        self.param2 = param2
        
        # Set up metrics
        self.documents_processed_counter = Metrics.counter(
            self.__class__.__name__, 'documents_processed')
        # Add more metrics as needed
    
    def expand(self, input_pcoll):
        """Process the input PCollection and produce outputs."""
        
        # Implement your processing logic
        processed = (
            input_pcoll
            | "First Step" >> beam.Map(self._first_step)
            | "Second Step" >> beam.Filter(self._second_step)
            # Add more steps as needed
        )
        
        return processed
    
    def _first_step(self, element):
        """First processing step."""
        self.documents_processed_counter.inc()
        # Implement your logic
        return element
    
    def _second_step(self, element):
        """Second processing step."""
        # Implement your logic
        return True  # For filter operations
```

## Step 4: Create Configuration Files

Create a configuration file for your transform:

```yaml
# Your Transform Configuration
# This file shows how to configure the YourTransform

transforms:
  - name: your_transform
    class: use_cases.your_use_case.your_transform.YourTransform
    config:
      # Your configuration parameters
      param1: value1
      param2: value2
      
      # More complex configuration
      nested_param:
        - item1: value1
          item2: value2
        - item1: value3
          item2: value4

# Input configuration
input:
  collection: your_collection
  database: your_database
  
# Output configuration  
output:
  topic: projects/your-project/topics/your-topic
```

## Step 5: Write Unit Tests

Create unit tests for your transform in the `tests/unit/use_cases` directory:

```python
# In tests/unit/use_cases/test_your_use_case.py

"""Tests for the YourTransform."""

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from use_cases.your_use_case.your_transform import YourTransform


class YourTransformTest(unittest.TestCase):
    """Test cases for the YourTransform."""

    def test_basic_functionality(self):
        """Test the transform's basic functionality."""
        # Sample input data
        input_data = [
            {"field1": "value1", "field2": 100},
            {"field1": "value2", "field2": 200},
            # More test data
        ]

        # Expected output data
        expected_output = [
            {"field1": "value1", "field2": 100, "processed": True},
            {"field1": "value2", "field2": 200, "processed": True},
            # Expected results
        ]

        # Create and run test pipeline
        with TestPipeline() as p:
            # Create PCollection from input data
            input_pcoll = p | "Create Input" >> beam.Create(input_data)
            
            # Apply the transform
            transform = YourTransform(param1="test", param2=123)
            results = input_pcoll | "Process Data" >> transform
            
            # Assert the expected outputs
            assert_that(results, equal_to(expected_output))
```

It's also good practice to create a simple import test to verify your module structure:

```python
# In tests/unit/use_cases/test_imports.py

import unittest

class ImportTest(unittest.TestCase):
    """Test importing the use case modules."""
    
    def test_import_transform(self):
        """Test that the transform can be imported."""
        try:
            from use_cases.your_use_case.your_transform import YourTransform
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import YourTransform: {e}")
```

## Step 6: Integrate with the Pipeline

Update the main pipeline configuration to include your transform:

```yaml
# In config/config.yaml

transforms:
  transforms:
    # Your custom transform
    - name: "your_transform"
      module: "use_cases.your_use_case.your_transform"
      enabled: true
      config:
        param1: value1
        param2: value2
```

## Step 7: Create Integration Tests

Create integration tests to verify your transform works with the full pipeline:

1. Create the directory structure for your integration tests:

```bash
# From the project root
mkdir -p tests/integration/pipeline/your_use_case/tests
mkdir -p tests/integration/pipeline/your_use_case/utils
mkdir -p tests/integration/pipeline/your_use_case/core
touch tests/integration/pipeline/your_use_case/README.md
```

2. Create a test script that verifies your use case:

```python
# In tests/integration/pipeline/your_use_case/tests/test_your_use_case.py

#!/usr/bin/env python3
"""
Integration Test for Your Use Case Pipeline

This test verifies the end-to-end functionality of your use case by:
1. Generating and loading test data into MongoDB
2. Running the pipeline to process the data
3. Monitoring Pub/Sub for the expected outputs
"""

import os
import sys
import logging
import asyncio
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Import local modules
from tests.integration.pipeline.your_use_case.utils.data_generator import (
    YourDataGenerator, load_data_to_mongodb
)
from tests.integration.pipeline.your_use_case.utils.output_monitor import OutputMonitor

# Your test implementation here...

async def main():
    """Run the integration test."""
    # Your test logic here
    
if __name__ == "__main__":
    asyncio.run(main())
```

3. Create utilities for data generation and output monitoring:

```python
# In tests/integration/pipeline/your_use_case/utils/data_generator.py

"""Data generation utilities for your use case tests."""

import random
import datetime
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class YourDataGenerator:
    """Generator for test data."""
    
    def __init__(self, config):
        """Initialize the generator with test configuration."""
        self.config = config
    
    def generate_data(self) -> List[Dict[str, Any]]:
        """Generate test data for your use case."""
        # Your data generation logic here
        
async def load_data_to_mongodb(uri, database, collection, documents):
    """Load test data to MongoDB."""
    # Your data loading logic here
```

4. Create a pull messages script in the core directory:

```python
# In tests/integration/pipeline/your_use_case/core/pull_messages.py

#!/usr/bin/env python3
"""
Utility to pull and display messages from your use case pipeline.

This script allows you to verify the output of your pipeline by retrieving
messages from the Pub/Sub subscriptions.
"""

import argparse
import logging
from google.cloud import pubsub_v1

# Your message pulling implementation

def main():
    """Pull messages from Pub/Sub subscriptions."""
    # Your implementation here
    
if __name__ == "__main__":
    main()
```

Organize your integration tests similarly to the existing stock monitoring tests, but adapted for your specific use case.

## Example: Inventory Monitoring Use Case

Here's an example of transforming the stock monitoring use case into an inventory monitoring use case:

### Domain Model

For inventory monitoring:
- Monitor inventory levels across multiple warehouses
- Track items by SKU
- Generate alerts when inventory falls below thresholds
- Support both additions and removals from inventory

### Transform Implementation

```python
"""
InventoryMonitor - A transform for monitoring inventory levels from warehouse data.

This transform monitors and aggregates inventory transactions to track levels
and generate alerts when inventory thresholds are reached.
"""

import logging
import time
from typing import Dict, List, Any, Optional

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import window


class InventoryMonitor(beam.PTransform):
    """
    A beam transform that monitors inventory levels from transaction data.
    
    The transform performs the following operations:
    1. Filters for valid inventory transaction documents.
    2. Groups transactions by warehouse and SKU within a sliding time window.
    3. Calculates net inventory based on addition/removal activities.
    4. Compares inventory levels against thresholds to generate alerts.
    
    Args:
        thresholds: List of threshold configurations with warehouse, sku, and min_level values.
        window_size: Time window size in seconds for inventory calculation.
    """
    
    def __init__(self, thresholds: List[Dict[str, Any]], window_size: int = 3600):
        """Initialize the transform with thresholds and window configuration."""
        super().__init__()
        self.thresholds = thresholds
        self.window_size = window_size
        
        # Create a lookup dictionary for faster threshold checking
        self.threshold_map = {}
        for threshold in self.thresholds:
            key = f"{threshold['warehouse']}:{threshold['sku']}"
            self.threshold_map[key] = threshold['min_level']
        
        # Metrics for monitoring the transform's performance
        self.documents_processed_counter = Metrics.counter(
            self.__class__.__name__, 'documents_processed')
        self.valid_transactions_counter = Metrics.counter(
            self.__class__.__name__, 'valid_transactions')
        self.inventory_monitored_counter = Metrics.counter(
            self.__class__.__name__, 'inventory_monitored')
        self.alerts_generated_counter = Metrics.counter(
            self.__class__.__name__, 'alerts_generated')
    
    def expand(self, input_pcoll):
        """Process the input PCollection and produce inventory and alert outputs."""
        
        # Filter valid transaction documents
        transactions = (
            input_pcoll
            | "Count All Documents" >> beam.Map(self._count_document)
            | "Filter Valid Transactions" >> beam.Filter(self._is_valid_transaction)
            | "Format Transaction Data" >> beam.Map(self._format_transaction)
        )
        
        # Apply windowing to group transactions in time windows
        windowed_transactions = (
            transactions
            | "Apply Sliding Window" >> beam.WindowInto(
                window.SlidingWindows(self.window_size, self.window_size // 2),
                timestamp_fn=lambda x: self._get_timestamp(x)
            )
        )
        
        # Group by warehouse and SKU
        grouped_transactions = (
            windowed_transactions
            | "Group By Warehouse and SKU" >> beam.GroupBy(
                lambda x: (x['warehouse'], x['sku'])
            )
            .aggregate_field(
                lambda x: x['quantity'] * (1 if x['transaction_type'] == 'add' else -1),
                sum,
                'level'
            )
        )
        
        # Calculate inventory levels and check thresholds
        inventory = (
            grouped_transactions
            | "Count Inventory" >> beam.Map(self._count_inventory)
            | "Add Window Info" >> beam.Map(self._add_window_info)
        )
        
        # Generate alerts for inventory below thresholds
        alerts = (
            inventory
            | "Check Thresholds" >> beam.Map(self._check_threshold)
            | "Filter Alerts" >> beam.Filter(lambda x: x.get('alert', False))
            | "Count Alerts" >> beam.Map(self._count_alert)
        )
        
        # Return combined inventory and alerts
        return (inventory, alerts) | "Combine Results" >> beam.Flatten()
    
    def _is_valid_transaction(self, document: Dict[str, Any]) -> bool:
        """Check if the document is a valid inventory transaction."""
        is_valid = (
            isinstance(document, dict) and
            'warehouse' in document and
            'sku' in document and
            'quantity' in document and
            'transaction_type' in document and
            document.get('transaction_type') in ['add', 'remove']
        )
        
        if is_valid:
            self.valid_transactions_counter.inc()
        
        return is_valid
    
    def _format_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize transaction document format."""
        return {
            'warehouse': transaction['warehouse'],
            'sku': transaction['sku'],
            'quantity': transaction['quantity'],
            'transaction_type': transaction['transaction_type'],
            'timestamp': transaction.get('timestamp', time.time())
        }
    
    # Additional helper methods...
    
    def _check_threshold(self, inventory: Dict[str, Any]) -> Dict[str, Any]:
        """Check if inventory is below threshold and generate alert if needed."""
        warehouse = inventory.get('warehouse')
        sku = inventory.get('sku')
        level = inventory.get('level', 0)
        
        key = f"{warehouse}:{sku}"
        min_level = self.threshold_map.get(key)
        
        if min_level is not None and level < min_level:
            return {
                **inventory,
                'min_level': min_level,
                'alert': True
            }
        
        return inventory
```

### Configuration Example

```yaml
# Inventory Monitor Configuration

transforms:
  - name: inventory_monitor
    class: use_cases.inventory_monitoring.inventory_monitor.InventoryMonitor
    config:
      # Time window in hours for calculating inventory
      window_size: 3600  # 1 hour
      
      # Inventory thresholds that trigger alerts
      thresholds:
        # Electronics warehouse
        - warehouse: warehouse_east
          sku: LAPTOP-001
          min_level: 50  # Alert if inventory falls below 50 units
        
        - warehouse: warehouse_west
          sku: TABLET-X7
          min_level: 75  # Alert if inventory falls below 75 units
        
        # Office supplies
        - warehouse: warehouse_central
          sku: PAPER-A4
          min_level: 500  # Alert if inventory falls below 500 units
```

## Best Practices

1. **Domain-Specific vs. Generic**: Keep domain-specific logic in your use case, and use the core pipeline for common functionality.

2. **Testability**: Design your transforms to be testable with clear input/output expectations.

3. **Documentation**: Thoroughly document your use case, including:
   - Overview and purpose
   - Data formats and requirements
   - Configuration options
   - Example usage
   - Expected outputs

4. **Metrics**: Include comprehensive metrics for monitoring and debugging.

5. **Configuration**: Make your transform highly configurable to support different scenarios.

6. **Error Handling**: Include appropriate error handling and validation.

7. **Reusability**: Design your transforms to be reusable across different applications.

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure your package structure is correct and imports are properly defined.

2. **Configuration Issues**: Verify your configuration file has the correct format and all required parameters.

3. **Pipeline Integration**: Check that your transform is properly registered in the main pipeline configuration.

4. **Testing Failures**: Use Apache Beam's testing utilities to diagnose issues with your transform.

5. **GCP Connectivity**: Verify your GCP credentials and project settings.

For more assistance, refer to the [Apache Beam Documentation](https://beam.apache.org/documentation/) and the [MongoDB Streaming Pipeline Guide](./pipeline_guide.md). 