# Stock Position Monitoring Example

This directory contains an example transform that monitors stock trading positions and generates alerts when thresholds are exceeded.

## Overview

The `StockPositionMonitor` transform demonstrates how to:

1. Process a stream of trading data in real-time
2. Group trades by site and stock symbol within time windows
3. Calculate net positions based on buy/sell operations
4. Generate alerts when positions exceed defined thresholds
5. Track metrics for monitoring and analysis

## Files

- `stock_monitoring.py`: Transform implementation
- `stock_monitoring_config.yaml`: Example configuration
- `test_stock_monitoring.py`: Unit tests

## How It Works

The transform processes trade documents with the following workflow:

1. **Filter and Format**: Validates incoming documents and standardizes the trade format
2. **Window Application**: Groups trades into sliding time windows (default: 5 minutes)
3. **Position Calculation**: Aggregates buy/sell quantities to calculate net positions
4. **Threshold Checking**: Compares positions against configured thresholds
5. **Alert Generation**: Adds alert metadata to documents exceeding thresholds

## Using the Transform

### Trade Document Format

The transform expects documents in this format:

```json
{
  "site": "fidelity",        // Trading platform
  "symbol": "AAPL",          // Stock symbol
  "quantity": 100,           // Number of shares
  "operation": "buy",        // "buy" or "sell"
  "timestamp": 1617293054    // Unix timestamp or ISO string
}
```

### Configuration

Configure the transform in your pipeline config:

```yaml
transforms:
  - name: "StockPositionMonitor"
    module: "pipeline.transforms.examples.stock_monitoring"
    enabled: true
    config:
      thresholds:
        - site: "fidelity"
          stock: "AAPL"
          limit: 500
        - site: "etrade"
          stock: "GOOGL"
          limit: 200
      window_size: 300  # seconds
```

### Testing with Live GCP Resources

To test the stock monitoring transform with live GCP resources:

1. Set up your GCP project with Pub/Sub topics and subscriptions:
   ```bash
   # Create topics for each trading site
   gcloud pubsub topics create fidelity-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub topics create etrade-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub topics create robinhood-stock-trades --project=YOUR_PROJECT_ID
   
   # Create subscriptions for each topic
   gcloud pubsub subscriptions create fidelity-stock-trades-sub --topic=fidelity-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub subscriptions create etrade-stock-trades-sub --topic=etrade-stock-trades --project=YOUR_PROJECT_ID
   gcloud pubsub subscriptions create robinhood-stock-trades-sub --topic=robinhood-stock-trades --project=YOUR_PROJECT_ID
   ```

2. Run the integration test:
   ```bash
   python tests/integration/pipeline/stock_monitoring/test_stock_monitoring.py --duration 60
   ```

3. View the output messages:
   ```bash
   python tests/integration/pipeline/stock_monitoring/pull_messages.py --project-id YOUR_PROJECT_ID
   ```

4. Publish test messages manually:
   ```bash
   gcloud pubsub topics publish etrade-stock-trades --project=YOUR_PROJECT_ID \
     --message='{"stock_symbol":"AAPL", "trade_type":"BUY", "quantity":100, "price":185.50}'
   ```

### Python Example

```python
from pipeline.transforms.examples.stock_monitoring import StockPositionMonitor
import apache_beam as beam

# Define thresholds
thresholds = [
    {"site": "fidelity", "stock": "AAPL", "limit": 500},
    {"site": "etrade", "stock": "GOOGL", "limit": 200}
]

# Create pipeline
with beam.Pipeline() as p:
    trades = p | beam.Create([...])  # Your source of trade data
    
    # Apply the StockPositionMonitor transform
    results = trades | StockPositionMonitor(
        thresholds=thresholds,
        window_size=300  # 5 minutes
    )
    
    # Process results
    results | beam.Map(print)
```

## Output

The transform produces two types of output records:

### Position Records

```json
{
  "site": "fidelity",
  "stock": "AAPL",
  "position": 450,
  "window_end": "2023-04-07T14:30:00Z"
}
```

### Alert Records

```json
{
  "site": "fidelity",
  "stock": "AAPL",
  "position": 550,
  "window_end": "2023-04-07T14:30:00Z",
  "threshold": 500,
  "alert": true
}
```

## Metrics

The transform tracks the following metrics:

- `documents_processed`: Total documents processed
- `valid_trades`: Count of valid trade documents
- `positions_monitored`: Number of positions calculated
- `alerts_generated`: Number of threshold alerts generated
- `alert_rate`: Ratio of alerts to positions monitored

## Viewing Pub/Sub Messages

Use the included utility script to view messages in Pub/Sub subscriptions:

```bash
python tests/integration/pipeline/stock_monitoring/pull_messages.py --project-id YOUR_PROJECT_ID
```

Options:
- `--project-id`: Your GCP project ID
- `--sites`: Comma-separated list of sites to pull messages from (default: all)
- `--max-messages`: Maximum number of messages to pull per subscription (default: 10)
- `--acknowledge`: Whether to acknowledge and remove messages from the subscription

## Testing

Run the unit tests:

```bash
pytest src/pipeline/transforms/examples/test_stock_monitoring.py
```

## Extending

To create your own version of this transform:

1. Copy the file and modify as needed
2. Customize the threshold checking logic for your use case
3. Add additional aggregations or alerts as required
4. Update metrics collection for any new metrics

For more advanced examples, see the transform documentation. 