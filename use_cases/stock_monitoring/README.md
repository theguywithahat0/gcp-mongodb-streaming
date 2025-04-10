# Stock Position Monitoring

This use case demonstrates how to monitor stock positions from trading data and generate alerts when positions exceed configured thresholds.

## Overview

The `StockPositionMonitor` transform processes a stream of stock trade data to:

1. Filter and validate trade documents
2. Group trades by site and stock symbol within time windows
3. Calculate net positions based on buy/sell activities
4. Generate alerts when positions exceed configured thresholds
5. Track metrics for monitoring and analysis

## Usage

### Input Data Format

The transform expects trade documents in this format:

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

Configure the transform in your pipeline configuration:

```yaml
transforms:
  - name: "stock_position_monitor"
    module: "use_cases.stock_monitoring.stock_monitoring"
    class: "StockPositionMonitor"
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

### Integration

Integrate the transform into your Apache Beam pipeline:

```python
from use_cases.stock_monitoring import StockPositionMonitor

# Configure thresholds
thresholds = [
    {'site': 'fidelity', 'stock': 'AAPL', 'limit': 100},
    {'site': 'etrade', 'stock': 'GOOGL', 'limit': 50}
]

# Create the transform
monitor = StockPositionMonitor(thresholds=thresholds, window_size=60)

# Apply to your pipeline
results = input_pcoll | "Monitor Stock Positions" >> monitor
```

## Output

The transform produces documents with position and alert information:

```json
{
  "site": "fidelity",
  "stock": "AAPL",
  "position": 125,
  "window_end": "2023-05-01T10:15:00Z",
  "threshold": 100,
  "alert": true
}
```

## Testing

See the integration tests in `tests/integration/pipeline/stock_monitoring/` for examples of how to test this use case with real data.

## Metrics

The transform tracks the following metrics:

- `documents_processed`: Total number of documents processed
- `valid_trades`: Number of valid trade documents
- `positions_monitored`: Number of positions calculated
- `alerts_generated`: Number of alerts generated
- `alert_rate`: Alerts as a percentage of valid trades 