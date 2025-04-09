# Core Utilities

This directory contains the core utilities for working with the stock monitoring pipeline.

## pull_messages.py

`pull_messages.py` is a utility script for pulling and displaying messages from Google Cloud Pub/Sub subscriptions. It's useful for:

- Debugging the pipeline output
- Verifying that messages are being published correctly
- Monitoring the pipeline in real-time

### Usage

```bash
python pull_messages.py --project-id YOUR_PROJECT_ID
```

#### Options

- `--project-id`: Your GCP project ID (required)
- `--sites`: Comma-separated list of sites to pull messages from (default: all)
- `--max-messages`: Maximum number of messages to pull per subscription (default: 10)
- `--acknowledge`: Whether to acknowledge and remove messages from the subscription (default: false)

### Example

```bash
# Pull from all sites
python pull_messages.py --project-id jh-testing-project

# Pull from specific sites
python pull_messages.py --project-id jh-testing-project --sites fidelity,etrade

# Pull and acknowledge messages (will remove them from the subscription)
python pull_messages.py --project-id jh-testing-project --acknowledge
```

### Output

The script will display the content of the messages, including:
- Stock symbol
- Trade type (buy/sell)
- Quantity
- Price
- Publish time

Example output:
```
Message: AAPL | BUY | 100 | $185.50 | 2025-04-09T21:12:06.985000+00:00
``` 