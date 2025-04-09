#!/bin/bash
# Script to run a test with Pub/Sub emulator or cloud

# Parse command line arguments
PROJECT_ID=${1:-jh-testing-project}
NUM_MESSAGES=${2:-5}
DURATION=${3:-30}
USE_CLOUD=${4:-false}  # Default to emulator

echo "=== Stock Monitoring Pub/Sub Test ==="
echo "Project: $PROJECT_ID"
echo "Messages per topic: $NUM_MESSAGES"
echo "Monitoring duration: $DURATION seconds"

# Directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../../.." &> /dev/null && pwd )"

# Set up logging directory
REPORTS_DIR="$PROJECT_ROOT/tests/integration/pipeline/reports"
mkdir -p "$REPORTS_DIR"

# Check if we should use emulator
if [ "$USE_CLOUD" = "false" ]; then
  # Use emulator - set environment variable
  echo "Using Pub/Sub emulator"
  export PUBSUB_EMULATOR_HOST=localhost:8918
  export USE_CLOUD_PUBSUB=false
  EMULATOR_FLAG="--emulator"
else
  echo "Using cloud Pub/Sub"
  export USE_CLOUD_PUBSUB=true
  # Clear emulator host if set
  unset PUBSUB_EMULATOR_HOST
  EMULATOR_FLAG=""
fi

# First verify and create topics/subscriptions
echo "Setting up topics and subscriptions..."
cd $PROJECT_ROOT
python tests/integration/pipeline/stock_monitoring/test_pubsub.py
if [ $? -ne 0 ]; then
  echo "Error: Failed to set up topics and subscriptions. Exiting."
  exit 1
fi

# Start monitor in background
echo "Starting Pub/Sub monitor (background)..."
python tests/integration/pipeline/stock_monitoring/test_mock.py --duration $DURATION $EMULATOR_FLAG &
MONITOR_PID=$!
echo "Monitor started with PID: $MONITOR_PID"

# Wait for monitor to start
echo "Waiting for monitor to start..."
sleep 3

# Publish test messages
echo "Publishing test messages..."
python tests/integration/pipeline/stock_monitoring/publish_test_messages.py --messages $NUM_MESSAGES $EMULATOR_FLAG
PUBLISH_RESULT=$?

if [ $PUBLISH_RESULT -ne 0 ]; then
  echo "Publishing failed with exit code: $PUBLISH_RESULT"
  kill $MONITOR_PID
  exit $PUBLISH_RESULT
fi

# Wait for monitor to complete
echo "Waiting for monitor to complete..."
wait $MONITOR_PID
MONITOR_RESULT=$?

# Check results
echo "Monitor completed with exit code: $MONITOR_RESULT"

# Display latest results file
LATEST_RESULT=$(ls -t "$REPORTS_DIR"/mock_test_results_*.json | head -1)
if [ -n "$LATEST_RESULT" ]; then
  echo "Test results:"
  cat "$LATEST_RESULT" | python -m json.tool
else
  echo "No results file found."
fi

exit $MONITOR_RESULT 