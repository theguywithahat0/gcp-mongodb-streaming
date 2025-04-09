#!/bin/bash
# Script to run Pub/Sub-based tests

# Parse command line arguments
PROJECT_ID=${1:-jh-testing-project}
TEST_TYPE=${2:-pubsub}  # Either 'pubsub' or 'mock'

echo "=== Stock Monitoring Test Runner ==="
echo "Project: $PROJECT_ID"
echo "Test type: $TEST_TYPE"

# Directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../../.." &> /dev/null && pwd )"

# Run the appropriate test
echo "Running $TEST_TYPE test..."
cd $PROJECT_ROOT

if [ "$TEST_TYPE" = "pubsub" ]; then
  python tests/integration/pipeline/stock_monitoring/test_pubsub.py
elif [ "$TEST_TYPE" = "mock" ]; then
  # Run the mock test with a shorter duration
  python tests/integration/pipeline/stock_monitoring/test_mock.py --duration 10
else
  echo "Unknown test type: $TEST_TYPE. Valid options are 'pubsub' or 'mock'."
  exit 1
fi

# Capture test result
TEST_RESULT=$?

# Report results
if [ $TEST_RESULT -eq 0 ]; then
  echo "Test passed!"
else
  echo "Test failed with exit code: $TEST_RESULT"
fi

exit $TEST_RESULT 