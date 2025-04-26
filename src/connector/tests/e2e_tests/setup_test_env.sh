#!/bin/bash

# Exit on any error
set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Setting up test environment..."

# Check if running with sudo
if [ "$EUID" -ne 0 ]; then 
    echo "Please run with sudo"
    exit 1
fi

# Function to check if a port is in use
check_port() {
    local port=$1
    if netstat -tuln | grep -q ":$port "; then
        return 0
    else
        return 1
    fi
}

# Function to wait for a port to be available
wait_for_port() {
    local port=$1
    local timeout=$2
    local count=0
    echo -n "Waiting for port $port"
    while ! check_port "$port"; do
        if [ $count -ge "$timeout" ]; then
            echo " timeout!"
            return 1
        fi
        echo -n "."
        sleep 1
        count=$((count + 1))
    done
    echo " ready!"
    return 0
}

# Function to check if MongoDB is running and replica set is ready
check_mongodb_ready() {
    # Check replica set status
    local status
    status=$(mongosh --quiet --eval "
        const status = rs.status();
        if (status.ok === 1 && status.members && status.members[0].stateStr === 'PRIMARY') {
            print('ready');
        } else {
            print('not_ready');
        }
    ")
    
    if [ "$status" = "ready" ]; then
        return 0
    else
        return 1
    fi
}

# 1. MongoDB Setup
echo "Setting up MongoDB..."

# Run MongoDB setup script using absolute path
"$SCRIPT_DIR/setup_mongodb.sh" || {
    echo "Error: MongoDB setup failed"
    exit 1
}

# Verify MongoDB is running and replica set is ready
echo "Verifying MongoDB setup..."
TIMEOUT=30
while ! check_mongodb_ready && [ $TIMEOUT -gt 0 ]; do
    sleep 1
    TIMEOUT=$((TIMEOUT-1))
    echo -n "."
done
echo ""

if [ $TIMEOUT -eq 0 ]; then
    echo "Error: MongoDB failed to initialize properly"
    echo "Current replica set status:"
    mongosh --eval "rs.status()"
    exit 1
fi

echo "MongoDB setup verified successfully!"

# 2. Clean up existing emulators
echo "Cleaning up existing emulators..."
pkill -f "pubsub-emulator" || true
pkill -f "firestore-emulator" || true

# Wait for ports to be free
sleep 2

# 3. Start emulators
echo "Starting emulators..."

# Create log directory
mkdir -p /tmp/emulator_logs

# Start Pub/Sub emulator
echo "Starting Pub/Sub emulator..."
nohup gcloud beta emulators pubsub start --host-port=localhost:8085 > /tmp/emulator_logs/pubsub.log 2>&1 &
PUBSUB_PID=$!

# Start Firestore emulator
echo "Starting Firestore emulator..."
nohup gcloud beta emulators firestore start --host-port=localhost:8086 > /tmp/emulator_logs/firestore.log 2>&1 &
FIRESTORE_PID=$!

# Wait for emulators to start
echo "Waiting for emulators to start..."
if ! wait_for_port 8085 30; then
    echo "Error: Pub/Sub emulator failed to start"
    cat /tmp/emulator_logs/pubsub.log
    exit 1
fi

if ! wait_for_port 8086 30; then
    echo "Error: Firestore emulator failed to start"
    cat /tmp/emulator_logs/firestore.log
    exit 1
fi

# Save emulator PIDs for cleanup
echo $PUBSUB_PID > /tmp/pubsub-emulator.pid
echo $FIRESTORE_PID > /tmp/firestore-emulator.pid

# Set environment variables
export PUBSUB_EMULATOR_HOST=localhost:8085
export FIRESTORE_EMULATOR_HOST=localhost:8086

echo "Test environment setup complete!"
echo "Environment variables set:"
echo "PUBSUB_EMULATOR_HOST=$PUBSUB_EMULATOR_HOST"
echo "FIRESTORE_EMULATOR_HOST=$FIRESTORE_EMULATOR_HOST"
echo ""
echo "You can now run the tests with:"
echo "PYTHONPATH=$(pwd) PUBSUB_EMULATOR_HOST=localhost:8085 FIRESTORE_EMULATOR_HOST=localhost:8086 python src/connector/examples/basic_e2e_test.py"
echo ""
echo "To view emulator logs:"
echo "tail -f /tmp/emulator_logs/pubsub.log"
echo "tail -f /tmp/emulator_logs/firestore.log" 