#!/bin/bash

# Exit on any error
set -e

echo "Cleaning up test environment..."

# Check if running with sudo
if [ "$EUID" -ne 0 ]; then 
    echo "Please run with sudo"
    exit 1
fi

# Function to stop process by PID file
stop_process_by_pid_file() {
    local pid_file=$1
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping process with PID $pid"
            kill "$pid" || true
        fi
        rm -f "$pid_file"
    fi
}

# 1. Stop emulators
echo "Stopping emulators..."

# Stop by PID files
stop_process_by_pid_file "/tmp/pubsub-emulator.pid"
stop_process_by_pid_file "/tmp/firestore-emulator.pid"

# Kill any remaining emulator processes
echo "Cleaning up any remaining emulator processes..."
pkill -f "pubsub-emulator" || true
pkill -f "firestore-emulator" || true

# 2. Stop MongoDB
echo "Stopping MongoDB..."
systemctl stop mongod || true

# 3. Clean up MongoDB data (optional)
read -p "Do you want to clean MongoDB data? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleaning MongoDB data..."
    rm -rf /var/lib/mongodb/*
    rm -rf /var/log/mongodb/*
    echo "MongoDB data cleaned"
fi

# 4. Clean up temporary files
echo "Cleaning up temporary files..."
rm -f /tmp/mongodb-27017.sock
rm -rf /tmp/emulator_logs

# 5. Clean up environment variables
unset PUBSUB_EMULATOR_HOST
unset FIRESTORE_EMULATOR_HOST

echo "Cleanup complete!"
echo "Note: If you want to restart the test environment, run setup_test_env.sh" 