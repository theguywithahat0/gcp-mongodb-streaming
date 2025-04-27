# MongoDB Change Stream to Pub/Sub Connector Testing Guide

This guide explains how to set up and run the end-to-end test for the MongoDB Change Stream to Pub/Sub connector.

## Prerequisites

Before running the tests, ensure you have the following installed:
- MongoDB (version 4.0 or later)
- Python (version 3.8 or later)
- Python virtual environment
- sudo privileges (required for MongoDB setup)
- Required Python packages (especially `psutil`)

## Setting Up the Test Environment

1. **Clean Up Any Previous Test Environment**
   ```bash
   sudo ./src/connector/tests/e2e_tests/cleanup_test_env.sh
   ```
   This ensures any previous MongoDB instance and emulators are properly stopped.

2. **Start MongoDB in Replica Set Mode**
   ```bash
   sudo ./src/connector/tests/e2e_tests/setup_test_env.sh
   ```
   This script will:
   - Set up MongoDB in replica set mode
   - Start the Pub/Sub emulator on port 8085
   - Start the Firestore emulator on port 8086
   - Configure all necessary environment variables

3. **Verify Setup**
   - MongoDB should be running in replica set mode
     ```bash
     mongosh --eval "rs.status()"
     ```
     You should see `"set": "rs0"` and `"myState": 1` in the output.
   
   - Pub/Sub emulator should be running on port 8085
   - Firestore emulator should be running on port 8086
     ```bash
     netstat -tuln | grep -E '8085|8086|27017'
     ```
     All three ports should appear in the output.

## Running the End-to-End Test

After the environment is set up, run the test:

```bash
PYTHONPATH=$(pwd) \
PUBSUB_EMULATOR_HOST=localhost:8085 \
FIRESTORE_EMULATOR_HOST=localhost:8086 \
python src/connector/tests/e2e_tests/basic_e2e_test.py
```

If you get a `ModuleNotFoundError` for any required packages such as `psutil`, install them:
```bash
pip install psutil
```

### What the Test Does

The end-to-end test (`basic_e2e_test.py`) validates:
1. **Insert Operation**
   - Inserts a test document into MongoDB
   - Verifies the change is captured and published to Pub/Sub

2. **Update Operation**
   - Updates the test document
   - Verifies the update is captured and published

3. **Delete Operation**
   - Deletes the test document
   - Verifies the deletion is captured and published

The test ensures that:
- Change events are properly captured from MongoDB
- Messages are correctly formatted and published to Pub/Sub
- Operations are processed in the correct order
- Message deduplication is working

### Verifying Test Success

The test will log its progress. A successful test will end with:
```
INFO:__main__:All test assertions passed!
```

You can also check the test_run.log file in the project root for detailed test logs.

## Cleaning Up

When you're done testing, clean up the environment:

```bash
sudo ./src/connector/tests/e2e_tests/cleanup_test_env.sh
```

This will:
- Stop all emulators
- Stop MongoDB replica set
- Clean up temporary files

## Troubleshooting

### Common Issues

1. **MongoDB Replica Set Already Initialized**
   
   Error: `MongoServerError: already initialized`
   
   Solution:
   ```bash
   sudo systemctl stop mongod
   sudo rm -rf /data/db/rs0-0/*
   sudo systemctl start mongod
   sleep 5
   mongosh --eval "rs.initiate();"
   ```

2. **Missing Python Dependencies**
   
   Error: `ModuleNotFoundError: No module named 'psutil'`
   
   Solution:
   ```bash
   pip install psutil
   ```

3. **MongoDB Not Starting**
   - Check MongoDB logs: `sudo journalctl -u mongod`
   - Verify MongoDB is installed: `mongod --version`
   - Ensure ports are available: `netstat -tuln | grep 27017`

4. **Emulator Issues**
   - Verify ports 8085 and 8086 are free: `netstat -tuln | grep -E '8085|8086'`
   - Check emulator processes: `ps aux | grep emulator`
   - Ensure environment variables are set correctly

5. **Test Failures**
   - Check that MongoDB replica set is initialized: `mongosh --eval "rs.status()"`
   - Verify all emulators are running
   - Ensure PYTHONPATH includes project root

### Logs

- MongoDB logs: `sudo journalctl -u mongod`
- Test logs: Check console output or `test_run.log` for detailed logging
- Emulator logs: Check `/tmp/emulator_logs/pubsub.log` and `/tmp/emulator_logs/firestore.log` 