#!/bin/bash

# Exit on any error
set -e

echo "Setting up MongoDB replica set..."

# Stop MongoDB if running
systemctl stop mongod || true

# Create directories if they don't exist
mkdir -p /data/db/rs0-0
chown -R mongodb:mongodb /data/db

# Create MongoDB configuration file
cat > /etc/mongod.conf << EOF
systemLog:
  destination: file
  path: /var/log/mongodb/mongod.log
  logAppend: true

storage:
  dbPath: /data/db/rs0-0

net:
  bindIp: localhost
  port: 27017

replication:
  replSetName: rs0

processManagement:
  timeZoneInfo: /usr/share/zoneinfo
EOF

# Start MongoDB
systemctl start mongod

# Wait for MongoDB to start
echo "Waiting for MongoDB to start..."
sleep 5

# Initialize replica set
echo "Initializing replica set..."
mongosh --eval '
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "localhost:27017" }
    ]
  })
'

# Wait for replica set to initialize
echo "Waiting for replica set to initialize..."
sleep 5

echo "MongoDB replica set setup complete!" 