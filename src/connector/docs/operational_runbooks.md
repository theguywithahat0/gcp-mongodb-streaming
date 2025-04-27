# MongoDB Change Stream Connector
# Operational Runbooks

This document contains runbooks for diagnosing and resolving common operational issues with the MongoDB Change Stream Connector.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisite Knowledge](#prerequisite-knowledge)
3. [Common Failure Modes](#common-failure-modes)
   - [MongoDB Connection Issues](#mongodb-connection-issues)
   - [Pub/Sub Publishing Failures](#pubsub-publishing-failures)
   - [Resume Token Storage Issues](#resume-token-storage-issues)
   - [Message Transformation Errors](#message-transformation-errors)
   - [Resource Exhaustion](#resource-exhaustion)
   - [Data Protection Issues](#data-protection-issues)
4. [Monitoring Alerts Reference](#monitoring-alerts-reference)
5. [Regular Maintenance Tasks](#regular-maintenance-tasks)

## Overview

The MongoDB Change Stream Connector streams data changes from MongoDB collections to Google Cloud Pub/Sub. This document provides troubleshooting procedures for common failure scenarios, helping operators quickly resolve issues to maintain system availability.

### Component Architecture

The connector consists of the following main components:
- MongoDB change stream watchers
- Document transformers (including data protection)
- Pub/Sub publishers
- Resume token management
- Health check endpoints

## Prerequisite Knowledge

Operators should be familiar with:
- Basic MongoDB concepts (collections, change streams, etc.)
- Google Cloud Pub/Sub topics and subscriptions
- Google Cloud logging and monitoring
- Basic knowledge of Python applications

## Common Failure Modes

### MongoDB Connection Issues

#### Symptoms
- Log messages: "Failed to connect to MongoDB", "Connection reset by peer"
- Health check endpoint shows MongoDB connection as unhealthy
- No messages being published to Pub/Sub topics

#### Possible Causes
1. Network connectivity issues
2. MongoDB server down
3. Authentication failure
4. MongoDB replica set reconfiguration

#### Remediation Steps

**Check MongoDB connectivity:**
```bash
# From the connector environment, test connectivity
mongo mongodb://<host>:<port>/test --ssl --username <username> --password <password>
```

**Verify network path:**
```bash
# Test network connectivity
nc -zv <mongodb_host> <mongodb_port>
```

**Check MongoDB server status:**
If you have direct access to the MongoDB server:
```bash
# Check MongoDB process
systemctl status mongod

# Check MongoDB logs
tail -100 /var/log/mongodb/mongod.log
```

**Check authentication configuration:**
Verify that the credentials in the connector's configuration match what's configured in MongoDB.

**Restart connector after resolving upstream issue:**
```bash
# If running on Cloud Run, restart the service
gcloud run services update mongodb-connector --region=<region>
```

### Pub/Sub Publishing Failures

#### Symptoms
- Log messages: "Failed to publish message to Pub/Sub", "DeadlineExceeded"
- Health check shows Pub/Sub as unhealthy
- Change events are processed but not delivered to subscribers

#### Possible Causes
1. Pub/Sub service disruption
2. Topic doesn't exist
3. Insufficient permissions
4. Resource exhaustion (quotas)

#### Remediation Steps

**Check Pub/Sub topic existence and permissions:**
```bash
# List topics to ensure they exist
gcloud pubsub topics list | grep <topic_name>

# Check IAM policy
gcloud pubsub topics get-iam-policy <topic_name>
```

**Verify service account permissions:**
```bash
# List service account roles
gcloud projects get-iam-policy <project_id> \
  --flatten="bindings[].members" \
  --format="table(bindings.role)" \
  --filter="bindings.members:serviceAccount:<service_account>"
```

**Check Pub/Sub quotas and metrics:**
In Google Cloud Console, navigate to Pub/Sub > Topics > [Your Topic] > Metrics to check for quota issues or unusual patterns.

**Restart connector with circuit breaker reset:**
```bash
# Restart the service to reset circuit breakers
gcloud run services update mongodb-connector --region=<region>
```

### Resume Token Storage Issues

#### Symptoms
- Log messages: "Failed to save resume token", "Failed to retrieve resume token"
- Duplicate messages appearing in Pub/Sub
- Missing events (gaps in data)

#### Possible Causes
1. Firestore connectivity issues
2. Permissions problems
3. Corrupted resume token

#### Remediation Steps

**Check Firestore connectivity and permissions:**
```bash
# Check Firestore database existence
gcloud firestore databases list

# Verify IAM permissions for Firestore
gcloud projects get-iam-policy <project_id> \
  --flatten="bindings[].members" \
  --format="table(bindings.role)" \
  --filter="bindings.members:serviceAccount:<service_account> AND bindings.role:roles/datastore*"
```

**Verify resume token documents:**
Use the Firestore console to examine the resume token collection for corrupted or missing tokens.

**Reset resume token (last resort):**
```bash
# Using firestore CLI or console, delete the resume token
# WARNING: This will cause reprocessing of data and potential duplicates
gcloud firestore documents delete projects/<project-id>/databases/(default)/documents/resumeTokens/<collection_name>
```

### Message Transformation Errors

#### Symptoms
- Log messages: "Failed to transform document", "Schema validation error", "Data protection error"
- Events not appearing in Pub/Sub
- Increasing error count metrics

#### Possible Causes
1. Schema validation failures
2. Data format changes in source system
3. Security transformation errors
4. Custom transformation errors

#### Remediation Steps

**Examine error logs for specific transformation errors:**
```bash
# Query logs for transformation failures
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=mongodb-connector AND textPayload:\"transformation failed\"" --limit=10
```

**Check document format:**
Use MongoDB's console to examine the problematic documents:

```javascript
// Connect to MongoDB and query for recent documents
db.collection.find().sort({_id: -1}).limit(5)
```

**Verify schema registry configuration:**
Check if the schema definition matches the actual document structure.

**Test data protection with problematic document:**
Use the data protection example script to test processing the problematic document:

```bash
# Run example with problematic document
python -m connector.examples.data_protection_example --document='{...}'
```

**Update schema or transformation logic if needed:**
If the source data format has legitimately changed, update the schema or transformation logic accordingly.

### Resource Exhaustion

#### Symptoms
- Log messages: "Memory usage exceeds threshold", "Out of memory error"
- Container restarts
- Slow processing or increased latency

#### Possible Causes
1. Insufficient memory allocation
2. Memory leaks
3. Large message processing
4. Excessive concurrent processing

#### Remediation Steps

**Check resource usage metrics:**
In Google Cloud Console, navigate to Cloud Run > [Service] > Metrics to check CPU and memory usage.

**Adjust resource allocation:**
```bash
# Increase memory allocation
gcloud run services update mongodb-connector \
  --memory=2Gi \
  --region=<region>
```

**Check for large change stream batch sizes:**
If processing large batches of changes, adjust the batch size configuration:

```yaml
# In config.yaml
mongodb:
  change_stream:
    batch_size: 100  # Reduce this value
```

**Enable memory profiling for debugging:**
For persistent issues, enable memory profiling and analyze the results:

```bash
# Set environment variable to enable profiling
gcloud run services update mongodb-connector \
  --set-env-vars="ENABLE_MEMORY_PROFILING=true" \
  --region=<region>

# After reproducing the issue, retrieve and analyze profiles
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=mongodb-connector AND textPayload:\"Memory profile saved\"" --limit=10
```

### Data Protection Issues

#### Symptoms
- Log messages: "Encryption failed", "Failed to apply protection to field", "Key rotation failed"
- Security sensitive data appearing unprotected in Pub/Sub messages
- Errors during document transformation

#### Possible Causes
1. Missing encryption key
2. Invalid or corrupted key
3. Unsupported data types
4. Configuration errors in field matchers

#### Remediation Steps

**Verify encryption key availability:**
```bash
# Check if the ENCRYPTION_KEY environment variable is set
gcloud run services describe mongodb-connector --region=<region> | grep ENCRYPTION_KEY
```

**Check data protection configuration:**
Review the data protection rules configuration for errors:

```yaml
# Example data protection configuration
data_protection:
  rules:
    - field_matcher: "customer.email"
      protection_level: "hash"
      options:
        algorithm: "sha256"
        include_salt: true
```

**Rotate encryption key (if corrupted):**
```bash
# Generate new key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Update the key in the service
gcloud run services update mongodb-connector \
  --set-env-vars="ENCRYPTION_KEY=<new_key>" \
  --region=<region>
```

**Debug field matching problems:**
Use logging to identify which fields aren't being properly matched:

```bash
# Enable debug logging temporarily
gcloud run services update mongodb-connector \
  --set-env-vars="LOG_LEVEL=DEBUG" \
  --region=<region>

# Check logs for field matching problems
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=mongodb-connector AND textPayload:\"field matcher\"" --limit=20
```

## Monitoring Alerts Reference

This section describes common monitoring alerts and their suggested response actions.

### High Error Rate Alert

**Alert condition:** Error rate exceeds 5% of operations for 5 minutes

**Response:**
1. Check logs for error pattern and specific error messages
2. If MongoDB connection errors, follow [MongoDB Connection Issues](#mongodb-connection-issues) remediation
3. If Pub/Sub errors, follow [Pub/Sub Publishing Failures](#pubsub-publishing-failures) remediation
4. If transformation errors, follow [Message Transformation Errors](#message-transformation-errors) remediation
5. If unresolved, escalate to development team

### Resume Token Failure Alert

**Alert condition:** 3 consecutive failures to save or retrieve resume tokens

**Response:**
1. Check Firestore status and connectivity
2. Verify service account permissions
3. Follow [Resume Token Storage Issues](#resume-token-storage-issues) remediation
4. Monitor for duplicate messages after resolution

### Memory Usage Alert

**Alert condition:** Memory usage exceeds 85% for 10 minutes

**Response:**
1. Check for sudden increase in change stream volume
2. Follow [Resource Exhaustion](#resource-exhaustion) remediation
3. Consider scaling up the service temporarily
4. Monitor for regular patterns requiring permanent scaling

### Data Protection Failure Alert

**Alert condition:** Data protection errors occur for 3 consecutive minutes

**Response:**
1. Check if encryption key is properly configured
2. Verify no unauthorized changes to protection rules
3. Follow [Data Protection Issues](#data-protection-issues) remediation
4. Consider temporarily pausing the service if sensitive data might be exposed

## Regular Maintenance Tasks

These tasks should be performed regularly to ensure the connector's health:

### Weekly Tasks

1. **Review error logs:**
   ```bash
   gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=mongodb-connector AND severity>=ERROR" --limit=100
   ```

2. **Check resource utilization trends:**
   Review CPU, memory, and network usage patterns in Cloud Monitoring dashboards.

3. **Verify health check status:**
   ```bash
   curl https://<connector-url>/health
   curl https://<connector-url>/readiness
   ```

### Monthly Tasks

1. **Rotate encryption keys:**
   Generate new encryption keys and update the connector configuration.

2. **Review access permissions:**
   Audit service account permissions to ensure least privilege.

3. **Check for connector version updates:**
   Review release notes for new versions and security updates.

4. **Test disaster recovery:**
   Perform a DR drill by stopping and restarting the connector to verify resume token functionality.

### Quarterly Tasks

1. **Performance review:**
   Analyze throughput metrics, latency, and resource utilization.

2. **Security review:**
   Review data protection rules and ensure compliance with security policies.

3. **Configuration audit:**
   Review all configuration parameters for adherence to best practices.

4. **Documentation update:**
   Update runbooks with any new failure modes or improved remediation procedures. 