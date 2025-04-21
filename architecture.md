# E-commerce Inventory Management System
## Python-Based Architecture Documentation

This document outlines the complete architecture for the E-commerce Inventory Management real-time monitoring and alerting system, implemented using Python rather than Java. This system tracks inventory levels across multiple warehouses, calculates aggregate stock positions, and triggers alerts when configurable thresholds are breached.

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Component Details](#component-details)
   - [MongoDB Data Sources](#mongodb-data-sources)
   - [Change Stream Connector](#change-stream-connector)
   - [Pub/Sub Topics](#pubsub-topics)
   - [Dataflow Pipeline](#dataflow-pipeline)
   - [Output Components](#output-components)
   - [Monitoring System](#monitoring-system)
4. [Data Flow](#data-flow)
5. [State Management](#state-management)
6. [Threshold Management](#threshold-management)
7. [Multiple Database Handling](#multiple-database-handling)
8. [Deployment Guide](#deployment-guide)
9. [Operations Guide](#operations-guide)
10. [Troubleshooting](#troubleshooting)

---

## Executive Summary

The E-commerce Inventory Management System is a GCP-based streaming pipeline that processes inventory data from MongoDB collections, tracks aggregated stock levels across multiple dimensions, and generates alerts when predefined thresholds are exceeded. The system provides real-time monitoring of inventory across multiple warehouses, product categories, brands, and individual SKUs.

**Primary Business Goals:**
- Monitor real-time inventory levels
- Alert when stock falls below critical thresholds
- Prevent revenue loss from stockouts
- Optimize inventory carrying costs
- Support multiple warehouses and fulfillment centers
- Process millions of transactions efficiently
- Provide reliable alerting with minimal latency

**Key Technical Features:**
- Real-time processing via GCP Dataflow using Python
- Stateful aggregation across product hierarchy
- Configurable threshold management
- Support for multiple database instances
- Fault-tolerant operation with guaranteed delivery
- Comprehensive monitoring and observability

---

## Architecture Overview

![System Architecture Diagram](https://storage.googleapis.com/upload/architecture-diagram-placeholder.png)

The system consists of six primary components:

1. **MongoDB Data Sources** - Inventory and transaction collections
2. **MongoDB Change Stream Connector** - Captures real-time changes and publishes to Pub/Sub
3. **GCP Pub/Sub Topics** - Buffers and distributes messages throughout the system
4. **Dataflow Pipeline** - Core processing engine that maintains state and generates alerts
5. **Output Systems** - Pub/Sub topics for alerts and downstream integrations
6. **Monitoring & Observability** - Cloud Monitoring dashboards and alert mechanisms

---

## Component Details

### MongoDB Data Sources

**Purpose:** Store inventory data and transaction records.

**Collections:**

1. **Product Inventory Collection:**
```javascript
{
  "_id": ObjectId("60a2f3e5c8e4b0e9b9f7c6d5"),
  "sku": "IPHONE13-PRO-256-SILVER",
  "product_id": "IPHONE13-PRO",
  "brand_id": "APPLE",
  "category_id": "SMARTPHONES",
  "department_id": "ELECTRONICS",
  "warehouse_id": "WH-EAST-1",
  "current_stock": 156,
  "reserved_stock": 12,
  "available_stock": 144,
  "min_threshold": 50,
  "reorder_threshold": 100,
  "restock_in_progress": false,
  "last_updated": ISODate("2022-04-20T07:00:00.000Z"),
  "metadata": {
    "weight_kg": 0.2,
    "dimensions_cm": {
      "length": 15,
      "width": 7.5,
      "height": 0.8
    },
    "shelf_location": "E42-B7",
    "supplier_id": "SUPP-APPLE-1"
  }
}
```

2. **Inventory Transaction Collection:**
```javascript
{
  "_id": ObjectId("60b3f4d6c8e4b0e9b9f7c6e6"),
  "transaction_id": "TRX-123456",
  "transaction_type": "ORDER",  // ORDER, RESTOCK, RETURN, ADJUSTMENT
  "sku": "IPHONE13-PRO-256-SILVER",
  "warehouse_id": "WH-EAST-1",
  "quantity": -1,  // Negative for outgoing, positive for incoming
  "order_id": "ORD-789012",  // If applicable
  "po_number": null,  // For restocks
  "return_id": null,  // For returns
  "adjustment_reason": null,  // For adjustments
  "timestamp": ISODate("2022-04-20T07:00:00.000Z"),
  "user_id": "system",  // Who performed the transaction
  "status": "COMPLETED",  // PENDING, COMPLETED, CANCELLED
  "batch_id": "BATCH-456"  // For grouping related transactions
}
```

3. **Threshold Configuration Collection:**
```javascript
{
  "_id": ObjectId("60c4f5e7c8e4b0e9b9f7c6f7"),
  "level_type": "CATEGORY",  // GLOBAL, DEPARTMENT, CATEGORY, BRAND, PRODUCT, SKU
  "level_id": "SMARTPHONES",
  "warehouse_id": "WH-EAST-1",  // null for global settings
  "min_threshold_qty": 50,
  "reorder_threshold_qty": 100,
  "min_threshold_days": 7,  // Stock days coverage
  "reorder_threshold_days": 14,  // Stock days coverage
  "alert_settings": {
    "notification_channel": "EMAIL",
    "recipients": ["inventory@example.com"],
    "cooldown_period_minutes": 60,
    "enabled": true
  },
  "created_at": ISODate("2022-01-01T00:00:00.000Z"),
  "updated_at": ISODate("2022-04-15T00:00:00.000Z"),
  "created_by": "admin"
}
```

**Indexes:**
```javascript
// Inventory Collection Indexes
db.inventory.createIndex({ "warehouse_id": 1, "sku": 1 }, { unique: true })
db.inventory.createIndex({ "warehouse_id": 1, "category_id": 1 })
db.inventory.createIndex({ "warehouse_id": 1, "brand_id": 1 })
db.inventory.createIndex({ "available_stock": 1 })
db.inventory.createIndex({ "last_updated": 1 })

// Transaction Collection Indexes
db.transactions.createIndex({ "sku": 1, "timestamp": 1 })
db.transactions.createIndex({ "warehouse_id": 1, "timestamp": 1 })
db.transactions.createIndex({ "order_id": 1 })
db.transactions.createIndex({ "transaction_type": 1, "timestamp": 1 })
```

**Scaling Considerations:**
- Sharding strategy based on warehouse_id
- Time-based partitioning for transaction history
- Archive strategy for old transactions

### Change Stream Connector

**Purpose:** Streams real-time changes from MongoDB collections into GCP Pub/Sub.

**Key Features:**
- Connects to MongoDB change streams for inventory and transaction collections
- Supports monitoring multiple database instances (warehouses or regions)
- Manages resume tokens for fault tolerance
- Publishes structured messages to Pub/Sub

**Implementation Requirements:**
- Deploy as containerized Cloud Run services
- One service per database instance or shard
- Implement robust error handling and retry mechanisms
- Store resume tokens in a persistent storage (Cloud Firestore recommended)
- Include source identifier with each message
- Implement heartbeat mechanism for health monitoring


**Scaling Considerations:**
- Auto-scale based on CPU utilization and request volume
- Set appropriate memory limits based on document sizes
- Configure max instances based on expected peak load
- Consider one connector instance per database shard

### Pub/Sub Topics

**Input Topics:**
1. **`inventory-transactions-topic`**
   - Purpose: Receives all inventory transaction events
   - Retention: 7 days
   - Message size: Up to 10MB

2. **`inventory-updates-topic`**
   - Purpose: Receives direct inventory level updates
   - Retention: 7 days
   - Message size: Up to 10MB

3. **`thresholds-topic`**
   - Purpose: Receives threshold configuration updates
   - Retention: 7 days
   - Message size: Up to 1MB

4. **`control-topic`**
   - Purpose: Operational commands for pipeline management
   - Retention: 3 days
   - Message size: Up to 1MB

**Output Topics:**
1. **`alerts-topic`**
   - Purpose: Inventory alerts (low stock, stockout)
   - Retention: 7 days
   - Message size: Up to 1MB
   
   **Sample Alert Message:**
   ```json
   {
     "alert_type": "LOW_STOCK",
     "priority": "HIGH",
     "warehouse_id": "WH-EAST-1",
     "sku": "IPHONE13-PRO-256-SILVER",
     "product_name": "iPhone 13 Pro 256GB Silver",
     "current_stock": 45,
     "threshold": 50,
     "timestamp": "2022-04-20T07:05:30.000Z",
     "hierarchy": {
       "department": "Electronics",
       "category": "Smartphones",
       "brand": "Apple",
       "product": "iPhone 13 Pro"
     },
     "restock_recommendation": {
       "suggested_quantity": 100,
       "supplier_id": "SUPP-APPLE-1",
       "typical_lead_time_days": 5
     },
     "alert_id": "alt-20220420-WH-EAST-1-IPHONE13-45",
     "first_breach_time": "2022-04-20T07:00:00.000Z"
   }
   ```

2. **`status-topic`**
   - Purpose: Pipeline status updates
   - Retention: 3 days
   - Message size: Up to 1MB

3. **`dead-letter-topic`**
   - Purpose: Failed message handling
   - Retention: 14 days
   - Message size: Up to 10MB

**Topic Configuration:**
- Enable message ordering for warehouse-specific topics
- Configure appropriate throughput limits based on expected load
- Set up dead-letter topics for each primary topic
- Implement proper IAM permissions

### Dataflow Pipeline

**Purpose:** Process inventory data, maintain state, and generate alerts when thresholds are exceeded.

**Pipeline Stages:**

1. **Input Stage**
   - Reads from `inventory-transactions-topic` and `inventory-updates-topic`
   - Parses JSON messages
   - Routes messages based on type and warehouse ID

2. **Document Processing Stage**
   - Extracts relevant fields from inventory documents
   - Transforms transaction data to stock impacts
   - Normalizes data formats (converts strings to numerics)
   - Filters out irrelevant events (e.g., canceled transactions)

3. **Warehouse & Hierarchy Assignment Stage**
   - Partitions data by warehouse ID
   - Maps data to the appropriate place in the product hierarchy
   - Extracts and validates hierarchy identifiers:
     - Department ID
     - Category ID
     - Brand ID
     - Product ID
     - SKU

4. **State Management Stage**
   - Maintains hierarchical state store (see State Management section)
   - Updates stock levels based on transactions
   - Tracks low stock conditions
   - Implements state cleanup for discontinued products

5. **Threshold Evaluation Stage**
   - Compares current stock against configured thresholds
   - Identifies threshold breaches
   - Implements hysteresis to prevent alert storms
   - Generates alert messages

6. **Output Stage**
   - Enriches alerts with product details and context
   - Publishes alerts to `alerts-topic`
   - Emits status updates to `status-topic`
   - Routes failed messages to `dead-letter-topic`

**Main Pipeline Implementation:**
```python
def run_pipeline():
    """Creates and runs the inventory monitoring pipeline."""
    pipeline_options = PipelineOptions([
        '--project=your-gcp-project-id',
        '--runner=DataflowRunner',
        '--region=us-central1',
        '--staging_location=gs://your-bucket/staging',
        '--temp_location=gs://your-bucket/temp',
        '--streaming=true',
        '--job_name=inventory-monitoring-pipeline',
    ])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Pub/Sub
        inventory_updates = (
            pipeline
            | "Read Inventory Updates" >> beam.io.ReadFromPubSub(
                subscription="projects/your-gcp-project-id/subscriptions/inventory-updates-subscription"
            )
            | "Decode Updates" >> beam.Map(lambda x: x.decode('utf-8'))
        )
        
        inventory_transactions = (
            pipeline
            | "Read Inventory Transactions" >> beam.io.ReadFromPubSub(
                subscription="projects/your-gcp-project-id/subscriptions/inventory-transactions-subscription"
            )
            | "Decode Transactions" >> beam.Map(lambda x: x.decode('utf-8'))
        )
        
        # Combine streams
        inventory_events = ((inventory_updates, inventory_transactions)
            | "Merge Streams" >> beam.Flatten()
            | "Parse Messages" >> ParseAndValidateMessages()
        )
        
        # Create composite key for state lookup
        keyed_events = (
            inventory_events
            | "Create Keys" >> beam.Map(
                lambda event: (f"{event.warehouse_id}:{event.sku}", event)
            )
        )
        
        # Process events and update state
        alerts = (
            keyed_events
            | "Update State & Generate Alerts" >> beam.ParDo(UpdateInventoryStateFn())
            | "Flatten Alerts" >> beam.FlatMap(lambda alerts: alerts)
        )
        
        # Enrich alerts with product details (could use side inputs)
        enriched_alerts = (
            alerts
            | "Enrich Alerts" >> beam.Map(enrich_alert)
        )
        
        # Output alerts to Pub/Sub
        enriched_alerts | "Publish Alerts" >> beam.io.WriteToPubSub(
            topic="projects/your-gcp-project-id/topics/alerts-topic",
            with_attributes=False
        )

def enrich_alert(alert):
    """Enriches alert with product details and recommendations."""
    # In a real implementation, you would fetch product details from a database
    # or use side inputs to enrich the alert with more context
    
    # For demonstration purposes, we'll add some static enrichment
    sku = alert.get("sku", "")
    alert["product_name"] = f"Product for SKU {sku}"
    alert["hierarchy"] = {
        "department": "Example Department",
        "category": "Example Category",
        "brand": "Example Brand",
        "product": "Example Product"
    }
    alert["restock_recommendation"] = {
        "suggested_quantity": 100,
        "supplier_id": "SUPP-EXAMPLE-1",
        "typical_lead_time_days": 5
    }
    alert["alert_id"] = f"alt-{datetime.now().strftime('%Y%m%d')}-{alert.get('warehouse_id', '')}-{sku}"
    
    return alert

if __name__ == "__main__":
    run_pipeline()
```

**Dataflow Configuration:**
- **Runner**: Dataflow Runner (production)
- **Processing Guarantees**: At-least-once 
- **Streaming Engine**: Enabled
- **Autoscaling**: Enabled with appropriate min/max workers
- **Machine Type**: n1-standard-2 (adjust based on load testing)
- **Disk Size**: 50GB (adjust based on state size projections)
- **Max Workers**: Calculated based on expected throughput
- **Job Type**: Streaming

### Output Components

**Alerts Output:**
- **Format**: Structured JSON
- **Delivery**: Pub/Sub with guaranteed delivery
- **Alert Enrichment**:
  - Product details (name, category)
  - Supplier information
  - Restock recommendations
  - Historical context (stock trend)

**Status Output:**
- Periodic updates on pipeline health
- Aggregated metrics for monitoring
- Batch summary statistics

**Dead Letter Handling:**
- Captures messages that fail processing
- Includes error details and stack traces
- Enables manual or automated reprocessing

### Monitoring System

**Cloud Monitoring Dashboards:**
1. **Pipeline Health Dashboard**
   - Processing latency
   - Throughput metrics
   - Error rates
   - Worker utilization

2. **Inventory Metrics Dashboard**
   - Stock levels by category/warehouse
   - Items below threshold
   - Stockout rates
   - Restock status

**Alert Policies:**
1. **Operational Alerts:**
   - Pipeline stalled (no progress for > 5 minutes)
   - High error rate (> 1% of messages)
   - Excessive processing latency (> 2 minutes)
   - Connector disconnection

2. **Business Alerts:**
   - Multiple stockouts in same category
   - Unusual depletion rates
   - Restock delivery failures
   - Threshold breach trends

**Log-based Metrics:**
- Custom metrics based on structured logging
- Error categorization and trending
- Performance benchmarking

---

## Data Flow

### 1. Data Capture Flow
1. Inventory transaction occurs (order, restock, return)
2. Transaction is recorded in MongoDB
3. Change stream captures the modification
4. MongoDB connector formats and publishes to Pub/Sub
5. Message arrives in appropriate Pub/Sub topic

### 2. Processing Flow
1. Dataflow pipeline reads from Pub/Sub topics
2. Message is parsed and routed based on type and warehouse
3. For transaction messages:
   - Transaction is converted to stock impact
   - Hierarchical keys are extracted
   - Current state is retrieved
   - Stock levels are updated
   - Thresholds are evaluated
   - State is updated and persisted
4. For inventory update messages:
   - Direct stock level update
   - Thresholds are evaluated
   - State is updated

### 3. Alert Flow
1. Threshold breach is detected
2. Alert message is generated with context
3. Alert is published to `alerts-topic`
4. Downstream systems consume the alert
5. Alert state is updated to prevent duplicates

### 4. Cleanup Flow
1. Periodically check for inactive inventory items
2. Archive state data for historical analysis
3. Remove state to free resources

---

## State Management

The state management system maintains current stock levels and alert status for all inventory items.

### State Structure

**Hierarchical Keys:**
- Level 1: Warehouse ID
- Level 2: Department ID 
- Level 3: Category ID
- Level 4: Brand ID
- Level 5: Product ID
- Level 6: SKU

**State Entry Structure:**
```json
{
  "key": "WH-EAST-1:ELECTRONICS:SMARTPHONES:APPLE:IPHONE13-PRO:IPHONE13-PRO-256-SILVER",
  "hierarchy": {
    "warehouse_id": "WH-EAST-1",
    "department_id": "ELECTRONICS",
    "category_id": "SMARTPHONES",
    "brand_id": "APPLE",
    "product_id": "IPHONE13-PRO",
    "sku": "IPHONE13-PRO-256-SILVER"
  },
  "names": {
    "department_name": "Electronics",
    "category_name": "Smartphones",
    "brand_name": "Apple",
    "product_name": "iPhone 13 Pro",
    "variant_name": "256GB Silver"
  },
  "stock_levels": {
    "current_stock": 155,
    "reserved_stock": 12,
    "available_stock": 143,
    "inbound_stock": 0
  },
  "thresholds": {
    "min_threshold": 50,
    "reorder_threshold": 100
  },
  "alert_status": {
    "stockout_alerted": false,
    "low_stock_alerted": false,
    "reorder_alerted": false,
    "last_alert_time": null,
    "restock_in_progress": false
  },
  "metadata": {
    "last_transaction_id": "TRX-123456",
    "last_transaction_time": "2022-04-20T07:00:00.000Z",
    "last_update_time": "2022-04-20T07:01:10.000Z",
    "transaction_count": 157,
    "active": true
  }
}
```

### State Management Operations

**1. State Initialization:**
- Create initial state entries when first transaction is received
- Populate hierarchy and metadata fields
- Initialize stock levels
- Fetch applicable thresholds

**2. State Updates:**
- Process transactions to update stock levels
- Handle different transaction types (orders, restocks, returns, adjustments)
- Update metadata (transaction count, timestamps)
- Handle direct inventory updates

**3. Threshold Evaluation:**
- Compare current stock to threshold values
- Track alert status to prevent duplicates
- Implement cooldown periods between alerts
- Reset alert flags when stock recovers

**4. State Aggregation:**
- Maintain aggregate totals at each hierarchical level
- Support querying by category, brand, or warehouse
- Enable drill-down from aggregates to individual SKUs

**5. State Cleanup:**
- Use activity timestamps for cleanup decisions
- Implement TTL-based cleanup
- Archive state before removal
- Handle discontinued products

### State Backend Implementation

Using Apache Beam's Python state API:

```python
# State specification for inventory item
INVENTORY_STATE = ValueStateSpec('inventoryState')

# Timer specification for cleanup
CLEANUP_TIMER = TimerSpec('cleanupTimer', TimeDomain.PROCESSING_TIME)

# Example of using the state API in a DoFn
def process(self, element, 
           inventory_state=beam.DoFn.StateParam(INVENTORY_STATE),
           cleanup_timer=beam.DoFn.TimerParam(CLEANUP_TIMER)):
    # Read state
    current_state_json = inventory_state.read()
    
    # Process and update state
    # ...
    
    # Write state back
    inventory_state.write(json.dumps(updated_state.to_dict()))
    
    # Set timer for cleanup
    cleanup_timer.set(time.time() + 24 * 60 * 60)
```

**Partitioning Strategy:**
- Primary partition by warehouse ID
- Secondary partition by category ID
- Balance partitioning for optimal performance
- Monitor partition sizes and adjust as needed

---

## Threshold Management

### Threshold Structure

```json
{
  "level_type": "CATEGORY",
  "level_id": "SMARTPHONES",
  "warehouse_id": "WH-EAST-1",
  "min_threshold_qty": 50,
  "reorder_threshold_qty": 100,
  "min_threshold_days": 7,
  "reorder_threshold_days": 14,
  "alert_settings": {
    "notification_channel": "EMAIL",
    "recipients": ["inventory@example.com"],
    "cooldown_period_minutes": 60,
    "enabled": true
  }
}
```

### Threshold Hierarchy

Thresholds can be defined at multiple levels with inheritance:
1. **Global Level** - Applies to all products unless overridden
2. **Department Level** - Applies to all products in department
3. **Category Level** - Applies to all products in category
4. **Brand Level** - Applies to all products of a brand
5. **Product Level** - Applies to all SKUs of a product
6. **SKU Level** - Specific to a single SKU

### Threshold Resolution Logic

1. Start at the most specific level (SKU)
2. If no threshold found, move up to Product level
3. Continue up the hierarchy until a threshold is found
4. If no threshold found at any level, use system default

### Threshold Types

1. **Quantity-based Thresholds**:
   - Fixed quantity values
   - Simple to implement
   - Same across all products

2. **Coverage-based Thresholds**:
   - Based on days of inventory coverage
   - Requires sales velocity data
   - Adapts to product demand patterns

3. **Dynamic Thresholds**:
   - Calculated based on historical data
   - Adjusts for seasonality
   - Requires machine learning models

### Threshold Updates

1. Threshold updates come through the `thresholds-topic`
2. Pipeline processes updates and refreshes threshold store
3. Affected inventory items are immediately re-evaluated
4. Changes are logged for audit purposes

---

## Multiple Database Handling

### Multi-Database Architecture

This system supports multiple MongoDB databases, which can represent different warehouses, regions, or business units.

**Architecture Approaches:**

1. **Multiple Connectors** (Recommended):
   - One connector per database instance
   - Each runs as a separate Cloud Run service
   - Independent resume token management
   - Parallel processing of change streams

2. **Single Connector with Multiple Streams**:
   - One connector managing multiple change streams
   - More complex resume token management
   - Potential bottleneck for high-volume scenarios
   - Simpler deployment but less scalable

### Multiple Connector Implementation

**Deployment Model:**
- One Cloud Run service per warehouse/database
- Individual service account for each connector
- Separate resume token management
- Common message format with source identification

**Connector Configuration:**
```yaml
# Example connector configuration for Warehouse East-1
CONNECTOR_ID: "connector-wh-east-1"
WAREHOUSE_ID: "WH-EAST-1"
MONGODB_URI: "mongodb://user:pass@mongodb-east-1:27017/inventory"
PUBSUB_PROJECT: "your-gcp-project-id"
INVENTORY_TOPIC: "inventory-updates-topic"
TRANSACTIONS_TOPIC: "inventory-transactions-topic"
STATUS_TOPIC: "status-topic"
RESUME_TOKEN_COLLECTION: "resumeTokens"
COLLECTIONS_TO_WATCH: "inventory,transactions"
```

**Message Enrichment:**
Every message published includes:
- Database source identifier (warehouse_id)
- Connector identifier
- Timestamp when processed by connector

### Synchronization Considerations

**Consistent Processing:**
- All messages include source identification
- Pipeline partitions processing by source
- State management isolated by warehouse
- Thresholds can be warehouse-specific

**Cross-Warehouse Aggregation:**
- Secondary pipeline can aggregate across warehouses
- Global inventory view maintained separately
- Transfer transactions handled specially

**Asynchronous Nature:**
- Each connector operates independently
- Processing latency may vary between warehouses
- Dashboard shows connectivity status of each connector
- System handles warehouse unavailability gracefully

### Scaling Strategy

**Horizontal Scaling:**
- Add new connector for each new warehouse
- No changes needed to existing connectors
- Pipeline automatically handles new data sources
- Alert rules can be customized per warehouse

**Balancing Considerations:**
- Monitor connector resource usage
- Adjust memory/CPU allocations based on database size
- Consider throughput needs for high-volume warehouses
- Balance Dataflow workers across warehouse partitions

---

## Deployment Guide

### 1. Infrastructure Setup

**Prerequisites:**
- GCP Project with billing enabled
- Required API services activated:
  - Pub/Sub API
  - Dataflow API
  - Monitoring API
  - Cloud Run API
- Service accounts with appropriate permissions
- Network configuration (VPC, firewall rules, etc.)

**Resource Creation:**
```bash
# Create Pub/Sub topics
gcloud pubsub topics create inventory-transactions-topic
gcloud pubsub topics create inventory-updates-topic
gcloud pubsub topics create thresholds-topic
gcloud pubsub topics create control-topic
gcloud pubsub topics create alerts-topic
gcloud pubsub topics create status-topic
gcloud pubsub topics create dead-letter-topic

# Create subscriptions
gcloud pubsub subscriptions create alerts-subscription --topic=alerts-topic
gcloud pubsub subscriptions create status-subscription --topic=status-topic
gcloud pubsub subscriptions create dead-letter-subscription --topic=dead-letter-topic
```

### 2. MongoDB Connector Deployment

**Build and Deploy:**
```bash
# For each warehouse/database
# Create Dockerfile
cat > Dockerfile << EOF
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY connector.py .

CMD ["python", "connector.py"]
EOF

# Create requirements.txt
cat > requirements.txt << EOF
pymongo==4.1.1
google-cloud-pubsub==2.13.0
google-cloud-firestore==2.6.0
EOF

# Build Docker image
docker build -t gcr.io/[PROJECT-ID]/mongodb-connector-[WAREHOUSE-ID]:v1 .

# Push to Container Registry
docker push gcr.io/[PROJECT-ID]/mongodb-connector-[WAREHOUSE-ID]:v1

# Deploy to Cloud Run
gcloud run deploy mongodb-connector-[WAREHOUSE-ID] \
  --image gcr.io/[PROJECT-ID]/mongodb-connector-[WAREHOUSE-ID]:v1 \
  --memory 1Gi \
  --concurrency 80 \
  --max-instances 10 \
  --set-env-vars="MONGODB_URI=mongodb://user:pass@mongodb-[WAREHOUSE-ID]:27017/inventory,WAREHOUSE_ID=[WAREHOUSE-ID],PUBSUB_PROJECT=[PROJECT-ID]"
```

### 3. Dataflow Pipeline Deployment

**Setup and Deploy:**
```bash
# Create requirements.txt for Dataflow
cat > requirements.txt << EOF
apache-beam[gcp]==2.40.0
EOF

# Set environment variables
export PROJECT=[PROJECT-ID]
export REGION=us-central1
export BUCKET=$PROJECT-dataflow
export PIPELINE_FOLDER=gs://$BUCKET/pipelines

# Create GCS bucket if it doesn't exist
gsutil mb -l $REGION gs://$BUCKET

# Deploy the pipeline
python pipeline.py \
  --project=$PROJECT \
  --region=$REGION \
  --staging_location=$PIPELINE_FOLDER/staging \
  --temp_location=$PIPELINE_FOLDER/temp \
  --runner=DataflowRunner \
  --streaming=True \
  --job_name=inventory-monitoring-pipeline \
  --setup_file=./setup.py
```

### 4. Monitoring Setup

**Create Dashboards:**
```bash
# Create monitoring dashboard from template
gcloud monitoring dashboards create --config-from-file=dashboard.json
```

**Configure Alert Policies:**
```bash
# Create alert policies from template
gcloud alpha monitoring policies create --policy-from-file=alerts.json
```

---

## Operations Guide

### Daily Operations

**Health Checks:**
- Verify MongoDB connectors are running for all warehouses
- Check Dataflow pipeline status and worker health
- Monitor error rates and processing latency
- Review alert logs for unusual patterns

**Scaling Considerations:**
- Monitor resource utilization during peak shopping periods
- Adjust autoscaling parameters as needed
- Consider manual scaling before major sales events

**Data Validation:**
- Periodically validate stock levels against source data
- Verify threshold evaluations are working correctly
- Check for data consistency across the pipeline

### Incident Response

**Pipeline Stalled:**
1. Check Dataflow UI for worker errors
2. Verify MongoDB connectivity for all warehouses
3. Check for backlog in Pub/Sub
4. Restart pipeline if necessary

**High Error Rate:**
1. Examine error logs for patterns
2. Check for schema changes or data format issues
3. Verify MongoDB connectors are functioning correctly
4. Deploy fixes and restart components as needed

**Missing Alerts:**
1. Verify threshold configurations
2. Check stock level calculations
3. Examine alert generation logic
4. Test with controlled test data

### Maintenance Procedures

**Pipeline Updates:**
1. Deploy new version in parallel
2. Validate output matches expectations
3. Switch traffic to new version
4. Monitor for any issues
5. Keep previous version running until stability confirmed

**Adding New Warehouse:**
1. Deploy new MongoDB connector for the warehouse
2. Configure resume token storage
3. Verify data flow through pipeline
4. Set up appropriate thresholds
5. Add to monitoring dashboards

---

## Troubleshooting

### Common Issues and Solutions

**Issue: MongoDB Connector Disconnections**
- **Symptoms:** Gaps in data, stalled pipeline
- **Possible Causes:** Network issues, MongoDB server restarts
- **Solutions:**
  - Verify network connectivity
  - Check MongoDB logs for server issues
  - Ensure resume token storage is working
  - Implement automatic reconnection with backoff

**Issue: Processing Latency Spikes**
- **Symptoms:** Increased end-to-end latency, growing backlog
- **Possible Causes:** Resource constraints, hot keys, complex documents
- **Solutions:**
  - Check Dataflow autoscaling
  - Review key distribution for hotspots
  - Optimize document parsing
  - Add more workers temporarily

**Issue: Missing or Duplicate Alerts**
- **Symptoms:** Alerts not firing or firing multiple times
- **Possible Causes:** State corruption, threshold misconfiguration
- **Solutions:**
  - Verify threshold configurations
  - Check state store integrity
  - Review alert deduplication logic
  - Reset state for affected entities

**Issue: State Size Growth**
- **Symptoms:** Increasing memory usage, slower state operations
- **Possible Causes:** Insufficient cleanup, many active products
- **Solutions:**
  - Verify cleanup logic is running
  - Adjust TTL settings
  - Implement state compression
  - Archive old state data

### Debugging Tools

**Logging:**
- Enable debug logging temporarily for components
- Use structured logging for easier analysis
- Create log-based metrics for recurring patterns

**Monitoring:**
- Set up custom metrics for specific components
- Create focused dashboards for problem areas
- Use Cloud Trace for performance analysis

**Testing:**
- Implement end-to-end test scenarios
- Use synthetic data for load testing
- Create chaos tests for resilience validation