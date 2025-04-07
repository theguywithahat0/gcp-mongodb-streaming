# Google Cloud Pub/Sub Integration

This directory contains a custom implementation of a Pub/Sub sink for publishing MongoDB documents to Google Cloud Pub/Sub.

## ⚠️ Important Notice ⚠️

**This implementation will not be used in the production pipeline**. After evaluation, we've decided to use the native Apache Beam Pub/Sub sink instead, which provides similar functionality with better integration and maintenance.

## Why We're Not Using This

Native Apache Beam provides built-in support for Pub/Sub through the `WriteToPubSub` transform, which already:

1. Handles retries and backoff
2. Provides batching capabilities
3. Includes monitoring and metrics
4. Is regularly updated and tested at scale

For MongoDB integration, we can use a simple `beam.Map` operation before the Pub/Sub writer to extract metadata and prepare messages.

## Code Example: Using Native Beam Instead

Instead of our custom implementation, we'll use the native Beam approach:

```python
def add_attributes(document):
    # Extract metadata from MongoDB document
    metadata = document.get("_metadata", {})
    
    # Return as a tuple of (data, attributes)
    return (
        json.dumps(document).encode('utf-8'),
        {
            "source": metadata.get("source", "mongodb"),
            "connection_id": metadata.get("connection_id", ""),
            "source_name": metadata.get("source_name", "")
        }
    )

# In pipeline
(pipeline
 | beam.io.ReadFromMongoDB(...)  # MongoDB source
 | beam.Map(transform_document)   # Apply transformations
 | beam.Map(add_attributes)      # Format for Pub/Sub
 | beam.io.WriteToPubSub(
     topic="projects/your-project/topics/your-topic",
     with_attributes=True
   )
)
```

For dynamic topic routing:

```python
def determine_topic(document):
    # Logic to determine which topic to use
    if document.get("type") == "order":
        topic = "orders"
    else:
        topic = "default"
    
    # Add topic as an element property
    return beam.pvalue.TaggedOutput(topic, document)

# In pipeline
result = (
    pipeline
    | beam.io.ReadFromMongoDB(...)
    | beam.Map(transform_document)
    | beam.ParDo(determine_topic).with_outputs()
)

# Write to each topic
result.orders | beam.io.WriteToPubSub(topic="projects/your-project/topics/orders")
result.default | beam.io.WriteToPubSub(topic="projects/your-project/topics/default")
```

## Why We Kept This Code

We're keeping this custom implementation as a reference for:

1. How to implement a custom Pub/Sub client with specific MongoDB features
2. Future scenarios where the native Beam implementation might not meet our requirements
3. Educational purposes to understand the Pub/Sub publishing process

## Features

Despite not using this in production, the implementation includes:

- Custom error handling and exception types
- MongoDB document metadata extraction
- Support for message attributes and ordering
- Configurable batch processing
- Multiple topic publishing 