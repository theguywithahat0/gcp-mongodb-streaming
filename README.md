# MongoDB Change Stream to Google Cloud Pub/Sub Connector

A high-performance, fault-tolerant connector that streams MongoDB change events to Google Cloud Pub/Sub topics.

## Features

### Core Functionality
- Real-time streaming of MongoDB change events to Pub/Sub
- Support for multiple collections and topics
- Full document lookups for change events
- Schema validation and migration
- Configurable retry policies
- Heartbeat monitoring
- Resume token persistence

### Performance & Reliability
- **Message Batching**: Efficient batching of messages with configurable:
  - Maximum batch size
  - Maximum batch bytes
  - Maximum latency
  - Retry settings

- **Circuit Breaker Pattern**: Prevents system overload and cascading failures:
  - Three states: CLOSED (normal), OPEN (failing), HALF-OPEN (testing)
  - Configurable failure thresholds and recovery timeouts
  - Automatic state transitions based on success/failure rates
  - Per-topic circuit breaker settings

- **Message Deduplication**: Prevents duplicate message processing:
  - Two-level deduplication (memory and persistent storage)
  - In-memory cache for fast duplicate detection
  - Firestore-based persistent deduplication
  - Configurable TTL and cache sizes per collection
  - Automatic cleanup of expired entries

- **Log Sampling**: Intelligent log reduction for high-volume environments:
  - Multiple sampling strategies:
    - Probabilistic: Random sampling based on rates
    - Rate-limiting: Maximum messages per time window
    - Deterministic: Consistent sampling based on content
  - Configurable per log level and message type
  - Preservation of critical logs (errors and above)
  - Automatic cleanup of sampling caches

### Monitoring & Observability
- Structured logging with sampling
- Health check endpoints
- Heartbeat monitoring
- Performance metrics
- Error tracking and reporting

## Configuration

### Message Batching
```yaml
pubsub:
  publisher:
    batch_settings:
      max_messages: 100      # Maximum messages per batch
      max_bytes: 1048576     # Maximum batch size (1MB)
      max_latency: 0.05      # Maximum wait time (50ms)
    retry_settings:
      initial: 1.0
      maximum: 60.0
      multiplier: 2.0
      deadline: 600.0
```

### Circuit Breaker
```yaml
pubsub:
  publisher:
    circuit_breaker:
      failure_threshold: 5    # Failures before opening
      reset_timeout: 60.0    # Seconds before recovery attempt
      half_open_max_calls: 3  # Test calls in half-open state
```

### Message Deduplication
```yaml
collections:
  - name: "transactions"
    deduplication:
      enabled: true
      memory_ttl: 7200        # 2 hours in memory
      memory_max_size: 20000  # Maximum cached messages
      persistent_enabled: true
      persistent_ttl: 172800  # 48 hours in Firestore
      cleanup_interval: 3600  # Cleanup every hour
```

### Log Sampling
```yaml
monitoring:
  logging:
    sampling:
      enabled: true
      default_rate: 1.0      # Sample all by default
      rules:
        DEBUG:
          "change_event_processed":
            rate: 0.1        # Sample 10% of events
            strategy: "probabilistic"
        INFO:
          "batch_published":
            rate: 0.5        # Sample 50% of batches
            strategy: "rate_limiting"
            ttl: 3600        # Cache cleanup after 1 hour
```

## Usage

1. Configure your MongoDB and Google Cloud settings in `config.yaml`
2. Set up the required environment variables:
   ```bash
   export GOOGLE_CLOUD_PROJECT="your-project-id"
   export MONGODB_URI="your-mongodb-uri"
   ```
3. Run the connector:
   ```bash
   python -m src.connector.main
   ```

## Monitoring

The connector provides several monitoring endpoints:
- `/health`: Overall health status
- `/readiness`: Readiness status
- `/metrics`: Performance metrics

## Error Handling

The connector implements multiple layers of error handling:
1. Circuit breaker for Pub/Sub publishing failures
2. Configurable retry policies
3. Message deduplication to prevent duplicates
4. Automatic resume token management
5. Graceful shutdown handling

## Best Practices

1. **Message Batching**: Adjust batch settings based on your message volume and latency requirements:
   - Higher `max_messages` and `max_bytes` for better throughput
   - Lower `max_latency` for reduced latency
   - Balance between batching and latency based on your needs

2. **Circuit Breaker**: Configure thresholds based on your error tolerance:
   - Lower `failure_threshold` for sensitive operations
   - Higher `reset_timeout` for unstable services
   - Adjust `half_open_max_calls` based on recovery patterns

3. **Deduplication**: Tune settings based on your data patterns:
   - Increase `memory_ttl` for high-latency systems
   - Adjust `memory_max_size` based on message volume
   - Set appropriate `persistent_ttl` based on business requirements

4. **Log Sampling**: Configure based on log volume and importance:
   - Use probabilistic sampling for high-volume, low-importance logs
   - Use rate limiting for burst-prone events
   - Keep critical logs unsampled
   - Adjust sampling rates based on monitoring needs

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 