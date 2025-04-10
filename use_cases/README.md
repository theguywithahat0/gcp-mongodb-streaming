# Use Cases

This directory contains domain-specific implementations and use cases that leverage the core pipeline framework. 
Each subdirectory represents a specific business domain or application.

## Available Use Cases

### Stock Monitoring

The `stock_monitoring` directory contains transforms and configurations for monitoring stock positions and generating alerts when thresholds are exceeded.

- **Key Features**:
  - Real-time position monitoring
  - Configurable thresholds
  - Windowed aggregation
  - Alert generation

See the [Stock Monitoring README](stock_monitoring/README.md) for more details.

## Creating New Use Cases

To create a new use case:

1. Create a new directory with a descriptive name for your use case
2. Implement domain-specific transforms that extend the base transforms
3. Create configuration files for your transforms
4. Update the integration tests to verify your use case
5. Document your use case in a README.md file

## Best Practices

- Keep domain-specific logic in this directory
- Use the core pipeline framework for common functionality
- Follow the same patterns as existing use cases
- Implement comprehensive tests for your domain logic 