# Performance Testing

This directory contains performance tests and benchmarks for the GCP MongoDB Streaming connector.

## Schema Validation Benchmarks

The `test_schema_validation_benchmark.py` file contains benchmarks for evaluating the performance of schema validation operations.

### Running the Benchmarks

To run the schema validation benchmarks:

```bash
# Run all benchmarks
pytest -xvs test_schema_validation_benchmark.py

# Run a specific benchmark
pytest -xvs test_schema_validation_benchmark.py::test_inventory_validation_benchmark

# Run with more detailed statistics
pytest -xvs test_schema_validation_benchmark.py --benchmark-histogram
```

### Available Benchmarks

The following benchmarks are available:

1. `test_inventory_validation_benchmark` - Tests validation of 1000 inventory documents
2. `test_transaction_validation_benchmark` - Tests validation of 1000 transaction documents
3. `test_single_inventory_validation_benchmark` - Tests validation of a single inventory document
4. `test_single_transaction_validation_benchmark` - Tests validation of a single transaction document
5. `test_schema_lookup_benchmark` - Tests 100 schema registry lookups
6. `test_mixed_batch_validation_benchmark` - Tests validation of a mixed batch of 200 documents
7. `test_schema_version_comparison_benchmark` - Tests validation across different schema versions

### Interpreting Results

The benchmark results will show:

- Min/max/mean execution time
- Standard deviation
- Median and interquartile range
- Number of rounds and iterations

Use these metrics to identify performance bottlenecks and measure the impact of changes to the schema validation code.

### Additional pytest-benchmark Options

```bash
# Compare against previous runs
pytest --benchmark-autosave --benchmark-compare

# Generate a histogram
pytest --benchmark-histogram

# Save as JSON
pytest --benchmark-json=output.json

# Disable garbage collection during the benchmark
pytest --benchmark-disable-gc
```

For more options, see the [pytest-benchmark documentation](https://pytest-benchmark.readthedocs.io/en/latest/usage.html). 