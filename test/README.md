# Data Pipeline Testing Suite

This directory contains comprehensive tests for ACID compliance and performance validation of the Bronze->Silver and Silver->Gold data pipelines.

## Test Files

### comprehensive_test.py
Contains both ACID compliance and performance tests for the data pipelines.

**ACID Tests:**
- **Atomicity**: Ensures all-or-nothing transaction behavior
- **Consistency**: Validates data integrity constraints
- **Isolation**: Tests that concurrent operations don't interfere
- **Durability**: Confirms data persistence across sessions

**Performance Tests:**
- Execution time measurement
- Throughput calculation
- Data volume integrity checks

### performance_test_new.py
Focused performance testing including:
- Pipeline execution timing
- Scalability analysis
- Throughput metrics
- Data flow validation

### acid_test.py (Legacy)
Original ACID test file with basic rollback testing.

## Running the Tests

### Prerequisites
- Apache Spark with Iceberg support
- Python 3.x
- PySpark

### Execution
```bash
# Run comprehensive tests
python comprehensive_test.py

# Run performance tests
python performance_test_new.py

# Run legacy ACID tests
python acid_test.py
```

### Test Configuration
- Tests use a test catalog (`test_catalog`) with local warehouse
- Sample data is generated automatically for testing
- Performance thresholds are configurable in the test files

## Test Coverage

### Bronze -> Silver Pipeline
- Data cleaning and validation
- Duplicate removal
- Null value handling
- Data type consistency

### Silver -> Gold Pipeline
- Dimension table creation
- Fact table generation
- Key generation and uniqueness
- Relationship integrity

## Performance Metrics

The tests measure:
- **Execution Time**: Total time for pipeline runs
- **Throughput**: Records processed per second
- **Scalability**: Performance with increasing data volumes
- **Data Integrity**: Record count consistency across layers

## Thresholds

Default performance thresholds (adjust based on your environment):
- Bronze->Silver: < 30 seconds
- Silver->Gold: < 30 seconds
- Throughput: > 10 records/second
- Scalability ratio: < 3x degradation

## Notes

- Tests create temporary tables in the test_catalog
- Iceberg ensures ACID properties at the storage level
- Performance metrics depend on cluster configuration
- Tests are designed to run in isolated environments