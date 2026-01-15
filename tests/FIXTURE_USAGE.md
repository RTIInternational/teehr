# Test Fixture Usage Guide

## Overview
The test suite now has optimized fixtures for faster Spark and Iceberg testing. This guide shows how to use them effectively.

## Available Fixtures

### 1. `spark_session` (session-scoped)
Optimized Spark session configured for testing with reduced shuffle partitions and disabled UI.

```python
def test_something(spark_session):
    df = spark_session.createDataFrame([...])
    # Use spark_session for custom operations
```

### 2. `evaluation_v0_3` (function-scoped) ⭐ **Most Common**
Fast Evaluation setup with isolated test data. Each test gets a fresh instance.

```python
def test_metrics(evaluation_v0_3):
    ev = evaluation_v0_3
    # Test code that modifies data
    ev.joined_timeseries.add_calculated_fields([...]).write()
```

**Use when**: Your test modifies data (writes, inserts, updates)

### 3. `evaluation_v0_3_module` (module-scoped)
Shared Evaluation across all tests in a module. Faster for read-only operations.

```python
def test_read_only_query(evaluation_v0_3_module):
    ev = evaluation_v0_3_module
    # Read-only operations
    df = ev.joined_timeseries.to_sdf()
    count = df.count()
    assert count > 0
```

**Use when**: Your test only reads data and doesn't modify it

### 4. `cached_joined_timeseries_df` (module-scoped)
Pre-cached joined timeseries DataFrame for maximum speed.

```python
def test_metrics_fast(cached_joined_timeseries_df):
    df = cached_joined_timeseries_df
    # DataFrame is already loaded and cached in memory
    result = df.filter("primary_location_id = 'gage-A'").count()
```

**Use when**: You need the joined timeseries data repeatedly in read-only tests

### 5. `cached_primary_timeseries_df` (module-scoped)
Pre-cached primary timeseries DataFrame.

```python
def test_primary_data(cached_primary_timeseries_df):
    df = cached_primary_timeseries_df
    # Use cached DataFrame
```

## Migration Examples

### Before (Old Pattern)
```python
def test_add_row_udfs(tmpdir, spark_session):
    ev = setup_v0_3_study(tmpdir, spark_session)
    sdf = ev.joined_timeseries.to_sdf()
    # test code
    # ev.spark.stop()  # Don't do this anymore!
```

### After (New Pattern - Function-scoped)
```python
def test_add_row_udfs(evaluation_v0_3):
    ev = evaluation_v0_3
    sdf = ev.joined_timeseries.to_sdf()
    # test code
    # No spark.stop() needed - fixture handles cleanup
```

### After (New Pattern - Module-scoped for read-only)
```python
def test_read_metrics(evaluation_v0_3_module):
    ev = evaluation_v0_3_module
    # Read-only test code
```

### After (New Pattern - Cached DataFrame)
```python
def test_with_cached_df(cached_joined_timeseries_df):
    df = cached_joined_timeseries_df
    # Work directly with cached DataFrame
```

## Performance Benefits

### Old Pattern (per test):
- Unpack tar.gz: ~2-3s
- Create Iceberg tables: ~2-3s
- `.count()` calls: ~1-2s
- Spark session creation: ~5-7s (if not reused)
- **Total: ~10-15s per test**

### New Pattern (per test):
- Unpack tar.gz: ~0s (cached, done once)
- Create Iceberg tables: ~1-2s
- `.count()` calls: ~0s (removed)
- Spark session: ~0s (reused)
- **Total: ~1-2s per test** ⚡

**Speedup: 5-10x faster!**

## Test Markers

Mark your tests for selective execution:

```python
import pytest

@pytest.mark.fast
def test_quick_operation(evaluation_v0_3):
    pass

@pytest.mark.slow
def test_bootstrap_intensive(evaluation_v0_3):
    pass

@pytest.mark.spark
@pytest.mark.iceberg
def test_full_integration(evaluation_v0_3):
    pass
```

Run selectively:
```bash
pytest -m "not slow"          # Skip slow tests
pytest -m fast                # Only fast tests
pytest -m "spark and iceberg" # Only Spark+Iceberg tests
```

## Choosing the Right Fixture

### Use `evaluation_v0_3` when:
- ✅ Test writes or modifies data
- ✅ Test needs isolation from other tests
- ✅ Default choice for most tests

### Use `evaluation_v0_3_module` when:
- ✅ Test only reads data
- ✅ Multiple tests in module can share data
- ✅ Want maximum speed for read-only tests

### Use `cached_*_df` when:
- ✅ Test operates directly on DataFrames
- ✅ Don't need full Evaluation object
- ✅ Want fastest possible execution

### Continue using `setup_v0_3_study()` when:
- ⚠️ Only for backward compatibility during migration
- ⚠️ Consider migrating to fixtures for better performance

## Common Patterns

### Metric Calculation Tests
```python
def test_metrics(evaluation_v0_3):
    ev = evaluation_v0_3
    nse = teehr.DeterministicMetrics.NashSutcliffeEfficiency()
    result = ev.metrics.query(
        include_metrics=[nse],
        group_by=["primary_location_id"]
    ).to_pandas()
    assert not result.empty
```

### UDF Tests
```python
def test_udfs(evaluation_v0_3):
    ev = evaluation_v0_3
    ev.joined_timeseries.add_calculated_fields([
        rcf.Month(),
        rcf.Year()
    ]).write()
    # Verify fields were added
```

### Query/Filter Tests (Read-only)
```python
def test_filters(evaluation_v0_3_module):
    ev = evaluation_v0_3_module
    df = ev.joined_timeseries.filter(
        TimeseriesFilter(location_ids=["gage-A"])
    ).to_pandas()
    assert len(df) > 0
```

### Direct DataFrame Tests (Fastest)
```python
def test_calculations(cached_joined_timeseries_df):
    df = cached_joined_timeseries_df
    result = df.groupBy("primary_location_id").count().collect()
    assert len(result) > 0
```

## Tips for Maximum Performance

1. **Use module-scoped fixtures for read-only tests**
   - Group related read-only tests in same module
   - Share `evaluation_v0_3_module` across all

2. **Use cached DataFrames when possible**
   - Fastest option for DataFrame operations
   - No Evaluation overhead

3. **Mark slow tests**
   - Use `@pytest.mark.slow` for tests >5s
   - Skip during rapid development: `pytest -m "not slow"`

4. **Enable parallel execution**
   ```bash
   pip install pytest-xdist
   pytest -n auto  # Uses all CPU cores
   ```

5. **Avoid calling `.count()` in tests**
   - Use `.isEmpty()` or check first row instead
   - `.count()` triggers full table scan

## Troubleshooting

### Error: "Table already exists"
**Solution**: Use function-scoped `evaluation_v0_3` instead of module-scoped for tests that write data.

### Error: "Spark session already stopped"
**Solution**: Don't call `ev.spark.stop()` - let fixtures handle cleanup.

### Tests interfere with each other
**Solution**: Use function-scoped `evaluation_v0_3` instead of module-scoped.

### Need custom Spark config
**Solution**: You can still use `setup_v0_3_study()` or create a custom fixture.
