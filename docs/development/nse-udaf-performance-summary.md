# NSE Spark UDAF: Implementation and Performance Summary

## Context
A new test compares NSE computed through:
- TEEHR Python metrics path (`DeterministicMetrics.NashSutcliffeEfficiency()`), and
- Scala Spark UDAF path (`nse(primary_value, secondary_value)` via Spark SQL/DataFrame aggregation).

Observed result from the test run: **~1.55x speedup** for the Scala UDAF path.

## How the Spark UDAF Is Implemented

### Core aggregator (Scala)
The NSE UDAF is implemented as a typed Spark aggregator in [scala/src/main/scala/com/rti/teehr/aggregations/NashSutcliffe.scala](scala/src/main/scala/com/rti/teehr/aggregations/NashSutcliffe.scala).

- Input type: `(Double, Double)` for `(primary, secondary)`.
- Buffer/state type: `NseState` with fields:
  - `count`
  - `primarySum`
  - `primarySumSquares`
  - `sumSquaredError`
  - `isValid`
- Output type: `Double` NSE value.

The final metric is computed as:

$$
\mathrm{NSE} = 1 - \frac{\sum (p_i - s_i)^2}{\sum (p_i - \bar{p})^2}
$$

where the denominator is derived from streaming sums in the buffer:

$$
\sum (p_i - \bar{p})^2 = \sum p_i^2 - \frac{(\sum p_i)^2}{n}
$$

### Registration
UDAF registration is handled by [scala/src/main/scala/com/rti/teehr/aggregations/NseRegistration.scala](scala/src/main/scala/com/rti/teehr/aggregations/NseRegistration.scala) and wired into session startup in [src/teehr/evaluation/spark_session_utils.py](src/teehr/evaluation/spark_session_utils.py).

At session creation time (`create_spark_session(..., register_teehr_udafs=True)`):
- Python calls `_register_teehr_sql_udafs(spark)`
- That reaches JVM object `NseRegistration.registerNseStandard(...)`
- Functions registered into the active Spark session include:
  - `nse`
  - `nse_log`
  - `nse_log_eps`
  - `nse_sqrt`

This is why the UDAF can be invoked directly in DataFrame expressions:

```python
from pyspark.sql import functions as F

df.groupBy("primary_location_id", "configuration_name").agg(
    F.expr("nse(primary_value, secondary_value)").alias("nse")
)
```

## Driver vs Executor Responsibilities

### Driver does
- Builds and optimizes the query plan.
- Resolves the registered routine name (`nse`) in the session catalog.
- Schedules tasks and coordinates shuffle stages.
- Merges final partition-level aggregates and returns results.

### Executors do
- Scan partitioned input rows.
- Run per-row `reduce(...)` logic for the aggregator state.
- Maintain partition-local `NseState` buffers.
- Merge intermediate states (`merge(...)`) during grouped aggregation/shuffle.
- Compute final aggregate value (`finish(...)`) per output group.

In short: the driver orchestrates and executors perform the heavy data processing and numeric accumulation.

## Why the Scala UDAF Is Faster
The measured ~1.55x speedup is consistent with these effects:

1. JVM-native execution path
- UDAF logic runs in Spark's JVM execution engine.
- Avoids Python-level per-row/per-group overhead in the metric computation path.

2. Better distributed aggregation behavior
- `reduce` + `merge` state model allows tree-style partial aggregation across partitions.
- Only compact aggregate state moves through merge boundaries, not raw row-by-row Python work.

3. Lower cross-language overhead
- The UDAF path avoids repeated Python callbacks for metric math.
- Less Py4J boundary traffic for inner-loop numeric operations.

4. Spark optimizer integration
- Aggregation is represented as native SQL/DataFrame expression logic.
- Catalyst/Tungsten execution can optimize around JVM-level aggregation better than Python-side execution paths.

## Why the Speedup Is Not Huge Yet (1.55x)
A moderate speedup is expected on smaller or medium test data because:
- Fixed Spark overheads (planning, task setup, shuffle coordination) dominate.
- Both paths still include DataFrame materialization and result collection overhead.
- Test warehouse size/group cardinality may be too small to fully expose compute-bound differences.

## Informed Scaling Estimate for Larger Datasets

### Expected trend
As dataset size grows, UDAF advantage should usually increase, because more time is spent in aggregation math where JVM-native code has an edge.

### Practical estimate (workload-dependent)
- Small datasets: often near parity to low single-digit speedup.
- Medium datasets: commonly around low-to-mid single-digit speedup.
- Large datasets with many rows/group operations: potential for higher speedups, often several x.

A reasonable expectation is that **1.55x is an early/low-end signal**, and larger workloads should move upward if:
- Groups are numerous or large,
- Cluster resources are sufficient,
- Shuffle skew is controlled,
- No bottlenecks dominate outside aggregation (I/O, cache misses, driver collect, etc.).

## What to Measure Next
To validate scaling empirically, benchmark with increasing row counts and group cardinalities while keeping the same query shape:

1. Row counts: e.g., 1e5, 1e6, 1e7.
2. Group counts: low, medium, high cardinality.
3. Metrics to record:
- end-to-end runtime
- shuffle read/write volume
- task skew (max vs median task time)
- executor CPU utilization

If the UDAF path remains compute-bound and skew is managed, speedup should generally improve with scale.
