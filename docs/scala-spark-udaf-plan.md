## Plan: Scala Spark Aggregation Library

Build a standalone Scala/Spark library for reusable custom aggregation functions, with a thin Spark-facing API and a small, opinionated test surface. The current teehr repo is Python-first, so it should be treated as a reference for API shape and metric ergonomics, not as the implementation host. The goal is to create a JVM-native aggregation layer that is easy to package, benchmark, and reuse across Spark jobs.

**Steps**
1. Define the public aggregation API and packaging boundary. Decide the core abstraction for custom aggregates, such as typed `Aggregator`s or `UserDefinedAggregateFunction`s, and specify how callers will register and invoke them from Spark SQL and DataFrame code. Keep the API small enough to support generic UDAFs and future domain-specific functions. *Depends on product scope decisions.*
2. Create the Scala project skeleton and build tool setup. Add a new standalone Scala build with Spark dependency management, test framework, formatting, and a release layout suitable for publishing a jar. If the library must be consumed from Python later, reserve a package namespace and artifact naming convention now. *Depends on step 1.*
3. Implement the aggregation core in Scala. Build the reusable accumulator/state types, null handling, safe division guards, and merge logic for each custom aggregate. Prefer one shared internal aggregation pattern where possible, then layer specific functions on top so future aggregates are cheap to add. *Depends on step 2.*
4. Add Spark registration and integration helpers. Expose functions for DataFrame usage and, if needed, SQL registration so downstream jobs can call the aggregates without knowing implementation details. Keep registration separate from the aggregation logic so it stays testable. *Depends on step 3.*
5. Write parity and failure-mode tests. Cover nulls, empty groups, single-row groups, merge correctness, serialization, and Spark plan behavior. Add tests that compare aggregate outputs against known expectations and, where useful, a small reference implementation. *Parallel with step 4 once the core logic exists.*
6. Add documentation and usage examples. Document how to add a new aggregate, how to register the library in Spark jobs, and what constraints callers need to know, especially around schema requirements and distributed execution semantics. *Depends on steps 3-4.*
7. Decide whether a Python wrapper is needed. If consumers need Python access, add a thin PySpark wrapper that only handles registration and invocation, while the heavy lifting stays in Scala. Keep this as an optional follow-on unless cross-language use is an explicit requirement. *Optional follow-up after the JVM library is stable.*

**Relevant files**
- `/Users/mdenno/repos/teehr/src/teehr/metrics/engine.py` — reference for routing between execution paths and public aggregation entrypoints.
- `/Users/mdenno/repos/teehr/src/teehr/metrics/format.py` — reference for metric grouping, output shaping, and aggregation orchestration patterns.
- `/Users/mdenno/repos/teehr/src/teehr/metrics/spark_native.py` — reference for Spark-native aggregation rules, null safety, and parity-sensitive implementations.
- `/Users/mdenno/repos/teehr/src/teehr/metrics/models/base.py` — reference for how metric configuration objects are modeled and validated.
- `/Users/mdenno/repos/teehr/tests/query/test_metrics_engine_routing.py` — reference for the kinds of parity and plan-level tests worth keeping in the new library.

**Verification**
1. Run unit tests for each aggregate against fixed input cases, including nulls, empty partitions, duplicate keys, and merge-order variations.
2. Run Spark integration tests that execute the aggregations through DataFrame groupBy calls and, if enabled, SQL registration.
3. Compare outputs against a small reference dataset or a known-good implementation for numeric parity.
4. Check the physical plan for the intended execution path when performance matters, especially if the library avoids Python UDFs.
5. Validate packaging by building the jar and loading it in a clean Spark session.

**Decisions**
- Chosen direction: Scala/JVM library, not a Python-only Spark extension.
- Chosen scope: reusable generic UDAFs first, not a one-off metric bundle.
- Chosen packaging model: separate library rather than adding JVM code to the current teehr Python package.
- Excluded for now: Python implementation details, unless a thin wrapper becomes necessary later.

**Further Considerations**
1. If you want direct PySpark usability, the next decision is whether the Python layer should only register JVM functions or also provide a higher-level convenience API.
2. If you already know the first custom aggregate you want, the implementation plan can be tightened around that one function first, then generalized after the initial parity tests.