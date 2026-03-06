# TEEHR Test Suite Guide

## Running Tests in VS Code

### Using the Test Explorer UI

VS Code's Python extension provides a visual Test Explorer for running and debugging tests:

1. **Enable Test Discovery**:
   - Open Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`)
   - Type "Python: Configure Tests"
   - Select `pytest`
   - Select root directory: `tests`

2. **View Tests**:
   - Click the Testing icon in the Activity Bar (beaker icon)
   - All tests will be displayed in a tree structure

3. **Run/Debug Tests with Green Arrows**:
   - **Run a single test**: Click the green ▶️ arrow next to any test
   - **Debug a single test**: Right-click test → "Debug Test" (or click debug icon)
   - **Run a test file**: Click ▶️ arrow next to the file name
   - **Run all tests**: Click ▶️ arrow at the top of the Test Explorer

4. **Debugging Features**:
   - Set breakpoints by clicking in the gutter (left of line numbers)
   - When debugging, use the Debug toolbar to step through code
   - Inspect variables in the Variables pane
   - View call stack in the Call Stack pane

### Running Tests from Terminal

#### Basic Commands

```bash
# Activate virtual environment first
source .venv/bin/activate

# Run all tests
pytest

# Run tests in a specific directory
pytest tests/query/

# Run a specific test file
pytest tests/query/test_filters.py

# Run a specific test function
pytest tests/query/test_filters.py::test_chain_filter_single_str

# Run tests matching a pattern
pytest -k "filter"
```

## Common pytest Options

### Output and Verbosity

```bash
# Show print statements and logging output
pytest -s

# Verbose output (show each test name)
pytest -v

# Very verbose (show more details)
pytest -vv

# Quiet mode (less output)
pytest -q

# Show local variables in tracebacks
pytest -l
```

### Test Selection

```bash
# Run tests marked with specific marker
pytest -m session_scope_test_warehouse

# Run tests NOT marked with marker
pytest -m "not slow"

# Run tests matching keyword expression
pytest -k "metrics and not bootstrap"

# Run only tests that failed in last run
pytest --lf

# Run failed tests first, then others
pytest --ff
```

### Performance and Timing

```bash
# Show slowest 10 tests
pytest --durations=10

# Show all test durations
pytest --durations=0

# Show durations for tests slower than 1 second
pytest --durations-min=1.0

# Exit on first failure (fast feedback)
pytest -x

# Exit after N failures
pytest --maxfail=3
```

### Parallel Execution

```bash
# Run tests in parallel (auto-detect CPU cores)
pytest -n auto

# Run with specific number of workers
pytest -n 4

# Run in parallel with live output
pytest -n auto -v
```

### Test Coverage

```bash
# Run with coverage report
pytest --cov=teehr

# Show missing lines
pytest --cov=teehr --cov-report=term-missing

# Generate HTML coverage report
pytest --cov=teehr --cov-report=html

# Focus coverage on specific module
pytest --cov=teehr.evaluation tests/evaluations/
```

### Debugging

```bash
# Drop into debugger on failures
pytest --pdb

# Drop into debugger on errors (not assertion failures)
pytest --pdbcls=IPython.terminal.debugger:TerminalPdb

# Show full diff for assertions
pytest -vv

# Don't capture output (useful with debugger)
pytest -s --pdb
```

### Filtering by Markers

Available markers in this project (see `pytest.ini`):

```bash
# Session-scoped fixtures (fastest, shared data)
pytest -m session_scope_evaluation_template
pytest -m session_scope_test_warehouse

# Module-scoped fixtures (shared within test file)
pytest -m module_scope_test_warehouse

# Function-scoped fixtures (isolated per test, slowest)
pytest -m function_scope_evaluation_template
pytest -m function_scope_test_warehouse
pytest -m function_scope_large_ensemble_warehouse
pytest -m function_scope_small_ensemble_warehouse
pytest -m function_scope_two_location_warehouse
```

### Combining Options

```bash
# Run specific markers with timing and verbose output
pytest -m session_scope_test_warehouse -v --durations=10

# Debug a specific test with output
pytest tests/query/test_filters.py::test_chain_filter_single_str -s --pdb

# Fast feedback: parallel, stop on first failure, show output
pytest -n auto -x -v

# Performance profiling
pytest tests/query/ --durations=0 -v
```

## Test Fixtures Overview

### Fixture Scopes and Usage

| Fixture | Scope | Use When | Speed |
|---------|-------|----------|-------|
| `spark_shared_session` | session | Need Spark session | ⚡⚡⚡ |
| `session_scope_test_warehouse` | session | Read-only operations | ⚡⚡⚡ |
| `session_scope_evaluation_template` | session | Read-only template access | ⚡⚡⚡ |
| `module_scope_test_warehouse` | module | Tests share data within file | ⚡⚡ |
| `function_scope_evaluation_template` | function | Write/modify operations | ⚡ |
| `function_scope_test_warehouse` | function | Need isolated warehouse | ⚡ |
| `function_scope_*_ensemble_warehouse` | function | Ensemble-specific tests | ⚡ |

### Guidelines

- **Use session/module scope when possible** for faster tests
- **Use function scope only when tests modify data**
- All fixtures share the same Spark session (`spark_shared_session`)

## Writing New Tests

### Test Structure

```python
import pytest
from pathlib import Path

@pytest.mark.session_scope_test_warehouse
def test_my_feature(session_scope_test_warehouse):
    """Test description."""
    ev = session_scope_test_warehouse

    # Your test code
    result = ev.metrics.query(...).to_pandas()

    assert result.shape[0] > 0
```

### Naming Conventions

- Test files: `test_*.py`
- Test functions: `def test_*()`
- Test classes: `class Test*:`
- Be descriptive: `test_filter_by_location_id` not `test_filter1`

### Assertions

```python
# Use descriptive assertions
assert result.shape[0] == 3, "Expected 3 rows"

# Use pytest assertions for better error messages
import pytest

# Check for exceptions
with pytest.raises(ValueError, match="Invalid location"):
    ev.locations.load_csv("bad_file.csv")

# Approximate comparisons
assert result == pytest.approx(expected, rel=1e-5)

# Check warnings
with pytest.warns(UserWarning):
    function_that_warns()
```

## Troubleshooting

### Common Issues

**Tests hang or are very slow**:
```bash
# Check if test is using function-scoped fixture unnecessarily
# Run with verbose output to see which test is slow
pytest -v --durations=10
```

**Spark errors or table conflicts**:
```bash
# Clear cached tables and restart
pytest --cache-clear
```

**Import errors**:
```bash
# Reinstall dependencies
poetry install
```

**PySpark/Spark warnings**:
```python
# These are normal and suppressed in conftest.py
# Check tests/conftest.py::pytest_configure for logging config
```

### Debugging Tips

1. **Use print statements**: Add `print()` statements and run with `-s`
   ```bash
   pytest tests/my_test.py -s
   ```

2. **Use logging**: Tests use Python logging
   ```python
   import logging
   logger = logging.getLogger(__name__)
   logger.info("Debug message")
   ```

3. **Inspect fixture values**: Print fixture contents
   ```python
   def test_something(session_scope_test_warehouse):
       ev = session_scope_test_warehouse
       print(f"LocalReadWriteEvaluation dir: {ev.dir_path}")
       print(f"Tables: {ev.spark.sql('SHOW TABLES').collect()}")
   ```

4. **Use VS Code debugger**: Set breakpoints and right-click → "Debug Test"

## Performance Optimization

### Speed Comparison

```bash
# Baseline timing
time pytest

# With parallel execution (2-4x faster)
time pytest -n auto

# Run only fast tests
time pytest -m "not slow"

# Profile slowest tests
pytest --durations=0 > test_timings.txt
```

### Best Practices

1. **Use appropriate fixture scope**: Session > Module > Function
2. **Run tests in parallel**: Use `-n auto` for faster feedback
3. **Run subset during development**: Use `-k` or specific paths
4. **Use `--lf`** to re-run only failures
5. **Mark slow tests**: Add `@pytest.mark.slow` decorator

## CI/CD Integration

Tests can be run in CI with:

```bash
# Full test suite with coverage
pytest --cov=teehr --cov-report=xml --cov-report=term

# With JUnit XML output
pytest --junit-xml=test-results.xml

# Parallel execution in CI
pytest -n auto --maxfail=5
```

## Additional Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-xdist (parallel testing)](https://pytest-xdist.readthedocs.io/)
- [VS Code Python Testing](https://code.visualstudio.com/docs/python/testing)
- Project-specific: See `tests/FIXTURE_USAGE.md` for detailed fixture documentation
