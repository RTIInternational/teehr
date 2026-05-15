"""Pytest configuration for Scala NSE parity tests."""
import pytest
from pathlib import Path


def pytest_collection_modifyitems(config, items):
    """Mark slow tests for optional execution."""
    for item in items:
        if "slow" in item.nodeid:
            item.add_marker(pytest.mark.slow)
