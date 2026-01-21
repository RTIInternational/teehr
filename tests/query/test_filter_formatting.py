"""Tests for the filter formatting functions."""
from datetime import datetime
from teehr.models.filters import (
    TimeseriesFilter,
    FilterOperators
)
from teehr.querying.filter_format import (
    format_filter
)
import pandas as pd
import tempfile
import time
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
# from data.setup_v0_3_study import setup_v0_3_study  # noqa

@pytest.mark.read_only_warehouse
def test_filter_eq_str(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    # ev = setup_v0_3_study(tmpdir, spark_session)
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value="foo"
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name = 'foo'"

@pytest.mark.read_only_warehouse
def test_filter_in_str(read_only_test_warehouse):
    """Test the format_filter_to_str function with in_str."""
    # ev = setup_v0_3_study(tmpdir, spark_session)
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.isin,
        value=["foo", "bar"]
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"

@pytest.mark.read_only_warehouse
def test_filter_in_str_series(read_only_test_warehouse):
    """Test the format_filter_to_str function with in_str."""
    # ev = setup_v0_3_study(tmpdir, spark_session)
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.isin,
        value=pd.Series(["foo", "bar"])
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"

@pytest.mark.read_only_warehouse
def test_filter_gt_dt(read_only_test_warehouse):
    """Test the format_filter_to_str function with gt_dt."""
    # ev = setup_v0_3_study(tmpdir, spark_session)
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.reference_time,
        operator=FilterOperators.gt,
        value=datetime(2021, 1, 1)
    )
    filter_str = format_filter(filter)
    assert filter_str == "reference_time > '2021-01-01 00:00:00'"


@pytest.mark.read_only_warehouse
def test_filter_lt_int(read_only_test_warehouse):
    """Test the format_filter_to_str function lt_int."""
    # ev = setup_v0_3_study(tmpdir, spark_session)
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.lt,
        value=50
    )
    filter_str = format_filter(filter)
    assert filter_str == "value < 50"