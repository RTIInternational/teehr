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

from data.setup_v0_3_study import setup_v0_3_study


def test_filter_eq_str(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value="foo"
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name = 'foo'"


def test_filter_in_str(tmpdir):
    """Test the format_filter_to_str function with in_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.isin,
        value=["foo", "bar"]
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"


def test_filter_in_str_series(tmpdir):
    """Test the format_filter_to_str function with in_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.isin,
        value=pd.Series(["foo", "bar"])
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"


def test_filter_gt_dt(tmpdir):
    """Test the format_filter_to_str function with gt_dt."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.reference_time,
        operator=FilterOperators.gt,
        value=datetime(2021, 1, 1)
    )
    filter_str = format_filter(filter)
    assert filter_str == "reference_time > '2021-01-01 00:00:00'"


def test_filter_lt_int(tmpdir):
    """Test the format_filter_to_str function lt_int."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.lt,
        value=50
    )
    filter_str = format_filter(filter)
    assert filter_str == "value < 50"


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_filter_eq_str(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_filter_in_str(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_filter_gt_dt(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_filter_lt_int(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_filter_in_str_series(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
