"""Tests for the filter formatting functions."""
from datetime import datetime
from teehr.models.filters import (
    TableFilter,
    FilterOperators
)
from teehr.querying.filter_format import (
    format_filter
)
import pandas as pd
import pytest


def test_filter_eq_str():
    """Test the format_filter_to_str function with eq_str."""
    filter = TableFilter(
        column="variable_name",
        operator=FilterOperators.eq,
        value="foo"
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name = 'foo'"


def test_filter_in_str():
    """Test the format_filter_to_str function with in_str."""
    filter = TableFilter(
        column="variable_name",
        operator=FilterOperators.isin,
        value=["foo", "bar"]
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"


def test_filter_in_str_series():
    """Test the format_filter_to_str function with in_str."""
    filter = TableFilter(
        column="variable_name",
        operator=FilterOperators.isin,
        value=pd.Series(["foo", "bar"])
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"


def test_filter_gt_dt():
    """Test the format_filter_to_str function with gt_dt."""
    filter = TableFilter(
        column="reference_time",
        operator=FilterOperators.gt,
        value=datetime(2021, 1, 1)
    )
    filter_str = format_filter(filter)
    assert filter_str == "reference_time > '2021-01-01 00:00:00'"


def test_filter_lt_int():
    """Test the format_filter_to_str function lt_int."""
    filter = TableFilter(
        column="value",
        operator=FilterOperators.lt,
        value=50
    )
    filter_str = format_filter(filter)
    assert filter_str == "value < 50"