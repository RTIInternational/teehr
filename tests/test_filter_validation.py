"""This module tests the filter validation function."""
from datetime import datetime
from teehr.models.dataset.filters import (
    TimeseriesFilter,
    FilterOperators
)
import pandas as pd
from teehr.models.dataset.table_models import Timeseries
from teehr.querying.filter_format import validate_filter

import tempfile
import pytest

from setup_v0_3_study import setup_v0_3_study


def test_filter_string_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value="foo"
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == "foo"


def test_filter_int_to_string_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value=10
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == "10"


def test_filter_float_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.eq,
        value=10.1
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == 10.1


def test_filter_int_to_float_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.eq,
        value=10
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == 10.0


def test_filter_str_to_float_fails(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        eval = setup_v0_3_study(tmpdir)
        fields = eval.primary_timeseries.field_enum()
        filter = TimeseriesFilter(
            column=fields.value,
            operator=FilterOperators.eq,
            value="foo"
        )
        validate_filter(filter, Timeseries)


def test_filter_datetime_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value=datetime(2021, 1, 1)
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes2(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value="2021-01-01"
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes3(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value="2021-01-01T00:00:00"
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes4(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value=pd.Timestamp("2021-01-01T00:00:00")
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes5(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = setup_v0_3_study(tmpdir)
    fields = eval.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value="2021-01-01 00:00"
    )
    filter = validate_filter(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_fails(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        eval = setup_v0_3_study(tmpdir)
        fields = eval.primary_timeseries.field_enum()
        filter = TimeseriesFilter(
            column=fields.value_time,
            operator=FilterOperators.eq,
            value="10"
        )
        validate_filter(filter, Timeseries)


def test_filter_in_str_fails(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        eval = setup_v0_3_study(tmpdir)
        fields = eval.primary_timeseries.field_enum()
        filter = TimeseriesFilter(
            column=fields.configuration,
            operator=FilterOperators.isin,
            value="10"
        )
        validate_filter(filter, Timeseries)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_filter_string_passes(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_filter_int_to_string_passes(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_filter_float_passes(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_filter_str_to_float_fails(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_filter_datetime_passes(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
        test_filter_datetime_fails(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir
            )
        )
        test_filter_int_to_float_passes(
            tempfile.mkdtemp(
                prefix="9-",
                dir=tempdir
            )
        )
        test_filter_datetime_passes2(
            tempfile.mkdtemp(
                prefix="10-",
                dir=tempdir
            )
        )
        test_filter_datetime_passes3(
            tempfile.mkdtemp(
                prefix="11-",
                dir=tempdir
            )
        )
        test_filter_datetime_passes4(
            tempfile.mkdtemp(
                prefix="12-",
                dir=tempdir
            )
        )
        test_filter_in_str_fails(
            tempfile.mkdtemp(
                prefix="13-",
                dir=tempdir
            )
        )
        test_filter_datetime_passes5(
            tempfile.mkdtemp(
                prefix="14-",
                dir=tempdir
            )
        )
