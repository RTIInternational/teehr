"""This module tests the filter validation function."""
from teehr import Evaluation
from datetime import datetime
from teehr.models.dataset.filters import (
    TimeseriesFilter,
    FilterOperatorEnum
)
import pandas as pd
from teehr.models.dataset.table_models import Timeseries
from teehr.querying.filter_format import validate_filter_values

import tempfile
import pytest


def test_filter_string_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperatorEnum.eq,
        value="foo"
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == "foo"


def test_filters_string_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperatorEnum.eq,
        value="foo"
    )
    filters = validate_filter_values([filter], Timeseries)
    assert filters[0].value == "foo"


def test_filters_string_passes2(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = [
        TimeseriesFilter(
            column=fields.variable_name,
            operator=FilterOperatorEnum.eq,
            value="foo"
        ),
        TimeseriesFilter(
            column=fields.variable_name,
            operator=FilterOperatorEnum.eq,
            value="bar"
        )
    ]
    filters = validate_filter_values(filter, Timeseries)
    assert filters[0].value == "foo"
    assert filters[1].value == "bar"


def test_filter_int_to_string_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperatorEnum.eq,
        value=10
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == "10"


def test_filter_float_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperatorEnum.eq,
        value=10.1
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == 10.1


def test_filter_int_to_float_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperatorEnum.eq,
        value=10
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == 10.0


def test_filter_str_to_float_fails(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        eval = Evaluation(dir_path=tmpdir)
        fields = eval.fields.get_timeseries_fields()
        filter = TimeseriesFilter(
            column=fields.value,
            operator=FilterOperatorEnum.eq,
            value="foo"
        )
        validate_filter_values(filter, Timeseries)


def test_filter_datetime_passes(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperatorEnum.eq,
        value=datetime(2021, 1, 1)
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes2(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperatorEnum.eq,
        value="2021-01-01"
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes3(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperatorEnum.eq,
        value="2021-01-01T00:00:00"
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes4(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperatorEnum.eq,
        value=pd.Timestamp("2021-01-01T00:00:00")
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_passes5(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    eval = Evaluation(dir_path=tmpdir)
    fields = eval.fields.get_timeseries_fields()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperatorEnum.eq,
        value="2021-01-01 00:00"
    )
    filter = validate_filter_values(filter, Timeseries)
    assert filter.value == datetime(2021, 1, 1)


def test_filter_datetime_fails(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        eval = Evaluation(dir_path=tmpdir)
        fields = eval.fields.get_timeseries_fields()
        filter = TimeseriesFilter(
            column=fields.value_time,
            operator=FilterOperatorEnum.eq,
            value="10"
        )
        validate_filter_values(filter, Timeseries)


def test_filter_in_str_fails(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        eval = Evaluation(dir_path=tmpdir)
        fields = eval.fields.get_timeseries_fields()
        filter = TimeseriesFilter(
            column=fields.configuration,
            operator=FilterOperatorEnum.isin,
            value="10"
        )
        validate_filter_values(filter, Timeseries)


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
        test_filters_string_passes(
            tempfile.mkdtemp(
                prefix="7-",
                dir=tempdir
            )
        )
        test_filters_string_passes2(
            tempfile.mkdtemp(
                prefix="8-",
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
