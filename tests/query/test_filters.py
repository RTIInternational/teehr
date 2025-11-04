"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
from datetime import timedelta
import tempfile
import pytest
from teehr import RowLevelCalculatedFields as rcf
from teehr.models.filters import (
    TimeseriesFilter,
    JoinedTimeseriesFilter,
    FilterOperators,
    TableFilter,
    TableNamesEnum
)
import pyspark.sql as ps

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa


def test_chain_filter_single_str(tmpdir):
    """Test filter string."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.filter("location_id = 'gage-A'").to_pandas()
    assert len(df) == 26


def test_chain_filter_single_str2(tmpdir):
    """Test filter string with invalid id."""
    ev = setup_v0_3_study(tmpdir)
    with pytest.raises(Exception):
        ev.primary_timeseries.filter("id = 'gage-A'").to_pandas()


def test_chain_filter_single_dict(tmpdir):
    """Test filter dict."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.filter({
        "column": "location_id",
        "operator": "=",
        "value": "gage-A"
    }).to_pandas()
    assert len(df) == 26


def test_chain_filter_single_dict2(tmpdir):
    """Test filter dict with invalid id."""
    ev = setup_v0_3_study(tmpdir)
    with pytest.raises(Exception):
        ev.primary_timeseries.filter({
            "column": "id",
            "operator": "=",
            "value": "gage-A"
        }).to_pandas()


def test_chain_filter_single_model(tmpdir):
    """Test filter model."""
    ev = setup_v0_3_study(tmpdir)
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.filter(
        TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


def test_chain_filter_single_model2(tmpdir):
    """Test filter model."""
    ev = setup_v0_3_study(tmpdir)
    flds = ev.primary_timeseries.field_enum()
    with pytest.raises(Exception):
        ev.primary_timeseries.filter(
            TimeseriesFilter(
                column=flds.id,
                operator=FilterOperators.eq,
                value="gage-A"
            )
        ).to_pandas()


def test_chain_filter_list_str(tmpdir):
    """Test filter list of strings."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.filter([
        "location_id = 'gage-A'",
        "value_time > '2022-01-01T12:00:00'"
    ]).to_pandas()
    assert len(df) == 13


def test_chain_filter_list_dict(tmpdir):
    """Test filter list of dicts."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.filter([
        {
            "column": "location_id",
            "operator": "=",
            "value": "gage-A"
        },
        {
            "column": "value_time",
            "operator": ">",
            "value": "2022-01-01T12:00:00Z"
        }
    ]).to_pandas()
    assert len(df) == 13


def test_chain_filter_list_model(tmpdir):
    """Test filter list of models."""
    ev = setup_v0_3_study(tmpdir)
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.filter([
        TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        ),
        TimeseriesFilter(
            column=flds.value_time,
            operator=FilterOperators.gt,
            value="2022-01-01T12:00:00Z"
        )
    ]).to_pandas()
    assert len(df) == 13


def test_query_single_str(tmpdir):
    """Test query string."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.query(
        filters="location_id = 'gage-A'"
    ).to_pandas()
    assert len(df) == 26


def test_query_single_dict(tmpdir):
    """Test query dict."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.query(
        filters={
            "column": "location_id",
            "operator": "=",
            "value": "gage-A"
        }
    ).to_pandas()
    assert len(df) == 26


def test_query_single_model(tmpdir):
    """Test query model."""
    ev = setup_v0_3_study(tmpdir)
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.query(
        filters=TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


def test_query_list_str(tmpdir):
    """Test query list of strings."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.query(
        filters=[
            "location_id = 'gage-A'",
            "value_time > '2022-01-01T12:00:00'"
        ]
    ).to_pandas()
    assert len(df) == 13


def test_query_list_dict(tmpdir):
    """Test query list of dicts."""
    ev = setup_v0_3_study(tmpdir)
    df = ev.primary_timeseries.query(
        filters=[
            {
                "column": "location_id",
                "operator": "=",
                "value": "gage-A"
            },
            {
                "column": "value_time",
                "operator": ">",
                "value": "2022-01-01T12:00:00Z"
            }
        ]
    ).to_pandas()
    assert len(df) == 13


def test_query_list_model(tmpdir):
    """Test query list of models."""
    ev = setup_v0_3_study(tmpdir)
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.query(
        filters=[
            TimeseriesFilter(
                column=flds.location_id,
                operator=FilterOperators.eq,
                value="gage-A"
            ),
            TimeseriesFilter(
                column=flds.value_time,
                operator=FilterOperators.gt,
                value="2022-01-01T12:00:00Z"
            )
        ]
    ).to_pandas()
    assert len(df) == 13


def test_filter_by_lead_time(tmpdir):
    """Test filter by lead time."""
    ev = setup_v0_3_study(tmpdir)
    ev.joined_timeseries.add_calculated_fields([
        rcf.ForecastLeadTime(),
    ]).write()
    filter_value = timedelta(days=0, hours=18)
    flds = ev.joined_timeseries.field_enum()
    df = ev.joined_timeseries.query(
            JoinedTimeseriesFilter(
                column=flds.forecast_lead_time,
                operator=FilterOperators.gt,
                value=filter_value
            )
    ).to_pandas()
    assert len(df) == 45
    df = ev.joined_timeseries.filter(
        filters=[
            {
                "column": "forecast_lead_time",
                "operator": ">",
                "value": filter_value
            }
        ]
    ).to_pandas()
    assert len(df) == 45

    df = ev.joined_timeseries.filter(
        "forecast_lead_time > interval 18 hours"
    ).to_pandas()
    assert len(df) == 45

    df = ev.joined_timeseries.filter(
        "forecast_lead_time < interval 1 day"
    ).to_pandas()
    assert len(df) == 216

    df = ev.joined_timeseries.filter(
        "forecast_lead_time < interval 3600 seconds"
    ).to_pandas()
    assert len(df) == 9


def test_evaluation_filter(tmpdir):
    """Test table filter model."""
    ev = setup_v0_3_study(tmpdir)

    df = ev.primary_timeseries.to_pandas()
    tbl_filter = TableFilter(
        table_name="primary_timeseries",
        filters=[
            "configuration_name = 'usgs_observations'"
        ]
    )
    sdf = ev.filter(table_filter=tbl_filter)
    assert sdf.count() == len(df)

    for tbl_name in TableNamesEnum:
        sdf = ev.filter(table_name=tbl_name)
        assert isinstance(sdf, ps.DataFrame)

    sdf = ev.filter(
        table_name="primary_timeseries",
        filters="configuration_name = 'usgs_observations'"
    )
    assert sdf.count() == len(df)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_chain_filter_single_str(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_chain_filter_single_str2(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_chain_filter_single_dict(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_chain_filter_single_dict2(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_chain_filter_single_model(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
        test_chain_filter_single_model2(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir
            )
        )
        test_chain_filter_list_str(
            tempfile.mkdtemp(
                prefix="7-",
                dir=tempdir
            )
        )
        test_chain_filter_list_dict(
            tempfile.mkdtemp(
                prefix="8-",
                dir=tempdir
            )
        )
        test_chain_filter_list_model(
            tempfile.mkdtemp(
                prefix="9-",
                dir=tempdir
            )
        )
        test_query_single_str(
            tempfile.mkdtemp(
                prefix="10-",
                dir=tempdir
            )
        )
        test_query_single_dict(
            tempfile.mkdtemp(
                prefix="11-",
                dir=tempdir
            )
        )
        test_query_single_model(
            tempfile.mkdtemp(
                prefix="12-",
                dir=tempdir
            )
        )
        test_query_list_str(
            tempfile.mkdtemp(
                prefix="13-",
                dir=tempdir
            )
        )
        test_query_list_dict(
            tempfile.mkdtemp(
                prefix="14-",
                dir=tempdir
            )
        )
        test_query_list_model(
            tempfile.mkdtemp(
                prefix="15-",
                dir=tempdir
            )
        )
        test_filter_by_lead_time(
            tempfile.mkdtemp(
                prefix="16-",
                dir=tempdir
            )
        )
        test_table_filter(
            tempfile.mkdtemp(
                prefix="17-",
                dir=tempdir
            )
        )
