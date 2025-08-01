"""Tests for the TEEHR study creation."""
import tempfile
import teehr
from teehr import RowLevelCalculatedFields as rcf
from teehr import TimeseriesAwareCalculatedFields as tcf

from setup_v0_3_study import setup_v0_3_study

import pyspark.sql.types as T
import numpy as np


def test_add_row_udfs_null_reference(tmpdir):
    """Test adding row level UDFs with null reference time."""
    ev = teehr.Evaluation(dir_path=tmpdir, create_dir=True)
    ev.clone_from_s3("e0_2_location_example")
    ev.joined_timeseries.create(add_attrs=False, execute_scripts=False)

    ev.joined_timeseries.add_calculated_fields([
        rcf.Month(),
        rcf.Year(),
        rcf.WaterYear(),
        rcf.Seasons()
    ]).write()

    ev.spark.stop()


def test_add_row_udfs(tmpdir):
    """Test adding row level UDFs."""
    ev = setup_v0_3_study(tmpdir)
    sdf = ev.joined_timeseries.to_sdf()

    sdf = rcf.Month().apply_to(sdf)

    sdf = rcf.Year().apply_to(sdf)

    sdf = rcf.WaterYear().apply_to(sdf)

    sdf = rcf.NormalizedFlow().apply_to(sdf)

    sdf = rcf.Seasons().apply_to(sdf)

    sdf = rcf.ForecastLeadTime().apply_to(sdf)

    sdf = rcf.ThresholdValueExceeded().apply_to(sdf)

    sdf = rcf.DayOfYear().apply_to(sdf)

    cols = sdf.columns
    check_sdf = sdf[sdf["primary_location_id"] == "gage-A"]

    assert "month" in cols
    assert sdf.schema["month"].dataType == T.IntegerType()
    check_vals = check_sdf.select("month").distinct().collect()
    for row in check_vals:
        assert row["month"] == 1

    assert "year" in cols
    assert sdf.schema["year"].dataType == T.IntegerType()
    check_vals = check_sdf.select("year").distinct().collect()
    for row in check_vals:
        assert row["year"] == 2022

    assert "water_year" in cols
    assert sdf.schema["water_year"].dataType == T.IntegerType()
    check_vals = check_sdf.select("water_year").distinct().collect()
    for row in check_vals:
        assert row["water_year"] == 2022

    assert "normalized_flow" in cols
    assert sdf.schema["normalized_flow"].dataType == T.FloatType()
    check_vals = check_sdf.select("normalized_flow").collect()
    assert np.round(check_vals[0]["normalized_flow"], 3) == 0.003

    assert "season" in cols
    assert sdf.schema["season"].dataType == T.StringType()
    check_vals = check_sdf.select("season").distinct().collect()
    for row in check_vals:
        assert row["season"] in ["winter", "spring", "summer", "fall"]

    assert "forecast_lead_time" in cols
    assert sdf.schema["forecast_lead_time"].dataType == T.LongType()
    row = check_sdf.collect()[1]
    expected_val = (row["value_time"] - row["reference_time"]).total_seconds()
    test_val = row["forecast_lead_time"]
    assert expected_val == test_val

    assert "threshold_value_exceeded" in cols
    assert sdf.schema["threshold_value_exceeded"].dataType == T.BooleanType()
    check_vals = check_sdf.select("threshold_value_exceeded").distinct().collect()
    assert check_vals[0]["threshold_value_exceeded"] is True

    assert "day_of_year" in cols
    assert sdf.schema["day_of_year"].dataType == T.IntegerType()
    check_vals = check_sdf.select("day_of_year").distinct().collect()
    for row in check_vals:
        assert row["day_of_year"] in [1, 2]

    ev.spark.stop()


def test_add_timeseries_udfs(tmpdir):
    """Test adding a timeseries aware UDF."""
    ev = setup_v0_3_study(tmpdir)
    sdf = ev.joined_timeseries.to_sdf()

    ped = tcf.PercentileEventDetection()
    sdf = ped.apply_to(sdf)

    cols = sdf.columns
    assert "event" in cols
    assert "event_id" in cols

    ev.spark.stop()


def test_add_udfs_write(tmpdir):
    """Test adding UDFs and write DataFrame back to table."""
    ev = setup_v0_3_study(tmpdir)

    ped = tcf.PercentileEventDetection()
    ev.joined_timeseries.add_calculated_fields(ped).write()

    flt = rcf.ForecastLeadTime()
    ev.joined_timeseries.add_calculated_fields(flt).write()

    new_sdf = ev.joined_timeseries.to_sdf()

    cols = new_sdf.columns
    assert "event" in cols
    assert "event_id" in cols
    assert "forecast_lead_time" in cols

    ev.spark.stop()


def test_location_event_detection(tmpdir):
    """Test event detection and metrics per event."""
    ev = setup_v0_3_study(tmpdir)

    ped = tcf.PercentileEventDetection()
    sdf = ev.metrics.add_calculated_fields(ped).query(
        group_by=["configuration_name", "primary_location_id", "event_id"],
        include_metrics=[
            teehr.SignatureMetrics.Maximum(
                input_field_names=["primary_value"],
                output_field_name="max_primary_value"
            ),
            teehr.SignatureMetrics.Maximum(
                input_field_names=["secondary_value"],
                output_field_name="max_secondary_value"
            )
        ]
    ).to_sdf()

    assert sdf.count() == 6

    assert "configuration_name" in sdf.columns
    assert "primary_location_id" in sdf.columns
    assert "event_id" in sdf.columns
    assert "max_primary_value" in sdf.columns
    assert "max_secondary_value" in sdf.columns

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_add_row_udfs_null_reference(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tempdir
            )
        )
        test_add_row_udfs(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_add_timeseries_udfs(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_add_udfs_write(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_location_event_detection(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
