"""Tests for the TEEHR study creation."""
import tempfile
import teehr
from teehr import RowLevelCalculatedFields as rcf
from teehr import TimeseriesAwareCalculatedFields as tcf

from setup_v0_3_study import setup_v0_3_study

import pyspark.sql.types as T

def test_add_row_udfs(tmpdir):
    """Test adding row level UDFs."""
    ev = setup_v0_3_study(tmpdir)
    sdf = ev.joined_timeseries.to_sdf()

    sdf = rcf.Month().apply_to(sdf)

    sdf = rcf.Year().apply_to(sdf)

    sdf = rcf.WaterYear().apply_to(sdf)

    sdf = rcf.NormalizedFlow().apply_to(sdf)

    sdf = rcf.Seasons().apply_to(sdf)

    cols = sdf.columns
    assert "month" in cols
    assert "year" in cols
    assert "water_year" in cols
    assert "normalized_flow" in cols
    assert "season" in cols

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
    new_sdf = ev.joined_timeseries.to_sdf()

    cols = new_sdf.columns
    assert "event" in cols
    assert "event_id" in cols

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
