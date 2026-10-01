"""Tests for the default series key and the old joined-column-name guard."""
import pytest

from teehr.calculated_fields.engine import apply_calculated_fields_with_engine
from teehr.calculated_fields.timeseries_aware_pandas import default_series_key
from teehr.metrics.engine import aggregate_metrics_with_engine
from teehr.calculated_fields.models.timeseries_aware import (
    AbovePercentileEventDetection,
)
from teehr import DeterministicMetrics as m

JOINED_COLUMNS = [
    "reference_time", "value_time", "location_id", "primary_location_id",
    "secondary_location_id", "primary_value", "secondary_value",
    "primary_configuration_name", "secondary_configuration_name",
    "primary_variable_name", "secondary_variable_name", "unit_name", "member",
]
PRIMARY_VIEW_COLUMNS = [
    "reference_time", "value_time", "value", "variable_name",
    "configuration_name", "unit_name", "location_id",
]
SECONDARY_VIEW_COLUMNS = PRIMARY_VIEW_COLUMNS + ["member", "primary_location_id"]


@pytest.mark.parametrize(
    "columns, expected",
    [
        (JOINED_COLUMNS, [
            "reference_time", "location_id", "primary_location_id",
            "secondary_location_id", "primary_configuration_name",
            "secondary_configuration_name", "primary_variable_name",
            "secondary_variable_name", "unit_name", "member",
        ]),
        (PRIMARY_VIEW_COLUMNS, [
            "reference_time", "location_id", "configuration_name",
            "variable_name", "unit_name",
        ]),
        (SECONDARY_VIEW_COLUMNS, [
            "reference_time", "location_id", "primary_location_id",
            "configuration_name", "variable_name", "unit_name", "member",
        ]),
    ],
)
def test_default_series_key(columns, expected):
    """The default key is the series-identifying columns present, in order."""
    assert default_series_key(columns) == expected


def _old_joined_sdf(spark):
    return spark.createDataFrame(
        [("gage-A", "fcst-1", 1.0, 2.0, "nwm30_retrospective",
          "streamflow_hourly_inst", "m^3/s")],
        "primary_location_id string, secondary_location_id string, "
        "primary_value double, secondary_value double, "
        "configuration_name string, variable_name string, unit_name string",
    )


def test_metrics_reject_old_joined_names(spark_shared_session):
    """Aggregating a pre-0.9 joined table raises a regenerate message."""
    sdf = _old_joined_sdf(spark_shared_session)
    with pytest.raises(ValueError, match="Regenerate it"):
        aggregate_metrics_with_engine(
            sdf, ["primary_location_id"], [m.KlingGuptaEfficiency()]
        )


def test_calculated_fields_reject_old_joined_names(spark_shared_session):
    """Calculated fields on a pre-0.9 joined table raise a regenerate message."""
    sdf = _old_joined_sdf(spark_shared_session)
    with pytest.raises(ValueError, match="Regenerate it"):
        apply_calculated_fields_with_engine(
            sdf, [AbovePercentileEventDetection()]
        )
