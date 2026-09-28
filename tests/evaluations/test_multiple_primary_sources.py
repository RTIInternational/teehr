"""Tests for multiple primary sources: location ID aliases and config pairs."""
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as F
import pytest

from teehr import Configuration

TEST_DATA_DIR = Path("tests", "data", "test_warehouse_data")
GEO_DIR_PATH = Path(TEST_DATA_DIR, "geo")
GEOJSON_GAGES_FILEPATH = Path(GEO_DIR_PATH, "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(GEO_DIR_PATH, "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)
TIMESERIES_FIELD_MAPPING = {
    "reference_time": "reference_time",
    "value_time": "value_time",
    "configuration": "configuration_name",
    "measurement_unit": "unit_name",
    "variable_name": "variable_name",
    "value": "value",
    "location_id": "location_id",
}


def _setup_evaluation(ev):
    """Load a single-primary-source evaluation plus location attributes."""
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)
    for name, ts_type in [
        ("usgs_observations", "primary"),
        ("alt_observations", "primary"),
        ("nwm30_retrospective", "secondary"),
    ]:
        ev.configurations.add(
            Configuration(
                name=name,
                timeseries_type=ts_type,
                description=f"test {ts_type} configuration",
            )
        )
    ev.primary_timeseries.load_parquet(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping=TIMESERIES_FIELD_MAPPING,
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "usgs_observations",
        },
    )
    ev.location_crosswalks.load_csv(in_path=CROSSWALK_FILEPATH)
    ev.secondary_timeseries.load_parquet(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping=TIMESERIES_FIELD_MAPPING,
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "nwm30_retrospective",
        },
    )
    ev.location_attributes.load_parquet(
        in_path=GEO_DIR_PATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
        update_attrs_table=True,
    )


def _load_alternative_source(ev):
    """Alias alt-A to gage-A and load a second primary source under alt-A."""
    ev.location_id_aliases.load_dataframe(
        df=pd.DataFrame({
            "primary_location_id": ["gage-A"],
            "alternative_location_id": ["alt-A"],
        })
    )
    ev.primary_timeseries.load_dataframe(df=_alt_source_sdf(ev))


def _alt_source_sdf(ev):
    """Copy of gage-A observations as a second source under alt-A.

    Built in Spark, since a pandas round trip shifts naive timestamps.
    """
    return (
        ev.primary_timeseries.filter("location_id = 'gage-A'").to_sdf()
        .withColumn("location_id", F.lit("alt-A"))
        .withColumn("configuration_name", F.lit("alt_observations"))
        .withColumn("value", F.col("value") + 1.0)
    )


@pytest.mark.function_scope_evaluation_template
def test_joined_view_without_aliases_is_unchanged(
    function_scope_evaluation_template
):
    """With no aliases, source ID equals the primary ID."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)

    df = ev.joined_timeseries_view().to_pandas()

    assert ev.location_id_aliases.to_pandas().empty
    assert (df["primary_location_id"] == df["primary_source_location_id"]).all()
    assert set(df["primary_configuration_name"]) == {"usgs_observations"}


@pytest.mark.function_scope_evaluation_template
def test_joined_view_resolves_aliases(function_scope_evaluation_template):
    """Alternative primary sources join under the canonical location ID."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)
    baseline = ev.joined_timeseries_view().to_pandas()
    _load_alternative_source(ev)

    df = ev.joined_timeseries_view(add_attrs=True).to_pandas()

    # Only canonical IDs appear as primary_location_id.
    assert "alt-A" not in set(df["primary_location_id"])

    gage_a = df[df["primary_location_id"] == "gage-A"]
    n_baseline_a = (baseline["primary_location_id"] == "gage-A").sum()
    assert len(gage_a) == 2 * n_baseline_a
    assert set(gage_a["primary_configuration_name"]) == {
        "usgs_observations", "alt_observations"
    }
    alt_rows = gage_a[gage_a["primary_configuration_name"] == "alt_observations"]
    assert set(alt_rows["primary_source_location_id"]) == {"alt-A"}
    # Attributes are joined via the canonical ID.
    assert alt_rows["drainage_area"].notna().all()

    # Other locations are unaffected.
    others = df[df["primary_location_id"] != "gage-A"]
    assert len(others) == (baseline["primary_location_id"] != "gage-A").sum()


@pytest.mark.function_scope_evaluation_template
def test_primary_attributes_and_geometry_resolve_aliases(
    function_scope_evaluation_template
):
    """Primary rows under an alias ID get the canonical attrs and geometry."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)
    _load_alternative_source(ev)

    view_df = ev.primary_timeseries_view(add_attrs=True).to_pandas()
    assert view_df[view_df["location_id"] == "alt-A"]["drainage_area"].notna().any()

    attrs_df = ev.primary_timeseries.add_attributes().to_pandas()
    assert (attrs_df["location_id"] == "alt-A").any()

    gdf = ev.primary_timeseries.to_geopandas()
    assert gdf[gdf["location_id"] == "alt-A"].geometry.notna().all()


@pytest.mark.function_scope_evaluation_template
def test_alias_validation(function_scope_evaluation_template):
    """Aliases must point at a location and must not be a location."""
    ev = function_scope_evaluation_template
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    with pytest.raises(ValueError, match="Foreign key constraint violation"):
        ev.location_id_aliases.load_dataframe(
            df=pd.DataFrame({
                "primary_location_id": ["not-a-location"],
                "alternative_location_id": ["alt-A"],
            })
        )

    with pytest.raises(ValueError, match="is found in"):
        ev.location_id_aliases.load_dataframe(
            df=pd.DataFrame({
                "primary_location_id": ["gage-A"],
                "alternative_location_id": ["gage-B"],
            })
        )


@pytest.mark.function_scope_evaluation_template
def test_primary_timeseries_requires_location_or_alias(
    function_scope_evaluation_template
):
    """Primary IDs outside locations and aliases are still rejected."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)
    with pytest.raises(ValueError, match="Foreign key constraint violation"):
        ev.primary_timeseries.load_dataframe(df=_alt_source_sdf(ev))

    _load_alternative_source(ev)
    assert (ev.primary_timeseries.to_pandas()["location_id"] == "alt-A").any()


def _load_pairs(ev, pairs):
    ev.configuration_pairs.load_dataframe(
        df=pd.DataFrame(
            pairs,
            columns=["primary_configuration_name", "secondary_configuration_name"],
        )
    )


@pytest.mark.function_scope_evaluation_template
def test_configuration_pairs_restrict_join(function_scope_evaluation_template):
    """Paired secondary configs join only to their primary configs."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)
    _load_alternative_source(ev)

    # No pairs: both primary sources join at gage-A.
    unpaired = ev.joined_timeseries_view().to_pandas()
    assert set(unpaired["primary_configuration_name"]) == {
        "usgs_observations", "alt_observations"
    }

    _load_pairs(ev, [("alt_observations", "nwm30_retrospective")])
    df = ev.joined_timeseries_view().to_pandas()
    assert set(df["primary_configuration_name"]) == {"alt_observations"}
    assert set(df["primary_location_id"]) == {"gage-A"}
    assert len(df) == (
        unpaired["primary_configuration_name"] == "alt_observations"
    ).sum()

    # Pairing with both primaries restores the fan-out.
    _load_pairs(ev, [("usgs_observations", "nwm30_retrospective")])
    both = ev.joined_timeseries_view().to_pandas()
    assert len(both) == len(unpaired)


@pytest.mark.function_scope_evaluation_template
def test_unpaired_secondary_joins_all_primaries(
    function_scope_evaluation_template
):
    """Pairs for one secondary config don't restrict other secondary configs."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)
    _load_alternative_source(ev)
    ev.configurations.add(
        Configuration(
            name="other_forecast",
            timeseries_type="secondary",
            description="test secondary configuration",
        )
    )
    _load_pairs(ev, [("usgs_observations", "other_forecast")])

    df = ev.joined_timeseries_view().to_pandas()
    assert set(df["primary_configuration_name"]) == {
        "usgs_observations", "alt_observations"
    }


@pytest.mark.function_scope_evaluation_template
def test_configuration_pairs_validation(function_scope_evaluation_template):
    """Pairs must reference existing configurations."""
    ev = function_scope_evaluation_template
    _setup_evaluation(ev)

    with pytest.raises(ValueError, match="Foreign key constraint violation"):
        _load_pairs(ev, [("not_a_config", "nwm30_retrospective")])
