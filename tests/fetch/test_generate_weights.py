"""Test the generation of weights."""
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path
import tempfile
import pytest
from teehr.utilities.generate_weights import generate_weights_file
from teehr.fetching.const import CONUS_NWM_WKT


TEST_DIR = Path("tests", "data", "nwm30")
TEMPLATE_FILEPATH = Path(TEST_DIR, "nwm_retro_v3_template_grid.nc")
ZONES_FILEPATH = Path(TEST_DIR, "one_huc10_conus_1016000606.parquet")
WEIGHTS_FILEPATH = Path(TEST_DIR, "one_huc10_1016000606_teehr_weights.parquet")


def _generate(tmpdir, **kwargs):
    """Generate weights into tmpdir and return the resulting dataframe."""
    out = Path(tmpdir, "weights.parquet")
    generate_weights_file(
        zone_polygons=ZONES_FILEPATH,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=out,
        unique_zone_id="id",
        **kwargs,
    )
    return pd.read_parquet(out)


def test_weights(tmpdir):
    """Generated weights match the reference file."""
    df = _generate(tmpdir)
    df_test = pd.read_parquet(WEIGHTS_FILEPATH)
    assert df.equals(df_test)


def test_weights_conserve_polygon_area(tmpdir):
    """Coverage fractions sum to the polygon's area in pixel units.

    The grid is 1km, so a zone's weights must sum to its area in km2. Catches
    dropped boundary pixels, which a stored-file comparison cannot.
    """
    df = _generate(tmpdir)
    zones = gpd.read_parquet(ZONES_FILEPATH).to_crs(CONUS_NWM_WKT)
    expected_km2 = zones.geometry.area.sum() / 1e6
    assert df.weight.astype("float64").sum() == pytest.approx(
        expected_km2, rel=1e-6
    )


def test_weights_are_valid_fractions(tmpdir):
    """Every weight is a real fraction of a pixel, and no pixel is repeated."""
    df = _generate(tmpdir)
    assert df.weight.between(0, 1, inclusive="right").all()
    assert not df.duplicated(subset=["location_id", "row", "col"]).any()
    assert df[["row", "col"]].min().min() >= 0


def test_weights_location_id_prefix(tmpdir):
    """The location_id prefix is applied to the zone ids."""
    df = _generate(tmpdir, location_id_prefix="ngen")
    assert df.location_id.str.startswith("ngen-").all()


def test_weights_zone_outside_grid_is_dropped(tmpdir):
    """A zone that intersects no pixel produces no rows rather than nulls."""
    zones = gpd.read_parquet(ZONES_FILEPATH).to_crs(CONUS_NWM_WKT)
    # Shift a copy far outside the CONUS grid.
    away = zones.copy()
    away["id"] = away["id"] + "-offgrid"
    away["geometry"] = away.geometry.translate(xoff=50_000_000)
    combined = gpd.GeoDataFrame(
        pd.concat([zones, away], ignore_index=True), crs=zones.crs
    )
    out = Path(tmpdir, "weights.parquet")
    generate_weights_file(
        zone_polygons=combined,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=out,
        unique_zone_id="id",
    )
    df = pd.read_parquet(out)
    assert not df.location_id.str.endswith("-offgrid").any()
    assert not df.isnull().any().any()


def test_weights_zone_id_named_id_is_not_replaced_by_fid(tmpdir):
    """A zone column named "id" survives.

    exactextract reads a column literally named "id" as the OGR feature id and
    returns 0, 1, 2 ... instead of the real values, which would silently write
    a weights file keyed by row number.
    """
    zones = gpd.read_parquet(ZONES_FILEPATH).to_crs(CONUS_NWM_WKT)
    second = zones.copy()
    second["id"] = "huc10-9999999999"
    second["geometry"] = second.geometry.translate(xoff=20_000)
    combined = gpd.GeoDataFrame(
        pd.concat([zones, second], ignore_index=True), crs=zones.crs
    )
    out = Path(tmpdir, "weights.parquet")
    generate_weights_file(
        zone_polygons=combined,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=out,
        unique_zone_id="id",
    )
    df = pd.read_parquet(out)
    assert set(df.location_id) == {"huc10-1016000606", "huc10-9999999999"}


def test_weighted_average_matches_exactextract_mean(tmpdir):
    """Weights fed through teehr's weighted average reproduce a zonal mean.

    exactextract's ``mean`` op is an independent path -- it never sees the
    weights file -- so this checks the row/col convention and the averaging
    together, rather than against a value this code produced.
    """
    import xarray as xr
    from exactextract import exact_extract
    from teehr.fetching.nwm.grid_utils import compute_weighted_average

    ds = xr.open_dataset(TEMPLATE_FILEPATH)
    da = ds["RAINRATE"].rio.write_crs(CONUS_NWM_WKT, inplace=True)
    # Deterministic values, so agreement is not an artefact of a near-constant
    # field.
    rng = np.random.default_rng(0)
    da = da.copy(data=rng.random(da.shape).astype("float32"))
    da = da.rio.write_crs(CONUS_NWM_WKT, inplace=True)

    weights = generate_weights_file(
        zone_polygons=ZONES_FILEPATH,
        template_dataset=da.to_dataset(name="RAINRATE"),
        variable_name="RAINRATE",
        crs_wkt=CONUS_NWM_WKT,
        output_weights_filepath=None,
        unique_zone_id="id",
    )
    grid_values = da.values[weights["row"].values, weights["col"].values]
    got = compute_weighted_average(grid_values, weights.copy())

    zones = gpd.read_parquet(ZONES_FILEPATH).to_crs(CONUS_NWM_WKT)
    zones["zone_key"] = zones["id"].astype(str)
    expected = exact_extract(
        rast=da, vec=zones, ops=["mean"], include_cols=["zone_key"],
        output="pandas",
    )

    merged = got.merge(
        expected, left_on="location_id", right_on="zone_key", validate="1:1"
    )
    assert len(merged) == len(zones)
    for row in merged.itertuples():
        # rel=1e-6: compute_weighted_average accumulates in float32.
        assert row.value == pytest.approx(row.mean, rel=1e-6)


def test_weights_row_col_are_absolute(tmpdir):
    """row/col index the full template grid, so layers share one space."""
    import xarray as xr

    df = _generate(tmpdir)
    ds = xr.open_dataset(TEMPLATE_FILEPATH)
    da = ds["RAINRATE"].rio.write_crs(CONUS_NWM_WKT, inplace=True)
    height, width = da.rio.height, da.rio.width

    assert df["row"].max() < height
    assert df["col"].max() < width
    # This HUC10 sits well inside CONUS; a clipped origin would start near 0.
    assert df["row"].min() > 100
    assert df["col"].min() > 100

    # Indexing the grid's coordinate arrays by row/col must round-trip.
    from rasterio.transform import rowcol

    back_row, back_col = rowcol(
        da.rio.transform(),
        da.x.values[df["col"].values],
        da.y.values[df["row"].values],
    )
    assert np.array_equal(np.asarray(back_row), df["row"].values)
    assert np.array_equal(np.asarray(back_col), df["col"].values)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="teehr-") as tempdir:
        test_weights(tempdir)
        test_weights_conserve_polygon_area(tempdir)
        test_weights_are_valid_fractions(tempdir)
        test_weights_location_id_prefix(tempdir)
        test_weights_zone_outside_grid_is_dropped(tempdir)
        test_weights_zone_id_named_id_is_not_replaced_by_fid(tempdir)
        test_weighted_average_matches_exactextract_mean(tempdir)
        test_weights_row_col_are_absolute(tempdir)
