"""Module for generating area-based weights for grid layer pixels."""
from typing import Union, Dict
from pathlib import Path
import logging

from geopandas import GeoDataFrame
import numpy as np
import xarray as xr
from rasterio.transform import rowcol
import pandas as pd
from exactextract import exact_extract
import rioxarray  # noqa: F401 - needed to add rio accessors to xarray objects

from teehr.fetching.nwm.grid_utils import update_location_id_prefix
from teehr.loading.utils import read_spatial_file
from teehr.fetching.const import LOCATION_ID
import teehr.models.pandera_dataframe_schemas as schemas

logger = logging.getLogger(__name__)


def _zone_pixel_coverage(
    src_da: xr.DataArray,
    zone_gdf: GeoDataFrame,
    unique_zone_id: str,
) -> pd.DataFrame:
    """Fraction of each grid pixel covered by each zone polygon.

    One row per intersecting (zone, pixel) pair. ``row``/``col`` index the full
    grid, never a clipped subset: exactextract windows the raster per polygon
    so clipping saves nothing, and clipping drops boundary pixels that still
    overlap the zone.
    """
    # A column named "id" is read as the OGR feature id, silently yielding
    # 0, 1, 2 ... instead of the real values, so pass the ids under a private
    # name and hand over nothing else.
    zones = GeoDataFrame(
        {"_teehr_zone_id": zone_gdf[unique_zone_id].astype(str).values},
        geometry=zone_gdf.geometry.make_valid().values,
        crs=zone_gdf.crs,
    )

    result = exact_extract(
        rast=src_da,
        vec=zones,
        ops=["cell_id", "coverage"],
        include_cols=["_teehr_zone_id"],
        output="pandas",
    )
    # One row per zone holding lists of cells; a zone intersecting nothing
    # contributes a single null row.
    result = result.explode(["cell_id", "coverage"])
    result = result.dropna(subset=["cell_id", "coverage"])

    cell_id = result["cell_id"].to_numpy(dtype=np.int64)
    width = src_da.rio.width
    # cell_id runs row-major from the north-west corner. Either axis may be
    # stored in the opposite order, so resolve through coordinate values and
    # let rowcol map them back.
    y_values = src_da.y.values
    if y_values[0] < y_values[-1]:
        y_values = y_values[::-1]
    x_values = src_da.x.values
    if x_values[0] > x_values[-1]:
        x_values = x_values[::-1]
    rows, cols = rowcol(
        src_da.rio.transform(),
        x_values[cell_id % width],
        y_values[cell_id // width],
    )

    return pd.DataFrame({
        "row": np.asarray(rows, dtype=np.int64),
        "col": np.asarray(cols, dtype=np.int64),
        "weight": result["coverage"].to_numpy(dtype=np.float32),
        LOCATION_ID: result["_teehr_zone_id"].to_numpy(dtype=object),
    })


def generate_weights_file(
    zone_polygons: Union[Path, str, GeoDataFrame],
    template_dataset: Union[str, Path, xr.Dataset],
    variable_name: str,
    output_weights_filepath: Union[str, Path, None],
    crs_wkt: str,
    unique_zone_id: str,
    location_id_prefix: str = None,
    **read_args: Dict,
) -> pd.DataFrame:
    """Generate a file of area weights for pixels intersecting zone polyons.

    Parameters
    ----------
    zone_polygons : Union[Path, str, GeoDataFrame]
        Path to the polygons geoparquet file or GeoDataFrame.
    template_dataset : Union[str, Path, xr.Dataset]
        Path to the grid dataset or an xarray Dataset to use as a template.
    variable_name : str
        Name of the variable within the dataset.
    output_weights_filepath : str or None
        Path to write the weights file to. If None, the weights are returned
        without being written.
    crs_wkt : str
        Coordinate system for given template gridded dataset as WKT string.
        The zone_polygons will be reprojected to this CRS.
    unique_zone_id : str
        Name of the field in the zone polygon file containing unique IDs.
    location_id_prefix : str
        Prefix to add to the location_id field.
    **read_args : dict, optional
        Keyword arguments to be passed to GeoPandas read_file().
        read_parquet(), and read_feather() methods.

    Returns
    -------
    pd.DataFrame
        Columns ``row``, ``col``, ``weight``, ``location_id``. ``row`` and
        ``col`` index the full template grid.

    Examples
    --------
    Here we generate weights for grid pixels intersecting a given
    polygon(s). The algorithm accounts for the fraction of the pixel
    area that is within the polygon. We'll use the Nextgen divides/
    catchments as the polygons and a NWM v2.2 forcing file as the
    template grid.

    Import the necessary modules.

    >>> from teehr.utilities.generate_weights import generate_weights_file
    >>> from teehr.fetching.nwm.const import CONUS_NWM_WKT

    Define the input variables.

    >>> TEST_DIR = Path("tests", "data", "nwm22")
    >>> TEMP_DIR = Path("tests", "data", "temp")
    >>> TEMPLATE_FILEPATH = Path(TEST_DIR, "test_template_grid.nc")
    >>> ZONES_FILEPATH = Path(TEST_DIR, "test_ngen_divides.parquet")
    >>> WEIGHTS_FILEPATH = Path(TEST_DIR, "test_weights_results.parquet")

    Perform the calculation, writing to the output directory, or optionally
    returning the dataframe if no output path is specified.

    >>> df = generate_weights_file(
    >>>     zone_polygon_filepath=ZONES_FILEPATH,
    >>>     template_dataset=TEMPLATE_FILEPATH,
    >>>     variable_name="RAINRATE",
    >>>     crs_wkt=CONUS_NWM_WKT,
    >>>     output_weights_filepath=None,
    >>>     location_id_prefix="ngen",
    >>>     unique_zone_id="id",
    >>> )
    """
    if unique_zone_id is None:
        logger.error("unique_zone_id must be provided.")
        raise ValueError("unique_zone_id must be provided.")

    if isinstance(zone_polygons, (str, Path)):
        zone_gdf = read_spatial_file(zone_polygons, **read_args)
        zone_gdf = zone_gdf.to_crs(crs_wkt)
    elif isinstance(zone_polygons, GeoDataFrame):
        zone_gdf = zone_polygons.to_crs(crs_wkt)
    else:
        logger.error(
            "zone_polygons must be a path to a file or a GeoDataFrame."
        )
        raise ValueError(
            "zone_polygons must be a path to a file or a GeoDataFrame."
        )

    if isinstance(template_dataset, (str, Path)):
        template_dataset = xr.open_dataset(template_dataset)
    src_da = template_dataset[variable_name]

    if not all([dim in src_da.dims for dim in ["x", "y"]]):
        raise ValueError("Template dataset must have x and y dimensions.")

    # Only the grid geometry matters; drop any non-spatial dimension.
    extra_dims = [d for d in src_da.dims if d not in ("x", "y")]
    if extra_dims:
        src_da = src_da.isel({d: 0 for d in extra_dims}, drop=True)
    src_da = src_da.astype("float32")
    src_da = src_da.rio.write_crs(crs_wkt, inplace=True)

    df = _zone_pixel_coverage(src_da, zone_gdf, unique_zone_id)

    if location_id_prefix:
        df = update_location_id_prefix(
            df, new_prefix=location_id_prefix
        )

    schema = schemas.weights_file_schema()
    validated_df = schema.validate(df)
    if output_weights_filepath is not None:
        validated_df.to_parquet(output_weights_filepath)

    return validated_df
