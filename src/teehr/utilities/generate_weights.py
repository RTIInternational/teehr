from typing import Union
from pathlib import Path
import warnings

import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.transform import rowcol
import rasterio
import pandas as pd
import dask
import shapely

from teehr.loading.nwm22.utils_nwm import load_gdf
import teehr.loading.nwm22.const_nwm as const_nwm


@dask.delayed
def vectorize(data_array: xr.DataArray) -> gpd.GeoDataFrame:
    """
    Convert 2D xarray.DataArray into a geopandas.GeoDataFrame

    Heavily borrowed from GeoCube, see:
    https://github.com/corteva/geocube/blob/master/geocube/vector.py#L12
    """
    # nodata mask
    mask = None
    if np.isnan(data_array.rio.nodata):
        mask = ~data_array.isnull()
    elif data_array.rio.nodata is not None:
        mask = data_array != data_array.rio.nodata

    # Give all pixels a unique value
    data_array.values[:, :] = np.arange(0, data_array.values.size).reshape(
        data_array.shape
    )

    # vectorize generator
    vectorized_data = (
        (value, shapely.geometry.shape(polygon))
        for polygon, value in rasterio.features.shapes(
            data_array,
            transform=data_array.rio.transform(),
            mask=mask,
        )
    )
    gdf = gpd.GeoDataFrame(
        vectorized_data,
        columns=[data_array.name, "geometry"],
        crs=data_array.rio.crs,
    )
    xx, yy = np.meshgrid(data_array.x.values, data_array.y.values)
    gdf["x"] = xx.ravel()
    gdf["y"] = yy.ravel()

    return gdf


@dask.delayed
def overlay_zones(
    grid: gpd.GeoDataFrame, zones: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    with pd.option_context(
        "mode.chained_assignment", None
    ):  # to ignore setwithcopywarning
        grid.loc[:, "pixel_area"] = grid.geometry.area
        overlay_gdf = grid.overlay(zones, keep_geom_type=True)
        overlay_gdf.loc[:, "overlay_area"] = overlay_gdf.geometry.area
        overlay_gdf.loc[:, "weight"] = (
            overlay_gdf.overlay_area / overlay_gdf.pixel_area
        )
    return overlay_gdf


def vectorize_grid(
    src_da: xr.DataArray,
    nodata_val: float,
    vectorize_chunk: float = 40,
) -> gpd.GeoDataFrame:
    """Vectorize pixels in the template array in chunks using dask

    Note: Parameter vectorize_chunk determines how many pixels will
    be vectorized at one time
    (thousands of pixels)
    """
    src_da = src_da.persist()
    max_pixels = vectorize_chunk * 1000
    num_splits = np.ceil(src_da.values.size / max_pixels).astype(int)

    # Prepare each data array
    if num_splits > 0:
        da_list = np.array_split(src_da, num_splits)
        [da.rio.write_nodata(nodata_val, inplace=True) for da in da_list]
    else:
        src_da.rio.write_nodata(nodata_val, inplace=True)
        da_list = [src_da]

    results = []
    for da_subset in da_list:
        results.append(vectorize(da_subset))
    grid_gdf = pd.concat(dask.compute(results)[0])
    grid_gdf.crs = const_nwm.CONUS_NWM_WKT

    # Reindex to remove duplicates
    grid_gdf["index"] = np.arange(len(grid_gdf.index))
    grid_gdf.set_index("index", inplace=True)

    return grid_gdf


def calculate_weights(
    grid_gdf: gpd.GeoDataFrame,
    zone_gdf: gpd.GeoDataFrame,
    overlay_chunk: float = 250,
) -> gpd.GeoDataFrame:
    """Overlay vectorized pixels and zone polygons, and calculate
    areal weights, returning a geodataframe

    Note: Parameter overlay_chunk determines the size of the rectangular
    window that spatially subsets datasets for the operation
    (thousands of pixels)
    """
    # Make sure geometries are valid
    grid_gdf["geometry"] = grid_gdf.geometry.make_valid()
    zone_gdf["geometry"] = zone_gdf.geometry.make_valid()

    xmin, ymin, xmax, ymax = zone_gdf.total_bounds

    x_steps = np.arange(xmin, xmax, overlay_chunk * 1000)
    y_steps = np.arange(ymin, ymax, overlay_chunk * 1000)

    x_steps = np.append(x_steps, xmax)
    y_steps = np.append(y_steps, ymax)

    results = []
    for i in range(x_steps.size - 1):
        for j in range(y_steps.size - 1):
            xmin = x_steps[i]
            xmax = x_steps[i + 1]

            ymin = y_steps[j]
            ymax = y_steps[j + 1]

            zone = zone_gdf.cx[xmin:xmax, ymin:ymax]
            grid = grid_gdf.cx[xmin:xmax, ymin:ymax]

            if len(zone.index) == 0 or len(grid.index) == 0:
                continue
            results.append(overlay_zones(grid, zone))

    overlay_gdf = pd.concat(dask.compute(results)[0])

    return overlay_gdf


def generate_weights_file(
    zone_polygon_filepath: Union[Path, str],
    template_dataset: Union[str, Path],
    variable_name: str,
    output_weights_filepath: Union[str, Path],
    unique_zone_id: str = None,
    **read_args: str,
) -> None:
    """Generate a file of row/col indices and weights for pixels intersecting
       given zone polyons

    Parameters
    ----------
    zone_polygon_filepath : str
        Path to the polygons geoparquet file
    template_dataset : str
        Path to the grid dataset to use as a template
    variable_name : str
        Name of the variable within the dataset
    output_weights_filepath : str
        Path to the resultant weights file
    unique_zone_id: str
        Name of the field in the zone polygon file containing unique IDs
    save_to_disk: boolean
        Flag to indicate whether or not to save results to disk
    read_args: dict, optional
        Keyword arguments to be passed to GeoPandas read_file(),
        read_parquet(), and read_feather() methods
    """

    zone_gdf = load_gdf(zone_polygon_filepath, **read_args)
    zone_gdf = zone_gdf.to_crs(const_nwm.CONUS_NWM_WKT)

    ds = xr.open_dataset(template_dataset)
    src_da = ds[variable_name]
    src_da = src_da.rio.write_crs(const_nwm.CONUS_NWM_WKT, inplace=True)
    grid_transform = src_da.rio.transform()
    nodata_val = src_da.rio.nodata

    # Get the subset of the grid that intersects the total zone bounds
    bbox = tuple(zone_gdf.total_bounds)
    src_da = src_da.sel(x=slice(bbox[0], bbox[2]), y=slice(bbox[1], bbox[3]))[
        0
    ]
    src_da = src_da.astype("float32")
    src_da["x"] = np.float32(src_da.x.values)
    src_da["y"] = np.float32(src_da.y.values)

    # Vectorize source grid pixels
    grid_gdf = vectorize_grid(src_da, nodata_val)

    # Overlay and calculate areal weights of pixels within each zone
    # Note: Temporarily suppress the dask UserWarning: "Large object detected
    #  in task graph" until a better approach is found
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        weights_gdf = calculate_weights(grid_gdf, zone_gdf)
    weights_gdf = weights_gdf.drop_duplicates(
        subset=["x", "y", unique_zone_id]
    )

    # Convert x-y to row-col using original transform
    rows, cols = rowcol(
        grid_transform, weights_gdf.x.values, weights_gdf.y.values
    )
    weights_gdf["row"] = rows
    weights_gdf["col"] = cols

    if unique_zone_id:
        df = weights_gdf[["row", "col", "weight", unique_zone_id]].copy()
        df.rename(columns={unique_zone_id: "zone"}, inplace=True)
    else:
        df = weights_gdf[["row", "col", "weight"]]
        df["zone"] = weights_gdf.index.values

    if output_weights_filepath:
        df.to_parquet(output_weights_filepath)
        df = None

    return df


if __name__ == "__main__":
    # Local testing
    zone_polygon_filepath = "/mnt/sf_shared/data/ciroh/nextgen_03S.gpkg"
    template_dataset = "/mnt/sf_shared/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa
    variable_name = "RAINRATE"
    unique_zone_id = "id"
    output_weights_filepath = (
        "/mnt/sf_shared/data/ciroh/wbdhu10_medium_range_weights.parquet"
    )
    zone_polygon_filepath = (
        "/mnt/sf_shared/data/ciroh/test_ngen_divides.parquet"
    )

    generate_weights_file(
        zone_polygon_filepath,
        template_dataset,
        variable_name,
        output_weights_filepath,
        unique_zone_id,
    )
