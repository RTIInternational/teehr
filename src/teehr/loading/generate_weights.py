from typing import Optional, Dict, Union

from pathlib import Path
import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.features import rasterize
import pandas as pd

from teehr.loading.utils_nwm import load_gdf
import teehr.loading.const_nwm as const_nwm


def generate_weights(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: Union[str, Path],
    unique_zone_id: str = None,
) -> None:
    """Generate a weights file with row/col indices."""
    gdf_proj = gdf.to_crs(const_nwm.CONUS_NWM_WKT)

    df_list = []
    # This is a probably a really poor performing way to do this
    for index, row in gdf_proj.iterrows():
        geom_rasterize = rasterize(
            [(row["geometry"], 1)],
            out_shape=src.rio.shape,
            transform=src.rio.transform(),
            all_touched=True,
            fill=0,
            dtype="uint8",
        )
        inds_tuple = np.where(geom_rasterize == 1)
        weights_tuple = inds_tuple + (np.ones(inds_tuple[0].size),)
        df = pd.DataFrame(
            {
                "row": inds_tuple[0],
                "col": inds_tuple[1],
                "weight": weights_tuple[2],
            }
        )
        if unique_zone_id:
            df["zone"] = row[unique_zone_id]
        else:
            df["zone"] = index

        df_list.append(df)

    df = pd.concat(df_list)
    df.to_parquet(weights_filepath)


def generate_weights_file(
    zone_polygon_filepath: Union[str, Path],
    template_dataset: Union[str, Path],
    variable_name: str,
    output_weights_filepath: Union[str, Path],
    unique_zone_id: str,
    read_args: Optional[Dict] = {},
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
    read_args: dict, optional
        Key-value argument pairs passed to GeoPandas for reading the
        zone polygon file
    """
    # Not 100% sure how best to manage this yet.  Hope a pattern will emerge.
    zone_gdf = load_gdf(zone_polygon_filepath, read_args)
    ds = xr.open_dataset(template_dataset)
    src_da = ds[variable_name]
    generate_weights(
        zone_gdf,
        src_da,
        output_weights_filepath,
        unique_zone_id,
    )


if __name__ == "__main__":
    # Local testing
    zone_polygon_filepath = "/mnt/sf_shared/data/ciroh/wbdhu10_conus.parquet"
    template_dataset = "/mnt/sf_shared/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa
    variable_name = "RAINRATE"
    unique_zone_id = "huc10"
    output_weights_filepath = (
        "/mnt/sf_shared/data/ciroh/wbdhu10_medium_range_weights.parquet"
    )

    generate_weights_file(
        zone_polygon_filepath,
        template_dataset,
        variable_name,
        output_weights_filepath,
        unique_zone_id,
    )
