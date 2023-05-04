import pickle
import config
import const

import geopandas as gpd
import numpy as np
import xarray as xr

from rasterio.features import rasterize
from utils import parquet_to_gdf
from grid_to_parquet import get_dataset

TEMPLATE_BLOB_NAME = "/mnt/sf_shared/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa


def generate_weights_file(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
    crosswalk_dict_key: str = None,
):
    """Generate a weights file."""

    gdf_proj = gdf.to_crs(const.CONUS_NWM_WKT)

    crosswalk_dict = {}
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
        if crosswalk_dict_key:
            crosswalk_dict[row[crosswalk_dict_key]] = np.where(
                geom_rasterize == 1
            )
        else:
            crosswalk_dict[index] = np.where(geom_rasterize == 1)

    utils.save_weights_dict(crosswalk_dict, weights_filepath)


def main():
    """Generate the weights file."""
    # Not 100% sure how best to manage this yet.  Hope a pattern will emerge.
    huc10_gdf = parquet_to_gdf(config.HUC10_SHP_FILEPATH)
    ds = get_dataset(TEMPLATE_BLOB_NAME, use_cache=True)
    src = ds["RAINRATE"]
    generate_weights_file(
        huc10_gdf,
        src,
        config.HUC10_MEDIUM_RANGE_WEIGHTS_FILEPATH,
        crosswalk_dict_key="huc10",
    )


if __name__ == "__main__":
    main()
