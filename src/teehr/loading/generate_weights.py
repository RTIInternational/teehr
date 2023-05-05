# import config
# import const

import geopandas as gpd
import numpy as np
import xarray as xr

from rasterio.features import rasterize
from utils_nwm import get_dataset, parquet_to_gdf, save_weights_dict
import const_nwm
import pandas as pd

TEMPLATE_BLOB_NAME = "/mnt/sf_shared/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa
ZONE_POLYGON_FILEPATH = "/mnt/sf_shared/data/ciroh/wbdhu10_conus.parquet"
OUTPUT_WEIGHTS_FILEPATH = (
    "/mnt/sf_shared/data/ciroh/wbdhu10_medium_range_weights_SJL.pkl.json"
)


def generate_weights_file(
    gdf: gpd.GeoDataFrame,
    src: xr.DataArray,
    weights_filepath: str,
    crosswalk_dict_key: str = None,
):
    """Generate a weights file."""
    gdf_proj = gdf.to_crs(const_nwm.CONUS_NWM_WKT)

    df_list = []
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

        inds_tuple = np.where(geom_rasterize == 1)
        weights_tuple = inds_tuple + (np.ones(inds_tuple[0].size),)
        df = pd.DataFrame(
            {
                "row": inds_tuple[0],
                "col": inds_tuple[1],
                "weight": weights_tuple[2],
            }
        )
        if crosswalk_dict_key:
            crosswalk_dict[row[crosswalk_dict_key]] = weights_tuple

            df["zone"] = row[crosswalk_dict_key]
        else:
            crosswalk_dict[index] = weights_tuple

            df["zone"] = index

        df_list.append(df)

    df = pd.concat(df_list)
    df.to_parquet(weights_filepath)
    # print("Finished saving parquet")

    # save_weights_dict(crosswalk_dict, weights_filepath)
    # print("Finished saving json")


def main():
    """Generate the weights file."""
    # Not 100% sure how best to manage this yet.  Hope a pattern will emerge.
    zone_gdf = parquet_to_gdf(ZONE_POLYGON_FILEPATH)
    ds = xr.open_dataset(TEMPLATE_BLOB_NAME)
    src = ds["RAINRATE"]
    generate_weights_file(
        zone_gdf,
        src,
        OUTPUT_WEIGHTS_FILEPATH,
        crosswalk_dict_key="huc10",
    )


if __name__ == "__main__":
    main()
