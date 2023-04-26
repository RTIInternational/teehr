from pathlib import Path
from typing import Union, Iterable
from datetime import datetime

import fsspec
import xarray as xr
import pandas as pd
from kerchunk.combine import MultiZarrToZarr
import dask


from teehr.loading.utils_nwm import (
    validate_run_args,
    build_remote_nwm_filelist,
    build_zarr_references,
)


def fetch_and_format_nwm_points(
    lst_json_paths: list,
    location_ids: Iterable[int],
    run: str,
    variable_name: str,
    output_parquet_dir: str,
    concat_dims=["time"],
):
    """
    Reads in the single reference jsons, subsets the NWM data based on provided IDs
    and formats and saves the data as a parquet files usin Dask

    Parameters
    ----------
    lst_json_paths: list
        List of the single json reference filepaths
    location_ids : Iterable[int]
        Array specifying NWM IDs of interest
    run : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    """
    # Format file list into a dataframe and group by reference time
    days = []
    filepaths = []
    z_hours = []
    for path in lst_json_paths:
        filename = path.split("/")[6]
        filepaths.append(path)
        days.append(filename.split(".")[1])
        z_hours.append(filename.split(".")[3])
    df_refs = pd.DataFrame({"day": days, "z_hour": z_hours, "filepath": filepaths})
    gps = df_refs.groupby(["day", "z_hour"])
    results = []
    for gp in gps:
        results.append(
            fetch_and_format(
                gp, location_ids, run, variable_name, output_parquet_dir, concat_dims
            )
        )
    dask.compute(results)


@dask.delayed
def fetch_and_format(
    gp: pd.DataFrameGroupBy,
    location_ids: Iterable[int],
    run: str,
    variable_name: str,
    output_parquet_dir: str,
    concat_dims: list[str],
) -> None:
    """Helper function to fetch and format the NWM data using Dask

    Parameters
    ----------
    gp : Pandas group
        Contains a dataframe of reference json filepaths grouped by z_hour
    location_ids : Iterable[int]
        Array specifying NWM IDs of interest
    run : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    concat_dims : list
        List of dimensions to use when concatenating single file jsons to multifile
    """
    _, df = gp
    mzz = MultiZarrToZarr(
        df.filepath.tolist(),
        remote_protocol="gcs",
        remote_options={"anon": True},
        concat_dims=concat_dims,
    )
    json = mzz.translate()
    fs = fsspec.filesystem("reference", fo=json)
    m = fs.get_mapper("")
    ds_nwm_subset = xr.open_zarr(m, consolidated=False, chunks={}).sel(
        feature_id=location_ids
    )
    # Convert to dataframe and do some reformatting
    df_temp = ds_nwm_subset[variable_name].to_dataframe()
    df_temp.reset_index(inplace=True)
    df_temp.rename(
        columns={
            variable_name: "value",
            "time": "value_time",
            "feature_id": "location_id",
        },
        inplace=True,
    )
    df_temp.dropna(subset=["value"], inplace=True)
    df_temp["measurement_unit"] = ds_nwm_subset[variable_name].units
    ref_time = ds_nwm_subset.reference_time.values[0]
    df_temp["reference_time"] = ref_time
    df_temp["configuration"] = run
    df_temp["variable_name"] = variable_name
    df_temp["location_id"] = df_temp.location_id.astype(int)
    # Save to parquet
    ref_time_str = pd.to_datetime(ref_time).strftime("%Y%m%dT%HZ")
    parquet_filepath = Path(output_parquet_dir, f"{ref_time_str}.parquet")
    df_temp.to_parquet(parquet_filepath)


def nwm_to_parquet(
    run: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    location_ids: Iterable[int],
    json_dir: str,
    output_parquet_dir: str,
):
    """Fetches NWM point data, formats to tabular, and saves to parquet

    Parameters
    ----------
    run : str
        NWM forecast category ("analysis_assim", "short_range", ...)
    output_type : str
        Output component of the configuration ("channel_rt", "reservoir", ...)
    variable_name : str
        Name of the NWM data variable to download ("streamflow", "velocity", ...)
    start_date : str or datetime
        Date to begin data ingest.  str formats can include YYYY-MM-DD or MM/DD/YYYY
    ingest_days : int
        Number of days to ingest data after start date
    location_ids : Iterable[int]
        Array specifying NWM IDs of interest
    json_dir : str
        Directory path for saving json reference files
    output_parquet_dir : str
        Path to the directory for the final parquet files

    The NWM configuration variables, including run, output_type, and variable_name are stored
    in the NWM22_RUN_CONFIG dictionary in const_nwm.py.

    Forecast and assimilation data is grouped and saved one file per forecast reference time,
    using the file name convention "YYYYMMDDTHHZ".  The tabular output parquet files follow
    the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries
    """
    validate_run_args(run, output_type, variable_name)

    component_paths = build_remote_nwm_filelist(
        run, output_type, start_date, ingest_days
    )

    lst_json_paths = build_zarr_references(component_paths, json_dir)

    fetch_and_format_nwm_points(
        lst_json_paths,
        location_ids,
        run,
        variable_name,
        output_parquet_dir,
    )
