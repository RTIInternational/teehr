from pathlib import Path
from typing import Union, Iterable, Tuple, Optional, List
from datetime import datetime

import fsspec
import xarray as xr
import pandas as pd
from kerchunk.combine import MultiZarrToZarr
import dask
import ujson


from teehr.loading.utils_nwm import (
    validate_run_args,
    build_remote_nwm_filelist,
    build_zarr_references,
)


def fetch_and_format_nwm_points(
    json_paths: List[str],
    location_ids: Iterable[int],
    run: str,
    variable_name: str,
    output_parquet_dir: str,
    concat_dims=["time"],
):
    """Reads in the single reference jsons, subsets the
        NWM data based on provided IDs and formats and saves
        the data as a parquet files using Dask.

    Parameters
    ----------
    json_paths: list
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

    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    # Format file list into a dataframe and group by reference time
    days = []
    z_hours = []
    for path in json_paths:
        filename = Path(path).name
        days.append(filename.split(".")[1])
        z_hours.append(filename.split(".")[3])
    df_refs = pd.DataFrame(
        {
            "day": days,
            "z_hour": z_hours,
            "filepath": json_paths
        }
    )
    gps = df_refs.groupby(["day", "z_hour"])
    results = []
    for gp in gps:
        results.append(
            fetch_and_format(
                gp,
                location_ids,
                run,
                variable_name,
                output_parquet_dir,
                concat_dims
            )
        )
    dask.compute(results)


@dask.delayed
def fetch_and_format(
    gp: Tuple[Tuple[str, str], pd.DataFrame],
    location_ids: Iterable[int],
    run: str,
    variable_name: str,
    output_parquet_dir: str,
    concat_dims: List[str],
) -> None:
    """Helper function to fetch and format the NWM data using Dask.

    Parameters
    ----------
    gp : Tuple[Tuple[str, str], pd.Dataframe],
        A tuple containing a tuple of (day, z_hour) and a dataframe of
        reference json filepaths for a specific z_hour.  Results from
        pandas dataframe.groupby(["day", "z_hour"])
    location_ids : Iterable[int]
        Array specifying NWM IDs of interest
    run : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    concat_dims : list of strings
        List of dimensions to use when concatenating single file
        jsons to multifile
    """
    _, df = gp
    # Only combine if there is more than one file for this group,
    # otherwise a warning is thrown
    if len(df.index) > 1:
        mzz = MultiZarrToZarr(
            df.filepath.tolist(),
            remote_protocol="gcs",
            remote_options={"anon": True},
            concat_dims=concat_dims,
        )
        json = mzz.translate()
    else:
        json = ujson.load(open(df.filepath.iloc[0]))
    fs = fsspec.filesystem("reference", fo=json)
    m = fs.get_mapper("")
    ds_nwm_subset = xr.open_zarr(m, consolidated=False, chunks={}).sel(
        feature_id=location_ids
    )
    # Convert to dataframe and do some reformatting
    df_temp = ds_nwm_subset[variable_name].to_dataframe()
    df_temp.reset_index(inplace=True)
    if len(df.index) == 1:
        df_temp["time"] = ds_nwm_subset.time.values[0]
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
    df_temp["location_id"] = "nwm22-" + df_temp.location_id.astype(int).astype(str)
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
    t_minus_hours: Optional[Iterable[int]] = None,
):
    """Fetches NWM point data, formats to tabular, and saves to parquet

    Parameters
    ----------
    run : str
        NWM forecast category.
        (e.g., "analysis_assim", "short_range", ...)
    output_type : str
        Output component of the configuration.
        (e.g., "channel_rt", "reservoir", ...)
    variable_name : str
        Name of the NWM data variable to download.
        (e.g., "streamflow", "velocity", ...)
    start_date : str or datetime
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY
    ingest_days : int
        Number of days to ingest data after start date
    location_ids : Iterable[int]
        Array specifying NWM IDs of interest
    json_dir : str
        Directory path for saving json reference files
    output_parquet_dir : str
        Path to the directory for the final parquet files
    t_minus_hours: Optional[Iterable[int]]
        Specifies the look-back hours to include if an assimilation
        run is specified.

    The NWM configuration variables, including run, output_type, and
    variable_name are stored in the NWM22_RUN_CONFIG dictionary in
    const_nwm.py.

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHHZ".  The tabular output
    parquet files follow the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries  # noqa
    """
    validate_run_args(
        run,
        output_type,
        variable_name
    )

    component_paths = build_remote_nwm_filelist(
        run,
        output_type,
        start_date,
        ingest_days,
        t_minus_hours,
    )

    json_paths = build_zarr_references(
        component_paths,
        json_dir
    )

    fetch_and_format_nwm_points(
        json_paths,
        location_ids,
        run,
        variable_name,
        output_parquet_dir,
    )
