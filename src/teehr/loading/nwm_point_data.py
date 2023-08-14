from pathlib import Path
from typing import Union, Iterable, Optional, List, Tuple
from datetime import datetime

import pandas as pd
import dask
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from teehr.loading.utils_nwm import (
    build_remote_nwm_filelist,
    build_zarr_references,
    get_dataset,
)

from teehr.loading.point_config_models import PointConfigurationModel

from teehr.loading.const_nwm import NWM22_UNIT_LOOKUP


@dask.delayed
def file_chunk_loop(
    row: Tuple,
    location_ids: Iterable[int],
    variable_name: str,
    configuration: str,
    schema: pa.Schema,
):
    """Fetch NWM values and convert to tabular format for a single json"""
    ds = get_dataset(row.filepath).sel(feature_id=location_ids)
    vals = ds[variable_name].astype("float32").values
    nwm22_units = ds[variable_name].units
    teehr_units = NWM22_UNIT_LOOKUP.get(nwm22_units, nwm22_units)
    ref_time = ds.reference_time.values
    valid_time = ds.time.values
    feature_ids = ds.feature_id.astype("int32").values
    teehr_location_ids = [f"nwm22-{feat_id}" for feat_id in feature_ids]
    num_vals = vals.size

    output_table = pa.table(
        {
            "value": vals,
            "reference_time": np.full(vals.shape, ref_time),
            "location_id": teehr_location_ids,
            "value_time": np.full(vals.shape, valid_time),
            "configuration": num_vals * [configuration],
            "variable_name": num_vals * [variable_name],
            "measurement_unit": num_vals * [teehr_units],
        },
        schema=schema,
    )

    return output_table


def process_chunk_of_files(
    df: pd.DataFrame,
    location_ids: Iterable[int],
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
) -> None:
    """Assemble a table of NWM values for a chunk of NWM files"""

    schema = pa.schema(
        [
            ("value", pa.float32()),
            ("reference_time", pa.timestamp("ms")),
            ("location_id", pa.string()),
            ("value_time", pa.timestamp("ms")),
            ("configuration", pa.string()),
            ("variable_name", pa.string()),
            ("measurement_unit", pa.string()),
        ]
    )

    results = []
    for row in df.itertuples():
        results.append(
            file_chunk_loop(
                row, location_ids, variable_name, configuration, schema
            )
        )
    output = dask.compute(*results)
    output_table = pa.concat_tables(output)

    max_ref = pa.compute.max(output_table["reference_time"])
    min_ref = pa.compute.min(output_table["reference_time"])

    if max_ref != min_ref:
        min_ref_str = pa.compute.strftime(min_ref, format="%Y%m%dT%HZ")
        max_ref_str = pa.compute.strftime(max_ref, format="%Y%m%dT%HZ")
        filename = f"{min_ref_str}_{max_ref_str}.parquet"
    else:
        min_ref_str = pa.compute.strftime(min_ref, format="%Y%m%dT%HZ")
        filename = f"{min_ref_str}.parquet"

    pq.write_table(output_table, Path(output_parquet_dir, filename))


def fetch_and_format_nwm_points(
    json_paths: List[str],
    location_ids: Iterable[int],
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
    process_by_z_hour: bool,
    stepsize: int,
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
    configuration : str
        NWM forecast category
    variable_name : str
        Name of the NWM data variable to download
    output_parquet_dir : str
        Path to the directory for the final parquet files
    process_by_z_hour: bool
        A boolean flag that determines the method of grouping files
        for processing.
    stepsize: int
        The number of json files to process at one time.
    """

    output_parquet_dir = Path(output_parquet_dir)
    if not output_parquet_dir.exists():
        output_parquet_dir.mkdir(parents=True)

    # Format file list into a dataframe and group by specified method
    days = []
    z_hours = []
    for path in json_paths:
        filename = Path(path).name
        days.append(filename.split(".")[1])
        z_hours.append(filename.split(".")[3])
    df_refs = pd.DataFrame(
        {"day": days, "z_hour": z_hours, "filepath": json_paths}
    )

    if process_by_z_hour:
        # Option #1. Groupby day and z_hour
        gps = df_refs.groupby(["day", "z_hour"])
        dfs = [df for _, df in gps]
    else:
        # Option #2. Chunk by some number of files
        if stepsize > df_refs.index.size:
            num_partitions = 1
        else:
            num_partitions = int(df_refs.index.size / stepsize)
        dfs = np.array_split(df_refs, num_partitions)

    for df in dfs:
        process_chunk_of_files(
            df,
            location_ids,
            configuration,
            variable_name,
            output_parquet_dir,
        )


def nwm_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    location_ids: Iterable[int],
    json_dir: str,
    output_parquet_dir: str,
    t_minus_hours: Optional[Iterable[int]] = None,
    process_by_z_hour=True,
    stepsize=100,
):
    """Fetches NWM point data, formats to tabular, and saves to parquet

    Parameters
    ----------
    configuration : str
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
        configuration is specified.
    process_by_z_hour: bool
        A boolean flag that determines the method of grouping files
        for processing. The default is True, which groups by day and z_hour.
        False groups files sequentially into chunks, whose size is determined
        by stepsize. This allows users to process more data potentially more
        efficiently, but runs to risk of splitting up forecasts into separate
        output files.
    stepsize: int
        The number of json files to process at one time. Used if
        process_by_z_hour is set to False. Default value is 100. Larger values
        can result in greater efficiency but require more memory

    The NWM configuration variables, including configuration, output_type, and
    variable_name are stored as pydantic models in point_config_models.py

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHHZ".  The tabular output
    parquet files follow the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries  # noqa
    """

    # Parse input parameters
    vars = {
        "configuration": configuration,
        "output_type": output_type,
        "variable_name": variable_name,
        configuration: {
            output_type: variable_name,
        },
    }
    cm = PointConfigurationModel.model_validate(vars)

    component_paths = build_remote_nwm_filelist(
        cm.configuration.name,
        cm.output_type.name,
        start_date,
        ingest_days,
        t_minus_hours,
    )

    json_paths = build_zarr_references(component_paths, json_dir)

    fetch_and_format_nwm_points(
        json_paths,
        location_ids,
        cm.configuration.name,
        cm.variable_name.name,
        output_parquet_dir,
        process_by_z_hour,
        stepsize,
    )


if __name__ == "__main__":
    configuration = (
        "analysis_assim_extend"  # analysis_assim_extend, short_range
    )
    output_type = "channel_rt"
    variable_name = "streamflow"
    start_date = "2023-03-18"
    ingest_days = 3
    location_ids = [
        7086109,
        7040481,
        7053819,
        7111205,
        7110249,
        14299781,
        14251875,
        14267476,
        7152082,
        14828145,
    ]
    # location_ids = np.load(
    #     "/mnt/sf_shared/data/ciroh/temp_location_ids.npy"
    # )  # all 2.7 million
    json_dir = "/mnt/sf_shared/data/ciroh/jsons"
    output_parquet_dir = "/mnt/sf_shared/data/ciroh/parquet"

    # Start dask client here first?  Need to install dask[distributed]
    # python -m pip install "dask[distributed]" --upgrade
    # from dask.distributed import Client
    # client = Client(n_workers=10)

    nwm_to_parquet(
        configuration,
        output_type,
        variable_name,
        start_date,
        ingest_days,
        location_ids,
        json_dir,
        output_parquet_dir,
        t_minus_hours=[0, 1, 2],
    )
