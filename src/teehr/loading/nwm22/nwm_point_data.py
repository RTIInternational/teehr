from pathlib import Path
from typing import Union, Iterable, Optional, List, Tuple
from datetime import datetime

import pandas as pd
import dask
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from teehr.loading.nwm22.utils_nwm import (
    build_remote_nwm_filelist,
    build_zarr_references,
    get_dataset,
)

from teehr.loading.nwm22.point_config_models import PointConfigurationModel

from teehr.loading.nwm22.const_nwm import NWM22_UNIT_LOOKUP


@dask.delayed
def file_chunk_loop(
    row: Tuple,
    location_ids: np.array,
    variable_name: str,
    configuration: str,
    schema: pa.Schema,
    crash_on_missing_file: bool,
):
    """Fetch NWM values and convert to tabular format for a single json"""
    ds = get_dataset(row.filepath, crash_on_missing_file)
    if not ds:
        return None
    ds = ds.sel(feature_id=location_ids)
    vals = ds[variable_name].astype("float32").values
    nwm22_units = ds[variable_name].units
    teehr_units = NWM22_UNIT_LOOKUP.get(nwm22_units, nwm22_units)
    ref_time = pd.to_datetime(row.day) \
        + pd.to_timedelta(int(row.z_hour[1:3]), unit="H")

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
    process_by_z_hour: bool,
    crash_on_missing_file: bool,
) -> None:
    """Assemble a table for a chunk of NWM files"""

    location_ids = np.array(location_ids).astype(int)

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
                row,
                location_ids,
                variable_name,
                configuration,
                schema,
                crash_on_missing_file
            )
        )
    output = dask.compute(*results)

    if not any(output):
        raise FileNotFoundError("No NWM files for specified input"
                                "configuration were found in GCS!")

    output = [tbl for tbl in output if tbl is not None]
    output_table = pa.concat_tables(output)

    if process_by_z_hour:
        row = df.iloc[0]
        filename = f"{row.day}T{row.z_hour[1:3]}Z.parquet"
    else:
        # Use start and end dates including forecast hour
        #  for the output file name
        filepath_list = df.filepath.sort_values().tolist()
        start_json = filepath_list[0].split("/")[-1].split(".")
        start = f"{start_json[1]}T{start_json[3][1:3]}Z{start_json[6][1:]}F"
        end_json = filepath_list[-1].split("/")[-1].split(".")
        end = f"{end_json[1]}T{end_json[3][1:3]}Z{end_json[6][1:]}F"
        filename = f"{start}_{end}.parquet"

    pq.write_table(output_table, Path(output_parquet_dir, filename))


def fetch_and_format_nwm_points(
    json_paths: List[str],
    location_ids: Iterable[int],
    configuration: str,
    variable_name: str,
    output_parquet_dir: str,
    process_by_z_hour: bool,
    stepsize: int,
    crash_on_missing_file: bool,
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
    crash_on_missing_file: bool
        Flag specifying whether or not to fail if a missing NWM
        file is encountered
        True = fail
        False = skip and continue
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
            process_by_z_hour,
            crash_on_missing_file,
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
    process_by_z_hour: Optional[bool] = True,
    stepsize: Optional[int] = 100,
    crash_on_missing_file: Optional[bool] = True
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
    crash_on_missing_file: bool
        Flag specifying whether or not to fail if a missing NWM file is encountered
        True = fail
        False = skip and continue

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
    cm = PointConfigurationModel.parse_obj(vars)

    component_paths = build_remote_nwm_filelist(
        cm.configuration.name,
        cm.output_type.name,
        start_date,
        ingest_days,
        t_minus_hours,
        crash_on_missing_file,
    )

    json_paths = build_zarr_references(component_paths,
                                       json_dir,
                                       crash_on_missing_file)

    fetch_and_format_nwm_points(
        json_paths,
        location_ids,
        cm.configuration.name,
        cm.variable_name.name,
        output_parquet_dir,
        process_by_z_hour,
        stepsize,
        crash_on_missing_file,
    )


if __name__ == "__main__":
    configuration = (
        "short_range"  # analysis_assim_extend, short_range
    )
    output_type = "channel_rt"
    variable_name = "streamflow"
    start_date = "2023-08-24"
    ingest_days = 2
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

    process_by_z_hour = True
    stepsize = 100
    crash_on_missing_file = False

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
        process_by_z_hour=process_by_z_hour,
        stepsize=stepsize,
        crash_on_missing_file=crash_on_missing_file
    )
