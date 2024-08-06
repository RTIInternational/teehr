"""Module for fetchning and processing NWM point data."""
from typing import Union, Optional, List
from datetime import datetime
from pathlib import Path

from pydantic import validate_call

from teehr.loading.nwm.point_utils import (
    fetch_and_format_nwm_points,
)
from teehr.loading.utils import (
    generate_json_paths,
    build_remote_nwm_filelist,
    check_dates_against_nwm_version
)
from teehr.models.loading.utils import (
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod
)
from teehr.loading.const import (
    NWM22_UNIT_LOOKUP,
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)


@validate_call()
def nwm_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    location_ids: List[int],
    json_dir: Union[str, Path],
    output_parquet_dir: Union[str, Path],
    nwm_version: SupportedNWMOperationalVersionsEnum,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
    t_minus_hours: Optional[List[int]] = None,
    process_by_z_hour: Optional[bool] = True,
    stepsize: Optional[int] = 100,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = False,
):
    """Fetch NWM point data and save as a Parquet file in TEEHR format.

    Parameters
    ----------
    configuration : str
        NWM forecast category.
        (e.g., "analysis_assim", "short_range", ...).
    output_type : str
        Output component of the configuration.
        (e.g., "channel_rt", "reservoir", ...).
    variable_name : str
        Name of the NWM data variable to download.
        (e.g., "streamflow", "velocity", ...).
    start_date : str or datetime
        Date to begin data ingest.
        Str formats can include YYYY-MM-DD or MM/DD/YYYY.
    ingest_days : int
        Number of days to ingest data after start date.
    location_ids : List[int]
        Array specifying NWM IDs of interest.
    json_dir : str
        Directory path for saving json reference files.
    output_parquet_dir : str
        Path to the directory for the final parquet files.
    nwm_version : SupportedNWMOperationalVersionsEnum
        The NWM operational version
        "nwm22", or "nwm30".
    data_source : Optional[SupportedNWMDataSourcesEnum]
        Specifies the remote location from which to fetch the data
        "GCS" (default), "NOMADS", or "DSTOR"
        Currently only "GCS" is implemented.
    kerchunk_method : Optional[SupportedKerchunkMethod]
        When data_source = "GCS", specifies the preference in creating Kerchunk
        reference json files. "local" (default) will create new json files from
        netcdf files in GCS and save to a local directory if they do not already
        exist locally, in which case the creation is skipped. "remote" - read the
        CIROH pre-generated jsons from s3, ignoring any that are unavailable.
        "auto" - read the CIROH pre-generated jsons from s3, and create any that
        are unavailable, storing locally.
    t_minus_hours : Optional[List[int]]
        Specifies the look-back hours to include if an assimilation
        configuration is specified.
    process_by_z_hour : Optional[bool]
        A boolean flag that determines the method of grouping files
        for processing. The default is True, which groups by day and z_hour.
        False groups files sequentially into chunks, whose size is determined
        by stepsize. This allows users to process more data potentially more
        efficiently, but runs to risk of splitting up forecasts into separate
        output files.
    stepsize : Optional[int]
        The number of json files to process at one time. Used if
        process_by_z_hour is set to False. Default value is 100. Larger values
        can result in greater efficiency but require more memory.
    ignore_missing_file : Optional[bool]
        Flag specifying whether or not to fail if a missing NWM file is
        encountered. True = skip and continue; False = fail.
    overwrite_output : Optional[bool]
        Flag specifying whether or not to overwrite output files if they
        already exist.  True = overwrite; False = fail.

    Notes
    -----
    The NWM configuration variables, including configuration, output_type, and
    variable_name are stored as pydantic models in point_config_models.py

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHH".  The tabular output
    parquet files follow the timeseries data model described in the
    :ref:`data model <data_model>`.

    All dates and times within the files and in the file names are in UTC.

    Examples
    --------
    Here we fetch operational streamflow forecasts for NWM v2.2 from GCS, and
    save the output in the TEEHR :ref:`data model <data_model>` format.

    Import the necessary module.

    >>> import teehr.loading.nwm.nwm_points as tlp

    Specify the input variables.

    >>> CONFIGURATION = "short_range"
    >>> OUTPUT_TYPE = "channel_rt"
    >>> VARIABLE_NAME = "streamflow"
    >>> START_DATE = "2023-03-18"
    >>> INGEST_DAYS = 1
    >>> JSON_DIR = Path(Path.home(), "temp/parquet/jsons/")
    >>> OUTPUT_DIR = Path(Path.home(), "temp/parquet")
    >>> NWM_VERSION = "nwm22"
    >>> DATA_SOURCE = "GCS"
    >>> KERCHUNK_METHOD = "auto"
    >>> T_MINUS = [0, 1, 2]
    >>> IGNORE_MISSING_FILE = True
    >>> OVERWRITE_OUTPUT = True
    >>> PROCESS_BY_Z_HOUR = True
    >>> STEPSIZE = 100
    >>> LOCATION_IDS = [7086109,  7040481,  7053819]

    Fetch and format the data, writing to the specified directory.

    >>> tlp.nwm_to_parquet(
    >>>     configuration=CONFIGURATION,
    >>>     output_type=OUTPUT_TYPE,
    >>>     variable_name=VARIABLE_NAME,
    >>>     start_date=START_DATE,
    >>>     ingest_days=INGEST_DAYS,
    >>>     location_ids=LOCATION_IDS,
    >>>     json_dir=JSON_DIR,
    >>>     output_parquet_dir=OUTPUT_DIR,
    >>>     nwm_version=NWM_VERSION,
    >>>     data_source=DATA_SOURCE,
    >>>     kerchunk_method=KERCHUNK_METHOD,
    >>>     t_minus_hours=T_MINUS,
    >>>     process_by_z_hour=PROCESS_BY_Z_HOUR,
    >>>     stepsize=STEPSIZE,
    >>>     ignore_missing_file=IGNORE_MISSING_FILE,
    >>>     overwrite_output=OVERWRITE_OUTPUT,
    >>> )
    """ # noqa
    # Import appropriate config model and dicts based on NWM version
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm22:
        from teehr_v0_3.models.loading.nwm22_point import PointConfigurationModel
        analysis_config_dict = NWM22_ANALYSIS_CONFIG
        unit_lookup_dict = NWM22_UNIT_LOOKUP
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        from teehr_v0_3.models.loading.nwm30_point import PointConfigurationModel
        analysis_config_dict = NWM30_ANALYSIS_CONFIG
        unit_lookup_dict = NWM22_UNIT_LOOKUP
    else:
        raise ValueError("nwm_version must equal 'nwm22' or 'nwm30'")

    # Parse input parameters to validate configuration
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }
    cm = PointConfigurationModel.model_validate(vars)
    configuration = cm.configuration.name
    forecast_obj = getattr(cm, configuration)
    output_type = forecast_obj.output_type.name
    variable_name = getattr(forecast_obj, output_type).name

    # Check data_source
    if data_source == SupportedNWMDataSourcesEnum.NOMADS:
        # TODO
        raise ValueError("Loading from NOMADS is not yet implemented")
    elif data_source == SupportedNWMDataSourcesEnum.DSTOR:
        # TODO
        raise ValueError("Loading from DSTOR is not yet implemented")
    else:

        # Make sure start/end dates work with specified NWM version
        check_dates_against_nwm_version(nwm_version, start_date, ingest_days)

        # Build paths to netcdf files on GCS
        gcs_component_paths = build_remote_nwm_filelist(
            configuration,
            output_type,
            start_date,
            ingest_days,
            analysis_config_dict,
            t_minus_hours,
            ignore_missing_file,
        )

        # Create paths to local and/or remote kerchunk jsons
        json_paths = generate_json_paths(
            kerchunk_method,
            gcs_component_paths,
            json_dir,
            ignore_missing_file
        )

        # Fetch the data, saving to parquet files based on TEEHR data model
        fetch_and_format_nwm_points(
            json_paths,
            location_ids,
            configuration,
            variable_name,
            output_parquet_dir,
            process_by_z_hour,
            stepsize,
            ignore_missing_file,
            unit_lookup_dict,
            overwrite_output,
            nwm_version,
        )


# if __name__ == "__main__":
#     # analysis_assim_extend, short_range, analysis_assim_alaska
#     configuration = (
#         "analysis_assim_extend"
#     )
#     output_type = "channel_rt"
#     variable_name = "streamflow"
#     start_date = "2023-11-28"
#     ingest_days = 1
#     location_ids = [
#         7086109,
#         7040481,
#         7053819,
#         7111205,
#         7110249,
#         14299781,
#         14251875,
#         14267476,
#         7152082,
#         14828145,
#     ]
#     # location_ids = np.load(
#     #     "/mnt/sf_shared/data/ciroh/temp_location_ids.npy"
#     # )  # all 2.7 million
#     json_dir = "/mnt/data/ciroh/jsons"
#     output_parquet_dir = "/mnt/data/ciroh/parquet"

#     process_by_z_hour = True
#     stepsize = 100
#     ignore_missing_file = False

#     import time
#     t1 = time.time()

#     nwm_to_parquet(
#         configuration,
#         output_type,
#         variable_name,
#         start_date,
#         ingest_days,
#         location_ids,
#         json_dir,
#         output_parquet_dir,
#         nwm_version="nwm30",
#         data_source="GCS",
#         kerchunk_method="use_available",
#         t_minus_hours=[0],
#         process_by_z_hour=process_by_z_hour,
#         stepsize=stepsize,
#         ignore_missing_file=ignore_missing_file,
#         overwrite_output=False,
#     )

#     print(f"elapsed: {time.time() - t1:.2f} s")
