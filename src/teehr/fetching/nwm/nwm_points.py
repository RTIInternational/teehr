"""Module for fetchning and processing NWM point data."""
from typing import Union, Optional, List, Dict, Annotated
from datetime import datetime
from dateutil.parser import parse
from pathlib import Path
import logging
import pandas as pd

from pydantic import validate_call, Field

from teehr.fetching.nwm.point_utils import (
    fetch_and_format_nwm_points,
)
from teehr.fetching.utils import (
    generate_json_paths,
    build_remote_nwm_filelist,
    validate_operational_start_end_date,
    start_on_z_hour,
    end_on_z_hour,
    get_end_date_from_ingest_days
)
from teehr.models.fetching.utils import (
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.fetching.const import (
    NWM12_ANALYSIS_CONFIG,
    NWM20_ANALYSIS_CONFIG,
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)

logger = logging.getLogger(__name__)


@validate_call(config=dict(arbitrary_types_allowed=True))
def nwm_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    location_ids: List[int],
    json_dir: Union[str, Path],
    output_parquet_dir: Union[str, Path],
    nwm_version: SupportedNWMOperationalVersionsEnum,
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    ingest_days: Optional[int] = None,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
    prioritize_analysis_value_time: Optional[bool] = False,
    t_minus_hours: Optional[List[int]] = None,
    process_by_z_hour: Optional[bool] = True,
    stepsize: Optional[int] = 100,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = False,
    variable_mapper: Dict[str, Dict[str, str]] = None,
    timeseries_type: TimeseriesTypeEnum = "secondary",
    starting_z_hour: Optional[Annotated[int, Field(ge=0, le=23)]] = None,
    ending_z_hour: Optional[Annotated[int, Field(ge=0, le=23)]] = None,
    drop_overlapping_assimilation_values: Optional[bool] = True
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
    location_ids : List[int]
        Array specifying NWM IDs of interest.
    json_dir : str
        Directory path for saving json reference files.
    output_parquet_dir : str
        Path to the directory for the final parquet files.
    nwm_version : SupportedNWMOperationalVersionsEnum
        The NWM operational version.
        "nwm12", "nwm20", "nwm21", "nwm22", or "nwm30".
        Note that there is no change in NWM configuration between
        version 2.1 and 2.2, and they are treated as the same version.
        They are both allowed here for convenience.

        Availability of each version:

        - v1.2: 2018-09-17 - 2019-06-18
        - v2.0: 2019-06-19 - 2021-04-19
        - v2.1/2.2: 2021-04-20 - 2023-09-18
        - v3.0: 2023-09-19 - present
    start_date : Union[str, datetime, pd.Timestamp]
        Date and time to begin data ingest.
        Str formats can include YYYY-MM-DD HH:MM or MM/DD/YYYY HH:MM.
    end_date : Optional[Union[str, datetime, pd.Timestamp]],
        Date and time to end data ingest.
        Str formats can include YYYY-MM-DD HH:MM or MM/DD/YYYY HH:MM.
        If not provided, must provide ingest_days.
    ingest_days : Optional[int]
        Number of days to ingest data after start date. This is deprecated
        in favor of end_date, and will be removed in a future release.
        If both are provided, ingest_days takes precedence.
        If not provided, end_date must be specified.
    data_source : Optional[SupportedNWMDataSourcesEnum]
        Specifies the remote location from which to fetch the data
        "GCS" (default), "NOMADS", or "DSTOR".
        Currently only "GCS" is implemented.
    kerchunk_method : Optional[SupportedKerchunkMethod]
        When data_source = "GCS", specifies the preference in creating Kerchunk
        reference json files. "local" (default) will create new json files from
        netcdf files in GCS and save to a local directory if they do not already
        exist locally, in which case the creation is skipped. "remote" - read the
        CIROH pre-generated jsons from s3, ignoring any that are unavailable.
        "auto" - read the CIROH pre-generated jsons from s3, and create any that
        are unavailable, storing locally.
    prioritize_analysis_value_time : Optional[bool]
        A boolean flag that determines the method of fetching analysis data.
        When False (default), all hours of the reference time are included in the
        output. When True, only the hours within t_minus_hours are included.
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
    variable_mapper : Optional[Dict[str, Dict[str, str]]]
        A dictionary of dictionaries to map NWM variable and unit names to new names.
        If None, no mapping is applied and the original NWM variable names
        are used in the output.
        For example, to map "streamflow" to "discharge", and "mm s^-1" to "mm/s" use:
        variable_mapper = {"variable_name": {"streamflow": "discharge"},
        "unit_name": {"mm s^-1": "mm/s"}}
    timeseries_type : Optional[str]
        Whether to consider as the "primary" or "secondary" timeseries.
        Default is "secondary".
    starting_z_hour : Optional[int]
        The starting z_hour to include in the output. If None, z_hours
        for the first day are determined by ``start_date``. Default is None.
        Must be between 0 and 23.
    ending_z_hour : Optional[int]
        The ending z_hour to include in the output. If None, z_hours
        for the last day are determined by ``end_date`` if provided, otherwise
        all z_hours are included in the final day. Default is None.
        Must be between 0 and 23.
    drop_overlapping_assimilation_values: Optional[bool] = True
        Whether to drop assimilation values that overlap in value_time.
        Default is True. If True, values that overlap in value_time are dropped,
        keeping those with the most recent reference_time. In this case, all
        reference_time values are set to None. If False, overlapping values are
        kept and reference_time is retained.

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

    >>> import teehr.fetching.nwm.nwm_points as tlp

    Specify the input variables.

    >>> CONFIGURATION = "short_range"
    >>> OUTPUT_TYPE = "channel_rt"
    >>> VARIABLE_NAME = "streamflow"
    >>> START_DATE = "2023-03-18"
    >>> END_DATE = "2023-03-18"
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
    >>>     nwm_configuration=CONFIGURATION,
    >>>     output_type=OUTPUT_TYPE,
    >>>     variable_name=VARIABLE_NAME,
    >>>     start_date=START_DATE,
    >>>     end_date=END_DATE,
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
    logger.info(f"Fetching {configuration} data. Version: {nwm_version}")

    if isinstance(start_date, str):
        start_date = parse(start_date)

    if ingest_days is not None:
        end_date = get_end_date_from_ingest_days(
            start_date=start_date,
            ingest_days=ingest_days
        )
    elif end_date is None:
        raise ValueError(
            "Either 'end_date' or 'ingest_days' must be specified."
        )

    if isinstance(end_date, str):
        end_date = parse(end_date)

    # Import appropriate config model and dicts based on NWM version
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm12:
        from teehr.models.fetching.nwm12_point import PointConfigurationModel
        analysis_config_dict = NWM12_ANALYSIS_CONFIG
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm20:
        from teehr.models.fetching.nwm20_point import PointConfigurationModel
        analysis_config_dict = NWM20_ANALYSIS_CONFIG
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm21:
        from teehr.models.fetching.nwm22_point import PointConfigurationModel
        analysis_config_dict = NWM22_ANALYSIS_CONFIG
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm22:
        from teehr.models.fetching.nwm22_point import PointConfigurationModel
        analysis_config_dict = NWM22_ANALYSIS_CONFIG
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        from teehr.models.fetching.nwm30_point import PointConfigurationModel
        analysis_config_dict = NWM30_ANALYSIS_CONFIG
    else:
        raise ValueError(
            "nwm_version must equal "
            "'nwm12', 'nwm20', 'nwm21', 'nwm22' or 'nwm30'"
        )

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
        raise ValueError("Fetching from NOMADS is not yet implemented")
    elif data_source == SupportedNWMDataSourcesEnum.DSTOR:
        # TODO
        raise ValueError("Fetching from DSTOR is not yet implemented")
    else:

        # Make sure start/end dates work with specified NWM version
        validate_operational_start_end_date(
            nwm_version,
            start_date,
            end_date
        )

        # Build paths to netcdf files on GCS
        gcs_component_paths = build_remote_nwm_filelist(
            configuration,
            output_type,
            start_date,
            end_date,
            analysis_config_dict,
            t_minus_hours,
            ignore_missing_file,
            prioritize_analysis_value_time,
            drop_overlapping_assimilation_values,
            ingest_days
        )

        if starting_z_hour is None:
            starting_z_hour = start_date.hour
        if ending_z_hour is None:
            ending_z_hour = end_date.hour

        gcs_component_paths = start_on_z_hour(
            start_z_hour=starting_z_hour,
            gcs_component_paths=gcs_component_paths
        )

        gcs_component_paths = end_on_z_hour(
            end_z_hour=ending_z_hour,
            gcs_component_paths=gcs_component_paths
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
            overwrite_output,
            nwm_version,
            variable_mapper,
            timeseries_type
        )
