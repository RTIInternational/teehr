from typing import Union, Optional, List
from datetime import datetime

from pydantic import validate_arguments  # validate_call in > 2

from teehr.loading.nwm.point_utils import (
    fetch_and_format_nwm_points,
)
from teehr.loading.nwm.utils import (
    generate_json_paths,
    build_remote_nwm_filelist,
    check_dates_against_nwm_version
)
from teehr.models.loading.utils import (
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod
)
from teehr.loading.nwm.const import (
    NWM22_UNIT_LOOKUP,
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)


@validate_arguments
def nwm_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    location_ids: List[int],
    json_dir: str,
    output_parquet_dir: str,
    nwm_version: SupportedNWMOperationalVersionsEnum,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    kerchunk_method: Optional[SupportedKerchunkMethod] = "create",
    t_minus_hours: Optional[List[int]] = None,
    process_by_z_hour: Optional[bool] = True,
    stepsize: Optional[int] = 100,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = False,
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
    location_ids : List[int]
        Array specifying NWM IDs of interest
    json_dir : str
        Directory path for saving json reference files
    output_parquet_dir : str
        Path to the directory for the final parquet files
    nwm_version: str
        The NWM operational version
        ("nwm22", "nwm30")
    data_source: str
        Specifies the remote location from which to fetch the data
        ("GCS", "NOMADS", "DSTOR")
    kerchunk_method: str
        When data_source = "GCS", specifies the preference in creating Kerchunk
        reference json files.
        "create" - always create new json files from netcdf files in GCS and
                   save locally
        "use_available" - read the CIROH pre-generated jsons from s3, ignoring
                          any that are unavailable
        "auto" - read the CIROH pre-generated jsons from s3, and create
                          any that are unavailable, storing locally
    t_minus_hours: Optional[List[int]]
        Specifies the look-back hours to include if an assimilation
        configuration is specified.
    process_by_z_hour: Optional[bool]
        A boolean flag that determines the method of grouping files
        for processing. The default is True, which groups by day and z_hour.
        False groups files sequentially into chunks, whose size is determined
        by stepsize. This allows users to process more data potentially more
        efficiently, but runs to risk of splitting up forecasts into separate
        output files.
    stepsize: Optional[int]
        The number of json files to process at one time. Used if
        process_by_z_hour is set to False. Default value is 100. Larger values
        can result in greater efficiency but require more memory
    ignore_missing_file: Optional[bool]
        Flag specifying whether or not to fail if a missing NWM file is encountered
        True = skip and continue
        False = fail
    overwrite_output: Optional[bool]
        Flag specifying whether or not to overwrite output files if they already
        exist.  True = overwrite; False = fail

    The NWM configuration variables, including configuration, output_type, and
    variable_name are stored as pydantic models in point_config_models.py

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHHZ".  The tabular output
    parquet files follow the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries  # noqa
    """

    # Import appropriate config model and dicts based on NWM version
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm22:
        from teehr.models.loading.nwm22_point import PointConfigurationModel
        analysis_config_dict = NWM22_ANALYSIS_CONFIG
        unit_lookup_dict = NWM22_UNIT_LOOKUP
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        from teehr.models.loading.nwm30_point import PointConfigurationModel
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
    cm = PointConfigurationModel.parse_obj(vars)
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

        json_paths = generate_json_paths(
            kerchunk_method,
            gcs_component_paths,
            json_dir,
            ignore_missing_file
        )

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


if __name__ == "__main__":
    # analysis_assim_extend, short_range, analysis_assim_alaska
    configuration = (
        "short_range"
    )
    output_type = "channel_rt"
    variable_name = "streamflow"
    start_date = "2023-11-28"
    ingest_days = 1
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
    json_dir = "/mnt/data/ciroh/jsons"
    output_parquet_dir = "/mnt/data/ciroh/parquet"

    process_by_z_hour = True
    stepsize = 100
    ignore_missing_file = False

    import time
    t1 = time.time()

    nwm_to_parquet(
        configuration,
        output_type,
        variable_name,
        start_date,
        ingest_days,
        location_ids,
        json_dir,
        output_parquet_dir,
        nwm_version="nwm30",
        data_source="GCS",
        kerchunk_method="use_available",
        t_minus_hours=[0, 1, 2],
        process_by_z_hour=process_by_z_hour,
        stepsize=stepsize,
        ignore_missing_file=ignore_missing_file,
        overwrite_output=False,
    )

    print(f"elapsed: {time.time() - t1:.2f} s")
