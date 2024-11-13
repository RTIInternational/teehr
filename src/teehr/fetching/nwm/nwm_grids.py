"""Module for fetching and processing NWM gridded data."""
from typing import Union, List, Optional, Dict
from datetime import datetime
from pathlib import Path

from pydantic import validate_call

from teehr.fetching.nwm.grid_utils import fetch_and_format_nwm_grids
from teehr.fetching.utils import (
    build_remote_nwm_filelist,
    generate_json_paths,
    check_dates_against_nwm_version
)
from teehr.models.fetching.utils import (
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod
)
from teehr.fetching.const import (
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)


@validate_call()
def nwm_grids_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    zonal_weights_filepath: Union[Path, str],
    json_dir: Union[str, Path],
    output_parquet_dir: Union[str, Path],
    nwm_version: SupportedNWMOperationalVersionsEnum,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
    prioritize_analysis_valid_time: Optional[bool] = False,
    t_minus_hours: Optional[List[int]] = None,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = False,
    location_id_prefix: Optional[Union[str, None]] = None,
    variable_mapper: Dict[str, Dict[str, str]] = None
):
    """
    Fetch NWM gridded data, calculate zonal statistics (currently only
    mean is available) of selected variable for given zones, convert
    and save to TEEHR tabular format.

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
    zonal_weights_filepath : str
        Path to the array containing fraction of pixel overlap
        for each zone.
    json_dir : str
        Directory path for saving json reference files.
    output_parquet_dir : str
        Path to the directory for the final parquet files.
    nwm_version : SupportedNWMOperationalVersionsEnum
        The NWM operational version.
        "nwm22", or "nwm30".
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
    prioritize_analysis_valid_time : Optional[bool]
        A boolean flag that determines the method of fetching analysis data.
        When False (default), all hours of the reference time are included in the
        output. When True, only the hours within t_minus_hours are included.
    t_minus_hours : Optional[Iterable[int]]
        Specifies the look-back hours to include if an assimilation
        configuration is specified.
    ignore_missing_file : bool
        Flag specifying whether or not to fail if a missing NWM file is encountered
        True = skip and continue; False = fail.
    overwrite_output : bool
        Flag specifying whether or not to overwrite output files if they already
        exist.  True = overwrite; False = fail.
    location_id_prefix : Union[str, None]
        Optional location ID prefix to add (prepend) or replace.

    See Also
    --------
    teehr.utilities.generate_weights.generate_weights_file : Weighted average.

    Notes
    -----
    The NWM configuration variables, including configuration, output_type, and
    variable_name are stored as a pydantic model in grid_config_models.py.

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHH".  The tabular output
    parquet files follow the timeseries data model described in the
    :ref:`data model <data_model>`.

    Additionally, the location_id values in the zonal weights file are used as
    location ids in the output of this function, unless a prefix is specified which
    will be prepended to the location_id values if none exists, or will it replace
    the existing prefix. It is assumed that the location_id follows the pattern
    '[prefix]-[unique id]'.

    All dates and times within the files and in the file names are in UTC.

    Examples
    --------
    Here we will calculate mean areal precipitation using NWM forcing data for
    some watersheds (polygons) a using pre-calculated weights file
    (see: :func:`generate_weights_file()
    <teehr.utilities.generate_weights.generate_weights_file>` for weights calculation).

    Import the necessary module.

    >>> import teehr.fetching.nwm.nwm_grids as tlg

    Specify the input variables.

    >>> CONFIGURATION = "forcing_short_range"
    >>> OUTPUT_TYPE = "forcing"
    >>> VARIABLE_NAME = "RAINRATE"
    >>> START_DATE = "2020-12-18"
    >>> INGEST_DAYS = 1
    >>> ZONAL_WEIGHTS_FILEPATH = Path(Path.home(), "nextgen_03S_weights.parquet")
    >>> JSON_DIR = Path(Path.home(), "temp/parquet/jsons/")
    >>> OUTPUT_DIR = Path(Path.home(), "temp/parquet")
    >>> NWM_VERSION = "nwm22"
    >>> DATA_SOURCE = "GCS"
    >>> KERCHUNK_METHOD = "auto"
    >>> T_MINUS = [0, 1, 2]
    >>> IGNORE_MISSING_FILE = True
    >>> OVERWRITE_OUTPUT = True

    Perform the calculations, writing to the specified directory.

    >>> tlg.nwm_grids_to_parquet(
    >>>     configuration=CONFIGURATION,
    >>>     output_type=OUTPUT_TYPE,
    >>>     variable_name=VARIABLE_NAME,
    >>>     start_date=START_DATE,
    >>>     ingest_days=INGEST_DAYS,
    >>>     zonal_weights_filepath=ZONAL_WEIGHTS_FILEPATH,
    >>>     json_dir=JSON_DIR,
    >>>     output_parquet_dir=OUTPUT_DIR,
    >>>     nwm_version=NWM_VERSION,
    >>>     data_source=DATA_SOURCE,
    >>>     kerchunk_method=KERCHUNK_METHOD,
    >>>     t_minus_hours=T_MINUS,
    >>>     ignore_missing_file=IGNORE_MISSING_FILE,
    >>>     overwrite_output=OVERWRITE_OUTPUT
    >>> )
    """ # noqa
    # Import appropriate config model and dicts based on NWM version
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm22:
        from teehr.models.fetching.nwm22_grid import GridConfigurationModel
        analysis_config_dict = NWM22_ANALYSIS_CONFIG
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        from teehr.models.fetching.nwm30_grid import GridConfigurationModel
        analysis_config_dict = NWM30_ANALYSIS_CONFIG
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

    cm = GridConfigurationModel.model_validate(vars)
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
            prioritize_analysis_valid_time
        )

        # Create paths to local and/or remote kerchunk jsons
        json_paths = generate_json_paths(
            kerchunk_method,
            gcs_component_paths,
            json_dir,
            ignore_missing_file
        )

        # Fetch the data, saving to parquet files based on TEEHR data model
        fetch_and_format_nwm_grids(
            json_paths=json_paths,
            configuration_name=f"{nwm_version}_{configuration}",
            variable_name=variable_name,
            output_parquet_dir=output_parquet_dir,
            zonal_weights_filepath=zonal_weights_filepath,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            location_id_prefix=location_id_prefix,
            variable_mapper=variable_mapper
        )


# if __name__ == "__main__":
#     # Local testing
#     weights_parquet = "/mnt/data/ciroh/onehuc10_weights.parquet"

#     import time
#     t1 = time.time()

#     nwm_grids_to_parquet(
#         configuration="forcing_analysis_assim",
#         output_type="forcing",
#         variable_name="RAINRATE",
#         start_date="2023-11-28",
#         ingest_days=1,
#         zonal_weights_filepath=weights_parquet,
#         json_dir="/mnt/data/ciroh/jsons",
#         output_parquet_dir="/mnt/data/ciroh/parquet",
#         nwm_version="nwm30",
#         data_source="GCS",
#         kerchunk_method="auto",
#         t_minus_hours=[0],
#         ignore_missing_file=False,
#         overwrite_output=True,
#         location_id_prefix="wbd10"
#     )

#     print(f"elapsed: {time.time() - t1:.2f} s")
