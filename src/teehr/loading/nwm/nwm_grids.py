from typing import Union, Iterable, Optional
from datetime import datetime
from pathlib import Path

from teehr.loading.nwm.grid_utils import fetch_and_format_nwm_grids
from teehr.loading.nwm.utils import (
    build_remote_nwm_filelist,
    generate_json_paths,
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


def nwm_grids_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    zonal_weights_filepath: str,
    json_dir: Union[str, Path],
    output_parquet_dir: Union[str, Path],
    nwm_version: SupportedNWMOperationalVersionsEnum,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    kerchunk_method: Optional[SupportedKerchunkMethod] = "create",
    t_minus_hours: Optional[Iterable[int]] = None,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = False,
):
    """
    Fetches NWM gridded data, calculates zonal statistics (mean) of selected
    variable for given zones, converts and saves to TEEHR tabular format

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
    zonal_weights_filepath: str
        Path to the array containing fraction of pixel overlap
        for each zone
    json_dir : str
        Directory path for saving json reference files
    output_parquet_dir : str
        Path to the directory for the final parquet files
    nwm_version: SupportedNWMOperationalVersionsEnum
        The NWM operational version
        "nwm22", or "nwm30"
    data_source: Optional[SupportedNWMDataSourcesEnum]
        Specifies the remote location from which to fetch the data
        "GCS" (default), "NOMADS", or "DSTOR"
        Currently only "GCS" is implemented
    kerchunk_method: Optional[SupportedKerchunkMethod]
        When data_source = "GCS", specifies the preference in creating Kerchunk
        reference json files.
        "create" - (default) always create new json files from netcdf files in GCS and
                   save locally
        "use_available" - read the CIROH pre-generated jsons from s3, ignoring
                          any that are unavailable
        "auto" - read the CIROH pre-generated jsons from s3, and create
                          any that are unavailable, storing locally
    t_minus_hours: Optional[Iterable[int]]
        Specifies the look-back hours to include if an assimilation
        configuration is specified.
    ignore_missing_file: bool
        Flag specifying whether or not to fail if a missing NWM file is encountered
        True = skip and continue; False = fail
    overwrite_output: bool
        Flag specifying whether or not to overwrite output files if they already
        exist.  True = overwrite; False = fail

    The NWM configuration variables, including configuration, output_type, and
    variable_name are stored as a pydantic model in grid_config_models.py

    Forecast and assimilation data is grouped and saved one file per reference
    time, using the file name convention "YYYYMMDDTHHZ".  The tabular output
    parquet files follow the timeseries data model described here:
    https://github.com/RTIInternational/teehr/blob/main/docs/data_models.md#timeseries  # noqa
    """
    # Import appropriate config model and dicts based on NWM version
    if nwm_version == SupportedNWMOperationalVersionsEnum.nwm22:
        from teehr.models.loading.nwm22_grid import GridConfigurationModel
        analysis_config_dict = NWM22_ANALYSIS_CONFIG
        unit_lookup_dict = NWM22_UNIT_LOOKUP
    elif nwm_version == SupportedNWMOperationalVersionsEnum.nwm30:
        from teehr.models.loading.nwm30_grid import GridConfigurationModel
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

    cm = GridConfigurationModel.parse_obj(vars)
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
        fetch_and_format_nwm_grids(
            json_paths,
            configuration,
            variable_name,
            output_parquet_dir,
            zonal_weights_filepath,
            ignore_missing_file,
            unit_lookup_dict,
            overwrite_output,
        )


if __name__ == "__main__":
    # Local testing
    weights_parquet = "/mnt/data/ciroh/wbdhuc10_weights.parquet"

    import time
    t1 = time.time()

    nwm_grids_to_parquet(
        configuration="forcing_analysis_assim",
        output_type="forcing",
        variable_name="RAINRATE",
        start_date="2023-11-28",
        ingest_days=1,
        zonal_weights_filepath=weights_parquet,
        json_dir="/mnt/data/ciroh/jsons",
        output_parquet_dir="/mnt/data/ciroh/parquet",
        nwm_version="nwm30",
        data_source="GCS",
        kerchunk_method="use_available",
        t_minus_hours=[0],
        ignore_missing_file=False,
        overwrite_output=True
    )

    print(f"elapsed: {time.time() - t1:.2f} s")
