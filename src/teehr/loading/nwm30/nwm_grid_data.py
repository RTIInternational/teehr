from typing import Union, Iterable, Optional
from datetime import datetime

from teehr.loading.nwm_common.grid_utils import (
    fetch_and_format_nwm_grids
)
from teehr.loading.nwm_common.utils_nwm import (
    build_zarr_references,
    build_remote_nwm_filelist,
)
from teehr.loading.nwm22.const_nwm import NWM22_UNIT_LOOKUP

from teehr.models.loading.nwm30_grid import GridConfigurationModel
from teehr.loading.nwm30.const_nwm import NWM30_ANALYSIS_CONFIG


def nwm_grids_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    start_date: Union[str, datetime],
    ingest_days: int,
    zonal_weights_filepath: str,
    json_dir: str,
    output_parquet_dir: str,
    t_minus_hours: Optional[Iterable[int]] = None,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = True,
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
    t_minus_hours: Optional[Iterable[int]]
        Specifies the look-back hours to include if an assimilation
        configuration is specified.
    ignore_missing_file: bool
        Flag specifying whether or not to fail if a missing NWM file is encountered
        True = skip and continue
        False = fail
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

    component_paths = build_remote_nwm_filelist(
        configuration,
        output_type,
        start_date,
        ingest_days,
        NWM30_ANALYSIS_CONFIG,
        t_minus_hours,
        ignore_missing_file,
    )

    json_paths = build_zarr_references(component_paths,
                                       json_dir,
                                       ignore_missing_file)

    fetch_and_format_nwm_grids(
        json_paths,
        configuration,
        variable_name,
        output_parquet_dir,
        zonal_weights_filepath,
        ignore_missing_file,
        NWM22_UNIT_LOOKUP,
        overwrite_output,
    )


if __name__ == "__main__":
    # Local testing
    single_filepath = "/mnt/data/ciroh/nwm.20201218_forcing_short_range_nwm.t00z.short_range.forcing.f001.conus.nc"  # noqa
    weights_parquet = "/mnt/data/ciroh/wbdhuc10_weights.parquet"
    ignore_missing_file = False

    nwm_grids_to_parquet(
        "forcing_analysis_assim",
        "forcing",
        "RAINRATE",
        "2020-12-18",
        1,
        weights_parquet,
        "/home/sam/forcing_jsons",
        "/home/sam/forcing_parquet",
        [0],
        ignore_missing_file,
    )
