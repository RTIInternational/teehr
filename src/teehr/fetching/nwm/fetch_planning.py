"""Steps shared by NWM operational point and grid fetch planning."""
import importlib
from datetime import datetime
from typing import List, Literal, Optional, Tuple, Union

import pandas as pd
from dateutil.parser import parse

from teehr.fetching.const import (
    NWM12_ANALYSIS_CONFIG,
    NWM20_ANALYSIS_CONFIG,
    NWM22_ANALYSIS_CONFIG,
    NWM30_ANALYSIS_CONFIG,
)
from teehr.fetching.models.utils import (
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
)
from teehr.fetching.utils import (
    build_remote_nwm_filelist,
    validate_operational_start_end_date,
    validate_nwm_version_against_files,
    start_on_z_hour,
    end_on_z_hour,
    get_end_date_from_ingest_days
)

# NWM version -> (version whose configuration models apply, analysis config).
# v2.1 and v2.2 share a configuration; v3.1 reuses v3.0's analysis config.
_Versions = SupportedNWMOperationalVersionsEnum
_NWM_VERSION_CONFIGS = {
    _Versions.nwm12: ("nwm12", NWM12_ANALYSIS_CONFIG),
    _Versions.nwm20: ("nwm20", NWM20_ANALYSIS_CONFIG),
    _Versions.nwm21: ("nwm22", NWM22_ANALYSIS_CONFIG),
    _Versions.nwm22: ("nwm22", NWM22_ANALYSIS_CONFIG),
    _Versions.nwm30: ("nwm30", NWM30_ANALYSIS_CONFIG),
    _Versions.nwm31: ("nwm31", NWM30_ANALYSIS_CONFIG),
}


def plan_nwm_component_paths(
    kind: Literal["point", "grid"],
    configuration: str,
    output_type: str,
    variable_name: str,
    nwm_version: SupportedNWMOperationalVersionsEnum,
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Optional[Union[str, datetime, pd.Timestamp]],
    ingest_days: Optional[int],
    data_source: Optional[SupportedNWMDataSourcesEnum],
    prioritize_analysis_value_time: Optional[bool],
    t_minus_hours: Optional[List[int]],
    ignore_missing_file: Optional[bool],
    starting_z_hour: Optional[int],
    ending_z_hour: Optional[int],
    drop_overlapping_assimilation_values: Optional[bool],
) -> Tuple[List[str], str, str, str]:
    """List and validate the NWM files a point or grid fetch reads.

    The steps :func:`plan_nwm_point_fetch` and :func:`plan_nwm_grid_fetch`
    share; ``kind`` picks the point or grid configuration models. Arguments
    are those planners' own, already validated by them.

    Returns
    -------
    Tuple[List[str], str, str, str]
        The remote component paths, and the validated configuration, output
        type and variable name.
    """
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
    if nwm_version not in _NWM_VERSION_CONFIGS:
        raise ValueError(
            "nwm_version must equal "
            "'nwm12', 'nwm20', 'nwm21', 'nwm22', 'nwm30', or 'nwm31'"
        )
    model_version, analysis_config_dict = _NWM_VERSION_CONFIGS[nwm_version]
    models = importlib.import_module(
        f"teehr.fetching.models.{model_version}_{kind}"
    )
    ConfigurationModel = getattr(
        models, f"{kind.capitalize()}ConfigurationModel"
    )

    # Parse input parameters to validate configuration
    vars = {
        "configuration": configuration,
        configuration: {
            "output_type": output_type,
            output_type: variable_name,
        },
    }
    cm = ConfigurationModel.model_validate(vars)
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

    if len(gcs_component_paths) == 0:
        raise ValueError(
            "No NWM files found for the specified input arguments."
        )

    # Validate the requested NWM version against file metadata
    validate_nwm_version_against_files(
        gcs_component_paths,
        nwm_version
    )

    return gcs_component_paths, configuration, output_type, variable_name
