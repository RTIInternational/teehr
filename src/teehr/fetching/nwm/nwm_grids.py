"""Module for fetching and processing NWM gridded data."""
from typing import Union, List, Optional, Dict, Annotated
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import logging

from pydantic import validate_call, Field, InstanceOf
from geopandas import GeoDataFrame
import pandas as pd

from teehr.fetching.nwm.fetch_planning import plan_nwm_component_paths
from teehr.fetching.nwm.grid_utils import fetch_and_format_nwm_grids
from teehr.fetching.utils import (
    generate_json_paths,
    open_kerchunk_dataset,
    log_temperature_conversion_message
)
from teehr.fetching.models.utils import (
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.utilities.generate_weights import generate_weights_file

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NwmGridFetchPlan:
    """What a grid fetch resolved to: which files to read, under which names.

    Attributes
    ----------
    component_paths : List[str]
        Remote NWM files to read, as produced by
        :func:`build_remote_nwm_filelist` (``gcs://`` paths).
    configuration : str
        Validated configuration name.
    output_type : str
        Validated output type.
    variable_name : str
        Validated variable name.
    """

    component_paths: List[str]
    configuration: str
    output_type: str
    variable_name: str


@validate_call(config=dict(arbitrary_types_allowed=True))
def plan_nwm_grid_fetch(
    configuration: str,
    output_type: str,
    variable_name: str,
    nwm_version: SupportedNWMOperationalVersionsEnum,
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    ingest_days: Optional[int] = None,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    prioritize_analysis_value_time: Optional[bool] = False,
    t_minus_hours: Optional[List[int]] = None,
    ignore_missing_file: Optional[bool] = True,
    starting_z_hour: Optional[Annotated[int, Field(ge=0, le=23)]] = None,
    ending_z_hour: Optional[Annotated[int, Field(ge=0, le=23)]] = None,
    drop_overlapping_assimilation_values: Optional[bool] = True,
) -> NwmGridFetchPlan:
    """Work out which NWM files a grid fetch needs, without reading them.

    Everything :func:`nwm_grids_to_parquet` does before it builds kerchunk
    references: validating the configuration and dates, listing the NWM files
    in GCS, trimming to the requested z-hours, and checking the files' NWM
    version.

    Split out so a caller that reads the files itself -- a Prefect flow
    ingesting grids into Icechunk, say -- gets the file list from the same code
    path ``nwm_grids_to_parquet`` uses instead of reimplementing it.

    Parameters
    ----------
    configuration : str
        NWM forecast category, e.g. "forcing_short_range".
    output_type : str
        Output component of the configuration, e.g. "forcing".
    variable_name : str
        NWM data variable to fetch, e.g. "RAINRATE".
    nwm_version : SupportedNWMOperationalVersionsEnum
        NWM version of the requested data.
    start_date : str, datetime or pd.Timestamp
        Start of the period to fetch.
    end_date : Optional[str, datetime or pd.Timestamp]
        End of the period. Required unless ``ingest_days`` is given.
    ingest_days : Optional[int]
        Days to fetch from ``start_date``, instead of ``end_date``.
    data_source : Optional[SupportedNWMDataSourcesEnum]
        Where to fetch from; only GCS is implemented.
    prioritize_analysis_value_time : Optional[bool]
        For assimilation data, prefer value time over reference time.
    t_minus_hours : Optional[List[int]]
        Assimilation t-minus hours to include.
    ignore_missing_file : Optional[bool]
        Skip missing files rather than failing.
    starting_z_hour : Optional[int]
        First z-hour to include; defaults to the hour of ``start_date``.
    ending_z_hour : Optional[int]
        Last z-hour to include; defaults to the hour of ``end_date``.
    drop_overlapping_assimilation_values : Optional[bool]
        Drop assimilation values that overlap in value_time.

    Returns
    -------
    NwmGridFetchPlan
        The files to read and the validated names to read them with.

    Examples
    --------
    >>> plan = plan_nwm_grid_fetch(
    ...     configuration="forcing_analysis_assim", output_type="forcing",
    ...     variable_name="RAINRATE", nwm_version="nwm31",
    ...     start_date="2026-09-22", end_date="2026-09-23",
    ... )
    >>> plan.component_paths[0]
    'gcs://national-water-model/nwm.20260922/forcing_analysis_assim/...'
    """
    logger.info(
        f"Planning {configuration} fetch. Version: {nwm_version}"
    )

    component_paths, configuration, output_type, variable_name = (
        plan_nwm_component_paths(
            kind="grid",
            configuration=configuration,
            output_type=output_type,
            variable_name=variable_name,
            nwm_version=nwm_version,
            start_date=start_date,
            end_date=end_date,
            ingest_days=ingest_days,
            data_source=data_source,
            prioritize_analysis_value_time=prioritize_analysis_value_time,
            t_minus_hours=t_minus_hours,
            ignore_missing_file=ignore_missing_file,
            starting_z_hour=starting_z_hour,
            ending_z_hour=ending_z_hour,
            drop_overlapping_assimilation_values=drop_overlapping_assimilation_values,  # noqa
        )
    )

    return NwmGridFetchPlan(
        component_paths=component_paths,
        configuration=configuration,
        output_type=output_type,
        variable_name=variable_name,
    )


@validate_call(config=dict(arbitrary_types_allowed=True))
def nwm_grids_to_parquet(
    configuration: str,
    output_type: str,
    variable_name: str,
    zonal_weights_filepath: Union[Path, str],
    json_dir: Union[str, Path],
    output_parquet_dir: Union[str, Path],
    nwm_version: SupportedNWMOperationalVersionsEnum,
    start_date: Union[str, datetime, pd.Timestamp],
    end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
    ingest_days: Optional[int] = None,
    data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
    kerchunk_method: Optional[SupportedKerchunkMethod] = "auto",
    prioritize_analysis_value_time: Optional[bool] = False,
    t_minus_hours: Optional[List[int]] = None,
    ignore_missing_file: Optional[bool] = True,
    overwrite_output: Optional[bool] = False,
    location_id_prefix: Optional[Union[str, None]] = None,
    variable_mapper: Dict[str, Dict[str, Dict[str, str]]] = None,
    timeseries_type: TimeseriesTypeEnum = "primary",
    starting_z_hour: Optional[Annotated[int, Field(ge=0, le=23)]] = None,
    ending_z_hour: Optional[Annotated[int, Field(ge=0, le=23)]] = None,
    calculate_zonal_weights: bool = False,
    zone_polygons: Optional[Union[Path, str, InstanceOf[GeoDataFrame]]] = None,
    unique_zone_id: Optional[str] = None,
    drop_overlapping_assimilation_values: Optional[bool] = True,
    convert_k_to_c: bool = True,
    io_concurrency: Optional[int] = None,
    cpu_workers: Optional[int] = None
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
    zonal_weights_filepath : str
        Path to the array containing fraction of pixel overlap
        for each zone.
    json_dir : str
        Directory path for saving json reference files.
    output_parquet_dir : str
        Path to the directory for the final parquet files.
    nwm_version : SupportedNWMOperationalVersionsEnum
        The NWM operational version.
        "nwm12", "nwm20", "nwm21", "nwm22", "nwm30", or "nwm31".
        Note that there is no change in NWM configuration between
        version 2.1 and 2.2, and they are treated as the same version.
        They are both allowed here for convenience.

        Availability of each version. A switchover lands on a forecast cycle
        rather than at midnight, and the requested date range is validated at
        that resolution:

        - v1.2: 2018-09-17 t00z - 2019-06-19 t13z
        - v2.0: 2019-06-19 t14z - 2021-04-20 t13z
        - v2.1/2.2: 2021-04-20 t14z - 2023-09-19 t11z
        - v3.0: 2023-09-19 t12z - 2026-08-17 t23z
        - v3.1: 2026-08-18 t00z - present

        These are NOAA's intended boundaries, not a promise about every file:
        configurations do not all switch on the same cycle, so a file still
        reporting the outgoing version within a day of a boundary is accepted
        with a warning.
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
        reference json files. "local" - create new json files from
        netcdf files in GCS and save to a local directory if they do not already
        exist locally, in which case the creation is skipped. "remote" - read the
        CIROH pre-generated jsons from s3, ignoring any that are unavailable.
        "auto" (default) - read the CIROH pre-generated jsons from s3, and create any that
        are unavailable, storing locally.
    prioritize_analysis_value_time : Optional[bool]
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
    starting_z_hour : Optional[int]
        The starting z_hour to include in the output. If None, z_hours
        for the first day are determined by ``start_date``. Default is None.
        Must be between 0 and 23.
    ending_z_hour : Optional[int]
        The ending z_hour to include in the output. If None, z_hours
        for the last day are determined by ``end_date`` if provided, otherwise
        all z_hours are included in the final day. Default is None.
        Must be between 0 and 23.
    variable_mapper : Optional[Dict[str, Dict[str, Dict[str, str]]]]
        A dictionary of dictionaries to map NWM variable names and/or unit
        names to new names. Supports two top-level keys: ``"variable_name"``
        and ``"unit_name"``. If None, no mapping is applied and the original
        NWM variable and unit names are used in the output.
        For example, to map the variable name "streamflow" to "discharge" and
        the unit "m3 s-1" to "m^3/s" use:
        variable_mapper = {
            "variable_name": {"streamflow": {"name": "discharge",
                "long_name": "Discharge"}},
            "unit_name": {"m3 s-1": {"name": "m^3/s",
                "long_name": "Cubic Meters per Second"}}
        }
    timeseries_type : Optional[TimeseriesTypeEnum]
        The type of timeseries to generate.
        "primary" (default) or "secondary".
    calculate_zonal_weights : bool
        Flag to calculate zonal weights.
    zone_polygons : Union[Path, str, InstanceOf[GeoDataFrame]]
        Path to the polygons file or a GeoDataFrame.
    unique_zone_id : Optional[str]
        Name of the field in the zone polygon file containing unique IDs.
    drop_overlapping_assimilation_values: Optional[bool] = True
        Whether to drop assimilation values that overlap in value_time.
        Default is True. If True, values that overlap in value_time are dropped,
        keeping those with the most recent reference_time. In this case, all
        reference_time values are set to None. If False, overlapping values are
        kept and reference_time is retained.
    convert_k_to_c : bool, optional (default: True)
        If True, convert temperature values from Kelvin to Celsius by
        subtracting 273.15. The unit_name field will be set to "C".
        Note: this argument is only valid when variable_name is "T2D".
    io_concurrency : Optional[int]
        Remote reads in flight at once. Defaults to 48; lower it when
        something else is fetching in parallel.
    cpu_workers : Optional[int]
        Files processed at once. Defaults to the cpus available.

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
    >>> END_DATE = "2020-12-18"
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
    >>>     nwm_configuration=CONFIGURATION,
    >>>     output_type=OUTPUT_TYPE,
    >>>     variable_name=VARIABLE_NAME,
    >>>     start_date=START_DATE,
    >>>     end_date=END_DATE,
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
    log_temperature_conversion_message(
        variable_name=variable_name,
        convert_k_to_c=convert_k_to_c
    )

    plan = plan_nwm_grid_fetch(
        configuration=configuration,
        output_type=output_type,
        variable_name=variable_name,
        nwm_version=nwm_version,
        start_date=start_date,
        end_date=end_date,
        ingest_days=ingest_days,
        data_source=data_source,
        prioritize_analysis_value_time=prioritize_analysis_value_time,
        t_minus_hours=t_minus_hours,
        ignore_missing_file=ignore_missing_file,
        starting_z_hour=starting_z_hour,
        ending_z_hour=ending_z_hour,
        drop_overlapping_assimilation_values=drop_overlapping_assimilation_values,  # noqa
    )

    # Create paths to local and/or remote kerchunk jsons
    json_paths = generate_json_paths(
        kerchunk_method,
        plan.component_paths,
        json_dir,
        ignore_missing_file,
        io_concurrency,
        cpu_workers
    )

    # If specified, generate zonal weights file here.
    if calculate_zonal_weights:
        if zone_polygons is None:
            raise ValueError(
                "The zone polygons must be provided"
                " to calculate zonal weights. Can be a GeoDataFame"
                " or a filepath."
            )

        # Get a single timestep to use as a template grid.
        template_ds = open_kerchunk_dataset(
            json_paths[0],
            loadable_variables=[plan.variable_name, "x", "y", "crs"],
            ignore_missing_file=False,
        )

        generate_weights_file(
            zone_polygons=zone_polygons,
            template_dataset=template_ds,
            variable_name=plan.variable_name,
            crs_wkt=template_ds.crs.esri_pe_string,
            output_weights_filepath=zonal_weights_filepath,
            location_id_prefix=location_id_prefix,
            unique_zone_id=unique_zone_id
        )

    # Fetch the data, saving to parquet files based on TEEHR data model
    fetch_and_format_nwm_grids(
        json_paths=json_paths,
        nwm_configuration_name=plan.configuration,
        nwm_version=nwm_version,
        variable_name=plan.variable_name,
        output_parquet_dir=output_parquet_dir,
        zonal_weights_filepath=zonal_weights_filepath,
        ignore_missing_file=ignore_missing_file,
        overwrite_output=overwrite_output,
        location_id_prefix=location_id_prefix,
        variable_mapper=variable_mapper,
        timeseries_type=timeseries_type,
        drop_overlapping_assimilation_values=drop_overlapping_assimilation_values,  # noqa
        convert_k_to_c=convert_k_to_c,
        io_concurrency=io_concurrency,
        cpu_workers=cpu_workers
    )
