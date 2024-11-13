"""Component class for fetching data from external sources."""
from typing import Union, List, Optional
from datetime import datetime
import logging
from pathlib import Path

import pandas as pd

import teehr.const as const
from teehr.fetching.usgs.usgs import usgs_to_parquet
from teehr.fetching.nwm.nwm_points import nwm_to_parquet
from teehr.fetching.nwm.nwm_grids import nwm_grids_to_parquet
from teehr.fetching.nwm.retrospective_points import nwm_retro_to_parquet
from teehr.fetching.nwm.retrospective_grids import nwm_retro_grids_to_parquet
from teehr.loading.timeseries import (
    validate_and_insert_timeseries,
)
from teehr.evaluation.utils import (
    get_schema_variable_name,
)
from teehr.models.fetching.nwm22_grid import ForcingVariablesEnum
from teehr.models.fetching.utils import (
    USGSChunkByEnum,
    USGSServiceEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum,
    NWMChunkByEnum,
    ChannelRtRetroVariableEnum,
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod,
    TimeseriesTypeEnum
)
from teehr.fetching.const import (
    USGS_CONFIGURATION_NAME,
    USGS_VARIABLE_MAPPER,
    VARIABLE_NAME,
    NWM_VARIABLE_MAPPER
)

logger = logging.getLogger(__name__)


class Fetch:
    """Component class for fetching data from external sources."""

    def __init__(self, ev) -> None:
        """Initialize the Fetch class."""
        # Now we have access to the Evaluation object.
        self.ev = ev
        self.usgs_cache_dir = Path(
            ev.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.USGS_CACHE_DIR,
        )
        self.nwm_cache_dir = Path(
            ev.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.NWM_CACHE_DIR
        )
        self.kerchunk_cache_dir = Path(
            ev.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.KERCHUNK_DIR
        )
        self.weights_cache_dir = Path(
            ev.cache_dir,
            const.FETCHING_CACHE_DIR,
            const.WEIGHTS_DIR
        )

    def _get_secondary_location_ids(self, prefix: str) -> List[str]:
        """Get the secondary location IDs corresponding to primary IDs."""
        lcw_df = self.ev.location_crosswalks.query(
            filters={
                "column": "secondary_location_id",
                "operator": "like",
                "value": f"{prefix}-%"
            }
        ).to_pandas()

        location_ids = (
            lcw_df.secondary_location_id.
            str.removeprefix(f"{prefix}-").to_list()
        )

        return location_ids

    def usgs_streamflow(
        self,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        service: USGSServiceEnum = "iv",
        chunk_by: Union[USGSChunkByEnum, None] = None,
        filter_to_hourly: bool = True,
        filter_no_data: bool = True,
        convert_to_si: bool = True,
        overwrite_output: Optional[bool] = False,
        timeseries_type: TimeseriesTypeEnum = "primary"
    ):
        """Fetch USGS gage data and load into the TEEHR dataset.

        Data is fetched for all IDs in the locations table, and all
        dates and times within the files and in the cached file names are
        in UTC.

        Parameters
        ----------
        start_date : Union[str, datetime, pd.Timestamp]
            Start time of data to fetch.
        end_date : Union[str, datetime, pd.Timestamp]
            End time of data to fetch. Note, since start_date is inclusive for
            the USGS service, we subtract 1 minute from this time so we don't
            get overlap between consecutive calls.
        service : USGSServiceEnum, default = "iv"
            The USGS service to use for fetching data ('iv' for hourly
            instantaneous values or 'dv' for daily mean values).
        chunk_by : Union[str, None], default = None
            A string specifying how much data is fetched and read into
            memory at once. The default is to fetch all locations and all
            dates at once.
            Valid options =
            ["location_id", "day", "week", "month", "year", None].
        filter_to_hourly : bool = True
            Return only values that fall on the hour
            (i.e. drop 15 minute data).
        filter_no_data : bool = True
            Filter out -999 values.
        convert_to_si : bool = True
            Multiplies values by 0.3048**3 and sets ``measurement_units`` to
            "m3/s".
        overwrite_output : bool
            Flag specifying whether or not to overwrite output files if they
            already exist.  True = overwrite; False = fail.
        timeseries_type : str
            Whether to consider as the "primary" or "secondary" timeseries.
            Default is "primary".

        Examples
        --------
        Here we fetch over a year of USGS hourly streamflow data.
        Initially the data is saved to the cache directory, then it is
        validated and loaded into the TEEHR dataset.

        >>> import teehr
        >>> ev = teehr.Evaluation()

        Fetch the data for locations in the locations table.

        >>> eval.fetch.usgs_streamflow(
        >>>     start_date=datetime(2021, 1, 1),
        >>>     end_date=datetime(2022, 2, 28)
        >>> )

        .. note::

            USGS data can also be fetched outside of a TEEHR Evaluation
            by calling the method directly.

        >>> from teehr.fetching.usgs.usgs import usgs_to_parquet

        This requires specifying a list of USGS gage IDs and an output
        directory in addition to the above arguments.

        >>> usgs_to_parquet(
        >>>     sites=["02449838", "02450825"],
        >>>     start_date=datetime(2023, 2, 20),
        >>>     end_date=datetime(2023, 2, 25),
        >>>     output_parquet_dir=Path(Path().home(), "temp", "usgs"),
        >>>     chunk_by="day",
        >>>     overwrite_output=True
        >>> )
        """
        logger.info("Getting primary location IDs.")
        locations_df = self.ev.locations.query(
            filters={
                "column": "id",
                "operator": "like",
                "value": "usgs-%"
            }
        ).to_pandas()
        sites = locations_df["id"].str.removeprefix("usgs-").to_list()

        usgs_variable_name = USGS_VARIABLE_MAPPER[VARIABLE_NAME][service]

        # TODO: Get timeseries_type from the configurations table?

        usgs_to_parquet(
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.usgs_cache_dir,
                USGS_CONFIGURATION_NAME,
                usgs_variable_name
            ),
            chunk_by=chunk_by,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si,
            overwrite_output=overwrite_output
        )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.usgs_cache_dir
            ),
            timeseries_type=timeseries_type,
        )

    def nwm_retrospective_points(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: ChannelRtRetroVariableEnum,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        chunk_by: Union[NWMChunkByEnum, None] = None,
        overwrite_output: Optional[bool] = False,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
        timeseries_type: TimeseriesTypeEnum = "secondary"
    ):
        """Fetch NWM retrospective point data and load into the TEEHR dataset.

        Data is fetched for all secondary location IDs in the locations
        crosswalk table, and all dates and times within the files and in the
        cache file names are in UTC.

        Parameters
        ----------
        nwm_version : SupportedNWMRetroVersionsEnum
            NWM retrospective version to fetch.
            Currently `nwm20`, `nwm21`, and `nwm30` supported.
        variable_name : str
            Name of the NWM data variable to download.
            (e.g., "streamflow", "velocity", ...).
        start_date : Union[str, datetime, pd.Timestamp]
            Date to begin data ingest.
            Str formats can include YYYY-MM-DD or MM/DD/YYYY
            Rounds down to beginning of day.
        end_date : Union[str, datetime, pd.Timestamp],
            Last date to fetch.  Rounds up to end of day.
            Str formats can include YYYY-MM-DD or MM/DD/YYYY.
        chunk_by : Union[NWMChunkByEnum, None] = None,
            If None (default) saves all timeseries to a single file, otherwise
            the data is processed using the specified parameter.
            Can be: 'week', 'month', or 'year'.
        overwrite_output : bool = False,
            Whether output should overwrite files if they exist.
            Default is False.
        domain : str = "CONUS"
            Geographical domain when NWM version is v3.0.
            Acceptable values are "Alaska", "CONUS" (default), "Hawaii",
            and "PR". Only relevant when NWM version equals `nwm30`.
        timeseries_type : str
            Whether to consider as the "primary" or "secondary" timeseries.
            Default is "primary".

        Examples
        --------
        Here we fetch one days worth of NWM hourly streamflow data. Initially
        the data is saved to the cache directory, then it is validated and
        loaded into the TEEHR dataset.

        >>> import teehr
        >>> ev = teehr.Evaluation()

        >>> ev.fetch.nwm_retrospective_points(
        >>>     nwm_version="nwm30",
        >>>     variable_name="streamflow",
        >>>     start_date=datetime(2000, 1, 1),
        >>>     end_date=datetime(2000, 1, 2, 23)
        >>> )

        .. note::

            NWM data can also be fetched outside of a TEEHR Evaluation
            by calling the method directly.

        >>> import teehr.fetching.nwm.retrospective_points as nwm_retro

        Fetch and format the data, writing to the specified directory.

        >>> nwm_retro.nwm_retro_to_parquet(
        >>>     nwm_version="nwm20",
        >>>     variable_name="streamflow",
        >>>     start_date=Sdatetime(2000, 1, 1),
        >>>     end_date=datetime(2000, 1, 2, 23),
        >>>     location_ids=[7086109, 7040481],
        >>>     output_parquet_dir=Path(Path.home(), "nwm20_retrospective")
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.retrospective_points.nwm_retro_to_parquet`
        """ # noqa
        nwm_configuration = f"{nwm_version}_retrospective"
        schema_variable_name = get_schema_variable_name(variable_name)

        # TODO: Get timeseries_type from the configurations table?

        logger.info("Getting secondary location IDs.")
        location_ids = self._get_secondary_location_ids(
            prefix=nwm_version
        )

        nwm_retro_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            start_date=start_date,
            end_date=end_date,
            location_ids=location_ids,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                nwm_configuration,
                schema_variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            # dataset_path=self.ev.dataset_dir,
            timeseries_type=timeseries_type,
        )

    def nwm_retrospective_grids(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: ForcingVariablesEnum,
        zonal_weights_filepath: Union[str, Path],
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        chunk_by: Union[NWMChunkByEnum, None] = None,
        overwrite_output: Optional[bool] = False,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
        location_id_prefix: Optional[Union[str, None]] = None,
        timeseries_type: TimeseriesTypeEnum = "primary"
    ):
        """
        Fetch NWM retrospective gridded data, calculate zonal statistics (currently only
        mean is available) of selected variable for given zones, and load
        into the TEEHR dataset.

        Data is fetched for all location IDs in the locations
        table, and all dates and times within the files and in the
        cache file names are in UTC.

        Pixel values are summarized to zones based on a pre-computed
        zonal weights file.

        Parameters
        ----------
        nwm_version : SupportedNWMRetroVersionsEnum
            NWM retrospective version to fetch.
            Currently `nwm21` and `nwm30` supported.
        variable_name : str
            Name of the NWM forcing data variable to download.
            (e.g., "PRECIP", "PSFC", "Q2D", ...).
        zonal_weights_filepath : str,
            Path to the array containing fraction of pixel overlap
            for each zone. The values in the location_id field from
            the zonal weights file are used in the output of this function.
        start_date : Union[str, datetime, pd.Timestamp]
            Date to begin data ingest.
            Str formats can include YYYY-MM-DD or MM/DD/YYYY.
            Rounds down to beginning of day.
        end_date : Union[str, datetime, pd.Timestamp],
            Last date to fetch.  Rounds up to end of day.
            Str formats can include YYYY-MM-DD or MM/DD/YYYY.
        chunk_by : Union[NWMChunkByEnum, None] = None,
            If None (default) saves all timeseries to a single file, otherwise
            the data is processed using the specified parameter.
            Can be: 'week' or 'month' for gridded data.
        overwrite_output : bool = False,
            Whether output should overwrite files if they exist.
            Default is False.
        domain : str = "CONUS"
            Geographical domain when NWM version is v3.0.
            Acceptable values are "Alaska", "CONUS" (default), "Hawaii",
            and "PR". Only relevant when NWM version equals v3.0.
        location_id_prefix : Union[str, None]
            Optional location ID prefix to add (prepend) or replace.

        Notes
        -----
        The location_id values in the zonal weights file are used as
        location ids in the output of this function, unless a prefix is
        specified which will be prepended to the location_id values if none
        exists, or it will replace the existing prefix. It is assumed that
        the location_id follows the pattern '[prefix]-[unique id]'.

        Examples
        --------
        Here we will calculate mean areal precipitation using NWM forcing data for
        some watersheds (polygons) a using pre-calculated weights file
        (see: :func:`generate_weights_file()
        <teehr.utilities.generate_weights.generate_weights_file>` for weights calculation).

        >>> import teehr
        >>> ev = teehr.Evaluation()

        >>> ev.fetch.nwm_retrospective_grids(
        >>>     nwm_configuration="forcing_short_range",
        >>>     variable_name="RAINRATE",
        >>>     zonal_weights_filepath = Path(Path.home(), "nextgen_03S_weights.parquet"),
        >>>     start_date=datetime(2000, 1, 1),
        >>>     end_date=datetime(2001, 1, 1)
        >>> )

        .. note::

            NWM data can also be fetched outside of a TEEHR Evaluation
            by calling the method directly.

        >>> from teehr.fetching.nwm.retrospective_grids import nwm_retro_grids_to_parquet

        Perform the calculations, writing to the specified directory.

        >>> nwm_retro_grids_to_parquet(
        >>>     nwm_version="nwm22",
        >>>     nwm_configuration="forcing_short_range",
        >>>     variable_name="RAINRATE",
        >>>     zonal_weights_filepath=Path(Path.home(), "nextgen_03S_weights.parquet"),
        >>>     start_date=2020-12-18,
        >>>     end_date=2022-12-18,
        >>>     output_parquet_dir=Path(Path.home(), "temp/parquet")
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.nwm_grids.nwm_grids_to_parquet`
        """ # noqa
        nwm_configuration = f"{nwm_version}_retrospective"
        schema_variable_name = get_schema_variable_name(variable_name)

        # TODO: Get timeseries_type from the configurations table?

        nwm_retro_grids_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            zonal_weights_filepath=zonal_weights_filepath,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                nwm_configuration,
                schema_variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            location_id_prefix=location_id_prefix,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            # dataset_path=self.ev.dataset_dir,
            timeseries_type=timeseries_type,
        )

    def nwm_forecast_points(
        self,
        nwm_configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        nwm_version: SupportedNWMOperationalVersionsEnum,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        prioritize_analysis_valid_time: Optional[bool] = False,
        t_minus_hours: Optional[List[int]] = None,
        process_by_z_hour: Optional[bool] = True,
        stepsize: Optional[int] = 100,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        timeseries_type: TimeseriesTypeEnum = "secondary"
    ):
        """Fetch operational NWM point data and load into the TEEHR dataset.

        Data is fetched for all secondary location IDs in the locations
        crosswalk table, and all dates and times within the files and in the
        cache file names are in UTC.

        Parameters
        ----------
        nwm_configuration : str
            NWM forecast category.
            (e.g., "analysis_assim", "short_range", ...).
        output_type : str
            Output component of the nwm_configuration.
            (e.g., "channel_rt", "reservoir", ...).
        variable_name : str
            Name of the NWM data variable to download.
            (e.g., "streamflow", "velocity", ...).
        start_date : str or datetime
            Date to begin data ingest.
            Str formats can include YYYY-MM-DD or MM/DD/YYYY.
        ingest_days : int
            Number of days to ingest data after start date.
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
        prioritize_analysis_valid_time : Optional[bool]
            A boolean flag that determines the method of fetching analysis data.
            When False (default), all hours of the reference time are included in the
            output. When True, only the hours within t_minus_hours are included.
        t_minus_hours : Optional[List[int]]
            Specifies the look-back hours to include if an assimilation
            nwm_configuration is specified.
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
        timeseries_type : str
            Whether to consider as the "primary" or "secondary" timeseries.
            Default is "secondary".

        Notes
        -----
        The NWM variables, including nwm_configuration, output_type, and
        variable_name are stored as pydantic models in point_config_models.py

        The cached forecast and assimilation data is grouped and saved one file
        per reference time, using the file name convention "YYYYMMDDTHH".

        Examples
        --------
        Here we fetch operational streamflow forecasts for NWM v2.2 from GCS, and
        load into the TEEHR dataset.

        >>> import teehr
        >>> ev = teehr.Evaluation()

        >>> ev.fetch.nwm_forecast_points(
        >>>     nwm_configuration="short_range",
        >>>     output_type="channel_rt",
        >>>     variable_name="streamflow",
        >>>     start_date=datetime(2000, 1, 1),
        >>>     ingest_days=1,
        >>>     nwm_version="nwm22",
        >>>     data_source="GCS",
        >>>     kerchunk_method="auto"
        >>> )

        .. note::

            NWM data can also be fetched outside of a TEEHR Evaluation
            by calling the method directly.

        >>> from teehr.fetching.nwm.nwm_points import nwm_to_parquet

        Fetch and format the data, writing to the specified directory.

        >>> nwm_to_parquet(
        >>>     nwm_configuration="short_range",
        >>>     output_type="channel_rt",
        >>>     variable_name="streamflow",
        >>>     start_date="2023-03-18",
        >>>     ingest_days=1,
        >>>     location_ids=LOCATION_IDS,
        >>>     json_dir=Path(Path.home(), "temp/parquet/jsons/"),
        >>>     output_parquet_dir=Path(Path.home(), "temp/parquet"),
        >>>     nwm_version="nwm22",
        >>>     data_source="GCS",
        >>>     kerchunk_method="auto",
        >>>     prioritize_analysis_valid_time=True,
        >>>     t_minus_hours=[0, 1, 2],
        >>>     process_by_z_hour=True,
        >>>     stepsize=STEPSIZE,
        >>>     ignore_missing_file=True,
        >>>     overwrite_output=True,
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.nwm_points.nwm_to_parquet`
        """ # noqa
        logger.info("Getting primary location IDs.")
        location_ids = self._get_secondary_location_ids(
            prefix=nwm_version
        )

        # TODO: Read timeseries_type from the configurations table?

        schema_variable_name = get_schema_variable_name(variable_name)
        schema_configuration_name = f"{nwm_version}_{nwm_configuration}"
        nwm_to_parquet(
            configuration=nwm_configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            ingest_days=ingest_days,
            location_ids=location_ids,
            json_dir=self.kerchunk_cache_dir,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                schema_configuration_name,
                schema_variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            prioritize_analysis_valid_time=prioritize_analysis_valid_time,
            t_minus_hours=t_minus_hours,
            process_by_z_hour=process_by_z_hour,
            stepsize=stepsize,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            # dataset_path=self.ev.dataset_dir,
            timeseries_type=timeseries_type,
        )

    def nwm_forecast_grids(
        self,
        nwm_configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        zonal_weights_filepath: Union[Path, str],
        nwm_version: SupportedNWMOperationalVersionsEnum,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        prioritize_analysis_valid_time: Optional[bool] = False,
        t_minus_hours: Optional[List[int]] = None,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        location_id_prefix: Optional[Union[str, None]] = None,
        timeseries_type: TimeseriesTypeEnum = "primary"
    ):
        """
        Fetch NWM operational gridded data, calculate zonal statistics (currently only
        mean is available) of selected variable for given zones, and load into
        the TEEHR dataset.

        Data is fetched for all location IDs in the locations
        table, and all dates and times within the files and in the
        cache file names are in UTC.

        Parameters
        ----------
        nwm_configuration : str
            NWM forecast category.
            (e.g., "analysis_assim", "short_range", ...).
        output_type : str
            Output component of the nwm_configuration.
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
            nwm_configuration is specified.
        ignore_missing_file : bool
            Flag specifying whether or not to fail if a missing NWM file is encountered
            True = skip and continue; False = fail.
        overwrite_output : bool
            Flag specifying whether or not to overwrite output files if they already
            exist.  True = overwrite; False = fail.
        location_id_prefix : Union[str, None]
            Optional location ID prefix to add (prepend) or replace.
        timeseries_type : str
            Whether to consider as the "primary" or "secondary" timeseries.
            Default is "primary".

        Notes
        -----
        The NWM variables, including nwm_configuration, output_type, and
        variable_name are stored as a pydantic model in grid_config_models.py.

        The cached forecast and assimilation data is grouped and saved one file
        per reference time, using the file name convention "YYYYMMDDTHH".

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

        >>> import teehr
        >>> ev = teehr.Evaluation()

        >>> ev.fetch.nwm_forecast_grids(
        >>>     nwm_configuration="forcing_short_range",
        >>>     output_type="forcing",
        >>>     variable_name="RAINRATE",
        >>>     start_date=datetime(2000, 1, 1),
        >>>     ingest_days=1,
        >>>     Path(Path.home(), "nextgen_03S_weights.parquet"),
        >>>     nwm_version="nwm22",
        >>>     data_source="GCS",
        >>>     kerchunk_method="auto"
        >>> )

        .. note::

            NWM data can also be fetched outside of a TEEHR Evaluation
            by calling the method directly.

        >>> from teehr.fetching.nwm.nwm_grids import nwm_grids_to_parquet

        Perform the calculations, writing to the specified directory.

        >>> nwm_grids_to_parquet(
        >>>     nwm_configuration=forcing_short_range,
        >>>     output_type=forcing,
        >>>     variable_name=RAINRATE,
        >>>     start_date=2020-12-18,
        >>>     ingest_days=1,
        >>>     zonal_weights_filepath=Path(Path.home(), "nextgen_03S_weights.parquet"),
        >>>     json_dir=Path(Path.home(), "temp/parquet/jsons/"),
        >>>     output_parquet_dir=Path(Path.home(), "temp/parquet"),
        >>>     nwm_version="nwm22",
        >>>     data_source="GCS",
        >>>     kerchunk_method="auto",
        >>>     t_minus_hours=[0, 1, 2],
        >>>     ignore_missing_file=True,
        >>>     overwrite_output=True
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.nwm_grids.nwm_grids_to_parquet`
        """ # noqa

        # TODO: Get timeseries_type from the configurations table?

        schema_variable_name = get_schema_variable_name(variable_name)
        schema_configuration_name = f"{nwm_version}_{nwm_configuration}"
        nwm_grids_to_parquet(
            configuration=nwm_configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            ingest_days=ingest_days,
            zonal_weights_filepath=zonal_weights_filepath,
            json_dir=self.kerchunk_cache_dir,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                schema_configuration_name,
                schema_variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            prioritize_analysis_valid_time=prioritize_analysis_valid_time,
            t_minus_hours=t_minus_hours,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            location_id_prefix=location_id_prefix,
            variable_mapper=NWM_VARIABLE_MAPPER
        )

        pass

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            # dataset_path=self.ev.dataset_dir,
            timeseries_type=timeseries_type,
        )
