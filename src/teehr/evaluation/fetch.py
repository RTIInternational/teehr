"""Component class for fetching data from external sources."""
from typing import Union, List, Optional
from datetime import datetime
import logging
from pathlib import Path

import pandas as pd

from teehr.utils.utils import remove_dir_if_exists
import teehr.const as const
from teehr.fetching.usgs.usgs import usgs_to_parquet
from teehr.fetching.nwm.nwm_points import nwm_to_parquet
from teehr.fetching.nwm.nwm_grids import nwm_grids_to_parquet
from teehr.fetching.nwm.retrospective_points import nwm_retro_to_parquet
from teehr.fetching.nwm.retrospective_grids import nwm_retro_grids_to_parquet
from teehr.loading.timeseries import (
    validate_and_insert_timeseries,
)
from teehr.fetching.utils import (
    format_nwm_variable_name,
    format_nwm_configuration_metadata
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
from teehr.models.table_enums import TableWriteEnum
from teehr.fetching.const import (
    USGS_CONFIGURATION_NAME,
    USGS_VARIABLE_MAPPER,
    VARIABLE_NAME,
    NWM_VARIABLE_MAPPER
)
from teehr.models.pydantic_table_models import (
    Configuration
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

        if lcw_df.empty:
            logger.error(
                "No secondary location IDs were found in the crosswalk table"
                f" with the specified prefix: '{prefix}'"
            )
            raise ValueError(
                "No secondary location IDs were found in the crosswalk table"
                f" with the specified prefix: '{prefix}'"
            )

        location_ids = (
            lcw_df.secondary_location_id.
            str.removeprefix(f"{prefix}-").to_list()
        )

        return location_ids

    def _configuration_name_exists(
        self, configuration_name: str
    ) -> bool:
        """Check if a configuration name exists in the configurations table.

        True: Configuration name exists in the table.
        False: Configuration name does not exist in the table.
        """
        sdf = self.ev.configurations.filter(
            {
                "column": "name",
                "operator": "=",
                "value": configuration_name
            }
        ).to_sdf()
        return not sdf.rdd.isEmpty()

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
        timeseries_type: TimeseriesTypeEnum = "primary",
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
    ):
        """Fetch USGS gage data and load into the TEEHR dataset.

        Data is fetched for all USGS IDs in the locations table, and all
        dates and times within the files and in the cached file names are
        in UTC.

        Parameters
        ----------
        start_date : Union[str, datetime, pd.Timestamp]
            Start date and time of data to fetch.
        end_date : Union[str, datetime, pd.Timestamp]
            End date and time of data to fetch. Note, since start_date is inclusive for
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
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table. Options are "append" or "upsert".
            If "append", the Evaluation table will be appended with new data
            that does not already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
        drop_duplicates : bool
            Whether to drop duplicates in the data. Default is True.

        .. note::

           In some edge cases, a gage site may contain one or more
           sub-locations that also measure discharge. To differentiate
           these sub-locations, the ``usgs_to_parquet`` method should be
           called directly, and a dictionary can be passed in for a site.
           Each dictionary should contain the site number and a description
           of the sub-location. The description is used to filter the
           data to the specific sub-location. For example:
           [{"site_no": "02449838", "description": "Main Gage"}]
           Note that the dictionary must contain the keywords
           'site_no' and 'description'.


        .. note::

            Data in the cache is cleared before each call to the fetch method. So if a
            long-running fetch is interrupted before the data is automatically loaded
            into the Evaluation, it should be loaded or cached manually. This will
            prevent it from being deleted when the fetch job is resumed.

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
        """  # noqa
        logger.info("Getting primary location IDs.")
        locations_df = self.ev.locations.query(
            filters={
                "column": "id",
                "operator": "like",
                "value": f"usgs-%"
            }
        ).to_pandas()
        sites = locations_df["id"].str.removeprefix(f"usgs-").to_list()

        usgs_variable_name = USGS_VARIABLE_MAPPER[VARIABLE_NAME][service]

        # Clear out cache
        remove_dir_if_exists(self.usgs_cache_dir)

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
            overwrite_output=overwrite_output,
            timeseries_type=timeseries_type
        )

        if (
            not self._configuration_name_exists(USGS_CONFIGURATION_NAME)
        ):
            self.ev.configurations.add(
                Configuration(
                    name=USGS_CONFIGURATION_NAME,
                    type="primary",
                    description="USGS streamflow gauge observations"
                )
            )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.usgs_cache_dir
            ),
            timeseries_type=timeseries_type,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
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
        timeseries_type: TimeseriesTypeEnum = "secondary",
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
    ):
        """Fetch NWM retrospective point data and load into the TEEHR dataset.

        Data is fetched for all secondary location IDs in the locations
        crosswalk table that are prefixed by the NWM version, and all dates
        and times within the files and in the cache file names are in UTC.

        Parameters
        ----------
        nwm_version : SupportedNWMRetroVersionsEnum
            NWM retrospective version to fetch.
            Currently `nwm20`, `nwm21`, and `nwm30` supported.
            Note that since there is no change in NWM configuration between
            version 2.1 and 2.2, no retrospective run was produced for v2.2.
        variable_name : str
            Name of the NWM data variable to download.
            (e.g., "streamflow", "velocity", ...).
        start_date : Union[str, datetime, pd.Timestamp]
            Date and time to begin data ingest.
            Str formats can include YYYY-MM-DD HH:MM or MM/DD/YYYY HH:MM

            - v2.0: 1993-01-01
            - v2.1: 1979-01-01
            - v3.0: 1979-02-01
        end_date : Union[str, datetime, pd.Timestamp],
            Date and time to end data ingest.
            Str formats can include YYYY-MM-DD HH:MM or MM/DD/YYYY HH:MM

            - v2.0: 2018-12-31
            - v2.1: 2020-12-31
            - v3.0: 2023-01-31
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
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table. Options are "append" or "upsert".
            If "append", the Evaluation table will be appended with new data
            that does not already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
        drop_duplicates : bool
            Whether to drop duplicates in the data. Default is True.


        .. note::

            Data in the cache is cleared before each call to the fetch method. So if a
            long-running fetch is interrupted before the data is automatically loaded
            into the Evaluation, it should be loaded or cached manually. This will
            prevent it from being deleted when the fetch job is resumed.

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
        >>>     start_date=datetime(2000, 1, 1),
        >>>     end_date=datetime(2000, 1, 2, 23),
        >>>     location_ids=[7086109, 7040481],
        >>>     output_parquet_dir=Path(Path.home(), "nwm20_retrospective")
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.retrospective_points.nwm_retro_to_parquet`
        """ # noqa
        ev_configuration_name = f"{nwm_version}_retrospective"
        ev_variable_name = format_nwm_variable_name(variable_name)

        logger.info("Getting secondary location IDs.")
        location_ids = self._get_secondary_location_ids(
            prefix=nwm_version
        )

        # Clear out cache
        remove_dir_if_exists(self.nwm_cache_dir)

        nwm_retro_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            start_date=start_date,
            end_date=end_date,
            location_ids=location_ids,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                ev_configuration_name,
                ev_variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            variable_mapper=NWM_VARIABLE_MAPPER,
            timeseries_type=timeseries_type
        )

        if (
            not self._configuration_name_exists(ev_configuration_name)
        ):
            self.ev.configurations.add(
                Configuration(
                    name=ev_configuration_name,
                    type=timeseries_type,
                    description=f"{nwm_version} retrospective"
                )
            )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            timeseries_type=timeseries_type,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
        )

    def nwm_retrospective_grids(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: ForcingVariablesEnum,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        calculate_zonal_weights: bool = False,
        overwrite_output: Optional[bool] = False,
        chunk_by: Optional[NWMChunkByEnum] = None,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
        location_id_prefix: Optional[str] = None,
        timeseries_type: TimeseriesTypeEnum = "primary",
        write_mode: TableWriteEnum = "append",
        zonal_weights_filepath: Optional[Union[Path, str]] = None,
        drop_duplicates: bool = True,
    ):
        """
        Fetch NWM retrospective gridded data, calculate zonal statistics (currently only
        mean is available) of selected variable for given zones, and load
        into the TEEHR dataset.

        Data is fetched for the location IDs in the locations
        table having a given location_id_prefix. All dates and times within the files
        and in the cache file names are in UTC.

        The zonal weights file, which contains the fraction each grid pixel overlaps each
        zone is necessary, and can be calculated and saved to the cache directory
        if it does not already exist.

        Parameters
        ----------
        nwm_version : SupportedNWMRetroVersionsEnum
            NWM retrospective version to fetch.
            Currently `nwm21` and `nwm30` supported.
            Note that since there is no change in NWM configuration between
            version 2.1 and 2.2, no retrospective run was produced for v2.2.
        variable_name : str
            Name of the NWM forcing data variable to download.
            (e.g., "PRECIP", "PSFC", "Q2D", ...).
        start_date : Union[str, datetime, pd.Timestamp]
            Date and time to begin data ingest.
            Str formats can include YYYY-MM-DD HH:MM or MM/DD/YYYY HH:MM

            - v2.0: 1993-01-01
            - v2.1: 1979-01-01
            - v3.0: 1979-02-01
        end_date : Union[str, datetime, pd.Timestamp],
            Date and time to end data ingest.
            Str formats can include YYYY-MM-DD HH:MM or MM/DD/YYYY HH:MM

            - v2.0: 2018-12-31
            - v2.1: 2020-12-31
            - v3.0: 2023-01-31
        calculate_zonal_weights : bool
            Flag specifying whether or not to calculate zonal weights.
            True = calculate; False = use existing file. Default is False.
        location_id_prefix : Optional[str]
            Prefix to include when filtering the locations table for polygon
            primary_location_id. Default is None, all locations are included.
        overwrite_output : bool
            Flag specifying whether or not to overwrite output files if they already
            exist.  True = overwrite; False = fail.
        chunk_by : Optional[NWMChunkByEnum] = None,
            If None (default) saves all timeseries to a single file, otherwise
            the data is processed using the specified parameter.
            Can be: 'week' or 'month' for gridded data.
        domain : str = "CONUS"
            Geographical domain when NWM version is v3.0.
            Acceptable values are "Alaska", "CONUS" (default), "Hawaii",
            and "PR". Only relevant when NWM version equals v3.0.
        timeseries_type : str
            Whether to consider as the "primary" or "secondary" timeseries.
            Default is "primary".
        zonal_weights_filepath : Optional[Union[Path, str]]
            The path to the zonal weights file. If None and calculate_zonal_weights
            is False, the weights file must exist in the cache for the configuration.
            Default is None.
        drop_duplicates : bool
            Whether to drop duplicates in the data. Default is True.

        Examples
        --------
        Here we will calculate mean areal precipitation using NWM forcing data for
        the polygons in the locations table. Pixel weights (fraction of pixel overlap)
        are calculated for each polygon and stored in the evaluation cache directory.

        (see: :func:`generate_weights_file()
        <teehr.utilities.generate_weights.generate_weights_file>` for weights calculation).

        >>> import teehr
        >>> ev = teehr.Evaluation()

        >>> ev.fetch.nwm_retrospective_grids(
        >>>     nwm_version="nwm30",
        >>>     variable_name="RAINRATE",
        >>>     calculate_zonal_weights=True,
        >>>     start_date=datetime(2000, 1, 1),
        >>>     end_date=datetime(2001, 1, 1),
        >>>     location_id_prefix="huc10"
        >>> )

        .. note::

            NWM data can also be fetched outside of a TEEHR Evaluation
            by calling the method directly.

        >>> from teehr.fetching.nwm.retrospective_grids import nwm_retro_grids_to_parquet

        Perform the calculations, writing to the specified directory.

        >>> nwm_retro_grids_to_parquet(
        >>>     nwm_version="nwm30",
        >>>     variable_name="RAINRATE",
        >>>     zonal_weights_filepath=Path(Path.home(), "nextgen_03S_weights.parquet"),
        >>>     start_date=2020-12-18,
        >>>     end_date=2022-12-18,
        >>>     output_parquet_dir=Path(Path.home(), "temp/parquet"),
        >>>     location_id_prefix="huc10",
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.nwm_grids.nwm_grids_to_parquet`
        """ # noqa
        ev_configuration_name = f"{nwm_version}_retrospective"
        ev_variable_name = format_nwm_variable_name(variable_name)

        # Clear out cache
        remove_dir_if_exists(self.nwm_cache_dir)
        ev_weights_cache_dir = Path(
            self.weights_cache_dir, ev_configuration_name
        )
        ev_weights_cache_dir.mkdir(parents=True, exist_ok=True)

        # Note: If the weights file will be generated, use the location ID
        # prefix to filter the locations table for the correct locations,
        # defining the zone_polygons argument.
        logger.info("Getting primary location IDs.")
        if location_id_prefix is None:
            locations_gdf = self.ev.locations.to_geopandas()
        else:
            locations_gdf = self.ev.locations.query(
                filters={
                    "column": "id",
                    "operator": "like",
                    "value": f"{location_id_prefix}-%"
                }
            ).to_geopandas()

        if zonal_weights_filepath is None:
            zonal_weights_filepath = Path(
                ev_weights_cache_dir,
                f"{ev_configuration_name}_pixel_weights.parquet"
            )

        nwm_retro_grids_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            zonal_weights_filepath=zonal_weights_filepath,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                ev_configuration_name,
                ev_variable_name
            ),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            location_id_prefix=location_id_prefix,
            variable_mapper=NWM_VARIABLE_MAPPER,
            timeseries_type=timeseries_type,
            unique_zone_id="id",
            calculate_zonal_weights=calculate_zonal_weights,
            zone_polygons=locations_gdf
        )

        if (
            not self._configuration_name_exists(ev_configuration_name)
        ):
            self.ev.configurations.add(
                Configuration(
                    name=ev_configuration_name,
                    type=timeseries_type,
                    description=f"{nwm_version} retrospective"
                )
            )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            timeseries_type=timeseries_type,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates
        )

    def nwm_operational_points(
        self,
        nwm_configuration: str,
        output_type: str,
        variable_name: str,
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
        timeseries_type: TimeseriesTypeEnum = "secondary",
        starting_z_hour: Optional[int] = None,
        ending_z_hour: Optional[int] = None,
        write_mode: TableWriteEnum = "append",
        drop_duplicates: bool = True,
        drop_overlapping_assimilation_values: Optional[bool] = True
    ):
        """Fetch operational NWM point data and load into the TEEHR dataset.

        Data is fetched for all secondary location IDs in the locations
        crosswalk table that are prefixed by the NWM version, and all dates
        and times within the files and in the cache file names are in UTC.

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
        prioritize_analysis_value_time : Optional[bool]
            A boolean flag that determines the method of fetching analysis-assimilation
            data. When False (default), all non-overlapping value_time hours
            (prioritizing the most recent reference_time) are included in the
            output. When True, only the hours within t_minus_hours are included.
        t_minus_hours : Optional[List[int]]
            Specifies the look-back hours to include if an assimilation
            nwm_configuration is specified.
            Only utilized if assimilation data is requested and
            prioritize_analysis_value_time is True.
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
        starting_z_hour : Optional[int]
            The starting z_hour to include in the output. If None, z_hours
            for the first day are determined by ``start_date``. Default is None.
            Must be between 0 and 23.
        ending_z_hour : Optional[int]
            The ending z_hour to include in the output. If None, z_hours
            for the last day are determined by ``end_date`` if provided, otherwise
            all z_hours are included in the final day. Default is None.
            Must be between 0 and 23.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table. Options are "append" or "upsert".
            If "append", the Evaluation table will be appended with new data
            that does not already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
        drop_duplicates : bool
            Whether to drop duplicates in the data. Default is True.
        drop_overlapping_assimilation_values: Optional[bool] = True
            Whether to drop assimilation values that overlap in value_time.
            Default is True. If True, values that overlap in value_time are dropped,
            keeping those with the most recent reference_time. In this case, all
            reference_time values are set to None. If False, overlapping values are
            kept and reference_time is retained.



        .. note::

            Data in the cache is cleared before each call to the fetch method. So if a
            long-running fetch is interrupted before the data is automatically loaded
            into the Evaluation, it should be loaded or cached manually. This will
            prevent it from being deleted when the fetch job is resumed.

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

        >>> ev.fetch.nwm_operational_points(
        >>>     nwm_configuration="short_range",
        >>>     output_type="channel_rt",
        >>>     variable_name="streamflow",
        >>>     start_date=datetime(2000, 1, 1),
        >>>     end_date=datetime(2000, 1, 2),
        >>>     nwm_version="nwm21",
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
        >>>     end_date="2023-03-19",
        >>>     location_ids=LOCATION_IDS,
        >>>     json_dir=Path(Path.home(), "temp/parquet/jsons/"),
        >>>     output_parquet_dir=Path(Path.home(), "temp/parquet"),
        >>>     nwm_version="nwm21",
        >>>     data_source="GCS",
        >>>     kerchunk_method="auto",
        >>>     process_by_z_hour=True,
        >>>     ignore_missing_file=True,
        >>>     overwrite_output=True,
        >>> )

        See Also
        --------
        :func:`teehr.fetching.nwm.nwm_points.nwm_to_parquet`
        """ # noqa
        logger.info("Getting secondary location IDs.")

        location_ids = self._get_secondary_location_ids(
            prefix=nwm_version
        )

        ev_variable_name = format_nwm_variable_name(variable_name)
        ev_config = format_nwm_configuration_metadata(
            nwm_config_name=nwm_configuration,
            nwm_version=nwm_version
        )

        # Clear out cache
        remove_dir_if_exists(self.nwm_cache_dir)

        nwm_to_parquet(
            configuration=nwm_configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            end_date=end_date,
            ingest_days=ingest_days,
            location_ids=location_ids,
            json_dir=self.kerchunk_cache_dir,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                ev_config["name"],
                ev_variable_name
            ),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            prioritize_analysis_value_time=prioritize_analysis_value_time,
            t_minus_hours=t_minus_hours,
            process_by_z_hour=process_by_z_hour,
            stepsize=stepsize,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            variable_mapper=NWM_VARIABLE_MAPPER,
            timeseries_type=timeseries_type,
            starting_z_hour=starting_z_hour,
            ending_z_hour=ending_z_hour,
            drop_overlapping_assimilation_values=drop_overlapping_assimilation_values  # noqa
        )

        if (
            not self._configuration_name_exists(
                ev_config["name"]
            )
        ):
            self.ev.configurations.add(
                Configuration(
                    name=ev_config["name"],
                    type=timeseries_type,
                    description=ev_config["description"]
                )
            )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(
                self.nwm_cache_dir
            ),
            timeseries_type=timeseries_type,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            drop_overlapping_assimilation_values=drop_overlapping_assimilation_values  # noqa
        )

    def nwm_operational_grids(
        self,
        nwm_configuration: str,
        output_type: str,
        variable_name: str,
        nwm_version: SupportedNWMOperationalVersionsEnum,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Optional[Union[str, datetime, pd.Timestamp]] = None,
        ingest_days: Optional[int] = None,
        calculate_zonal_weights: bool = False,
        location_id_prefix: Optional[str] = None,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        prioritize_analysis_value_time: Optional[bool] = False,
        t_minus_hours: Optional[List[int]] = None,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        timeseries_type: TimeseriesTypeEnum = "secondary",
        starting_z_hour: Optional[int] = None,
        ending_z_hour: Optional[int] = None,
        write_mode: TableWriteEnum = "append",
        zonal_weights_filepath: Optional[Union[Path, str]] = None,
        drop_duplicates: bool = True,
        drop_overlapping_assimilation_values: bool = True,
    ):
        """
        Fetch NWM operational gridded data, calculate zonal statistics (currently only
        mean is available) of selected variable for given zones, and load into
        the TEEHR dataset.

        Data is fetched for the location IDs in the locations
        table having a given location_id_prefix. All dates and times within the files
        and in the cache file names are in UTC.

        The zonal weights file, which contains the fraction each grid pixel overlaps each
        zone is necessary, and can be calculated and saved to the cache directory
        if it does not already exist.

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
        calculate_zonal_weights : bool
            Flag specifying whether or not to calculate zonal weights.
            True = calculate; False = use existing file. Default is False.
        location_id_prefix : Optional[str]
            Prefix to include when filtering the locations table for polygon
            primary_location_id. Default is None, all locations are included.
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
            A boolean flag that determines the method of fetching analysis-assimilation
            data. When False (default), all non-overlapping value_time hours
            (prioritizing the most recent reference_time) are included in the
            output. When True, only the hours within t_minus_hours are included.
        t_minus_hours : Optional[Iterable[int]]
            Specifies the look-back hours to include if an assimilation
            nwm_configuration is specified.
            Only utilized if assimilation data is requested and
            prioritize_analysis_value_time is True.
        ignore_missing_file : bool
            Flag specifying whether or not to fail if a missing NWM file is encountered
            True = skip and continue; False = fail.
        overwrite_output : bool
            Flag specifying whether or not to overwrite output files if they already
            exist.  True = overwrite; False = fail.
        timeseries_type : str
            Whether to consider as the "primary" or "secondary" timeseries.
            Default is "secondary", unless the configuration is a analysis containing
            assimilation, in which case the default is "primary".
        starting_z_hour : Optional[int]
            The starting z_hour to include in the output. If None, z_hours
            for the first day are determined by ``start_date``. Default is None.
            Must be between 0 and 23.
        ending_z_hour : Optional[int]
            The ending z_hour to include in the output. If None, z_hours
            for the last day are determined by ``end_date`` if provided, otherwise
            all z_hours are included in the final day. Default is None.
            Must be between 0 and 23.
        write_mode : TableWriteEnum, optional (default: "append")
            The write mode for the table. Options are "append" or "upsert".
            If "append", the Evaluation table will be appended with new data
            that does not already exist.
            If "upsert", existing data will be replaced and new data that
            does not exist will be appended.
        zonal_weights_filepath : Optional[Union[Path, str]]
            The path to the zonal weights file. If None and calculate_zonal_weights
            is False, the weights file must exist in the cache for the configuration.
            Default is None.
        drop_duplicates : bool
            Whether to drop duplicates in the data. Default is True.
        drop_overlapping_assimilation_values: Optional[bool] = True
            Whether to drop assimilation values that overlap in value_time.
            Default is True. If True, values that overlap in value_time are dropped,
            keeping those with the most recent reference_time. In this case, all
            reference_time values are set to None. If False, overlapping values are
            kept and reference_time is retained.


        .. note::

            Data in the cache is cleared before each call to the fetch method. So if a
            long-running fetch is interrupted before the data is automatically loaded
            into the Evaluation, it should be loaded or cached manually. This will
            prevent it from being deleted when the fetch job is resumed.

        Notes
        -----
        The NWM variables, including nwm_configuration, output_type, and
        variable_name are stored as a pydantic model in grid_config_models.py.

        The cached forecast and assimilation data is grouped and saved one file
        per reference time, using the file name convention "YYYYMMDDTHH".

        All dates and times within the files and in the file names are in UTC.

        Examples
        --------
        Here we will calculate mean areal precipitation using operational NWM forcing data
        for the polygons in the locations table. Pixel weights (fraction of pixel overlap)
        are calculated for each polygon and stored in the evaluation cache directory.

        (see: :func:`generate_weights_file()
        <teehr.utilities.generate_weights.generate_weights_file>` for weights calculation).

        >>> import teehr
        >>> ev = teehr.Evaluation()

        >>> ev.fetch.nwm_operational_grids(
        >>>     nwm_configuration="forcing_short_range",
        >>>     output_type="forcing",
        >>>     variable_name="RAINRATE",
        >>>     start_date=datetime(2000, 1, 1),
        >>>     end_date=datetime(2000, 1, 2),
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
        >>>     start_date="2020-12-18",
        >>>     end_date="2020-12-19",
        >>>     zonal_weights_filepath=Path(Path.home(), "nextgen_03S_weights.parquet"),
        >>>     json_dir=Path(Path.home(), "temp/parquet/jsons/"),
        >>>     output_parquet_dir=Path(Path.home(), "temp/parquet"),
        >>>     nwm_version="nwm21",
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
        ev_variable_name = format_nwm_variable_name(variable_name)
        ev_config = format_nwm_configuration_metadata(
            nwm_config_name=nwm_configuration,
            nwm_version=nwm_version
        )

        if "forcing_analysis_assim" in nwm_configuration:
            timeseries_type = TimeseriesTypeEnum.primary

        # Clear out cache
        remove_dir_if_exists(self.nwm_cache_dir)

        ev_weights_cache_dir = Path(
            self.weights_cache_dir, ev_config["name"]
        )
        ev_weights_cache_dir.mkdir(parents=True, exist_ok=True)

        # Note: If the weights file will be generated, use the location ID
        # prefix to filter the locations table for the correct locations,
        # defining the zone_polygons argument.
        logger.info("Getting primary location IDs.")
        if location_id_prefix is None:
            locations_gdf = self.ev.locations.to_geopandas()
        else:
            locations_gdf = self.ev.locations.query(
                filters={
                    "column": "id",
                    "operator": "like",
                    "value": f"{location_id_prefix}-%"
                }
            ).to_geopandas()

        if zonal_weights_filepath is None:
            zonal_weights_filepath = Path(
                ev_weights_cache_dir,
                f"{ev_config['name']}_pixel_weights.parquet"
            )

        nwm_grids_to_parquet(
            configuration=nwm_configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            end_date=end_date,
            zonal_weights_filepath=zonal_weights_filepath,
            json_dir=self.kerchunk_cache_dir,
            output_parquet_dir=Path(
                self.nwm_cache_dir,
                ev_config["name"],
                ev_variable_name
            ),
            nwm_version=nwm_version,
            ingest_days=ingest_days,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            prioritize_analysis_value_time=prioritize_analysis_value_time,
            t_minus_hours=t_minus_hours,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            location_id_prefix=location_id_prefix,
            variable_mapper=NWM_VARIABLE_MAPPER,
            starting_z_hour=starting_z_hour,
            ending_z_hour=ending_z_hour,
            unique_zone_id="id",
            calculate_zonal_weights=calculate_zonal_weights,
            zone_polygons=locations_gdf,
            timeseries_type=timeseries_type,
            drop_overlapping_assimilation_values=drop_overlapping_assimilation_values  # noqa
        )

        if (
            not self._configuration_name_exists(
                ev_config["name"]
            )
        ):
            self.ev.configurations.add(
                Configuration(
                    name=ev_config["name"],
                    type=timeseries_type,
                    description=ev_config["description"]
                )
            )

        validate_and_insert_timeseries(
            ev=self.ev,
            in_path=Path(self.nwm_cache_dir),
            timeseries_type=timeseries_type,
            write_mode=write_mode,
            drop_duplicates=drop_duplicates,
            drop_overlapping_assimilation_values=drop_overlapping_assimilation_values  # noqa
        )
