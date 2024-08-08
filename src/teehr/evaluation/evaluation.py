"""Evaluation module."""
import pandas as pd
import geopandas as gpd
from typing import Union, List, Optional
from datetime import datetime
from enum import Enum
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging

from teehr.pre.project_creation import copy_template_to
from teehr.models.metrics.metrics import MetricsBasemodel
from teehr.evaluation.utils import _get_joined_timeseries_fields
from teehr.loading.usgs.usgs import usgs_to_parquet
from teehr.loading.nwm.nwm_points import nwm_to_parquet
from teehr.loading.nwm.nwm_grids import nwm_grids_to_parquet
from teehr.loading.nwm.retrospective_points import nwm_retro_to_parquet
from teehr.loading.nwm.retrospective_grids import nwm_retro_grids_to_parquet
from teehr.models.loading.nwm22_grid import ForcingVariablesEnum
from teehr.models.loading.utils import (
    USGSChunkByEnum,
    SupportedNWMRetroVersionsEnum,
    SupportedNWMRetroDomainsEnum,
    NWMChunkByEnum,
    SupportedNWMOperationalVersionsEnum,
    SupportedNWMDataSourcesEnum,
    SupportedKerchunkMethod
)

logger = logging.getLogger(__name__)

DATABASE_DIR = "database"
TEMP_DIR = "temp"
LOCATIONS_DIR = "locations"
PRIMARY_TIMESERIES_DIR = "primary_timeseries"
LOCATIONS_CROSSWALK_DIR = "locations_crosswalk"
SECONDARY_TIMESERIES_DIR = "secondary_timeseries"
JOINED_TIMESERIES_DIR = "joined_timeseries"


class Evaluation():
    """The Evaluation class.

    This is the main class for the TEEHR evaluation.
    """

    def __init__(
        self,
        dir_path: Union[str, Path],
        spark: SparkSession = None
    ):
        """Initialize the Evaluation class."""
        self.dir_path = dir_path
        self.spark = spark

        self.database_dir = Path(self.dir_path, DATABASE_DIR)
        self.temp_dir = Path(self.dir_path, TEMP_DIR)
        self.locations_dir = Path(self.database_dir, LOCATIONS_DIR)
        self.primary_timeseries_dir = Path(
            self.database_dir, PRIMARY_TIMESERIES_DIR
        )
        self.locations_crosswalk_dir = Path(
            self.database_dir, LOCATIONS_CROSSWALK_DIR
        )
        self.secondary_timeseries_dir = Path(
            self.database_dir, SECONDARY_TIMESERIES_DIR
        )
        self.joined_timeseries_dir = Path(
            self.database_dir, JOINED_TIMESERIES_DIR
        )
        self.primary_timeseries_cache_dir = Path(
            self.temp_dir, PRIMARY_TIMESERIES_DIR
        )
        self.secondary_timeseries_cache_dir = Path(
            self.temp_dir, SECONDARY_TIMESERIES_DIR
        )

        if not Path(self.dir_path).is_dir():
            logger.error(f"Directory {self.dir_path} does not exist.")
            raise NotADirectoryError

        # Create a local Spark Session if one is not provided.
        if not self.spark:
            logger.info("Creating a new Spark session.")
            conf = SparkConf().setAppName("TEERH").setMaster("local")
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    @property
    def fields(self) -> Enum:
        """The field names from the joined timeseries table."""
        # logger.info("Getting fields from the joined timeseries table.")
        return _get_joined_timeseries_fields(
            self.joined_timeseries_dir
        )

    def clone_template(self):
        """Create a study from the standard template.

        This method mainly copies the template directory to the specified
        evaluation directory.
        """
        teehr_root = Path(__file__).parent.parent
        template_dir = Path(teehr_root, "template")
        logger.info(f"Copying template from {template_dir} to {self.dir_path}")
        copy_template_to(template_dir, self.dir_path)

    def delete_study():
        """Delete a study.

        Includes removing directory and all contents.
        """
        pass

    def clean_temp():
        """Clean temporary files.

        Includes removing temporary files.
        """
        pass

    def clone_study():
        """Get a study from s3.

        Includes retrieving metadata and contents.
        """
        pass

    def import_primary_timeseries(path: Union[Path, str], type: str):
        """Import local primary timeseries data.

        Includes validation and importing data to database.
        """
        if type == "parquet":
            pass

        if type == "csv":
            pass

    def import_secondary_timeseries():
        """Import secondary timeseries data.

        Includes importing data and metadata.
        """
        pass

    def import_geometry():
        """Import geometry data.

        Includes importing data and metadata.
        """
        pass

    def import_location_crosswalk():
        """Import crosswalk data.

        Includes importing data and metadata.
        """
        pass

    def import_usgs(args):
        """Import xxx data.

        Includes importing data and metadata.
        """
        pass

    def import_nwm():
        """Import xxx data.

        Includes importing data and metadata.
        """
        pass

    def get_timeseries() -> pd.DataFrame:
        """Get timeseries data.

        Includes retrieving data and metadata.
        """
        pass

    def get_metrics(
        self,
        group_by: List[Union[str, Enum]],
        order_by: List[Union[str, Enum]],
        include_metrics: Union[List[MetricsBasemodel], str],
        filters: Union[List[dict], None] = None,
        include_geometry: bool = False,
        return_query: bool = False,
    ) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:
        """Get metrics data.

        Includes retrieving data and metadata.
        """
        logger.info("Calculating performance metrics.")
        pass

    def get_timeseries_chars():
        """Get timeseries characteristics.

        Includes retrieving data and metadata.
        """
        pass

    def _get_timeseries_cache_path(
        self, is_primary: Optional[bool]
    ) -> Path:
        """Get the cache sub-directory path."""
        if is_primary:
            return self.primary_timeseries_cache_dir
        else:
            return self.secondary_timeseries_cache_dir

    def fetch_usgs_streamflow(
        self,
        sites: List[str],
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        chunk_by: Union[USGSChunkByEnum, None] = None,
        filter_to_hourly: bool = True,
        filter_no_data: bool = True,
        convert_to_si: bool = True,
        overwrite_output: Optional[bool] = False,
        is_primary: Optional[bool] = True
    ):
        """Fetch USGS gage data and save as a Parquet file.

        All dates and times within the files and in the file names are in UTC.

        Parameters
        ----------
        sites : List[str]
            List of USGS gages sites to fetch.
            Must be string to preserve the leading 0.
        start_date : datetime
            Start time of data to fetch.
        end_date : datetime
            End time of data to fetch. Note, since startDt is inclusive for the
            USGS service, we subtract 1 minute from this time so we don't get
            overlap between consecutive calls.
        chunk_by : Union[str, None], default = None
            How to "chunk" the fetching and storing of the data.
            Valid options = ["location_id", "day", "week", "month",
            "year", None].
        filter_to_hourly : bool = True
            Return only values that fall on the hour
            (i.e. drop 15 minute data).
        filter_no_data : bool = True
            Filter out -999 values.
        convert_to_si : bool = True
            Multiplies values by 0.3048**3 and sets `measurement_units`
            to `m3/s`.
        overwrite_output : bool
            Flag specifying whether or not to overwrite output files if they
            already exist.  True = overwrite; False = fail.
        is_primary : bool = True
            Flag specifying whether the data should be considered the primary
            or secondary timeseries.

        Examples
        --------
        Here we fetch five days worth of USGS hourly streamflow data,
        to two gages, chunking by day.

        Import the module.

        >>> from teehr.loading.usgs.usgs import usgs_to_parquet

        Set the input variables.

        >>> SITES=["02449838", "02450825"]
        >>> START_DATE=datetime(2023, 2, 20)
        >>> END_DATE=datetime(2023, 2, 25)
        >>> OUTPUT_PARQUET_DIR=Path(Path().home(), "temp", "usgs")
        >>> CHUNK_BY="day",
        >>> OVERWRITE_OUTPUT=True

        Fetch the data, writing to the specified output directory.

        >>> usgs_to_parquet(
        >>>     sites=SITES,
        >>>     start_date=START_DATE,
        >>>     end_date=END_DATE,
        >>>     output_parquet_dir=TEMP_DIR,
        >>>     chunk_by=CHUNK_BY,
        >>>     overwrite_output=OVERWRITE_OUTPUT
        >>> )
        """
        logger.info("Fetching USGS streamflow data.")
        usgs_to_parquet(
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=self._get_timeseries_cache_path(is_primary),
            chunk_by=chunk_by,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si,
            overwrite_output=overwrite_output
        )

    def fetch_nwm_retrospective_points(
        self,
        nwm_version: SupportedNWMRetroVersionsEnum,
        variable_name: str,
        location_ids: List[int],
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        chunk_by: Union[NWMChunkByEnum, None] = None,
        overwrite_output: Optional[bool] = False,
        domain: Optional[SupportedNWMRetroDomainsEnum] = "CONUS",
        is_primary: Optional[bool] = False
    ):
        """Fetch NWM retrospective at NWM COMIDs and store as Parquet file.

        All dates and times within the files and in the file names are in UTC.

        Parameters
        ----------
        nwm_version : SupportedNWMRetroVersionsEnum
            NWM retrospective version to fetch.
            Currently `nwm20`, `nwm21`, and `nwm30` supported.
        variable_name : str
            Name of the NWM data variable to download.
            (e.g., "streamflow", "velocity", ...).
        location_ids : Iterable[int],
            NWM feature_ids to fetch.
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
            and "PR". Only used when NWM version equals `nwm30`.
        is_primary : bool = False
            Flag specifying whether the data should be considered the primary
            or secondary timeseries.

        Examples
        --------
        Here we fetch and format retrospective NWM v2.0 streamflow data
        for two locations.

        Import the module.

        >>> import teehr.loading.nwm.retrospective_points as nwm_retro

        Specify the input variables.

        >>> NWM_VERSION = "nwm20"
        >>> VARIABLE_NAME = "streamflow"
        >>> START_DATE = datetime(2000, 1, 1)
        >>> END_DATE = datetime(2000, 1, 2, 23)
        >>> LOCATION_IDS = [7086109, 7040481]
        >>> OUTPUT_ROOT = Path(Path().home(), "temp")
        >>> OUTPUT_DIR = Path(OUTPUT_ROOT, "nwm20_retrospective")

        Fetch and format the data, writing to the specified directory.

        >>> nwm_retro.nwm_retro_to_parquet(
        >>>     nwm_version=NWM_VERSION,
        >>>     variable_name=VARIABLE_NAME,
        >>>     start_date=START_DATE,
        >>>     end_date=END_DATE,
        >>>     location_ids=LOCATION_IDS,
        >>>     output_parquet_dir=OUTPUT_DIR
        >>> )
        """
        # NOTE: Locations IDs will come from the locations table and crosswalk.
        logger.info("Fetching NWM retrospective point data.")
        nwm_retro_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            start_date=start_date,
            end_date=end_date,
            location_ids=location_ids,
            output_parquet_dir=self._get_timeseries_cache_path(is_primary),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain
        )

    def fetch_nwm_retrospective_grids(
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
        is_primary: Optional[bool] = True
    ):
        """
        Compute the weighted average for NWM v2.1 or v3.0 gridded data.

        Pixel values are summarized to zones based on a pre-computed
        zonal weights file, and the output is saved to parquet files.

        All dates and times within the files and in the file names are in UTC.

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
            and "PR". Only used when NWM version equals v3.0.
        location_id_prefix : Union[str, None]
            Optional location ID prefix to add (prepend) or replace.
        is_primary : bool = True
            Flag specifying whether the data should be considered the primary
            or secondary timeseries.

        Notes
        -----
        The location_id values in the zonal weights file are used as
        location ids in the output of this function, unless a prefix is
        specified which will be prepended to the location_id values if none
        exists, or it will replace the existing prefix. It is assumed that
        the location_id follows the pattern '[prefix]-[unique id]'.
        """
        logger.info("Fetching NWM retrospective grid data.")
        nwm_retro_grids_to_parquet(
            nwm_version=nwm_version,
            variable_name=variable_name,
            zonal_weights_filepath=zonal_weights_filepath,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=self._get_timeseries_cache_path(is_primary),
            chunk_by=chunk_by,
            overwrite_output=overwrite_output,
            domain=domain,
            location_id_prefix=location_id_prefix
        )

    def fetch_nwm_forecast_points(
        self,
        configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        location_ids: List[int],
        nwm_version: SupportedNWMOperationalVersionsEnum,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        t_minus_hours: Optional[List[int]] = None,
        process_by_z_hour: Optional[bool] = True,
        stepsize: Optional[int] = 100,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        is_primary: Optional[bool] = False
    ):
        """Fetch NWM forecast point data.

        Includes fetching and storing data.

        - Get location IDs from the locations table.
        - Validate configuration.


        """
        logger.info("Fetching NWM forecast point data.")
        nwm_to_parquet(
            configuration=configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            ingest_days=ingest_days,
            location_ids=location_ids,
            json_dir=self.temp_dir,
            output_parquet_dir=self._get_timeseries_cache_path(is_primary),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            t_minus_hours=t_minus_hours,
            process_by_z_hour=process_by_z_hour,
            stepsize=stepsize,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output
        )
        pass

    def fetch_nwm_forecast_grids(
        self,
        configuration: str,
        output_type: str,
        variable_name: str,
        start_date: Union[str, datetime],
        ingest_days: int,
        zonal_weights_filepath: Union[Path, str],
        nwm_version: SupportedNWMOperationalVersionsEnum,
        data_source: Optional[SupportedNWMDataSourcesEnum] = "GCS",
        kerchunk_method: Optional[SupportedKerchunkMethod] = "local",
        t_minus_hours: Optional[List[int]] = None,
        ignore_missing_file: Optional[bool] = True,
        overwrite_output: Optional[bool] = False,
        location_id_prefix: Optional[Union[str, None]] = None,
        is_primary: Optional[bool] = True
    ):
        """Fetch NWM forecast grid data.

        Includes fetching and storing data.
        """
        logger.info("Fetching NWM forecast grid data.")
        nwm_grids_to_parquet(
            configuration=configuration,
            output_type=output_type,
            variable_name=variable_name,
            start_date=start_date,
            ingest_days=ingest_days,
            zonal_weights_filepath=zonal_weights_filepath,
            json_dir=self.temp_dir,
            output_parquet_dir=self._get_timeseries_cache_path(is_primary),
            nwm_version=nwm_version,
            data_source=data_source,
            kerchunk_method=kerchunk_method,
            t_minus_hours=t_minus_hours,
            ignore_missing_file=ignore_missing_file,
            overwrite_output=overwrite_output,
            location_id_prefix=location_id_prefix
        )
        pass
