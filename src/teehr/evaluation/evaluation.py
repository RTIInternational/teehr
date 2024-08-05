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
from teehr.evaluation.utils import get_joined_timeseries_fields
from teehr.loading.usgs.usgs import usgs_to_parquet
from teehr.models.loading.utils import USGSChunkByEnum

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
        return get_joined_timeseries_fields(
            Path(self.dir_path, JOINED_TIMESERIES_DIR)
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
    ):
        """Fetch USGS streamflow data from NWIS.

        Includes retrieving data and metadata.
        """
        logger.info("Fetching USGS streamflow data.")
        usgs_to_parquet(
            sites=sites,
            start_date=start_date,
            end_date=end_date,
            output_parquet_dir=self.primary_timeseries_dir,
            chunk_by=chunk_by,
            filter_to_hourly=filter_to_hourly,
            filter_no_data=filter_no_data,
            convert_to_si=convert_to_si,
            overwrite_output=overwrite_output
        )
