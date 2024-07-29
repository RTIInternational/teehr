"""Evaluation module."""
import pandas as pd
from typing import Union
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
from teehr.pre.project_creation import copy_template_to

logger = logging.getLogger(__name__)


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

        if not Path(self.dir_path).is_dir():
            logger.error(f"Directory {self.dir_path} does not exist.")
            raise NotADirectoryError

        # Create a local Spark Session if one is not provided.
        if not self.spark:
            logger.info("Creating a new Spark session.")
            conf = SparkConf().setAppName("TEERH").setMaster("local")
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

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

    def get_metrics() -> pd.DataFrame:
        """Get metrics data.

        Includes retrieving data and metadata.
        """
        pass

    def get_timeseries_chars():
        """Get timeseries characteristics.

        Includes retrieving data and metadata.
        """
        pass
