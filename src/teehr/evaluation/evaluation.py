import pandas as pd
from typing import Union
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkConf


class Evaluation(
    dir_path: Union[str, Path],
    spark: SparkSession = None
):

    def __init__(self, path: str):
        self.dir_path = dir_path

        if not Path[self.dir_path].isdir():
            raise NotADirectoryError

    def create_study(path: str):
        """Create a study.

        Includes creating directory skeleton, adding metadata and domains.
        """
        pass

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

    def load_study():
        """Get a study.

        Includes retrieving metadata and contents.
        """
        pass

    def import_primary_timeseries(type: str):
        """Import primary timeseries data.

        Includes validadtion and importing data.
        """
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
