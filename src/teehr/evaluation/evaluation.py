"""Evaluation module."""
import pandas as pd
import geopandas as gpd
from typing import Union, List
from enum import Enum
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
from teehr.pre.utils import (
    copy_template_to,
)
from teehr.evaluation.utils import (
    _get_joined_timeseries_fields,
)

import teehr.const as const
from teehr.models.metrics.metrics import MetricsBasemodel
from teehr.evaluation.fetch import Fetch
from teehr.evaluation.load import Load
from teehr.evaluation.query import Query


logger = logging.getLogger(__name__)


class Evaluation:
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

        self.dataset_dir = Path(
            self.dir_path, const.DATASET_DIR
        )
        self.cache_dir = Path(
            self.dir_path, const.CACHE_DIR
        )
        self.locations_dir = Path(
            self.dataset_dir, const.LOCATIONS_DIR
        )
        self.primary_timeseries_dir = Path(
            self.dataset_dir, const.PRIMARY_TIMESERIES_DIR
        )
        self.locations_crosswalk_dir = Path(
            self.dataset_dir, const.LOCATION_CROSSWALKS_DIR
        )
        self.secondary_timeseries_dir = Path(
            self.dataset_dir, const.SECONDARY_TIMESERIES_DIR
        )
        self.joined_timeseries_dir = Path(
            self.dataset_dir, const.JOINED_TIMESERIES_DIR
        )

        if not Path(self.dir_path).is_dir():
            logger.error(f"Directory {self.dir_path} does not exist.")
            raise NotADirectoryError

        # Create a local Spark Session if one is not provided.
        if not self.spark:
            logger.info("Creating a new Spark session.")
            conf = SparkConf().setAppName("TEEHR").setMaster("local")
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    @property
    def fetch(self) -> Fetch:
        """The fetch component class."""
        return Fetch(self)

    @property
    def load(self) -> Load:
        """The load component class."""
        return Load(self)

    @property
    def query(self) -> Load:
        """The query component class."""
        # TODO: Implement the Query class.
        return Query(self)

    @property
    def fields(self) -> Enum:
        """The field names from the joined timeseries table."""
        logger.info("Getting fields from the joined timeseries table.")
        return _get_joined_timeseries_fields(
            Path(self.joined_timeseries_dir)
        )

    def enable_logging(self):
        """Enable logging."""
        logger = logging.getLogger("teehr")
        # logger.addHandler(logging.StreamHandler())
        handler = logging.FileHandler(Path(self.dir_path, 'teehr.log'))
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(message)s"
            )
        )
        logger.addHandler(
            handler
        )
        logger.setLevel(logging.DEBUG)

    def clone_template(self):
        """Create a study from the standard template.

        This method mainly copies the template directory to the specified
        evaluation directory.
        """
        teehr_root = Path(__file__).parent.parent
        template_dir = Path(teehr_root, "template")
        logger.info(f"Copying template from {template_dir} to {self.dir_path}")
        copy_template_to(template_dir, self.dir_path)

    def clean_cache():
        """Clean temporary files.

        Includes removing temporary files.
        """
        pass

    def clone_study():
        """Get a study from s3.

        Includes retrieving metadata and contents.
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

