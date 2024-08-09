"""Evaluation module."""
import pandas as pd
import geopandas as gpd
from typing import Union, List
from enum import Enum
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
from teehr.pre.evaluation import copy_template_to
from teehr.pre.locations import (
    convert_locations,
    validate_and_insert_locations,
)
from teehr.pre.location_crosswalks import (
    convert_location_crosswalks,
    validate_and_insert_location_crosswalks,
)
from teehr.pre.location_attributes import (
    convert_location_attributes,
    validate_and_insert_location_attributes,
)
from teehr.pre.timeseries import (
    convert_timeseries,
    validate_and_insert_timeseries
)
from teehr.models.metrics import MetricsBasemodel
from teehr.evaluation.utils import get_joined_timeseries_fields
from teehr.models.domain_tables import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
from teehr.pre.add_domains import (
    add_configuration,
    add_unit,
    add_variable,
    add_attribute,
)
import teehr.const as const

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
    def fields(self) -> Enum:
        """The field names from the joined timeseries table."""
        logger.info("Getting fields from the joined timeseries table.")
        return get_joined_timeseries_fields(
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

    def add_configuration(
        self,
        configuration: Union[Configuration, List[Configuration]]
    ):
        """Add a configuration domain to the evaluation."""
        add_configuration(self.dataset_dir, configuration)

    def add_unit(
        self,
        unit: Union[Unit, List[Unit]]
    ):
        """Add a unit to the evaluation."""
        add_unit(self.dataset_dir, unit)

    def add_variable(
        self,
        variable: Union[Variable, List[Variable]]
    ):
        """Add a unit to the evaluation."""
        add_variable(self.dataset_dir, variable)

    def add_attribute(
        self,
        attribute: Union[Attribute, List[Attribute]]
    ):
        """Add an attribute to the evaluation."""
        add_attribute(self.dataset_dir, attribute)

    def import_locations(
            self,
            in_path: Union[Path, str],
            field_mapping: dict = None,
            pattern: str = "**/*.parquet",
            **kwargs
    ):
        """Import geometry data.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Any file format that can be read by GeoPandas.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files.
            Only used when in_path is a directory.
        **kwargs
            Additional keyword arguments are passed to GeoPandas read_file().

        File is first read by GeoPandas, field names renamed and
        then validated and inserted into the dataset.
        """
        temp_dirpath = Path(
            self.cache_dir, const.LOCATIONS_DIR
        )
        convert_locations(
            in_path,
            temp_dirpath,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        validate_and_insert_locations(temp_dirpath, self.dataset_dir)

    def import_location_crosswalks(
            self,
            in_path: Union[Path, str],
            field_mapping: dict = None,
            pattern: str = "**/*.parquet",
            **kwargs
    ):
        """Import location crosswalks.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files.
            Only used when in_path is a directory.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv().
        """
        temp_dirpath = Path(
            self.cache_dir,
            const.LOCATION_CROSSWALKS_DIR
        )
        convert_location_crosswalks(
            in_path,
            temp_dirpath,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        validate_and_insert_location_crosswalks(
            temp_dirpath, self.dataset_dir
        )

    def import_location_attributes(
            self,
            in_path: Union[Path, str],
            field_mapping: dict = None,
            pattern: str = "**/*.parquet",
            **kwargs
    ):
        """Import location_attributes.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files.
            Only used when in_path is a directory.
        **kwargs
            Additional keyword arguments are passed to pd.read_csv().
        """
        temp_dirpath = Path(
            self.cache_dir,
            const.LOCATION_ATTRIBUTES_DIR
        )
        convert_location_attributes(
            in_path,
            temp_dirpath,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        validate_and_insert_location_attributes(
            temp_dirpath, self.dataset_dir
        )

    def import_secondary_timeseries(
        self,
        in_path: Union[Path, str],
        pattern="**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None
    ):
        """Import secondary timeseries data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory).
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files if in_path is a directory.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}

        Includes validation and importing data to database.
        """
        temp_path = Path(
            self.cache_dir,
            const.LOCATION_ATTRIBUTES_DIR
        )
        temp_path.mkdir(parents=True, exist_ok=True)

        convert_timeseries(
            in_path=in_path,
            out_path=temp_path,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            pattern=pattern
        )

        if pattern.endswith(".csv"):
            pattern = pattern.replace(".csv", ".parquet")

        validate_and_insert_timeseries(
            in_path=temp_path,
            dataset_path=self.dataset_dir,
            timeseries_type="secondary",
            pattern=pattern,
        )

    def import_primary_timeseries(
        self,
        in_path: Union[Path, str],
        pattern="**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None
    ):
        """Import primary timeseries data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory).
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files if in_path is a directory.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}

        Includes validation and importing data to database.
        """
        temp_path = Path(
            self.cache_dir,
            const.PRIMARY_TIMESERIES_DIR
        )
        temp_path.mkdir(parents=True, exist_ok=True)

        convert_timeseries(
            in_path=in_path,
            out_path=temp_path,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            pattern=pattern
        )

        if pattern.endswith(".csv"):
            pattern = pattern.replace(".csv", ".parquet")

        validate_and_insert_timeseries(
            in_path=temp_path,
            dataset_path=self.dataset_dir,
            timeseries_type="primary",
            pattern=pattern
        )

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
