"""Evaluation module."""
import psutil
from datetime import datetime
from typing import Union, Literal, List
from pathlib import Path
from teehr.evaluation.tables.attribute_table import AttributeTable
from teehr.evaluation.tables.configuration_table import ConfigurationTable
from teehr.evaluation.tables.location_attribute_table import LocationAttributeTable
from teehr.evaluation.tables.location_crosswalk_table import LocationCrosswalkTable
from teehr.evaluation.tables.location_table import LocationTable
from teehr.evaluation.tables.primary_timeseries_table import PrimaryTimeseriesTable
from teehr.evaluation.tables.secondary_timeseries_table import SecondaryTimeseriesTable
from teehr.evaluation.tables.unit_table import UnitTable
from teehr.evaluation.tables.variable_table import VariableTable
from teehr.evaluation.tables.joined_timeseries_table import JoinedTimeseriesTable
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
from teehr.loading.utils import copy_template_to
from teehr.loading.s3.clone_from_s3 import (
    list_s3_evaluations,
    clone_from_s3
)
from teehr.models.filters import TableFilter, FilterBaseModel, TableNamesEnum
import teehr.const as const
from teehr.evaluation.fetch import Fetch
from teehr.evaluation.metrics import Metrics
from teehr.evaluation.generate import GeneratedTimeseries
import pandas as pd
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor # noqa
import re
import teehr
import s3fs
from fsspec.implementations.local import LocalFileSystem
import pyspark.sql as ps
from teehr.querying.filter_format import validate_and_apply_filters


logger = logging.getLogger(__name__)


class Evaluation:
    """The Evaluation class.

    This is the main class for the TEEHR evaluation.
    """

    def __init__(
        self,
        dir_path: Union[str, Path, S3Path],
        create_dir: bool = False,
        spark: SparkSession = None
    ):
        """
        Initialize the Evaluation class.

        Parameters
        ----------
        dir_path : Union[str, Path, S3Path]
            The path to the evaluation directory.
        spark : SparkSession, optional
            The SparkSession object, by default None
        """
        self.dir_path = to_path_or_s3path(dir_path)

        self.is_s3 = False
        if isinstance(self.dir_path, S3Path):
            self.is_s3 = True
            logger.info(f"Using S3 path {self.dir_path}.  Evaluation will be read-only")

        self.spark = spark

        self.dataset_dir = to_path_or_s3path(
            self.dir_path, const.DATASET_DIR
        )
        self.cache_dir = to_path_or_s3path(
            self.dir_path, const.CACHE_DIR
        )
        self.scripts_dir = to_path_or_s3path(
            self.dir_path, const.SCRIPTS_DIR
        )

        if not self.is_s3 and not Path(self.dir_path).is_dir():
            if create_dir:
                logger.info(f"Creating directory {self.dir_path}.")
                Path(self.dir_path).mkdir(parents=True, exist_ok=True)
            else:
                logger.error(f"Directory {self.dir_path} does not exist.")
                raise NotADirectoryError

        # Check version of Evaluation
        if create_dir is False:
            self.check_evaluation_version()

        # Create a local Spark Session if one is not provided.
        if not self.spark:
            logger.info("Creating a new Spark session.")
            memory_info = psutil.virtual_memory()
            driver_memory = 0.75 * memory_info.available / (1024**3)  # Use 75% of available memory
            driver_maxresultsize = 0.5 * driver_memory
            conf = (
                SparkConf()
                .setAppName("TEEHR")
                .setMaster("local[*]")
                .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
                .set("spark.sql.execution.arrow.pyspark.enabled", "true")
                .set("spark.sql.session.timeZone", "UTC")
                .set("spark.driver.host", "localhost")
                .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
                .set("spark.sql.parquet.enableVectorizedReader", "false")
                .set("spark.driver.memory", f"{int(driver_memory)}g")
                .set("spark.driver.maxResultSize", f"{int(driver_maxresultsize)}g")
            )
            self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    @property
    def generate(self) -> GeneratedTimeseries:
        """The generate component class for generating synthetic data."""
        return GeneratedTimeseries(self)

    @property
    def fetch(self) -> Fetch:
        """The fetch component class for accessing external data."""
        if self.is_s3:
            logger.error("Cannot fetch data and save to S3 yet.")
            raise Exception("Cannot fetch data and save to S3 yet.")

        return Fetch(self)

    @property
    def metrics(self) -> Metrics:
        """The metrics component class for calculating performance metrics."""
        return Metrics(self)

    @property
    def units(self) -> UnitTable:
        """Access the units table."""
        return UnitTable(self)

    @property
    def variables(self) -> VariableTable:
        """Access the variables table."""
        return VariableTable(self)

    @property
    def attributes(self) -> AttributeTable:
        """Access the attributes table."""
        return AttributeTable(self)

    @property
    def configurations(self) -> ConfigurationTable:
        """Access the configurations table."""
        return ConfigurationTable(self)

    @property
    def locations(self) -> LocationTable:
        """Access the locations table."""
        return LocationTable(self)

    @property
    def location_attributes(self) -> LocationAttributeTable:
        """Access the location attributes table."""
        return LocationAttributeTable(self)

    @property
    def location_crosswalks(self) -> LocationCrosswalkTable:
        """Access the location crosswalks table."""
        return LocationCrosswalkTable(self)

    @property
    def primary_timeseries(self) -> PrimaryTimeseriesTable:
        """Access the primary timeseries table."""
        return PrimaryTimeseriesTable(self)

    @property
    def secondary_timeseries(self) -> SecondaryTimeseriesTable:
        """Access the secondary timeseries table."""
        return SecondaryTimeseriesTable(self)

    @property
    def joined_timeseries(self) -> JoinedTimeseriesTable:
        """Access the joined timeseries table."""
        return JoinedTimeseriesTable(self)

    def enable_logging(self):
        """Enable logging."""
        logger = logging.getLogger("teehr")
        # logger.addHandler(logging.StreamHandler())
        if self.is_s3:
            logger_path = Path(Path.home, 'teehr.log')
        else:
            logger_path = Path(self.dir_path, 'teehr.log')

        handler = logging.FileHandler(logger_path)
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
        if self.is_s3:
            logger.error("Cannot clone template to S3.")
            raise Exception("Cannot clone template to S3.")

        teehr_root = Path(__file__).parent.parent
        template_dir = Path(teehr_root, "template")
        logger.info(f"Copying template from {template_dir} to {self.dir_path}")
        copy_template_to(template_dir, self.dir_path)

    @staticmethod
    def list_s3_evaluations(
        format: Literal["pandas", "list"] = "pandas"
    ) -> Union[list, pd.DataFrame]:
        """List the evaluations available on S3.

        Parameters
        ----------
        format : str, optional
            The format of the output. Either "pandas" or "list".
            The default is "pandas".
        """
        return list_s3_evaluations(format=format)

    def clone_from_s3(
        self,
        evaluation_name: str,
        primary_location_ids: List[str] = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
    ):
        """Fetch the study data from S3.

        Copies the study from s3 to the local directory, with the option
        to subset the dataset by primary location ID, start and end dates.

        Parameters
        ----------
        evaluation_name : str
            The name of the evaluation to clone from S3.
            Use the list_s3_evaluations method to get the available
            evaluations.
        primary_location_ids : List[str], optional
            The list of primary location ids to subset the data.
            The default is None.
        start_date : Union[str, datetime], optional
            The start date to subset the data.
            The default is None.
        end_date : Union[str, datetime], optional
            The end date to subset the data.
            The default is None.

        Notes
        -----

        Includes the following tables:
            - units
            - variables
            - attributes
            - configurations
            - locations
            - location_attributes
            - location_crosswalks
            - primary_timeseries
            - secondary_timeseries
            - joined_timeseries

        Also includes the user_defined_fields.py script.

        """
        if self.is_s3:
            logger.error("Cannot clone from S3 to S3.")
            raise Exception("Cannot clone from S3 to S3.")

        return clone_from_s3(
            self,
            evaluation_name,
            primary_location_ids,
            start_date,
            end_date
        )

    def clean_cache(self):
        """Clean temporary files.

        Includes removing temporary files.
        """
        if self.is_s3:
            logger.error("Cannot clean cache on S3.")
            raise Exception("Cannot clean cache on S3.")

        logger.info(f"Removing temporary files from {self.cache_dir}")
        remove_dir_if_exists(self.cache_dir)
        self.cache_dir.mkdir()

    def sql(self, query: str, create_temp_views: List[str] = None):
        """Run a SQL query on the Spark session against the TEEHR tables.

        Parameters
        ----------
        query : str
            The SQL query to run.
        create_temp_views : List[str], optional
            A list of tables to create temporary views for.
            The default is None which creates all.

        Returns
        -------
        pyspark.sql.DataFrame
            The result of the SQL query.
            This is lazily evaluated so you need to call an action (e.g., sdf.show()) to get the result.

        By default this method has access to the following tables preloaded as temporary views:
            - units
            - variables
            - attributes
            - configurations
            - locations
            - location_attributes
            - location_crosswalks
            - primary_timeseries
            - secondary_timeseries
            - joined_timeseries
        """
        if not create_temp_views:
            create_temp_views = [
                "units",
                "variables",
                "attributes",
                "configurations",
                "locations",
                "location_attributes",
                "location_crosswalks",
                "primary_timeseries",
                "secondary_timeseries",
                "joined_timeseries"
            ]

        if "units" in create_temp_views:
            self.units.to_sdf().createOrReplaceTempView("units")
        if "variables" in create_temp_views:
            self.variables.to_sdf().createOrReplaceTempView("variables")
        if "attributes" in create_temp_views:
            self.attributes.to_sdf().createOrReplaceTempView("attributes")
        if "configurations" in create_temp_views:
            self.configurations.to_sdf().createOrReplaceTempView("configurations")
        if "locations" in create_temp_views:
            self.locations.to_sdf().createOrReplaceTempView("locations")
        if "location_attributes" in create_temp_views:
            self.location_attributes.to_sdf().createOrReplaceTempView("location_attributes")
        if "location_crosswalks" in create_temp_views:
            self.location_crosswalks.to_sdf().createOrReplaceTempView("location_crosswalks")
        if "primary_timeseries" in create_temp_views:
            self.primary_timeseries.to_sdf().createOrReplaceTempView("primary_timeseries")
        if "secondary_timeseries" in create_temp_views:
            self.secondary_timeseries.to_sdf().createOrReplaceTempView("secondary_timeseries")
        if "joined_timeseries" in create_temp_views:
            self.joined_timeseries.to_sdf().createOrReplaceTempView("joined_timeseries")

        return self.spark.sql(query)

    def check_evaluation_version(self):
        """Check the version of the TEEHR Evaluation."""
        if self.is_s3:
            fs = s3fs.S3FileSystem(anon=True)
            version_file = self.dir_path.path + "/" + "version"
        else:
            fs = LocalFileSystem()
            version_file = Path(self.dir_path, "version")
        if not fs.exists(version_file):
            logger.error(f"Version file not found in {self.dir_path}.")
            if self.is_s3:
                err_msg = (
                    f"Please create a version file in {self.dir_path}."
                )
                logger.error(err_msg)
                raise Exception(err_msg)
            else:
                # TODO: Change this to raise an error in v0.6.
                version = teehr.__version__
                with fs.open(version_file, "w") as f:
                    f.write(version)
                logger.info(
                    f"Created version file in {self.dir_path}."
                    " In the future this will raise an error."
                )
        else:
            with fs.open(version_file) as f:
                version_txt = str(f.read().strip())
            match = re.findall(r'(\d+\.\d+\.\d+)', version_txt)  # Assumes semantic versioning
            if len(match) != 1:
                err_msg = f"Invalid version format in {self.dir_path}: {version_txt}"
                logger.error(err_msg)
                raise ValueError(err_msg)
            else:
                version = match[0]
        # TODO: Uncomment this in v0.6
        # if version < "0.6.0":
        #     err_msg = (
        #         f"Evaluation version {version} in {self.dir_path} is less than 0.6."
        #         " Please run the migration to upgrade to the latest version."
        #     )
        #     logger.error(err_msg)
        #     raise ValueError(err_msg)
        # else:
        #     # Update the version to the latest
        #     pass
        logger.info(
            f"Found evaluation version {version} in {self.dir_path}."
            " Future versions v0.6 and greater will require a conversion"
            " to a new format."
        )
        return version

    def filter(
        self,
        table_name: TableNamesEnum = None,
        filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        table_filter: TableFilter = None
    ) -> ps.DataFrame:
        """Apply filters to a table returning a sdf.

        Parameters
        ----------
        table_name: TableNamesEnum
            The name of the table to filter. Defaults to None.
        filters: Union[str, dict, FilterBaseModel, List[Union[str, dict, FilterBaseModel]]]
            The filters to apply to the table. Defaults to None.
        table_filter: TableFilter
            A TableFilter object containing the table name and filters.
            Defaults to None.
        """
        table_mapper = {
            "primary_timeseries": self.primary_timeseries,
            "secondary_timeseries": self.secondary_timeseries,
            "locations": self.locations,
            "units": self.units,
            "variables": self.variables,
            "configurations": self.configurations,
            "attributes": self.attributes,
            "location_attributes": self.location_attributes,
            "location_crosswalks": self.location_crosswalks,
            "joined_timeseries": self.joined_timeseries,
        }
        if table_filter is not None:
            table_name = table_filter.table_name
            filters = table_filter.filters
        if table_name is None:
            raise ValueError("Table name must be specified.")
        base_table = table_mapper.get(table_name)
        return validate_and_apply_filters(
            sdf=base_table.to_sdf(),
            filters=filters,
            filter_model=base_table.filter_model,
            fields_enum=base_table.field_enum(),
            dataframe_schema=base_table._get_schema("pandas"),
            validate=base_table.validate_filter_field_types
        )
