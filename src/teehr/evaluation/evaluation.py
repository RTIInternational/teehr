"""Evaluation module."""
from datetime import datetime
from typing import Union, Literal, List
from pathlib import Path
from teehr.evaluation.tables.attribute_table import AttributeTable
from teehr.evaluation.tables.configuration_table import ConfigurationTable
from teehr.evaluation.tables.location_attribute_table import (
    LocationAttributeTable
)
from teehr.evaluation.tables.location_crosswalk_table import (
    LocationCrosswalkTable
)
from teehr.evaluation.tables.location_table import LocationTable
from teehr.evaluation.tables.primary_timeseries_table import (
    PrimaryTimeseriesTable
)
from teehr.evaluation.tables.secondary_timeseries_table import (
    SecondaryTimeseriesTable
)
from teehr.evaluation.tables.unit_table import UnitTable
from teehr.evaluation.tables.variable_table import VariableTable
from teehr.evaluation.tables.joined_timeseries_table import (
    JoinedTimeseriesTable
)
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path, remove_dir_if_exists
from pyspark.sql import SparkSession
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
from teehr.evaluation.write import Write
from teehr.evaluation.extract import DataExtractor
from teehr.evaluation.validate import Validator
from teehr.evaluation.workflows import Workflow
from teehr.evaluation.tables.base_table import Table
from teehr.evaluation.read import Read
from teehr.evaluation.load import Load
from teehr.evaluation.utils import (
    create_spark_session,
    copy_migrations_dir,
    get_table_instance
)
import pandas as pd
import re
from fsspec.implementations.local import LocalFileSystem
import pyspark.sql as ps
from teehr.querying.filter_format import validate_and_apply_filters
from teehr.utilities import apply_migrations
from teehr.models.evaluation_base import (
    EvaluationBase,
    LocalCatalog,
    RemoteCatalog
)


logger = logging.getLogger(__name__)


class Evaluation(EvaluationBase):
    """The Evaluation class.

    This is the main class for the TEEHR evaluation.
    """

    def __init__(
        self,
        local_warehouse_dir: Union[str, Path] = None,  # maybe you don't want/need a local evaluation? -- if you just want to query against remote?
        local_catalog_name: str = "local",
        local_catalog_type: str = "hadoop",
        local_namespace_name: str = "teehr",
        create_local_dir: bool = False,
        remote_warehouse_dir: str = const.WAREHOUSE_S3_PATH,
        remote_catalog_name: str = "iceberg",
        remote_catalog_type: str = "rest",
        remote_catalog_uri: str = const.CATALOG_REST_URI,
        remote_namespace_name: str = "teehr",
        spark: SparkSession = None,
        check_evaluation_version: bool = True,
        app_name: str = "teehr-iceberg",
        driver_memory: Union[str, int, float] = None,
        driver_maxresultsize: Union[str, int, float] = None
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
        if local_warehouse_dir is not None:
            local_warehouse_dir = Path(local_warehouse_dir)
        self.local_catalog = LocalCatalog(
            warehouse_dir=local_warehouse_dir,
            catalog_name=local_catalog_name,
            namespace_name=local_namespace_name,
            catalog_type=local_catalog_type,
        )
        self.remote_catalog = RemoteCatalog(
            warehouse_dir=remote_warehouse_dir,
            catalog_name=remote_catalog_name,
            namespace_name=remote_namespace_name,
            catalog_type=remote_catalog_type,
            catalog_uri=remote_catalog_uri,
        )
        # Create local directory if it does not exist.
        if local_warehouse_dir is not None:
            if create_local_dir:
                logger.info(f"Creating directory {local_warehouse_dir}.")
                Path(local_warehouse_dir).mkdir(parents=True, exist_ok=True)

        # Initialize cache and scripts dir. These are only valid
        # when using a local catalog.
        self.cache_dir = None
        self.scripts_dir = None

        # Check version of Evaluation
        if check_evaluation_version is True and local_warehouse_dir is not None:
            if create_local_dir is False:
                self.check_evaluation_version()

        # If a Spark session is provided.
        self.spark = spark

        # Create a Spark Session if one is not provided.
        if not self.spark:
            logger.info("Creating a new Spark session.")
            self.spark = create_spark_session(
                local_warehouse_dir=self.local_catalog.warehouse_dir,
                local_catalog_name=self.local_catalog.catalog_name,
                local_catalog_type=self.local_catalog.catalog_type,
                remote_warehouse_dir=self.remote_catalog.warehouse_dir,
                remote_catalog_name=self.remote_catalog.catalog_name,
                remote_catalog_type=self.remote_catalog.catalog_type,
                remote_catalog_uri=self.remote_catalog.catalog_uri,
                driver_maxresultsize=driver_maxresultsize,
                driver_memory=driver_memory,
                app_name=app_name
            )
        # Set the local catalog as the active catalog by default.
        if local_warehouse_dir is not None:
            self.set_active_catalog("local")
        else:
            self.set_active_catalog("remote")

    @property
    def table(self) -> Table:
        """The table component class for managing data tables."""
        return Table(self)

    @property
    def validate(self) -> Validator:
        """The validate component class for validating data."""
        return Validator(self)

    @property
    def load(self) -> Load:
        """The load component class for loading data."""
        return Load(self)

    @property
    def extract(self) -> DataExtractor:
        """The extract component class for extracting data."""
        return DataExtractor(self)

    @property
    def workflows(self) -> Workflow:
        """The workflow component class for managing evaluation workflows."""
        return Workflow(self)

    @property
    def write(self) -> Write:
        """The write component class for writing data."""
        return Write(self)

    @property
    def read(self) -> Read:
        """The read component class for reading data."""
        return Read(self)

    @property
    def generate(self) -> GeneratedTimeseries:
        """The generate component class for generating synthetic data."""
        return GeneratedTimeseries(self)

    @property
    def fetch(self) -> Fetch:
        """The fetch component class for accessing external data."""
        # if self.is_s3:
        #     logger.error("Cannot fetch data and save to S3 yet.")
        #     raise Exception("Cannot fetch data and save to S3 yet.")

        return Fetch(self)

    @property
    def metrics(self) -> Metrics:
        """The metrics component class for calculating performance metrics."""
        cls = Metrics(self)
        return cls()

    @property
    def units(self) -> UnitTable:
        """Access the units table."""
        tbl = UnitTable(self)
        return tbl()

    @property
    def variables(self) -> VariableTable:
        """Access the variables table."""
        tbl = VariableTable(self)
        return tbl()

    @property
    def attributes(self) -> AttributeTable:
        """Access the attributes table."""
        tbl = AttributeTable(self)
        return tbl()

    @property
    def configurations(self) -> ConfigurationTable:
        """Access the configurations table."""
        tbl = ConfigurationTable(self)
        return tbl()

    @property
    def locations(self) -> LocationTable:
        """Access the locations table."""
        tbl = LocationTable(self)
        return tbl()

    @property
    def location_attributes(self) -> LocationAttributeTable:
        """Access the location attributes table."""
        tbl = LocationAttributeTable(self)
        return tbl()

    @property
    def location_crosswalks(self) -> LocationCrosswalkTable:
        """Access the location crosswalks table."""
        tbl = LocationCrosswalkTable(self)
        return tbl()

    @property
    def primary_timeseries(self) -> PrimaryTimeseriesTable:
        """Access the primary timeseries table."""
        tbl = PrimaryTimeseriesTable(self)
        return tbl()

    @property
    def secondary_timeseries(self) -> SecondaryTimeseriesTable:
        """Access the secondary timeseries table."""
        tbl = SecondaryTimeseriesTable(self)
        return tbl()

    @property
    def joined_timeseries(self) -> JoinedTimeseriesTable:
        """Access the joined timeseries table."""
        tbl = JoinedTimeseriesTable(self)
        return tbl()

    def set_active_catalog(self, catalog: Literal["local", "remote"]):
        """Set the active catalog to either local or remote.

        Parameters
        ----------
        catalog : Literal["local", "remote"]
            The catalog to set as active.
        """
        if catalog == "local":
            self.active_catalog = self.local_catalog
            self.spark.catalog.setCurrentCatalog(
                self.local_catalog.catalog_name
            )
            self.cache_dir = to_path_or_s3path(
                self.local_catalog.warehouse_dir, const.CACHE_DIR
            )
            self.scripts_dir = to_path_or_s3path(
                self.local_catalog.warehouse_dir, const.SCRIPTS_DIR
            )
            logger.info("Active catalog set to local.")
        elif catalog == "remote":
            self.active_catalog = self.remote_catalog
            self.spark.catalog.setCurrentCatalog(
                self.remote_catalog.catalog_name
            )
            logger.info("Active catalog set to remote.")
        else:
            raise ValueError("Catalog must be either 'local' or 'remote'.")

    def enable_logging(self):
        """Enable logging."""
        logger = logging.getLogger("teehr")
        # logger.addHandler(logging.StreamHandler())
        # if self.is_s3:
        #     logger_path = Path(Path.home, 'teehr.log')
        # else:
        logger_path = Path(self.active_catalog.warehouse_dir, 'teehr.log')

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

    def clone_template(
        self,
        catalog_name: str = None,
        namespace_name: str = None,
        local_warehouse_dir: Union[str, Path] = None
    ):
        """Create a study from the standard template.

        This method mainly copies the template directory to the specified
        evaluation directory.
        """
        # if self.is_s3:
        #     logger.error("Cannot clone template to S3.")
        #     raise Exception("Cannot clone template to S3.")

        # Set to local by default.
        if catalog_name is None:
            catalog_name = self.active_catalog.catalog_name
        if namespace_name is None:
            namespace_name = self.active_catalog.namespace_name
        if local_warehouse_dir is None:
            local_warehouse_dir = self.active_catalog.warehouse_dir

        if local_warehouse_dir is None:
            raise ValueError("local_warehouse_dir must be specified.")

        teehr_root = Path(__file__).parent.parent
        template_dir = Path(teehr_root, "template")
        logger.info(
            f"Copying template from {template_dir} to {local_warehouse_dir}"
        )

        copy_template_to(template_dir, local_warehouse_dir)
        # Copy in the schema
        copy_migrations_dir(
            target_dir=local_warehouse_dir
        )
        # Create initial iceberg tables.
        apply_migrations.evolve_catalog_schema(
            spark=self.spark,
            migrations_dir_path=local_warehouse_dir,
            catalog_name=catalog_name,
            namespace=namespace_name
        )

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
        remote_catalog_name: str = None,
        remote_namespace_name: str = None,
        primary_location_ids: List[str] = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        # spatial_filter: str = None
    ):
        """Pull down an evaluation from S3, potentially subsetting.

        Parameters
        ----------
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
        The options for subsetting could really be wide open now?

        """
        # You must configure the catalogs when initializing the Evaluation.
        if self.local_catalog.warehouse_dir is None:
            raise ValueError("The 'local_warehouse_dir' must be specified.")
        if remote_catalog_name is None:
            remote_catalog_name = self.remote_catalog.catalog_name
        if remote_namespace_name is None:
            remote_namespace_name = self.remote_catalog.namespace_name

        self.clone_template()

        # Now pull down the data from remote, applying any filtering, and
        # writing to the local template.
        clone_from_s3(
            ev=self,
            local_catalog_name=self.local_catalog.catalog_name,
            local_namespace_name=self.local_catalog.namespace_name,
            remote_catalog_name=remote_catalog_name,
            remote_namespace_name=remote_namespace_name,
            primary_location_ids=primary_location_ids,
            start_date=start_date,
            end_date=end_date
        )

    def clean_cache(self):
        """Clean temporary files.

        Includes removing temporary files.
        """
        # if self.is_s3:
        #     logger.error("Cannot clean cache on S3.")
        #     raise Exception("Cannot clean cache on S3.")

        logger.info(f"Removing temporary files from {self.active_catalog.cache_dir}")
        remove_dir_if_exists(self.active_catalog.cache_dir)
        self.active_catalog.cache_dir.mkdir()

    def sql(self, query: str, create_temp_views: List[str]):
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
        """ # noqa
        # if not create_temp_views:
        #     create_temp_views = [
        #         "units",
        #         "variables",
        #         "attributes",
        #         "configurations",
        #         "locations",
        #         "location_attributes",
        #         "location_crosswalks",
        #         "primary_timeseries",
        #         "secondary_timeseries",
        #         "joined_timeseries"
        #     ]  # joined_timeseries may not exist

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

    def check_evaluation_version(self, warehouse_dir: Union[str, Path] = None) -> str:
        """Check the version of the TEEHR Evaluation."""
        # if self.is_s3:
        #     fs = s3fs.S3FileSystem(anon=True)
        #     version_file = self.active_catalog.warehouse_dir.path + "/" + "version"
        # else:
        if warehouse_dir is not None:
            self.active_catalog.warehouse_dir = warehouse_dir

        fs = LocalFileSystem()
        version_file = Path(warehouse_dir, "version")

        if not fs.exists(version_file):
            logger.error(f"Version file not found in {warehouse_dir}.")
            if self.is_s3:
                err_msg = (
                    f"Please create a version file in {warehouse_dir}."
                )
                logger.error(err_msg)
                raise Exception(err_msg)
            else:
                # Raise an error if no version file is found.
                err_msg = (
                    "Incompatible Evaluation version."
                    f" No version file found in {warehouse_dir}."
                    " TEEHR v0.6 requires a version file to be present"
                    " in the evaluation directory."
                )
                logger.error(err_msg)
                raise ValueError(err_msg)
        else:
            with fs.open(version_file) as f:
                version_txt = str(f.read().strip())
            match = re.findall(r'(\d+\.\d+\.\d+)', version_txt)  # Assumes semantic versioning
            if len(match) != 1:
                err_msg = f"Invalid version format in {warehouse_dir}: {version_txt}"
                logger.error(err_msg)
                raise ValueError(err_msg)
            else:
                version = match[0]
        # Raise an error requiring migration to v0.6 warehouse.
        if version < "0.6.0":
            err_msg = (
                f"Evaluation version {version} in {warehouse_dir} is less than 0.6."
                " Please run the migration to upgrade to the latest version."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
        logger.info(
            f"Found evaluation version {version} in {warehouse_dir}."
            " Future versions v0.6 and greater will require a conversion"
            " to a new format."
        )
        return version

    # TODO: Remove?
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
        if table_filter is not None:
            table_name = table_filter.table_name
            filters = table_filter.filters
        if table_name is None:
            raise ValueError("Table name must be specified.")
        # TODO: Instantiate generic table?
        base_table = get_table_instance(self, table_name)
        return validate_and_apply_filters(
            sdf=base_table.to_sdf(),
            filters=filters,
            filter_model=base_table.filter_model,
            fields_enum=base_table.field_enum(),
            dataframe_schema=base_table._get_schema("pandas"),
            validate=base_table.validate_filter_field_types
        )

    def apply_schema_migration(
        self,
        catalog_name: str = None,
        namespace: str = None,
        warehouse_dir: Union[str, Path] = None
    ):
        """Migrate v0.5 Evalution to v0.6 Iceberg tables."""
        if catalog_name is None:
            catalog_name = self.active_catalog.catalog_name
        if namespace is None:
            namespace = self.active_catalog.namespace_name
        if warehouse_dir is None:
            warehouse_dir = Path(self.active_catalog.warehouse_dir)

        if Path(warehouse_dir, "migrations").exists() is False:
            logger.info("Copying migration scripts to evaluation directory.")
            copy_migrations_dir(
                target_dir=warehouse_dir
            )
        apply_migrations.evolve_catalog_schema(
            spark=self.spark,
            migrations_dir_path=warehouse_dir,
            catalog_name=catalog_name,
            namespace=namespace
        )
        logger.info(f"Schema evolution completed for {catalog_name}.")

    def list_tables(
        self,
        catalog_name: str = None,
        namespace: str = None
    ) -> pd.DataFrame:
        """List the tables in the catalog returning a Pandas DataFrame."""
        if catalog_name is None:
            catalog_name = self.active_catalog.catalog_name
        if namespace is None:
            namespace = self.active_catalog.namespace_name
        tbl_list = self.spark.catalog.listTables(
            f"{catalog_name}.{namespace}"
        )
        metadata = []
        # Note. "EXTERNAL" tables are those managed by REST catalog?
        # (ie, not hadoop)
        for tbl in tbl_list:
            if tbl.tableType == "VIEW":
                continue
            metadata.append({
                "name": tbl.name,
                "database": tbl.database,
                "description": tbl.description,
                "tableType": tbl.tableType,
                "isTemporary": tbl.isTemporary
            })
            logger.info(f"Table: {tbl.name}, Type: {tbl.tableType}")
        return pd.DataFrame(metadata)

    def list_views(self) -> pd.DataFrame:
        """List the views in the catalog returning a Pandas DataFrame."""
        return self.spark.sql("SHOW VIEWS").toPandas()
