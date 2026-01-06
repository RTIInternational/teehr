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
from teehr.utils.utils import remove_dir_if_exists
from pyspark.sql import SparkSession
import logging
from teehr.loading.utils import copy_template_to
from teehr.loading.s3.clone_from_s3 import (
    clone_from_s3
)
from teehr.evaluation.fetch import Fetch
from teehr.evaluation.metrics import Metrics
from teehr.evaluation.generate import GeneratedTimeseries
from teehr.evaluation.write import Write
from teehr.evaluation.extract import Extract
from teehr.evaluation.validate import Validate
from teehr.evaluation.workflows import Workflow
from teehr.evaluation.tables.base_table import Table
from teehr.evaluation.read import Read
from teehr.evaluation.load import Load
from teehr.evaluation.utils import copy_migrations_dir
from teehr.evaluation.spark_session_utils import (
    create_spark_session,
    log_session_config,
)
import pandas as pd
import re
from fsspec.implementations.local import LocalFileSystem
from teehr.utilities import apply_migrations
from teehr.models.evaluation_base import (
    EvaluationBase,
    LocalCatalog,
    RemoteCatalog
)
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor # noqa
from pydantic import BaseModel as PydanticBaseModel


logger = logging.getLogger(__name__)


class Evaluation(EvaluationBase):
    """The Evaluation class.

    This is the main class for the TEEHR evaluation.
    """

    def __init__(
        self,
        dir_path: Union[str, Path],
        create_dir: bool = False,
        check_evaluation_version: bool = True,
        spark: SparkSession = None
    ):
        """
        Initialize the Evaluation class.

        Parameters
        ----------
        dir_path : Union[str, Path]
            The path to the evaluation directory.
        create_dir : bool, optional
            Whether to create the local directory if it does not
             exist. The default is False.
        check_evaluation_version : bool, optional
            Whether to check the evaluation version in the local
            directory. The default is True.
        spark : SparkSession, optional
            The SparkSession object, by default None
        """
        # Create local directory if it does not exist.
        dir_path = Path(dir_path)

        # Initialize cache and scripts dir. These are only valid
        # when using a local catalog.
        self.dataset_dir = None
        self.cache_dir = None
        self.scripts_dir = None
        self.dir_path = dir_path

        if not self.dir_path.is_dir():
            if create_dir:
                logger.info(f"Creating directory {self.dir_path}.")
                Path(self.dir_path).mkdir(parents=True, exist_ok=True)
            else:
                logger.error(
                    f"Directory {self.dir_path} does not exist."
                    " Set create_dir=True to create it."
                )
                raise NotADirectoryError

        # Initialize Spark session
        if spark is not None:
            logger.info("Using provided Spark session.")
            self.spark = spark
        else:
            logger.info("Creating a new default Spark session.")
            self.spark = create_spark_session()
        # Need to update to local warehouse path based on dir_path
        # Note. Here 'warehouse_dir' should be 'catalog_dir'?
        local_catalog_name = self.spark.conf.get("local_catalog_name")
        warehouse_dir = dir_path / local_catalog_name
        self.spark.conf.set(
            f"spark.sql.catalog.{local_catalog_name}.warehouse",
            warehouse_dir.as_posix()
        )
        # Get the catalog metadata that was set during Spark configuration
        self.local_catalog = LocalCatalog(
            warehouse_dir=warehouse_dir,
            catalog_name=local_catalog_name,
            namespace_name=self.spark.conf.get("local_namespace_name"),
            catalog_type=self.spark.conf.get("local_catalog_type"),
        )
        self.remote_catalog = RemoteCatalog(
            warehouse_dir=self.spark.conf.get("remote_warehouse_dir"),
            catalog_name=self.spark.conf.get("remote_catalog_name"),
            namespace_name=self.spark.conf.get("remote_namespace_name"),
            catalog_type=self.spark.conf.get("remote_catalog_type"),
            catalog_uri=self.spark.conf.get("remote_catalog_uri"),
        )
        self.set_active_catalog("local")

        # Check version of Evaluation
        if create_dir is False and check_evaluation_version is True:
            self.check_evaluation_version()

    @property
    def table(self) -> Table:
        """The table component class for managing data tables."""
        return Table(self)

    @property
    def validate(self) -> Validate:
        """The validate component class for validating data."""
        return Validate(self)

    @property
    def load(self) -> Load:
        """The load component class for loading data."""
        return Load(self)

    @property
    def extract(self) -> Extract:
        """The extract component class for extracting data."""
        return Extract(self)

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
            self.cache_dir = self.local_catalog.cache_dir
            self.scripts_dir = self.local_catalog.scripts_dir
            self.dataset_dir = self.local_catalog.dataset_dir
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

    def clone_template(
        self,
        catalog_name: str = None,
        namespace_name: str = None,
        local_warehouse_dir: Union[str, Path] = None
    ):
        """Create a study from the standard template.

        This method mainly copies the template directory to the specified
        evaluation directory.

        Parameters
        ----------
        catalog_name : str, optional
            The catalog name to use, by default None which uses the
            active catalog name.
        namespace_name : str, optional
            The namespace name to use, by default None which uses the
            active namespace name.
        local_warehouse_dir : Union[str, Path], optional
            The local warehouse directory to use, by default None which uses the
            active warehouse directory: Path(dir_path, catalog_name).
        """
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
            local_catalog_name=catalog_name,
            local_namespace_name=namespace_name,
            target_catalog_name=catalog_name,
            target_namespace_name=namespace_name
        )

    def clone_from_s3(
        self,
        remote_catalog_name: str = None,
        remote_namespace_name: str = None,
        primary_location_ids: List[str] = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        # spatial_filter: str = None
    ):
        """Read data from the remote warehouse, potentially subsetting.

        Parameters
        ----------
        remote_catalog_name : str, optional
            The remote catalog name to pull from. The default is None,
            which uses the remote catalog name of the Evaluation.
        remote_namespace_name : str, optional
            The remote namespace name to pull from. The default is None,
            which uses the remote namespace name of the Evaluation.
        primary_location_ids : List[str], optional
            The list of primary location ids to subset the data.
            The default is None.
        start_date : Union[str, datetime], optional
            The start date to subset the data.
            The default is None.
        end_date : Union[str, datetime], optional
            The end date to subset the data.
            The default is None.
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
        if warehouse_dir is None:
            warehouse_dir = self.active_catalog.warehouse_dir
        fs = LocalFileSystem()
        version_file = Path(warehouse_dir, "version")

        if not fs.exists(version_file):
            logger.error(f"Version file not found in {warehouse_dir}.")
            err_msg = (
                f"Please create a version file in {warehouse_dir},"
                " or set 'check_evaluation_version'=False."
            )
            logger.error(err_msg)
            raise Exception(err_msg)
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
        )
        return version

    def apply_schema_migration(
        self,
        source_catalog: PydanticBaseModel = None,
        target_catalog: PydanticBaseModel = None
    ):
        """Apply the latest schema migration.

        Parameters
        ----------
        source_catalog : PydanticBaseModel, optional
            The source catalog to use for the source of the migration files.
            The default is None, which uses the local catalog.
        target_catalog : PydanticBaseModel, optional
            The target catalog to apply the migrations to.
            The default is None, which uses the remote catalog.
        """
        if source_catalog is None:
            source_catalog = self.local_catalog
        if target_catalog is None:
            target_catalog = self.remote_catalog

        ev_dir = Path(source_catalog.warehouse_dir).parent  # Get the next dir up from here
        if Path(ev_dir, "migrations").exists() is False:
            logger.info("Copying migration scripts to evaluation directory.")
            copy_migrations_dir(
                target_dir=ev_dir
            )
        apply_migrations.evolve_catalog_schema(
            spark=self.spark,
            migrations_dir_path=source_catalog.warehouse_dir,
            local_catalog_name=source_catalog.catalog_name,
            local_namespace_name=source_catalog.namespace_name,
            target_catalog_name=target_catalog.catalog_name,
            target_namespace_name=target_catalog.namespace_name
        )
        logger.info(
            f"Schema evolution completed for {target_catalog.catalog_name}."
        )

    def list_tables(
        self,
        catalog_name: str = None,
        namespace: str = None
    ) -> pd.DataFrame:
        """List the tables in the catalog returning a Pandas DataFrame.

        Parameters
        ----------
        catalog_name : str, optional
            The catalog name to list tables from, by default None, which means the
            catalog_name of the active catalog is used.
        namespace : str, optional
            The namespace name to list tables from, by default None, which means the
            namespace_name of the active catalog is used.
        """
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

    def log_spark_config(self):
        """Log the current Spark session configuration."""
        log_session_config(self.spark)

    # def update_spark_config(
    #     self,
    #     remove_configs: List[str] = None,
    #     update_configs: Dict[str, str] = None
    # ):
    #     """Update the Spark session configuration.

    #     Parameters
    #     ----------
    #     configs : Dict[str, str]
    #         A dictionary of Spark configurations to update.
    #     """
    #     # NOTE: You could theoretically update catalog configs
    #     # here, but they would not be reflected in the Local and
    #     # RemoteCatalog objects attached to the Evaluation.
    #     # For now, if you want to change the catalog configs,
    #     # you need to start a new session.
    #     remove_or_update_configs(
    #         spark=self.spark,
    #         remove_configs=remove_configs,
    #         update_configs=update_configs
    #     )
