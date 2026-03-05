"""Evaluation module."""
import tempfile
from datetime import datetime
from typing import Union, Literal, List
from pathlib import Path
from teehr.evaluation.tables import (
    AttributeTable,
    ConfigurationTable,
    LocationAttributeTable,
    LocationCrosswalkTable,
    LocationTable,
    PrimaryTimeseriesTable,
    SecondaryTimeseriesTable,
    UnitTable,
    VariableTable,
    get_table,
)
from teehr.evaluation.views import (
    JoinedTimeseriesView,
    LocationAttributesView,
    PrimaryTimeseriesView,
    SecondaryTimeseriesView,
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
from teehr.evaluation.read import Read
from teehr.evaluation.load import Load
from teehr.evaluation.download import Download
from teehr.evaluation.utils import copy_migrations_dir
from teehr.evaluation.spark_session_utils import (
    create_spark_session,
    log_session_config,
)
import pandas as pd
import re
import warnings
from fsspec.implementations.local import LocalFileSystem
from teehr.utilities import apply_migrations
from teehr.models.evaluation_base import (
    EvaluationBase,
    LocalCatalog,
    RemoteCatalog
)
import teehr
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
        self.read_only_remote = False

        # Cached component instances
        self._download_instance = None

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
        # Set local warehouse path and jdbc uri
        self.spark.conf.set(
            f"spark.sql.catalog.{local_catalog_name}.warehouse",
            warehouse_dir.as_posix()
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{local_catalog_name}.uri",
            f"jdbc:sqlite:{warehouse_dir.as_posix()}/local_catalog.db"
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
        # Need to create the warehouse dir if it does not exist
        if Path(warehouse_dir).is_dir() is False:
            Path(warehouse_dir).mkdir()
        self.set_active_catalog("local")  # Creates the JDBC .db file

        # Check version of Evaluation
        if create_dir is False and check_evaluation_version is True:
            self.check_evaluation_version()

    def table(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Get a table instance by name.

        This is a factory method that returns the appropriate table class
        for the given table name. For known table names (like 'primary_timeseries'),
        returns the specialized table class. For unknown names, returns a
        generic BaseTable instance.

        Parameters
        ----------
        table_name : str
            The name of the table to access.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.

        Returns
        -------
        BaseTable
            The appropriate table instance.

        Examples
        --------
        >>> # Access a known table
        >>> ev.table("primary_timeseries").query(...)

        >>> # Access a custom/user-defined table
        >>> ev.table("my_custom_table").to_pandas()
        """
        return get_table(self, table_name, namespace_name, catalog_name)

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
    def download(self) -> Download:
        """The download component class for managing data downloads."""
        if self._download_instance is None:
            self._download_instance = Download(self)
        return self._download_instance

    @property
    def metrics(self) -> Metrics:
        """The metrics component class for calculating performance metrics.

        .. deprecated::
            The ``metrics`` property is deprecated and will be removed in a
            future version. Use the ``query`` method on the table directly
            with the ``include_metrics`` argument instead. For example::

                ev.table("joined_timeseries").query(
                    include_metrics=[...],
                    group_by=[...],
                    order_by=[...],
                )
        """
        warnings.warn(
            "The 'metrics' property is deprecated and will be removed in a "
            "future version. Use the 'query' method on the table directly "
            "with the 'include_metrics' argument instead. For example:\n\n"
            "    ev.table(\"joined_timeseries\").query(\n"
            "        include_metrics=[...],\n"
            "        group_by=[...],\n"
            "        order_by=[...],\n"
            "    )",
            FutureWarning,
            stacklevel=2,
        )
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

    def joined_timeseries_view(
        self,
        primary_filters: Union[
            str, dict, List[Union[str, dict]]
        ] = None,
        secondary_filters: Union[
            str, dict, List[Union[str, dict]]
        ] = None,
        add_attrs: bool = False,
        attr_list: List[str] = None,
    ) -> JoinedTimeseriesView:
        """Create a computed view that joins primary and secondary timeseries.

        This returns a lazy view that computes the join on-the-fly when
        accessed. The view can be filtered, transformed, and optionally
        materialized to an iceberg table via write().

        Parameters
        ----------
        primary_filters : Union[str, dict, List[...]], optional
            Filters to apply to primary timeseries before joining.
        secondary_filters : Union[str, dict, List[...]], optional
            Filters to apply to secondary timeseries before joining.
        add_attrs : bool, optional
            Whether to add location attributes. Default False.
        attr_list : List[str], optional
            Specific attributes to add (if add_attrs=True).

        Returns
        -------
        JoinedTimeseriesView
            A lazy view of the joined timeseries.

        Examples
        --------
        Create different join views:

        >>> winter = ev.joined_timeseries_view(primary_filters=["month IN (12, 1, 2)"])
        >>> summer = ev.joined_timeseries_view(primary_filters=["month IN (6, 7, 8)"])

        Use directly (computes on-the-fly):

        >>> ev.joined_timeseries_view().to_pandas()

        Chain operations:

        >>> ev.joined_timeseries_view().filter("primary_location_id LIKE 'usgs%'").to_pandas()

        Compute metrics and materialize:

        >>> ev.joined_timeseries_view().query(
        ...     include_metrics=[KGE()],
        ...     group_by=["primary_location_id"]
        ... ).write("location_kge")

        Materialize joined data:

        >>> ev.joined_timeseries_view(add_attrs=True).write("joined_timeseries")
        """
        return JoinedTimeseriesView(
            ev=self,
            primary_filters=primary_filters,
            secondary_filters=secondary_filters,
            add_attrs=add_attrs,
            attr_list=attr_list,
        )

    def location_attributes_view(
        self,
        attr_list: List[str] = None,
    ) -> LocationAttributesView:
        """Create a computed view of pivoted location attributes.

        Transforms the location_attributes table from long format
        (location_id, attribute_name, value) to wide format where
        each attribute becomes a column.

        Parameters
        ----------
        attr_list : List[str], optional
            Specific attributes to include. If None, includes all.

        Returns
        -------
        LocationAttributesView
            A lazy view of the pivoted attributes.

        Examples
        --------
        Pivot all attributes:

        >>> ev.location_attributes_view().to_pandas()

        Pivot specific attributes:

        >>> ev.location_attributes_view(
        ...     attr_list=["drainage_area", "ecoregion"]
        ... ).to_pandas()

        With filters (chained):

        >>> ev.location_attributes_view().filter(
        ...     "location_id LIKE 'usgs%'"
        ... ).to_pandas()

        Materialize for later use:

        >>> ev.location_attributes_view().write("pivoted_attrs")
        """
        return LocationAttributesView(
            ev=self,
            attr_list=attr_list,
        )

    def primary_timeseries_view(
        self,
        add_attrs: bool = False,
        attr_list: List[str] = None,
    ) -> PrimaryTimeseriesView:
        """Create a computed view of primary timeseries with optional attrs.

        Parameters
        ----------
        add_attrs : bool, optional
            Whether to add location attributes. Default False.
        attr_list : List[str], optional
            Specific attributes to add. If None and add_attrs=True, adds all.

        Returns
        -------
        PrimaryTimeseriesView
            A lazy view of the primary timeseries.

        Examples
        --------
        Basic usage:

        >>> ev.primary_timeseries_view().to_pandas()

        With filters (chained):

        >>> ev.primary_timeseries_view().filter(
        ...     "location_id LIKE 'usgs%'"
        ... ).to_pandas()

        With location attributes:

        >>> ev.primary_timeseries_view(
        ...     add_attrs=True,
        ...     attr_list=["drainage_area", "ecoregion"]
        ... ).to_pandas()
        """
        return PrimaryTimeseriesView(
            ev=self,
            add_attrs=add_attrs,
            attr_list=attr_list,
        )

    def secondary_timeseries_view(
        self,
        add_attrs: bool = False,
        attr_list: List[str] = None,
    ) -> SecondaryTimeseriesView:
        """Create a computed view of secondary timeseries with crosswalk.

        Joins secondary timeseries with location_crosswalks to add
        primary_location_id, and optionally joins location attributes.

        Parameters
        ----------
        add_attrs : bool, optional
            Whether to add location attributes. Default False.
        attr_list : List[str], optional
            Specific attributes to add. If None and add_attrs=True, adds all.

        Returns
        -------
        SecondaryTimeseriesView
            A lazy view of the secondary timeseries with primary_location_id.

        Examples
        --------
        Basic usage (adds primary_location_id via crosswalk):

        >>> ev.secondary_timeseries_view().to_pandas()

        With filters (chained):

        >>> ev.secondary_timeseries_view().filter(
        ...     "configuration_name = 'nwm30_retrospective'"
        ... ).to_pandas()

        With location attributes:

        >>> ev.secondary_timeseries_view(
        ...     add_attrs=True,
        ...     attr_list=["drainage_area", "ecoregion"]
        ... ).to_pandas()
        """
        return SecondaryTimeseriesView(
            ev=self,
            add_attrs=add_attrs,
            attr_list=attr_list,
        )

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
        evaluation directory. It also creates a version file with the latest
        version of TEEHR.

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
        # Update the version file.
        version_file = Path(local_warehouse_dir) / "version"
        with open(version_file, "w") as f:
            f.write(teehr.__version__)

    def clone_from_s3(
        self,
        remote_catalog_name: str = None,
        remote_namespace_name: str = None,
        primary_location_ids: List[str] = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        clone_template: bool = True,
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
        clone_template : bool, optional
            Whether to clone the template to the local warehouse
            before pulling data from remote. The default is True.
            If a local warehouse already exists, an error will be raised, so
            set this to False in that case. Data will be written to the existing
            local warehouse using 'upsert' mode.
        """
        # You must configure the catalogs when initializing the Evaluation.
        if self.local_catalog.warehouse_dir is None:
            raise ValueError("The 'local_warehouse_dir' must be specified.")
        if remote_catalog_name is None:
            remote_catalog_name = self.remote_catalog.catalog_name
        if remote_namespace_name is None:
            remote_namespace_name = self.remote_catalog.namespace_name

        if clone_template is True:
            logger.info("Cloning template to local warehouse.")
            # This will raise an error if it already exists.
            self.clone_template()

        # Now pull down the data from remote, applying any filtering, and
        # write to the local template.
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

    def check_evaluation_version(self, warehouse_dir: Union[str, Path] = None) -> None:
        """Check the version of the TEEHR Evaluation.

        Parameters
        ----------
        warehouse_dir : Union[str, Path], optional
            Path to the warehouse directory containing the version file.
            If None, uses the active catalog's warehouse directory.

        Raises
        ------
        FileNotFoundError
            If the version file does not exist in the warehouse directory.
        ValueError
            If the version format in the file is invalid.
        """
        if warehouse_dir is None:
            warehouse_dir = self.active_catalog.warehouse_dir
        fs = LocalFileSystem()

        if fs.exists(warehouse_dir):
            # This is a v0.6+ evaluation, check for version file in warehouse dir:
            version_dir = warehouse_dir
        else:
            # This is a pre-v0.6 evaluation, check for version file in eval dir:
            version_dir = self.dir_path

        version_file = Path(version_dir, "version")
        if not fs.exists(version_file):
            err_msg = (
                f"No version file was found in {version_dir}."
                " Please first upgrade to v0.5 or create a text file named 'version'"
                f" in {version_dir} with the version number (e.g., '0.5.0')."
            )
            logger.error(err_msg)
            raise Exception(err_msg)
        else:
            with fs.open(version_file) as f:
                version_txt = str(f.read().strip())
            match = re.findall(r'(\d+\.\d+\.\d+)', version_txt)  # Assumes semantic versioning
            if len(match) != 1:
                err_msg = f"Invalid version format in {version_dir}: {version_txt}"
                logger.error(err_msg)
                raise ValueError(err_msg)
            else:
                version = match[0]
            if version < "0.6.0":
                err_msg = (
                    f"Evaluation version {version} in {version_dir} is less than 0.6."
                    " Please run the migration script to upgrade to this Evaluation to v0.6."
                    " To run the conversion to v0.6, import the function using: "
                    "'from teehr.utilities.convert_to_iceberg import convert_evaluation'"
                    f" and then call: 'convert_evaluation(\"{self.dir_path.as_posix()}\")'"
                )
                logger.error(err_msg)
                raise ValueError(err_msg)
            else:
                logger.info(f"Evaluation version {version} in {version_dir} is valid.")

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


class RemoteReadOnlyEvaluation(Evaluation):
    """A read-only Evaluation class for accessing remote catalogs.

    This class provides a convenient way to access a remote TEEHR catalog
    without needing to manage local directories. It automatically creates
    a temporary directory and sets the active catalog to remote.

    Note: This is intended for read-only access to remote data. Write
    operations to the remote catalog are not supported through this class.

    Currently only users in the TEEHR-Hub environment have access to
    the remote catalog, so this class is intended for use within that environment,
    until remote access is more broadly available.
    """

    def __init__(
        self,
        spark: SparkSession = None,
        temp_dir_path: Union[str, Path] = None,
    ):
        """
        Initialize the RemoteReadOnlyEvaluation class.

        Parameters
        ----------
        spark : SparkSession, optional
            The SparkSession object. If not provided, a new default
            Spark session will be created.
        temp_dir_path : Union[str, Path], optional
            The directory path to use for the temporary local catalog.
            If not provided, a temporary directory will be created in the default location.
            If it does not exist, it will be created.
        """
        # Create a temporary directory for the local catalog
        if temp_dir_path is not None:
            temp_dir_path = Path(temp_dir_path)
            if not temp_dir_path.is_dir():
                logger.info(f"Creating base directory {temp_dir_path} for temporary local catalog.")
                temp_dir_path.mkdir(parents=True, exist_ok=True)
            self._temp_dir = tempfile.TemporaryDirectory(dir=temp_dir_path.as_posix())
        else:
            self._temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self._temp_dir.name)

        # Initialize the parent Evaluation class
        super().__init__(
            dir_path=temp_path,
            create_dir=False,
            check_evaluation_version=False,
            spark=spark
        )
        # Check the configuration for remote catalog access
        if self.remote_catalog.catalog_uri == "" or self.remote_catalog.warehouse_dir == "":
            raise ValueError(
                "Currently you must be in the TEEHR-Hub environment to use the "
                "RemoteReadOnlyEvaluation and RemoteReadWriteEvaluation classes. "
                "When working locally, you can access data in the TEEHR-Cloud data warehouse "
                "by using the standard Evaluation class and the ev.download methods."
            )
        # Set the active catalog to remote
        self.set_active_catalog("remote")
        self.read_only_remote = True

    def __del__(self):
        """Clean up the temporary directory when the object is deleted."""
        if hasattr(self, '_temp_dir') and self._temp_dir is not None:
            try:
                self._temp_dir.cleanup()
            except Exception as e:
                logger.warning(f"Error cleaning up temporary directory: {e}")
                pass  # Ignore cleanup errors during garbage collection


class RemoteReadWriteEvaluation(Evaluation):
    """A read-write Evaluation class for access to remote catalogs.

    This class provides a convenient way to access a remote TEEHR catalog
    without needing to manage local directories. It automatically creates
    a temporary directory and sets the active catalog to remote.

    Note: This is intended for read-write access to remote data. Write
    operations to the remote catalog are supported through this class, however
    an AWS profile with write permissions is required in the Spark session.

    Currently only users in the TEEHR-Hub environment have access to
    the remote catalog, so this class is intended for use within that environment,
    until remote access is more broadly available.
    """

    def __init__(
        self,
        spark: SparkSession = None,
        temp_dir_path: Union[str, Path] = None,
    ):
        """
        Initialize the RemoteReadWriteEvaluation class.

        Parameters
        ----------
        spark : SparkSession, optional
            The SparkSession object. If not provided, a new default
            Spark session will be created.
        temp_dir_path : Union[str, Path], optional
            The directory path to use for the temporary local catalog.
            If not provided, a temporary directory will be created in the default location.
            If it does not exist, it will be created.
        """
        # Create a temporary directory for the local catalog
        if temp_dir_path is not None:
            temp_dir_path = Path(temp_dir_path)
            if not temp_dir_path.is_dir():
                logger.info(f"Creating base directory {temp_dir_path} for temporary local catalog.")
                temp_dir_path.mkdir(parents=True, exist_ok=True)
            self._temp_dir = tempfile.TemporaryDirectory(dir=temp_dir_path.as_posix())
        else:
            self._temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self._temp_dir.name)

        # Initialize the parent Evaluation class
        super().__init__(
            dir_path=temp_path,
            create_dir=False,
            check_evaluation_version=False,
            spark=spark
        )
        # Check the configuration for remote catalog access
        if self.remote_catalog.catalog_uri == "" or self.remote_catalog.warehouse_dir == "":
            raise ValueError(
                "Currently you must be in the TEEHR-Hub environment to use the "
                "RemoteReadOnlyEvaluation and RemoteReadWriteEvaluation classes. "
                "When working locally, you can access data in the TEEHR-Cloud data warehouse "
                "by using the standard Evaluation class and the ev.download methods."
            )
        # Set the active catalog to remote
        self.set_active_catalog("remote")
        self.read_only_remote = False

    def __del__(self):
        """Clean up the temporary directory when the object is deleted."""
        if hasattr(self, '_temp_dir') and self._temp_dir is not None:
            try:
                self._temp_dir.cleanup()
            except Exception as e:
                logger.warning(f"Error cleaning up temporary directory: {e}")
                pass  # Ignore cleanup errors during garbage collection