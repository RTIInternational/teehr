"""Evaluation module."""
import tempfile
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Union, List
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
from teehr.const import (
    LOCAL_NAMESPACE_NAME,
    LOCAL_CATALOG_DB_NAME,
    CACHE_DIR,
    REMOTE_CATALOG_REST_URI,
    REMOTE_WAREHOUSE_S3_PATH,
    REMOTE_NAMESPACE_NAME
)
from teehr.utils.utils import remove_dir_if_exists
from pyspark.sql import SparkSession
import logging
from teehr.evaluation.fetch import Fetch
from teehr.evaluation.metrics import Metrics
from teehr.evaluation.generate import GeneratedTimeseries
from teehr.evaluation.write import Write
from teehr.evaluation.extract import Extract
from teehr.evaluation.validate import Validate
from teehr.evaluation.read import Read
from teehr.evaluation.load import Load
from teehr.evaluation.download import Download
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
    EvaluationBaseModel,
    LocalCatalog,
    RemoteCatalog
)
import teehr


logger = logging.getLogger(__name__)


class BaseEvaluation(EvaluationBaseModel, ABC):
    """Abstract base class for TEEHR Evaluations.

    This is the main class for the TEEHR evaluation. Subclasses must implement
    the `catalog` property to provide their specific catalog type.
    """

    def __init__(
        self,
        dir_path: Union[str, Path],
        spark: SparkSession = None
    ):
        """
        Initialize the Evaluation class.

        Parameters
        ----------
        dir_path : Union[str, Path]
            The directory path to use for the local catalog.
        spark : SparkSession, optional
            The SparkSession object, by default None
        """
        self.read_only_remote = True
        self.dir_path = Path(dir_path)
        self.cache_dir = None

        # Initialize Spark session
        if spark is not None:
            logger.info("Using provided Spark session.")
            self.spark = spark
        else:
            logger.info("Creating a new default Spark session.")
            self.spark = create_spark_session()

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
        >>> ev.table("primary_timeseries").aggregate(...)

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

    @cached_property
    def download(self) -> Download:
        """The download component class for managing data downloads."""
        return Download(self)

    @property
    def metrics(self) -> Metrics:
        """The metrics component class for calculating performance metrics.

        .. deprecated:: 0.6.0
            The ``metrics`` property is deprecated and will be removed in a
            future version. Use the ``aggregate`` method on the table directly
            with the ``metrics`` argument instead. For example:

            .. code-block:: python

                ev.table("joined_timeseries").aggregate(
                    metrics=[...],
                    group_by=[...]
                )
        """
        warnings.warn(
            "The 'metrics' property is deprecated and will be removed in a "
            "future version. Use the 'aggregate' method on the table directly "
            "with the 'metrics' argument instead. For example:\n\n"
            "    ev.table(\"joined_timeseries\").aggregate(\n"
            "        metrics=[...],\n"
            "        group_by=[...],\n"
            "    ).order_by([...])\n\n",
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
        catalog_name: Union[str, None] = None,
        namespace_name: Union[str, None] = None,
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
        catalog_name : Union[str, None], optional
            The catalog containing the source tables. If None, uses the
            active catalog.
        namespace_name : Union[str, None], optional
            The namespace containing the source tables. If None, uses the
            active catalog's namespace.

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

        >>> ev.joined_timeseries_view().aggregate(
        ...     metrics=[KGE()],
        ...     group_by=["primary_location_id"]
        ... ).write("location_kge")

        Materialize joined data:

        >>> ev.joined_timeseries_view(add_attrs=True).write("joined_timeseries")

        Read from a remote catalog and namespace:

        >>> ev.joined_timeseries_view(
        ...     catalog_name="some_catalog",
        ...     namespace_name="some_namespace"
        ... ).aggregate(
        ...     metrics=[KGE()],
        ...     group_by=["primary_location_id"]
        ... ).write("location_kge")
        """
        return JoinedTimeseriesView(
            ev=self,
            primary_filters=primary_filters,
            secondary_filters=secondary_filters,
            add_attrs=add_attrs,
            attr_list=attr_list,
            catalog_name=catalog_name,
            namespace_name=namespace_name,
        )

    def location_attributes_view(
        self,
        attr_list: List[str] = None,
        catalog_name: Union[str, None] = None,
        namespace_name: Union[str, None] = None,
    ) -> LocationAttributesView:
        """Create a computed view of pivoted location attributes.

        Transforms the location_attributes table from long format
        (location_id, attribute_name, value) to wide format where
        each attribute becomes a column.

        Parameters
        ----------
        attr_list : List[str], optional
            Specific attributes to include. If None, includes all.
        catalog_name : Union[str, None], optional
            The catalog containing the source tables. If None, uses the
            active catalog.
        namespace_name : Union[str, None], optional
            The namespace containing the source tables. If None, uses the
            active catalog's namespace.

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
            catalog_name=catalog_name,
            namespace_name=namespace_name,
        )

    def primary_timeseries_view(
        self,
        add_attrs: bool = False,
        attr_list: List[str] = None,
        catalog_name: Union[str, None] = None,
        namespace_name: Union[str, None] = None,
    ) -> PrimaryTimeseriesView:
        """Create a computed view of primary timeseries with optional attrs.

        Parameters
        ----------
        add_attrs : bool, optional
            Whether to add location attributes. Default False.
        attr_list : List[str], optional
            Specific attributes to add. If None and add_attrs=True, adds all.
        catalog_name : Union[str, None], optional
            The catalog containing the source tables. If None, uses the
            active catalog.
        namespace_name : Union[str, None], optional
            The namespace containing the source tables. If None, uses the
            active catalog's namespace.

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
            catalog_name=catalog_name,
            namespace_name=namespace_name,
        )

    def secondary_timeseries_view(
        self,
        add_attrs: bool = False,
        attr_list: List[str] = None,
        catalog_name: Union[str, None] = None,
        namespace_name: Union[str, None] = None,
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
        catalog_name : Union[str, None], optional
            The catalog containing the source tables. If None, uses the
            active catalog.
        namespace_name : Union[str, None], optional
            The namespace containing the source tables. If None, uses the
            active catalog's namespace.

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
            catalog_name=catalog_name,
            namespace_name=namespace_name,
        )

    @property
    @abstractmethod
    def catalog(self):
        """The catalog for this evaluation.

        Subclasses must implement this property to return their specific
        catalog type (LocalCatalog or RemoteCatalog).

        Returns
        -------
        LocalCatalog or RemoteCatalog
            The catalog configuration for this evaluation.
        """
        pass

    @property
    def active_catalog(self):
        """Alias for catalog property (backwards compatibility).

        .. deprecated::
            Use ``catalog`` property instead. This alias will be removed
            in a future version.

        Returns
        -------
        LocalCatalog or RemoteCatalog
            The catalog configuration for this evaluation.
        """
        return self.catalog

    def _activate_catalog(self):
        """Activate this evaluation's catalog in Spark.

        Sets the Spark session's current catalog to this evaluation's catalog.
        Called automatically during initialization.
        """
        self.spark.catalog.setCurrentCatalog(self.catalog.catalog_name)
        logger.info(f"Active catalog set to {self.catalog.catalog_name}.")

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

    def list_tables(
        self,
        catalog_name: str = None,
        namespace: str = None
    ) -> pd.DataFrame:
        """List the tables in the catalog returning a Pandas DataFrame.

        Parameters
        ----------
        catalog_name : str, optional
            The catalog name to list tables from, by default None,
             which means the catalog_name of the active catalog is used.
        namespace : str, optional
            The namespace name to list tables from, by default None,
             which means the namespace_name of the active catalog is used.
        """
        if catalog_name is None:
            catalog_name = self.active_catalog.catalog_name
        if namespace is None:
            namespace = self.active_catalog.namespace_name
        tbl_list = self.spark.catalog.listTables(
            f"{catalog_name}.{namespace}"
        )
        metadata = []
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

    def list_namespaces(self, catalog_name: str = None) -> pd.DataFrame:
        """List the namespaces in the catalog returning a Pandas DataFrame.

        Parameters
        ----------
        catalog_name : str, optional
            The catalog name to list namespaces from, by default None,
             which means the catalog_name of the active catalog is used.
        """
        if catalog_name is None:
            catalog_name = self.active_catalog.catalog_name
        ns_list = self.spark.sql("SHOW NAMESPACES").toPandas()
        logger.info(f"Namespaces in catalog '{catalog_name}': {ns_list['namespace'].tolist()}")
        return ns_list

    def drop_table(
        self,
        table_name: str,
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None
    ):
        """Drop a user-created table from the catalog.

        Only non-core tables (user-created tables, materialized views, saved
        query results) can be dropped. Attempting to drop a core table
        (e.g., primary_timeseries, locations, units) will raise a ValueError.

        Parameters
        ----------
        table_name : str
            The name of the table to drop.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.

        Raises
        ------
        ValueError
            If the table is a core TEEHR table.

        Examples
        --------
        Write and then drop a user-created table:

        >>> ev.joined_timeseries_view().write("my_results")
        >>> ev.drop_table("my_results")
        """
        tbl = self.table(table_name, namespace_name, catalog_name)
        tbl.drop()

    def list_views(self) -> pd.DataFrame:
        """List the views in the catalog returning a Pandas DataFrame."""
        return self.spark.sql("SHOW VIEWS").toPandas()

    def log_spark_config(self):
        """Log the current Spark session configuration."""
        log_session_config(self.spark)

    def sql(self, query: str):
        """Execute a SQL query using the active catalog and namespace.

        This is a thin wrapper around ``spark.sql()`` that automatically sets
        the active catalog and namespace so the user does not have to qualify
        table names in their queries.

        Parameters
        ----------
        query : str
            The SQL query to execute. Table names can be unqualified (e.g.
            ``primary_timeseries``) or partially qualified (e.g.
            ``teehr.primary_timeseries``).  The active catalog
            (``ev.active_catalog.catalog_name``) and active namespace
            (``ev.active_catalog.namespace_name``) are set automatically
            before the query runs.

        Returns
        -------
        pyspark.sql.DataFrame
            The result of the SQL query as a Spark DataFrame.

        Examples
        --------
        Query a table without specifying the catalog or namespace:

        >>> df = ev.sql("SELECT * FROM primary_timeseries LIMIT 10")
        >>> df.toPandas()

        Use aggregate functions:

        >>> df = ev.sql(
        ...     "SELECT location_id, COUNT(*) as n "
        ...     "FROM primary_timeseries GROUP BY location_id"
        ... )
        """
        catalog_name = self.active_catalog.catalog_name
        namespace_name = self.active_catalog.namespace_name
        self.spark.sql(f"USE {catalog_name}")
        self.spark.sql(f"USE SCHEMA {namespace_name}")
        return self.spark.sql(query)


class LocalReadWriteEvaluation(BaseEvaluation):
    """A read-write Evaluation class for accessing local catalogs.

    This class establishes a local catalog in the specified directory
    and creates the tables according to the TEEHR schema.

    It is intended for use when working locally and when you want to
    manage your own local copy of the data.

    Examples
    --------
    Create a new evaluation with a new directory:

    >>> ev = LocalReadWriteEvaluation(dir_path="path/to/evaluation_dir", create_dir=True)

    Create an evaluation using an existing directory:

    >>> ev = LocalReadWriteEvaluation(dir_path="path/to/existing_evaluation_dir")

    Access tables and views:

    >>> ev.primary_timeseries.aggregate(...).to_pandas()
    >>> ev.joined_timeseries_view(...).aggregate(...).to_pandas()

    """

    def __init__(
        self,
        dir_path: Union[str, Path],
        create_dir: bool = False,
        check_evaluation_version: bool = True,
        spark: SparkSession = None,
        namespace_name: str = LOCAL_NAMESPACE_NAME
    ):
        """Initialize the Evaluation class.

        Parameters
        ----------
        dir_path : Union[str, Path]
            The directory path to use for the local catalog. This is where the
            local catalog's warehouse directory will be created. If the
            evaluation directory does not exist, it will be created if create_dir=True,
            otherwise an error will be raised.
        create_dir : bool, optional
            Whether to create the directory if it does not exist.
            Default is False.
        check_evaluation_version : bool, optional
            Whether to check the evaluation version if the directory already
            exists. Default is True.
        spark : SparkSession, optional
            The SparkSession object. If not provided, a new default Spark
            session will be created.
        namespace_name : str, optional
            The namespace name to use for the local catalog. Default is LOCAL_NAMESPACE_NAME ("teehr").
        """
        super().__init__(
            spark=spark,
            dir_path=Path(dir_path)
        )

        local_catalog_name = self.spark.conf.get("local_catalog_name")
        warehouse_dir = self.dir_path / local_catalog_name

        if not self.dir_path.is_dir():
            if create_dir:
                logger.info(f"Creating directory {self.dir_path}.")
                self.dir_path.mkdir(parents=True, exist_ok=True)
            else:
                logger.error(
                    f"Directory {self.dir_path} does not exist."
                    " Set create_dir=True to create it."
                )
                raise NotADirectoryError(
                    f"Directory {self.dir_path} does not exist."
                )
        else:
            logger.info(f"Using existing directory {self.dir_path}.")

        # Set local warehouse path and jdbc uri
        self.spark.conf.set(
            f"spark.sql.catalog.{local_catalog_name}.warehouse",
            warehouse_dir.as_posix()
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{local_catalog_name}.uri",
            f"jdbc:sqlite:{warehouse_dir.as_posix()}/{LOCAL_CATALOG_DB_NAME}"
        )
        # Get the catalog metadata that was set during Spark configuration
        self._catalog = LocalCatalog(
            warehouse_dir=warehouse_dir,
            catalog_name=local_catalog_name,
            namespace_name=namespace_name,
            catalog_type=self.spark.conf.get("local_catalog_type"),
        )
        self.cache_dir = self._catalog.cache_dir

        if create_dir is False and check_evaluation_version is True:
            self.check_evaluation_version(warehouse_dir=warehouse_dir)

        # Need to create the warehouse dir if it does not exist,
        # along with the cache dir.
        if warehouse_dir.is_dir() is False:
            self._catalog.cache_dir.mkdir(parents=True, exist_ok=True)

            version_file = Path(warehouse_dir) / "version"
            with open(version_file, "w") as f:
                f.write(teehr.__version__)

        apply_migrations.evolve_catalog_schema(
            spark=self.spark,
            migrations_dir_path=Path(__file__).parents[1] / "migrations",
            target_catalog_name=self._catalog.catalog_name,
            target_namespace_name=self._catalog.namespace_name
        )

        self._activate_catalog()  # Creates the JDBC .db file

    @property
    def catalog(self):
        """The local catalog for this evaluation.

        Returns
        -------
        LocalCatalog
            The local catalog configuration.
        """
        return self._catalog

    @property
    def local_catalog(self):
        """Alias for catalog property (backwards compatibility).

        .. deprecated::
            Use ``catalog`` property instead.

        Returns
        -------
        LocalCatalog
            The local catalog configuration.
        """
        return self._catalog

    def clean_cache(self):
        """Clean temporary files.

        Includes removing temporary files.
        """
        logger.info(
            f"Removing temporary files from {self.catalog.cache_dir}"
        )
        remove_dir_if_exists(self.catalog.cache_dir)
        self.catalog.cache_dir.mkdir()

    def check_evaluation_version(
        self,
        warehouse_dir: Union[str, Path] = None
    ) -> None:
        """Check the version of the TEEHR Evaluation.

        Parameters
        ----------
        warehouse_dir : Union[str, Path], optional
            Path to the warehouse directory containing the version file.
            If None, uses the local catalog's warehouse directory.

        Raises
        ------
        FileNotFoundError
            If the version file does not exist in the warehouse directory.
        ValueError
            If the version format in the file is invalid.
        """
        if warehouse_dir is None:
            warehouse_dir = self._catalog.warehouse_dir
        else:
            warehouse_dir = Path(warehouse_dir)

        if warehouse_dir.is_dir():
            # This is a v0.6+ evaluation,
            # check for version file in warehouse dir:
            version_dir = warehouse_dir
        else:
            # This is a pre-v0.6 evaluation,
            # check for version file in evaluation dir:
            version_dir = self.dir_path

        fs = LocalFileSystem()
        version_file = Path(version_dir, "version")
        if not fs.exists(version_file):
            err_msg = (
                f"No version file was found in {version_dir}."
                " Please first upgrade to v0.5 or create a text file named"
                f" 'version' in {version_dir} with the version number (e.g., '0.5.0')."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)
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

    def create_namespace(
        self,
        namespace_name: str,
        make_active: bool = True
    ):
        """Create a new namespace in the local catalog.

        Parameters
        ----------
        namespace_name : str
            The name of the namespace to create.
        make_active : bool, optional
            Whether to set the new namespace as the active namespace after creation.
            If True, subsequent queries will use the new namespace by default.
            Default is True.

        Examples
        --------
        >>> ev.create_namespace("my_namespace")

        Namespace name can be specified when accessing tables and views:
        >>> ev.table("primary_timeseries", namespace_name="my_namespace").to_pandas()
        """
        apply_migrations.evolve_catalog_schema(
            spark=self.spark,
            migrations_dir_path=Path(__file__).parents[1] / "migrations",
            target_catalog_name=self._catalog.catalog_name,
            target_namespace_name=namespace_name
        )
        logger.info(f"Namespace '{namespace_name}' created in catalog '{self._catalog.catalog_name}'.")
        if make_active:
            self._catalog = LocalCatalog(
                warehouse_dir=self._catalog.warehouse_dir,
                catalog_name=self._catalog.catalog_name,
                namespace_name=namespace_name,
                catalog_type=self.spark.conf.get("local_catalog_type"),
            )
            logger.info(f"Namespace '{namespace_name}' set as active namespace.")

    def set_active_namespace(self, namespace_name: str):
        """Set the active namespace for this evaluation.

        Parameters
        ----------
        namespace_name : str
            The name of the namespace to set as active. This namespace must already exist in the catalog.
            Access to tables and views will use this namespace by default after calling this method.

        Examples
        --------
        >>> ev.set_active_namespace("my_namespace")
        """
        available_namespaces = self.spark.sql("SHOW NAMESPACES").toPandas()["namespace"].tolist()
        if namespace_name not in available_namespaces:
            raise ValueError(
                f"Namespace '{namespace_name}' does not exist in catalog '{self._catalog.catalog_name}'."
            )
        self._catalog = LocalCatalog(
            warehouse_dir=self._catalog.warehouse_dir,
            catalog_name=self._catalog.catalog_name,
            namespace_name=namespace_name,
            catalog_type=self.spark.conf.get("local_catalog_type"),
        )
        logger.info(f"Namespace '{namespace_name}' set as active namespace.")


class Evaluation(LocalReadWriteEvaluation):
    """A read-write Evaluation class for accessing local catalogs.

    This class establishes a local catalog in the specified directory
    and creates the tables according to the TEEHR schema.

    It is intended for use when working locally and when you want to
    manage your own local copy of the data.

    .. deprecated:: 0.6.0
        This class is provided for convenience and backwards compatibility.
        It will be deprecated in favor of the more explicit
        :class:`LocalReadWriteEvaluation` class.
    """

    def __init__(
        self,
        dir_path: Union[str, Path],
        create_dir: bool = False,
        check_evaluation_version: bool = True,
        spark: SparkSession = None,
        namespace_name: str = LOCAL_NAMESPACE_NAME
    ):
        """Initialize the Evaluation class.

        Parameters
        ----------
        dir_path : Union[str, Path]
            The directory path to use for the local catalog. This is where the
            local catalog's warehouse directory will be created. If the
            evaluation directory does not exist, it will be created if create_dir=True,
            otherwise an error will be raised.
        create_dir : bool, optional
            Whether to create the directory if it does not exist.
            Default is False.
        check_evaluation_version : bool, optional
            Whether to check the evaluation version if the directory already
            exists. Default is True.
        spark : SparkSession, optional
            The SparkSession object. If not provided, a new default Spark
            session will be created.
        namespace_name : str, optional
            The namespace name to use for the local catalog. Default is LOCAL_NAMESPACE_NAME ("teehr").
        """
        super().__init__(
            spark=spark,
            dir_path=dir_path,
            create_dir=create_dir,
            check_evaluation_version=check_evaluation_version,
            namespace_name=namespace_name
        )


class RemoteReadOnlyEvaluation(BaseEvaluation):
    """A read-only Evaluation class for accessing remote catalogs.

    This class provides a convenient way to access a remote TEEHR catalog
    without needing to manage local directories. It automatically creates
    a temporary directory and sets the active catalog to remote.

    Note: This is intended for read-only access to remote data. Write
    operations to the remote catalog are not supported through this class.

    Currently only users in the TEEHR-Hub environment have access to
    the remote catalog, so this class is intended for use within that
    environment until remote access is more broadly available.
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
            If not provided, a temporary directory will be created in the
            default location. If it does not exist, it will be created.
        """
        # Create a temporary directory for the cache.
        if temp_dir_path is not None:
            temp_dir_path = Path(temp_dir_path)
            if not temp_dir_path.is_dir():
                logger.info(f"Creating base directory {temp_dir_path} for temporary caching.")
                temp_dir_path.mkdir(parents=True, exist_ok=True)
            self._temp_dir = tempfile.TemporaryDirectory(dir=temp_dir_path.as_posix())
        else:
            self._temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self._temp_dir.name)

        # Initialize the parent Evaluation class
        super().__init__(
            spark=spark,
            dir_path=temp_path
        )

        # Set up cache directory for temporary files
        local_catalog_name = self.spark.conf.get("local_catalog_name")
        cache_dir = temp_path / local_catalog_name / CACHE_DIR
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = cache_dir

        # Check the configuration for remote catalog access
        if REMOTE_CATALOG_REST_URI == "" or REMOTE_WAREHOUSE_S3_PATH == "":
            raise ValueError(
                "Currently you must be in the TEEHR-Hub environment to use the "
                "RemoteReadOnlyEvaluation and RemoteReadWriteEvaluation classes. "
                "When working locally, you can access data in the TEEHR-Cloud data warehouse "
                "by using the standard Evaluation class and the ev.download methods."
            )
        # Set up the remote catalog
        self._catalog = RemoteCatalog(
            warehouse_dir=self.spark.conf.get("remote_warehouse_dir"),
            catalog_name=self.spark.conf.get("remote_catalog_name"),
            namespace_name=REMOTE_NAMESPACE_NAME,
            catalog_type=self.spark.conf.get("remote_catalog_type"),
            catalog_uri=self.spark.conf.get("remote_catalog_uri"),
        )
        self._activate_catalog()

    @property
    def catalog(self):
        """The remote catalog for this evaluation.

        Returns
        -------
        RemoteCatalog
            The remote catalog configuration.
        """
        return self._catalog

    @property
    def remote_catalog(self):
        """Alias for catalog property (backwards compatibility).

        .. deprecated::
            Use ``catalog`` property instead.

        Returns
        -------
        RemoteCatalog
            The remote catalog configuration.
        """
        return self._catalog

    def __del__(self):
        """Clean up the temporary directory when the object is deleted."""
        if hasattr(self, '_temp_dir') and self._temp_dir is not None:
            try:
                self._temp_dir.cleanup()
            except Exception as e:
                logger.warning(f"Error cleaning up temporary directory: {e}")
                pass  # Ignore cleanup errors during garbage collection


class RemoteReadWriteEvaluation(RemoteReadOnlyEvaluation):
    """A read-write Evaluation class for access to remote catalogs.

    This class provides a convenient way to access a remote TEEHR catalog
    without needing to manage local directories. It automatically creates
    a temporary directory and sets the active catalog to remote.

    Note: This is intended for read-write access to remote data. Write
    operations to the remote catalog are supported through this class, however
    an AWS profile with write permissions is required in the Spark session.

    Currently only users in the TEEHR-Hub environment have access to
    the remote catalog, so this class is intended for use within that
    environment until remote access is more broadly available.
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
            If not provided, a temporary directory will be created in the
            default location. If it does not exist, it will be created.
        """
        super().__init__(
            spark=spark,
            temp_dir_path=temp_dir_path
        )
        self.read_only_remote = False
