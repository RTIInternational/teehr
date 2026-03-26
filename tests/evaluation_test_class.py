"""Extends the LocalReadWriteEvaluation class for tests."""
from typing import Union
from pathlib import Path
import re
import logging

from pyspark.sql import SparkSession
from fsspec.implementations.local import LocalFileSystem

import teehr
from teehr.evaluation.evaluation import BaseEvaluation
from teehr.models.evaluation_base import LocalCatalog
from teehr.const import LOCAL_CATALOG_DB_NAME
from teehr.utilities import apply_migrations
from teehr.utils.utils import remove_dir_if_exists

logger = logging.getLogger(__name__)


class TestEvaluation(BaseEvaluation):
    """Evaluation class for testing purposes."""

    def __init__(
        self,
        dir_path: Union[str, Path],
        namespace_name: str,
        create_dir: bool = False,
        check_evaluation_version: bool = True,
        spark: SparkSession = None
    ):
        """Initialize the Evaluation class.

        Parameters
        ----------
        dir_path : Union[str, Path]
            The directory path to use for the local catalog. This is where the
            local catalog's warehouse directory will be created. If the
            evaluation directory does not exist, it will be created if create_dir=True,
            otherwise an error will be raised.
        namespace_name : str
            The namespace name for the local catalog.
        create_dir : bool, optional
            Whether to create the directory if it does not exist.
            Default is False.
        check_evaluation_version : bool, optional
            Whether to check the evaluation version if the directory already
            exists. Default is True.
        spark : SparkSession, optional
            The SparkSession object. If not provided, a new default Spark
            session will be created.
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
            migrations_dir_path=Path(__file__).parents[1] / "src" / "teehr" / "migrations",
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