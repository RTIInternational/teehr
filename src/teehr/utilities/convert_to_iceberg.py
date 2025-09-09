"""Convert existing Evaluation data to Iceberg format for v0.6 and beyond."""  # noqa: E501
from typing import Union
from pathlib import Path
from teehr.utils.s3path import S3Path
import logging

from teehr.evaluation.utils import create_spark_session
from teehr.utilities import apply_migrations


logger = logging.getLogger(__name__)


def convert_evaluation(
    dir_path: Union[str, Path, S3Path],
    warehouse_path: Union[str, Path, S3Path],
    catalog_name: str = "local",
    schema_name: str = "db"
):
    """Convert v0.5.0 TEEHR Evaluation to v0.6 Iceberg.

    Parameters
    ----------
    dir_path : Union[str, Path, S3Path]
        The directory path to the v0.5 Evaluation.
    warehouse_path : Union[str, Path, S3Path]
        The path to the Iceberg warehouse.
    catalog_name : str, optional
        The name of the Iceberg catalog (default is "local").
    schema_name : str, optional
        The name of the Iceberg schema (default is "db").
    """
    if warehouse_path is None:
        warehouse_path = Path(dir_path) / "warehouse"

    spark = create_spark_session(
        warehouse_path=warehouse_path,
        catalog_name=catalog_name
    )
    apply_migrations.evolve_catalog_schema(
        spark,
        catalog_name,
        schema_name
    )
    logger.info(f"Schema evolution completed for {catalog_name}.")


if __name__ == "__main__":
    convert_evaluation(
        dir_path="playground/iceberg/evaluation_conversion/local-eval-warehouse",
        warehouse_path=None,
        catalog_name="local",
        schema_name="db"
    )