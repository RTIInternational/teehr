"""Convert existing Evaluation data to Iceberg format for v0.6 and beyond."""  # noqa: E501
from typing import Union
from pathlib import Path
import logging

import teehr
from teehr.evaluation.utils import create_spark_session, copy_schema_dir
from teehr.utilities import apply_migrations
from teehr.utils.s3path import S3Path


logger = logging.getLogger(__name__)


def convert_evaluation(
    dir_path: Union[str, Path, S3Path],
    warehouse_path: Union[str, Path, S3Path] = None,
    catalog_name: str = "local",
    catalog_dir_path: Union[str, Path, S3Path] = None,
    schema_name: str = "db"
):
    """Convert v0.5.0 TEEHR Evaluation to v0.6 Iceberg.

    Parameters
    ----------
    dir_path : Union[str, Path, S3Path]
        The directory path to the Evaluation to upgrade.
    warehouse_path : Union[str, Path, S3Path]
        The path to the Iceberg warehouse.
    catalog_name : str, optional
        The name of the Iceberg catalog (default is "local").
    catalog_dir_path : Union[str, Path, S3Path], optional
        The directory path where the catalog schema versions are stored
        (default is None, in which case the schema(s) from the template
        Evaluation are used).
    schema_name : str, optional
        The name of the Iceberg schema (default is "db").
    """
    if warehouse_path is None:
        warehouse_path = Path(dir_path) / "warehouse"

    if catalog_dir_path is None:
        teehr_root = Path(__file__).parent.parent
        catalog_dir_path = Path(teehr_root, "template")

    spark = create_spark_session(
        warehouse_path=warehouse_path,
        catalog_name=catalog_name
    )
    copy_schema_dir(
        target_dir=dir_path
    )
    apply_migrations.evolve_catalog_schema(
        spark=spark,
        catalog_dir_path=dir_path,
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    logger.info(f"Schema evolution completed for {catalog_name}.")

    # Now you have to copy over the data!
    ev = teehr.Evaluation(
        dir_path=dir_path,
        spark=spark,
        check_evaluation_version=False
    )

    units_sdf = ev.units.to_sdf()
    units_sdf.writeTo("local.db.units").append()

    configuration_sdf = ev.configurations.to_sdf()
    configuration_sdf.writeTo("local.db.configurations").append()

    variables_sdf = ev.variables.to_sdf()
    variables_sdf.writeTo("local.db.variables").append()

    attributes_sdf = ev.attributes.to_sdf()
    attributes_sdf.writeTo("local.db.attributes").append()

    locations_sdf = ev.locations.to_sdf()
    locations_sdf.writeTo("local.db.locations").append()

    location_attrs_sdf = ev.location_attributes.to_sdf()
    location_attrs_sdf.writeTo("local.db.location_attributes").append()

    location_attrs_sdf = ev.primary_timeseries.to_sdf()
    location_attrs_sdf.writeTo("local.db.primary_timeseries").append()

    secondary_timeseries_sdf = ev.secondary_timeseries.to_sdf()
    secondary_timeseries_sdf.writeTo("local.db.secondary_timeseries").append()

    location_crosswalk_sdf = ev.location_crosswalks.to_sdf()
    location_crosswalk_sdf.writeTo("local.db.location_crosswalks").append()
    logger.info(f"Local warehouse created in {dir_path}/warehouse.")

    # Update the version file.
    version_file = Path(dir_path) / "version"
    with open(version_file, "w") as f:
        f.write(teehr.__version__)

    logger.info(
        f"Updated evaluation version file at {version_file} to"
        f" {teehr.__version__}."
    )