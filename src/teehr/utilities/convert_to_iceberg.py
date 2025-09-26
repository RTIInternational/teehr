"""Convert existing Evaluation data to Iceberg format for v0.6 and beyond."""  # noqa: E501
from typing import Union
from pathlib import Path
import logging
import argparse

import teehr
from teehr.evaluation.utils import create_spark_session, copy_schema_dir
from teehr.utilities import apply_migrations
from teehr.utils.s3path import S3Path


logger = logging.getLogger(__name__)


def convert_evaluation(
    dir_path: Union[str, Path, S3Path],
    warehouse_path: Union[str, Path, S3Path] = None,
    catalog_name: str = "local",
    migrations_path: Union[str, Path, S3Path] = None,
    schema_name: str = "db"
):
    """Convert TEEHR Evaluation to v0.6 Iceberg.

    Parameters
    ----------
    dir_path : Union[str, Path, S3Path]
        The directory path to the Evaluation to upgrade.
    warehouse_path : Union[str, Path, S3Path]
        The path to the Iceberg warehouse.
    catalog_name : str, optional
        The name of the Iceberg catalog (default is "local").
    migrations_path : Union[str, Path, S3Path], optional
        The directory path where the catalog schema versions are stored
        (default is None, in which case the schema(s) from the template
        Evaluation are used).
    schema_name : str, optional
        The name of the Iceberg schema (default is "db").
    """
    dir_path = Path(dir_path)

    if warehouse_path is None:
        warehouse_path = Path(dir_path) / "warehouse"

    if migrations_path is None:
        teehr_root = Path(__file__).parent.parent
        migrations_path = Path(teehr_root, "template")

    spark = create_spark_session(
        warehouse_path=warehouse_path,
        catalog_name=catalog_name
    )
    copy_schema_dir(
        target_dir=dir_path
    )
    apply_migrations.evolve_catalog_schema(
        spark=spark,
        migrations_dir_path=dir_path,
        catalog_name=catalog_name,
        schema_name=schema_name
    )
    logger.info(f"Schema evolution completed for {catalog_name}.")
    spark.stop()

    # Now you have to copy over the data!
    ev = teehr.Evaluation(
        dir_path=dir_path,
        check_evaluation_version=False,

    )
    options = {
        "header": "true",
        "ignoreMissingFiles": "true"
    }

    units_table = ev.units
    schema = units_table.schema_func().to_structtype()
    units_sdf = ev.spark.read.format(units_table.format).options(**options).load(units_table.dir.as_posix(), schema=schema)
    units_sdf.writeTo(f"{catalog_name}.{schema_name}.units").append()

    configuration_table = ev.configurations
    schema = configuration_table.schema_func().to_structtype()
    configuration_sdf = ev.spark.read.format(configuration_table.format).options(**options).load(configuration_table.dir.as_posix(), schema=schema)
    configuration_sdf.writeTo(
        f"{catalog_name}.{schema_name}.configurations"
    ).append()

    variables_table = ev.variables
    schema = variables_table.schema_func().to_structtype()
    variables_sdf = ev.spark.read.format(variables_table.format).options(**options).load(variables_table.dir.as_posix(), schema=schema)
    variables_sdf.writeTo(
        f"{catalog_name}.{schema_name}.variables"
    ).append()

    attributes_table = ev.attributes
    schema = attributes_table.schema_func().to_structtype()
    attributes_sdf = ev.spark.read.format(attributes_table.format).options(**options).load(attributes_table.dir.as_posix(), schema=schema)
    attributes_sdf.writeTo(
        f"{catalog_name}.{schema_name}.attributes"
    ).append()

    locations_table = ev.locations
    schema = locations_table.schema_func().to_structtype()
    locations_sdf = ev.spark.read.format(locations_table.format).options(**options).load(locations_table.dir.as_posix(), schema=schema)
    locations_sdf.writeTo(
        f"{catalog_name}.{schema_name}.locations"
    ).append()

    location_attrs_table = ev.location_attributes
    schema = location_attrs_table.schema_func().to_structtype()
    location_attrs_sdf = ev.spark.read.format(location_attrs_table.format).options(**options).load(location_attrs_table.dir.as_posix(), schema=schema)
    location_attrs_sdf.writeTo(
        f"{catalog_name}.{schema_name}.location_attributes"
    ).append()

    primary_timeseries_table = ev.primary_timeseries
    schema = primary_timeseries_table.schema_func().to_structtype()
    primary_timeseries_sdf = ev.spark.read.format(primary_timeseries_table.format).options(**options).load(primary_timeseries_table.dir.as_posix(), schema=schema)
    primary_timeseries_sdf.writeTo(
        f"{catalog_name}.{schema_name}.primary_timeseries"
    ).append()

    secondary_timeseries_table = ev.secondary_timeseries
    schema = secondary_timeseries_table.schema_func().to_structtype()
    secondary_timeseries_sdf = ev.spark.read.format(secondary_timeseries_table.format).options(**options).load(secondary_timeseries_table.dir.as_posix(), schema=schema)
    secondary_timeseries_sdf.writeTo(
        f"{catalog_name}.{schema_name}.secondary_timeseries"
    ).append()

    joined_timeseries_table = ev.joined_timeseries
    schema = joined_timeseries_table.schema_func().to_structtype()
    joined_timeseries_sdf = ev.spark.read.format(joined_timeseries_table.format).options(**options).load(joined_timeseries_table.dir.as_posix(), schema=schema)
    joined_timeseries_sdf.writeTo(
        f"{catalog_name}.{schema_name}.joined_timeseries"
    ).append()

    location_crosswalk_table = ev.location_crosswalks
    schema = location_crosswalk_table.schema_func().to_structtype()
    location_crosswalk_sdf = ev.spark.read.format(location_crosswalk_table.format).options(**options).load(location_crosswalk_table.dir.as_posix(), schema=schema)
    location_crosswalk_sdf.writeTo(
        f"{catalog_name}.{schema_name}.location_crosswalks"
    ).append()
    logger.info(f"Local warehouse created in {dir_path}/warehouse.")

    # Update the version file.
    version_file = Path(dir_path) / "version"
    with open(version_file, "w") as f:
        f.write(teehr.__version__)

    logger.info(
        f"Updated evaluation version file at {version_file} to"
        f" {teehr.__version__}."
    )

    ev.spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a pre-v0.6 Evaluation dataset to Iceberg."
    )
    parser.add_argument(
        "dir_path",
        help="The directory path to the Evaluation to upgrade."
    )
    parser.add_argument(
        "--warehouse_path",
        default=None,
        help="Name of the Iceberg warehouse. Unless specified, the warehouse"
        " will be created in the 'warehouse' subdirectory of"
        " the Evaluation directory."
    )
    parser.add_argument(
        "--migrations_path",
        default=None,
        help="The directory path where the schema migrations are stored."
        " Unless specified, the migrations from the template Evaluation"
        " will be used."
    )
    parser.add_argument(
        "--catalog_name",
        default="local",
        help="Name of the Iceberg catalog, default is 'local'."
    )
    parser.add_argument(
        "--schema_name",
        default="db",
        help="Name of the Iceberg schema, default is 'db'."
    )
    args = parser.parse_args()

    convert_evaluation(
        dir_path=args.dir_path,
        warehouse_path=args.warehouse_path,
        catalog_name=args.catalog_name,
        migrations_path=args.migrations_path,
        schema_name=args.schema_name
    )
