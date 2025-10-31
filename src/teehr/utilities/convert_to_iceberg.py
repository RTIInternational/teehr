"""Convert existing Evaluation data to Iceberg format for v0.6 and beyond."""  # noqa: E501
from typing import Union
from pathlib import Path
import logging
import argparse

import teehr
from teehr.utils.s3path import S3Path

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


def convert_evaluation(
    dir_path: Union[str, Path, S3Path],
    catalog_name: str = "local",
    namespace: str = "teehr",
    spark: SparkSession = None,
):
    """Convert TEEHR Evaluation to v0.6 Iceberg.

    Parameters
    ----------
    dir_path : Union[str, Path, S3Path]
        The directory path to the Evaluation to upgrade.
    catalog_name : str, optional
        The name of the Iceberg catalog (default is "local").
    migrations_path : Union[str, Path, S3Path], optional
        The directory path where the catalog schema versions are stored
        (default is None, in which case the schema(s) from the template
        Evaluation are used).
    namespace : str, optional
        The name of the Iceberg schema (default is "teehr").
    """
    dir_path = Path(dir_path)

    ev = teehr.Evaluation(
        spark=spark,
        dir_path=dir_path,
        check_evaluation_version=False,
        create_dir=False,
        local_catalog_name=catalog_name,
        local_namespace_name=namespace,
        remote_namespace_name=namespace,
    )
    ev.apply_schema_migration()

    # Now copy in the e4 data.
    options = {
        "header": "true",
        "ignoreMissingFiles": "true",
        "recursiveFileLookup": "true"
    }

    # remote_catalog_name = ev.remote_catalog.catalog_name
    dataset_dir = dir_path / "dataset"

    units_table = ev.units
    schema = units_table.schema_func().to_structtype()
    units_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "units").as_posix(), schema=schema)
    units_sdf.writeTo(f"{catalog_name}.{namespace}.units").append()

    configuration_table = ev.configurations
    schema = configuration_table.schema_func().to_structtype()
    configuration_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "configurations").as_posix(), schema=schema)
    configuration_sdf.writeTo(
        f"{catalog_name}.{namespace}.configurations"
    ).append()

    variables_table = ev.variables
    schema = variables_table.schema_func().to_structtype()
    variables_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "variables").as_posix(), schema=schema)
    variables_sdf.writeTo(
        f"{catalog_name}.{namespace}.variables"
    ).append()

    attributes_table = ev.attributes
    schema = attributes_table.schema_func().to_structtype()
    attributes_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "attributes").as_posix(), schema=schema)
    attributes_sdf.writeTo(
        f"{catalog_name}.{namespace}.attributes"
    ).append()

    locations_table = ev.locations
    schema = locations_table.schema_func().to_structtype()
    locations_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "locations").as_posix(), schema=schema)
    locations_sdf.writeTo(
        f"{catalog_name}.{namespace}.locations"
    ).append()

    location_attrs_table = ev.location_attributes
    schema = location_attrs_table.schema_func().to_structtype()
    location_attrs_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "location_attributes").as_posix(), schema=schema)
    location_attrs_sdf.writeTo(
        f"{catalog_name}.{namespace}.location_attributes"
    ).append()

    primary_timeseries_table = ev.primary_timeseries
    schema = primary_timeseries_table.schema_func().to_structtype()
    primary_timeseries_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "primary_timeseries").as_posix(), schema=schema)
    primary_timeseries_sdf.writeTo(
        f"{catalog_name}.{namespace}.primary_timeseries"
    ).append()

    secondary_timeseries_table = ev.secondary_timeseries
    schema = secondary_timeseries_table.schema_func().to_structtype()
    secondary_timeseries_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "secondary_timeseries").as_posix(), schema=schema)
    secondary_timeseries_sdf.writeTo(
        f"{catalog_name}.{namespace}.secondary_timeseries"
    ).append()

    joined_timeseries_table = ev.joined_timeseries
    schema = joined_timeseries_table.schema_func().to_structtype()
    joined_timeseries_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "joined_timeseries").as_posix(), schema=schema)
    joined_timeseries_sdf.writeTo(
        f"{catalog_name}.{namespace}.joined_timeseries"
    ).append()

    location_crosswalk_table = ev.location_crosswalks
    schema = location_crosswalk_table.schema_func().to_structtype()
    location_crosswalk_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "location_crosswalks").as_posix(), schema=schema)
    location_crosswalk_sdf.writeTo(
        f"{catalog_name}.{namespace}.location_crosswalks"
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

    # convert_evaluation(dir_path="/mnt/c/data/ciroh/teehr/e4_evaluations/e0_2_location_example")

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
        "--catalog_name",
        default="local",
        help="Name of the Iceberg catalog, default is 'local'."
    )
    parser.add_argument(
        "--namespace",
        default="db",
        help="Name of the Iceberg schema, default is 'db'."
    )
    args = parser.parse_args()

    convert_evaluation(
        dir_path=args.dir_path,
        warehouse_path=args.warehouse_path,
        catalog_name=args.catalog_name,
        namespace=args.namespace
    )
