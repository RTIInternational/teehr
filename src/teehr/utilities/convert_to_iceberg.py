"""Convert existing Evaluation data to Iceberg format for v0.6 and beyond."""  # noqa: E501
from typing import Union
from pathlib import Path
import logging
import argparse

import teehr
from teehr.utils.s3path import S3Path


logger = logging.getLogger(__name__)


def convert_evaluation(
    dir_path: Union[str, Path, S3Path],
):
    """Convert TEEHR Evaluation to v0.6 Iceberg.

    Parameters
    ----------
    dir_path : Union[str, Path, S3Path]
        The directory path to the Evaluation to upgrade.
    """
    dir_path = Path(dir_path)

    ev = teehr.Evaluation(
        dir_path=dir_path,
        check_evaluation_version=False,
        create_dir=False,
    )

    ev.clone_template()

    # Now copy in the e4 data.
    dataset_dir = dir_path / "dataset"

    units_sdf = ev.spark.read.csv((dataset_dir / "units").as_posix(), header=True)
    ev.write.to_warehouse(table_name="units", source_data=units_sdf)

    configuration_sdf = ev.spark.read.csv((dataset_dir / "configurations").as_posix(), header=True)
    ev.write.to_warehouse(table_name="configurations", source_data=configuration_sdf)

    variables_sdf = ev.spark.read.csv((dataset_dir / "variables").as_posix(), header=True)
    ev.write.to_warehouse(table_name="variables", source_data=variables_sdf)

    attributes_sdf = ev.spark.read.csv((dataset_dir / "attributes").as_posix(), header=True)
    ev.write.to_warehouse(table_name="attributes", source_data=attributes_sdf)

    locations_sdf = ev.spark.read.parquet((dataset_dir / "locations").as_posix())
    ev.write.to_warehouse(table_name="locations", source_data=locations_sdf)

    location_attrs_sdf = ev.spark.read.parquet((dataset_dir / "location_attributes").as_posix())
    ev.write.to_warehouse(table_name="location_attributes", source_data=location_attrs_sdf)

    primary_timeseries_sdf = ev.spark.read.parquet((dataset_dir / "primary_timeseries").as_posix())
    ev.write.to_warehouse(table_name="primary_timeseries", source_data=primary_timeseries_sdf)

    secondary_timeseries_sdf = ev.spark.read.parquet((dataset_dir / "secondary_timeseries").as_posix())
    ev.write.to_warehouse(table_name="secondary_timeseries", source_data=secondary_timeseries_sdf)

    joined_timeseries_sdf = ev.spark.read.parquet((dataset_dir / "joined_timeseries").as_posix())
    ev.write.to_warehouse(table_name="joined_timeseries", source_data=joined_timeseries_sdf, write_mode="create_or_replace")

    location_crosswalk_sdf = ev.spark.read.parquet((dataset_dir / "location_crosswalks").as_posix())
    ev.write.to_warehouse(table_name="location_crosswalks", source_data=location_crosswalk_sdf)
    logger.info(f"Local warehouse created in {dir_path}/local.")

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

    convert_evaluation(dir_path="/mnt/c/data/ciroh/teehr/e4_evaluations/e0_2_location_example")

    # parser = argparse.ArgumentParser(
    #     description="Convert a pre-v0.6 Evaluation dataset to Iceberg."
    # )
    # parser.add_argument(
    #     "dir_path",
    #     help="The directory path to the Evaluation to upgrade."
    # )
    # args = parser.parse_args()

    # convert_evaluation(
    #     dir_path=args.dir_path
    # )
