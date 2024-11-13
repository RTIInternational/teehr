"""A module for cloning and optionally subsetting evaluations from s3."""
from pathlib import Path
import yaml
import fsspec
import pandas as pd
import teehr.const as const
from datetime import datetime
from typing import Union, Literal, List
import logging
import pyspark

logger = logging.getLogger(__name__)


def list_s3_evaluations(
        format: Literal["pandas", "list"] = "pandas"
) -> Union[list, pd.DataFrame]:
    """List the evaluations in s3.

    Parameters
    ----------
    format : str
        The format of the output. Either "pandas" or "list".
        Default is "pandas".

    """
    logger.info(f"Getting evaluations from s3: {const.S3_EVALUATIONS_PATH}")
    # Read the content of the file using fsspec
    with fsspec.open(const.S3_EVALUATIONS_PATH, 'r', anon=True) as file:
        yaml_content = file.read()

    # Load the YAML content into a dictionary
    yaml_dict = yaml.safe_load(yaml_content)

    if format == "pandas":
        return pd.DataFrame(yaml_dict["evaluations"])
    return yaml_dict["evaluations"]


def subset_the_table(
    ev,
    s3_dataset_path: str,
    sdf_in: pyspark.sql.DataFrame,
    name: str,
    primary_location_ids: Union[None, List[str]],
    start_date: Union[str, datetime, None],
    end_date: Union[str, datetime, None],
) -> pyspark.sql.DataFrame:
    """Subset the dataset based on location and start/end time."""
    if name == "locations" and primary_location_ids is not None:
        sdf_in = sdf_in.filter(sdf_in.id.isin(primary_location_ids))
    elif name == "location_attributes" and primary_location_ids is not None:
        sdf_in = sdf_in.filter(sdf_in.location_id.isin(primary_location_ids))
    elif name == "location_crosswalks" and primary_location_ids is not None:
        sdf_in = sdf_in.filter(
            sdf_in.primary_location_id.isin(primary_location_ids)
        )
    elif name == "primary_timeseries":
        if primary_location_ids is not None:
            sdf_in = sdf_in.filter(
                sdf_in.location_id.isin(primary_location_ids)
            )
    elif name == "secondary_timeseries":
        if primary_location_ids is not None:
            xwalk = (
                ev.
                spark.
                read.
                format("parquet").
                load(f"{s3_dataset_path}/location_crosswalks/")
            )
            secondary_ids = xwalk.filter(
                xwalk.primary_location_id.isin(primary_location_ids)
            ).select("secondary_location_id").rdd.flatMap(lambda x: x).collect()
            sdf_in = sdf_in.filter(sdf_in.location_id.isin(secondary_ids))
    elif name == "joined_timeseries":
        if primary_location_ids is not None:
            sdf_in = sdf_in.filter(
                sdf_in.location_id.isin(primary_location_ids)
            )
    if "timeseries" in name:
        if start_date is not None:
            sdf_in = sdf_in.filter(sdf_in.value_time >= start_date)
        if end_date is not None:
            sdf_in = sdf_in.filter(sdf_in.value_time <= end_date)

    return sdf_in


def clone_from_s3(
    ev,
    evaluation_name: str,
    primary_location_ids: Union[None, List[str]],
    start_date: Union[str, datetime, None],
    end_date: Union[str, datetime, None],
):
    """Clone an evaluation from s3.

    Copies the evaluation from s3 to the local directory.
    Includes the following tables:
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

    Also includes the user_defined_fields.py script.

    Parameters
    ----------
    ev : Evaluation
        The Evaluation object.
    evaluation_name : str
        The name of the evaluation to clone.

    Note: future version will allow subsetting the tables to clone.
    """

    # Make the Evlaution directories
    logger.info(f"Creating directories for evaluation: {evaluation_name}")
    Path(ev.cache_dir).mkdir()
    Path(ev.dataset_dir).mkdir()
    Path(ev.scripts_dir).mkdir()

    logger.info(f"Cloning evaluation from s3: {evaluation_name}")
    tables = [
        {
            "name": "units",
            "local_path": ev.units_dir,
            "format": "csv",
            "partitions": None
        },
        {
            "name": "variables",
            "local_path": ev.variables_dir,
            "format": "csv",
            "partitions": None
        },
        {
            "name": "attributes",
            "local_path": ev.attributes_dir,
            "format": "csv",
            "partitions": None
        },
        {
            "name": "configurations",
            "local_path": ev.configurations_dir,
            "format": "csv",
            "partitions": None
        },
        {
            "name": "locations",
            "local_path": ev.locations_dir,
            "format": "parquet",
            "partitions": None
        },
        {
            "name": "location_attributes",
            "local_path": ev.location_attributes_dir,
            "format": "parquet",
            "partitions": None
        },
        {
            "name": "location_crosswalks",
            "local_path": ev.location_crosswalks_dir,
            "format": "parquet",
            "partitions": None
        },
        {
            "name": "primary_timeseries",
            "local_path": ev.primary_timeseries_dir,
            "format": "parquet",
            "partitions": ["configuration_name", "variable_name"]
        },
        {
            "name": "secondary_timeseries",
            "local_path": ev.secondary_timeseries_dir,
            "format": "parquet",
            "partitions": ["configuration_name", "variable_name"]
        },
        {
            "name": "joined_timeseries",
            "local_path": ev.joined_timeseries_dir,
            "format": "parquet",
            "partitions": ["configuration_name", "variable_name"]
        },
    ]

    protocol_list = list_s3_evaluations(format="list")
    protocol = [p for p in protocol_list if p["name"] == evaluation_name][0]
    url = protocol["url"]
    url = url.replace("s3://", "s3a://")
    s3_dataset_path = f"{url}/dataset"

    logger.info(f"Cloning evaluation from s3: {s3_dataset_path}")

    for table in tables:
        local_path = table["local_path"]
        format = table["format"]
        name = table["name"]
        dir_name = str(Path(local_path).relative_to(ev.dataset_dir))

        logger.debug(f"Making directory {table['local_path']}")
        Path(local_path).mkdir()

        logger.debug(f"Cloning {name} from {s3_dataset_path}/{dir_name}/ to {local_path}")

        sdf_in = (
            ev.spark
            .read
            .format(format)
            .load(f"{s3_dataset_path}/{dir_name}/")
        )

        sdf_in = subset_the_table(
            ev=ev,
            s3_dataset_path=s3_dataset_path,
            sdf_in=sdf_in,
            name=name,
            primary_location_ids=primary_location_ids,
            start_date=start_date,
            end_date=end_date
        )

        if table["partitions"]:
            logger.debug(f"Partitioning {name} by {table['partitions']}.")
            (
                sdf_in
                .write
                .format(format)
                .mode("overwrite")
                .partitionBy(table["partitions"])
                .save(str(local_path))
            )
        else:
            logger.debug(f"Not partitioning is used for {name}.ß")
            (
                sdf_in
                .write
                .format(format)
                .mode("overwrite")
                .save(str(local_path))
            )

    # copy scripts path to ev.scripts_dir
    source = f"{url}/scripts/user_defined_fields.py"
    dest = f"{ev.scripts_dir}/user_defined_fields.py"
    logger.debug(f"Copying from {source}/ to {dest}")

    # ToDo: there is a permission issue that prevents copying the entire directory.
    # This works for now.
    with fsspec.open(source, 'r', anon=True) as file:
        with open(dest, 'w') as f:
            f.write(file.read())
