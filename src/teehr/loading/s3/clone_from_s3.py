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
    table,
    sdf_in: pyspark.sql.DataFrame,
    primary_location_ids: Union[None, List[str]],
    start_date: Union[str, datetime, None],
    end_date: Union[str, datetime, None],
) -> pyspark.sql.DataFrame:
    """Subset the dataset based on location and start/end time."""
    if table.name == "locations" and primary_location_ids is not None:
        sdf_in = sdf_in.filter(sdf_in.id.isin(primary_location_ids))
    elif table.name == "location_attributes" and primary_location_ids is not None:
        sdf_in = sdf_in.filter(sdf_in.location_id.isin(primary_location_ids))
    elif table.name == "location_crosswalks" and primary_location_ids is not None:
        sdf_in = sdf_in.filter(
            sdf_in.primary_location_id.isin(primary_location_ids)
        )
    elif table.name == "primary_timeseries":
        if primary_location_ids is not None:
            sdf_in = sdf_in.filter(
                sdf_in.location_id.isin(primary_location_ids)
            )
    elif table.name == "secondary_timeseries":
        if primary_location_ids is not None:
            secondary_ids = (
                ev.location_crosswalks.to_sdf()
                .select("secondary_location_id").rdd.flatMap(lambda x: x).collect()
            )
            sdf_in = sdf_in.filter(sdf_in.location_id.isin(secondary_ids))
    elif table.name == "joined_timeseries":
        if primary_location_ids is not None:
            sdf_in = sdf_in.filter(
                sdf_in.location_id.isin(primary_location_ids)
            )
    if (
        table.name == "primary_timeseries"
        or table.name == "secondary_timeseries"
        or table.name == "joined_timeseries"
    ):
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

    # Make the Evaluation directories
    logger.info(f"Creating directories for evaluation: {evaluation_name}")
    Path(ev.cache_dir).mkdir()
    Path(ev.dataset_dir).mkdir()
    Path(ev.scripts_dir).mkdir()

    logger.info(f"Cloning evaluation from s3: {evaluation_name}")
    tables = [
        {
            "table": ev.units
        },
        {
            "table": ev.variables
        },
        {
            "table": ev.attributes
        },
        {
            "table": ev.configurations
        },
        {
            "table": ev.locations
        },
        {
            "table": ev.location_attributes
        },
        {
            "table": ev.location_crosswalks
        },
        {
            "table": ev.primary_timeseries
        },
        {
            "table": ev.secondary_timeseries
        },
        {
            "table": ev.joined_timeseries
        },
    ]

    protocol_list = list_s3_evaluations(format="list")
    protocol = [p for p in protocol_list if p["name"] == evaluation_name][0]
    url = protocol["url"]
    url = url.replace("s3://", "s3a://")
    s3_dataset_path = f"{url}/dataset"

    logger.info(f"Cloning evaluation from s3: {s3_dataset_path}")

    for table in tables:
        table = table["table"]

        logger.debug(f"Making directory {table.dir}")
        Path(table.dir).mkdir()

        logger.debug(f"Cloning {table.name} from {s3_dataset_path}/{table.name}/ to {table.dir}")

        sdf_in = table._read_files(path=f"{s3_dataset_path}/{table.name}/")

        sdf_in = subset_the_table(
            ev=ev,
            sdf_in=sdf_in,
            table=table,
            primary_location_ids=primary_location_ids,
            start_date=start_date,
            end_date=end_date
        )

        table._write_spark_df(sdf_in)


    # copy scripts path to ev.scripts_dir
    source = f"{url}/scripts/user_defined_fields.py"
    dest = f"{ev.scripts_dir}/user_defined_fields.py"
    logger.debug(f"Copying from {source}/ to {dest}")

    # ToDo: there is a permission issue that prevents copying the entire directory.
    # This works for now.
    with fsspec.open(source, 'r', anon=True) as file:
        with open(dest, 'w') as f:
            f.write(file.read())
