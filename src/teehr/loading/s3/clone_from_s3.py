from pathlib import Path
import yaml
import fsspec
import pandas as pd
import teehr.const as const
from typing import Union, Literal
import logging

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
    logger.info(f"Getting evaluations from s3: {const.S3_EVALVALUATIONS_PATH}")
    # Read the content of the file using fsspec
    with fsspec.open(const.S3_EVALVALUATIONS_PATH, 'r', anon=True) as file:
        yaml_content = file.read()

    # Load the YAML content into a dictionary
    yaml_dict = yaml.safe_load(yaml_content)

    if format == "pandas":
        return pd.DataFrame(yaml_dict["evaluations"])
    return yaml_dict["evaluations"]


def clone_from_s3(ev, evaluation_name: str):
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
            "format": "csv"
        },
        {
            "name": "variables",
            "local_path": ev.variables_dir,
            "format": "csv"
        },
        {
            "name": "attributes",
            "local_path": ev.attributes_dir,
            "format": "csv"
        },
        {
            "name": "configurations",
            "local_path": ev.configurations_dir,
            "format": "csv"
        },
        {
            "name": "locations",
            "local_path": ev.locations_dir,
            "format": "parquet"
        },
        {
            "name": "location_attributes",
            "local_path": ev.location_attributes_dir,
            "format": "parquet"
        },
        {
            "name": "location_crosswalks",
            "local_path": ev.location_crosswalks_dir,
            "format": "parquet"
        },
        {
            "name": "primary_timeseries",
            "local_path": ev.primary_timeseries_dir,
            "format": "parquet"
        },
        {
            "name": "secondary_timeseries",
            "local_path": ev.secondary_timeseries_dir,
            "format": "parquet"
        },
        {
            "name": "joined_timeseries",
            "local_path": ev.joined_timeseries_dir,
            "format": "parquet"
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

        (
            sdf_in
            .write
            .format(format)
            .mode("overwrite")
            .save(str(local_path))
        )

    # copy scriopts path to ev.scripts_dir
    source = f"{url}/scripts/user_defined_fields.py"
    dest = f"{ev.scripts_dir}/user_defined_fields.py"
    logger.debug(f"Copying from {source}/ to {dest}")

    # ToDo: there is a permission issue that prevents copying the entire directory.
    # This works for now.
    with fsspec.open(source, 'r', anon=True) as file:
        with open(dest, 'w') as f:
            f.write(file.read())
