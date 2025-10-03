"""A module for cloning and optionally subsetting evaluations from s3."""
from pathlib import Path
import yaml
import fsspec
import s3fs
import pandas as pd
import teehr.const as const
from datetime import datetime
from typing import Union, Literal, List
import logging
import pyspark

from teehr.models.evaluation_base import LocalCatalog, RemoteCatalog

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


# def subset_the_table(
#     ev,
#     table,
#     sdf_in: pyspark.sql.DataFrame,
#     primary_location_ids: Union[None, List[str]],
#     start_date: Union[str, datetime, None],
#     end_date: Union[str, datetime, None],
# ) -> pyspark.sql.DataFrame:
#     """Subset the dataset based on location and start/end time."""
#     if table.name == "locations" and primary_location_ids is not None:
#         sdf_in = sdf_in.filter(sdf_in.id.isin(primary_location_ids))
#     elif table.name == "location_attributes" and primary_location_ids is not None:
#         sdf_in = sdf_in.filter(sdf_in.location_id.isin(primary_location_ids))
#     elif table.name == "location_crosswalks" and primary_location_ids is not None:
#         sdf_in = sdf_in.filter(
#             sdf_in.primary_location_id.isin(primary_location_ids)
#         )
#     elif table.name == "primary_timeseries":
#         if primary_location_ids is not None:
#             sdf_in = sdf_in.filter(
#                 sdf_in.location_id.isin(primary_location_ids)
#             )
#     elif table.name == "secondary_timeseries":
#         if primary_location_ids is not None:
#             secondary_ids = (
#                 ev.location_crosswalks.to_sdf()
#                 .select("secondary_location_id").rdd.flatMap(lambda x: x).collect()
#             )
#             sdf_in = sdf_in.filter(sdf_in.location_id.isin(secondary_ids))
#     elif table.name == "joined_timeseries":
#         if primary_location_ids is not None:
#             sdf_in = sdf_in.filter(
#                 sdf_in.primary_location_id.isin(primary_location_ids)
#             )
#     if (
#         table.name == "primary_timeseries"
#         or table.name == "secondary_timeseries"
#         or table.name == "joined_timeseries"
#     ):
#         if start_date is not None:
#             sdf_in = sdf_in.filter(sdf_in.value_time >= start_date)
#         if end_date is not None:
#             sdf_in = sdf_in.filter(sdf_in.value_time <= end_date)

#     return sdf_in


def clone_from_s3(
    ev,
    local_catalog_name: str,
    local_namespace_name: str,
    remote_catalog_name: str,
    remote_namespace_name: str,
    primary_location_ids: Union[None, List[str]],
    start_date: Union[str, datetime, None],
    end_date: Union[str, datetime, None]
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
    logger.info(f"Cloning evaluation from remote warehouse")
    # Properties needed:
    # - table name
    # - uniqueness fields
    # - partition by fields
    tables = [
        {
            "table": ev.units,
            "filters": [None]
        },
        {
            "table": ev.variables,
            "filters": [None]
        },
        {
            "table": ev.attributes,
            "filters": [None]
        },
        {
            "table": ev.configurations,
            "filters": [None]
        },
        {
            "table": ev.locations,
            "filters": [
                f"id in {primary_location_ids}" if primary_location_ids is not None else None
            ]
        },
        {
            "table": ev.location_attributes,
            "filters": [
                f"location_id in {primary_location_ids}" if primary_location_ids is not None else None
            ]
        },
        {
            "table": ev.location_crosswalks,
            "filters": [
                f"primary_location_id in {primary_location_ids}" if primary_location_ids is not None else None
            ]
        },
        {
            "table": ev.primary_timeseries,
            "filters": [
                f"location_id in {primary_location_ids}" if primary_location_ids is not None else None,
                f"value_time >= '{start_date}'" if start_date is not None else None,
                f"value_time <= '{end_date}'" if end_date is not None else None
            ]
        },
        {
            "table": ev.secondary_timeseries,
            "filters": [
                f"location_id in {primary_location_ids}" if primary_location_ids is not None else None,
                f"value_time >= '{start_date}'" if start_date is not None else None,
                f"value_time <= '{end_date}'" if end_date is not None else None
            ]
        },
        {
            "table": ev.joined_timeseries,
            "filters": [
                f"primary_location_id in {primary_location_ids}" if primary_location_ids is not None else None,
                f"value_time >= '{start_date}'" if start_date is not None else None,
                f"value_time <= '{end_date}'" if end_date is not None else None
            ]
        },
    ]

    logger.info(f"Cloning evaluation from s3: {ev.remote_catalog.warehouse_dir}")

    for table in tables:
        filters = table["filters"]
        table = table["table"]
        logger.debug(
            f"Cloning {table.name} from {remote_catalog_name}."
            f"{remote_namespace_name}.{table.name}."
        )
        sdf_in = ev.read.from_warehouse(
            table=table.name,
            catalog_name=remote_catalog_name,
            namespace=remote_namespace_name
        ).to_sdf()
        for filter in filters:
            if filter is not None:
                logger.debug(f"Applying filter: {filter}")
                sdf_in = sdf_in.filter(filter)

        if table.name == "joined_timeseries":
            ev.write.to_warehouse(
                source_data=sdf_in,
                catalog_name=local_catalog_name,
                namespace=local_namespace_name,
                target_table=table.name,
                write_mode="create_or_replace",
                uniqueness_fields=table.uniqueness_fields,
                partition_by=table.partition_by
            )
        else:
            ev.write.to_warehouse(
                source_data=sdf_in,
                catalog_name=local_catalog_name,
                namespace=local_namespace_name,
                target_table=table.name,
                write_mode="upsert",
                uniqueness_fields=table.uniqueness_fields,
                partition_by=table.partition_by
            )
