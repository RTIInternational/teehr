"""Convert and insert timeseries data into the dataset."""
from typing import Union
from pathlib import Path
import pandas as pd
from pyspark.sql.functions import lit
from teehr.loading.utils import (
    read_and_convert_netcdf_to_df,
    read_and_convert_xml_to_df
)
from teehr.models.table_enums import TableWriteEnum

import logging

logger = logging.getLogger(__name__)


def convert_single_timeseries(
    in_filepath: Union[str, Path],
    field_mapping: dict,
    **kwargs
):
    """Convert timeseries data to parquet format.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input file path.
    field_mapping : dict
        A dictionary mapping input fields to output fields.
        format: {input_field: output_field}
    **kwargs
        Additional keyword arguments are passed to
            pd.read_csv(), pd.read_parquet(), or xr.open_dataset().

    Steps:
    1. Read the file
    2. Rename the columns based on the field_mapping
    3. Add constant field values to dataframe,
        this may result in too many columns.
    4. Subset only the columns in the field_mapping
    """
    in_filepath = Path(in_filepath)

    logger.info(f"Converting timeseries data from: {in_filepath}")

    if in_filepath.suffix == ".parquet":
        # read and convert parquet file
        timeseries = pd.read_parquet(in_filepath, **kwargs)
    elif in_filepath.suffix == ".csv":
        # read and convert csv file
        timeseries = pd.read_csv(in_filepath, **kwargs)
    elif in_filepath.suffix == ".nc":
        # read and convert netcdf file
        timeseries = read_and_convert_netcdf_to_df(
            in_filepath,
            field_mapping,
            **kwargs
        )
    elif in_filepath.suffix == ".xml":
        # read and convert xml file
        timeseries = read_and_convert_xml_to_df(
            in_filepath,
            field_mapping,
            **kwargs
        )
    else:
        raise ValueError("Unsupported file type.")

    timeseries.rename(columns=field_mapping, inplace=True)

    return timeseries


# TODO: Update/remove this function. This is only used by fetching.
# Should this be part of a Loader class? This general workflow is repeated
# for each table.
def validate_and_insert_timeseries(
    ev,
    in_path: Union[str, Path],
    timeseries_type: str,
    pattern: str = "**/*.parquet",
    write_mode: TableWriteEnum = "append",
    drop_duplicates: bool = True,
    drop_overlapping_assimilation_values: bool = False
):
    """Validate and insert primary timeseries data.

    Parameters
    ----------
    ev : Evaluation
        The Evaluation object.
    in_path : Union[str, Path]
        Directory path or file path to the primary timeseries data.
    timeseries_type : str
        The type of timeseries data.
        Valid values: "primary", "secondary"
    pattern : str, optional (default: "**/*.parquet")
        The pattern to match files.
    write_mode : TableWriteEnum, optional (default: "append")
        The write mode for the table. Options are "append" or "upsert".
        If "append", the Evaluation table will be appended with new data
        that does not already exist.
        If "upsert", existing data will be replaced and new data that
        does not exist will be appended.
    drop_duplicates : bool, optional (default: True)
        Whether to drop duplicates in the dataframe before writing
        to the table.
    drop_overlapping_assimilation_values: Optional[bool] = True
        Whether to drop overlapping assimilation values. Default is True.
        If True, values that overlap in value_time are dropped, keeping those with
        the most recent reference_time. In this case, all reference_time values
        are set to None. If False, overlapping values are kept and reference_time
        is retained.
    """ # noqa
    in_path = Path(in_path)
    logger.info(f"Validating and inserting timeseries data from {in_path}")

    if timeseries_type == "primary":
        table = ev.primary_timeseries
    elif timeseries_type == "secondary":
        table = ev.secondary_timeseries
    else:
        raise ValueError("Invalid timeseries type.")

    # Read the converted files to Spark DataFrame
    df = ev.read.from_cache(
        path=in_path,
        table_schema_func=table.schema_func(),
        pattern=pattern,
    ).to_sdf()

    if drop_overlapping_assimilation_values:
        df = df.withColumn("reference_time", lit(None))

    # Validate datatypes, foreign keys, and drop duplicates.
    validated_df = ev.validate.schema(
        sdf=df,
        table_schema=table.schema_func(),
        drop_duplicates=drop_duplicates,
        foreign_keys=table.foreign_keys,
        uniqueness_fields=table.uniqueness_fields
    )
    # Write to the table
    ev.write.to_warehouse(
        source_data=validated_df,
        table_name=table.name,
        write_mode=write_mode,
        uniqueness_fields=table.uniqueness_fields
    )

    # Reload the table
    table._load_table()
