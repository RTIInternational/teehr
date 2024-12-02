"""Functions for formatting filters for querying."""
import pandas as pd
import pandera as pa
import warnings

from collections.abc import Iterable
from datetime import datetime
from typing import List, Union
import logging
from pyspark.sql import DataFrame
from teehr.models.str_enum import StrEnum

from teehr.models.filters import FilterBaseModel
from teehr.models.pydantic_table_models import TableBaseModel

logger = logging.getLogger(__name__)

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_datetime_list_string(values):
    """Get a datetime list as a list of strings."""
    return [f"'{v.strftime(SQL_DATETIME_STR_FORMAT)}'" for v in values]


def format_iterable_value(
    values: Iterable[Union[str, int, float, datetime]]
) -> str:
    """Return an SQL formatted string from list of values.

    Parameters
    ----------
    values : Iterable
        Contains values to be formatted as a string for SQL. Only one type of
        value (str, int, float, datetime) should be used. First value in list
        is used to determine value type. Values are not checked for type
        consistency.

    Returns
    -------
    str
        An SQL formatted string from list of values.
    """
    # string
    if isinstance(values[0], str):
        return f"""({",".join([f"'{v}'" for v in values])})"""
    # int or float
    elif isinstance(values[0], int) or isinstance(values[0], float):
        return f"""({",".join([f"{v}" for v in values])})"""
    # datetime
    elif isinstance(values[0], datetime):
        return f"""({",".join(get_datetime_list_string(values))})"""
    else:
        logger.warning(
            "Treating value as string because didn't know what else to do."
        )
        return f"""({",".join([f"'{str(v)}'" for v in values])})"""


def format_filter(
    filter: FilterBaseModel,
) -> str:
    r"""Return an SQL formatted string for single filter object.

    Parameters
    ----------
    filter : str or FilterBaseModel
        A single FilterBaseModel object or a subclass
        of FilterBaseModel.

    Returns
    -------
    str
        An SQL formatted string for single filter object.
    """
    column = filter.column.value
    operator = filter.operator.value

    if isinstance(filter.value, str):
        return f"""{column} {operator} '{filter.value}'"""
    elif (
        isinstance(filter.value, int)
        or isinstance(filter.value, float)
    ):
        return f"""{column} {operator} {filter.value}"""
    elif isinstance(filter.value, datetime):
        dt_str = filter.value.strftime(SQL_DATETIME_STR_FORMAT)
        return f"""{column} {operator} '{dt_str}'"""
    elif (
        isinstance(filter.value, Iterable)
        and not isinstance(filter.value, str)
    ):
        value = format_iterable_value(filter.value)
        return f"""{column} {operator} {value}"""
    else:
        logger.warning(
            f"Treating value {filter.value} as string because "
            "didn't know what else to do."
        )
        return f"""{column} {operator} '{str(filter.value)}'"""


def validate_filter(
    filter: FilterBaseModel,
    dataframe_schema: pa.DataFrameSchema
):
    """Validate a single model."""
    if filter.column.value not in dataframe_schema.columns:
        raise ValueError(f"Filter column not in model fields: {filter}")

    model_field_data_type = dataframe_schema.columns[filter.column.value].dtype.type
    logging.debug(
        f"Model field {filter.column.value} has type: {model_field_data_type}"
    )

    # if string or not iterable, make it iterable
    vals = filter.value
    value_was_iterable = True
    if isinstance(vals, str) or not isinstance(vals, Iterable):
        vals = [vals]
        value_was_iterable = False

    validate_vals = []
    for v in vals:
        logging.debug(f"Validating filter value: {v}")
        if model_field_data_type == str:
            validate_vals.append(str(v))
        elif model_field_data_type == int:
            validate_vals.append(int(v))
        elif (
            model_field_data_type == float or
            model_field_data_type == "float64" or
            model_field_data_type == "float32"
        ):
            validate_vals.append(float(v))
        elif (
            model_field_data_type == datetime or
            model_field_data_type == "datetime64[ns]" or
            model_field_data_type == "datetime64[ms]"
        ):
            validate_vals.append(pd.Timestamp(v))
        else:
            logger.warning(
                f"Treating value as string because "
                "didn't know what else to do."
            )
            validate_vals.append(str(v))

    if value_was_iterable:
        filter.value = validate_vals
    else:
        filter.value = validate_vals[0]

    return filter


def validate_and_apply_filters(
    sdf: DataFrame,
    filters: Union[str, dict, List[dict]],
    filter_model: FilterBaseModel,
    fields_enum: StrEnum,
    dataframe_schema: pa.DataFrameSchema = None,
    validate: bool = True
):
    """Validate and apply filters."""
    if isinstance(filters, str):
        logger.debug(f"Filter {filters} is already string.  Applying as is.")
        sdf = sdf.filter(filters)
        return sdf

    if not isinstance(filters, List):
        logger.debug("Filter is not a list.  Making a list.")
        filters = [filters]

    for filter in filters:
        logger.debug(f"Validating and applying {filter}")

        if not isinstance(filter, str):
            filter = filter_model.model_validate(
                filter,
                context={"fields_enum": fields_enum}
            )
            logger.debug(f"Filter: {filter.model_dump_json()}")
            if validate:
                filter = validate_filter(filter, dataframe_schema)
            filter = format_filter(filter)

        sdf = sdf.filter(filter)

    return sdf
