"""Module for parquet-based query models."""
from collections.abc import Iterable
from typing import List, Union
from pydantic import BaseModel as BaseModel
from pydantic import ValidationInfo, field_validator
from datetime import datetime, timedelta
import logging
from teehr.models.str_enum import StrEnum


logger = logging.getLogger(__name__)


class FilterOperators(StrEnum):
    """Filter symbols."""

    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class TableFilter(BaseModel):
    """Base model for filters."""

    column: str
    operator: FilterOperators
    value: Union[
        str,
        int,
        float,
        datetime,
        timedelta,
        List[Union[str, int, float, datetime, timedelta]]
    ]

    def is_iterable_not_str(obj):
        """Check if is type Iterable and not str.

        We should not have the case where a string is provided, but doesn't
        hurt to check.
        """
        if isinstance(obj, Iterable) and not isinstance(obj, str):
            return True
        return False

    @field_validator("value")
    def in_operator_must_have_iterable(cls, v, info: ValidationInfo):
        """Ensure that an 'in' operator has an iterable type."""
        if cls.is_iterable_not_str(v) and info.data["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")

        if info.data["operator"] == "in" and not cls.is_iterable_not_str(v):
            raise ValueError(
                "'in' operator can only be used with iterable value"
            )
        return v

    @field_validator("column", mode='before')
    def coerce_column_to_enum(cls, v, info: ValidationInfo):
        """Column name must exist in the database table."""
        if info.context is not None:
            # String column names require context with field_names (list of strings)
            if info.context is None:
                raise ValueError(
                    f"String column name '{v}' provided but no context found. "
                    f"Either pass the field enum directly (e.g., column=Fields.{v}) "
                    f"or use model_validate() with context={{'field_names': [list of field names]}}"
                )
            fields = info.context.get("field_names")
            if fields is None:
                raise ValueError(
                    "'field_names' not found in context. "
                    "Pass context={'field_names': [list of field names]}"
                )

            # Validate the column name exists in the provided fields list
            if not isinstance(fields, (list, tuple)):
                raise ValueError(
                    f"field_names must be a list of field names, got {type(fields)}"
                )
            if v not in fields:
                raise ValueError(
                    f"Column '{v}' not found in the provided fields list. "
                    f"Available fields: {fields}"
                )
        return v