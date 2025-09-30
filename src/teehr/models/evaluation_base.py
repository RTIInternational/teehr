"""Base model for Evaluation class."""
from pathlib import Path

from pydantic import BaseModel as PydanticBaseModel, ConfigDict, Field


class EvaluationBase:
    """Base model for the Evaluation.

    Just to solve circular import issues?

    Could be used to validate Evaluation fields?
    """

    pass

    # model_config = ConfigDict(
    #     arbitrary_types_allowed=True,
    #     validate_assignment=True,
    #     extra='forbid'  # raise an error if extra fields are passed
    # )


class CatalogConfigBase(PydanticBaseModel):
    """Base model for catalog configuration."""

    warehouse_dir: str | Path | None = Field(default=None)
    catalog_name: str | None = Field(default=None)
    namespace_name: str | None = Field(default=None)
    catalog_type: str | None = Field(default=None)
    catalog_uri: str | None = Field(default=None)
    dataset_dir: str | Path | None = Field(default=None)
    cache_dir: str | Path | None = Field(default=None)
    scripts_dir: str | Path | None = Field(default=None)
    scripts_dir: str | Path | None = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )