"""Base model for Evaluation class."""
from pathlib import Path
from typing import Dict, Any

from pydantic import BaseModel as PydanticBaseModel, ConfigDict, Field, model_validator

from teehr import const


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


# class CatalogConfigBase(PydanticBaseModel):
#     """Base model for catalog configuration."""

#     warehouse_dir: str | Path | None = Field(default=None)
#     catalog_name: str | None = Field(default=None)
#     namespace_name: str | None = Field(default=None)
#     catalog_type: str | None = Field(default=None)
#     catalog_uri: str | None = Field(default=None)
#     dataset_dir: str | Path | None = Field(default=None)
#     cache_dir: str | Path | None = Field(default=None)
#     scripts_dir: str | Path | None = Field(default=None)

#     model_config = ConfigDict(
#         arbitrary_types_allowed=True,
#         validate_assignment=True,
#         extra='forbid'  # raise an error if extra fields are passed
#     )

#     @model_validator(mode='before')
#     @classmethod
#     def calculate_field(cls, values: Dict[str, Any]) -> Dict[str, Any]:
#         """Assign dataset, cache, and scripts dirs based on warehouse dir."""
#         if "warehouse_dir" in values and values["warehouse_dir"] is not None:
#             values['dataset_dir'] = \
#                 Path(values["warehouse_dir"]) / const.DATASET_DIR
#             values['cache_dir'] = \
#                 Path(values["warehouse_dir"]) / const.CACHE_DIR
#             values['scripts_dir'] = \
#                 Path(values["warehouse_dir"]) / const.SCRIPTS_DIR
#         return values


class LocalCatalog(PydanticBaseModel):
    """Base model for local catalog configuration."""

    warehouse_dir: str | Path | None = Field(default=None)
    catalog_name: str | None = Field(default="local")
    namespace_name: str | None = Field(default="teehr")
    catalog_type: str | None = Field(default="hadoop")
    dataset_dir: str | Path | None = Field(default=None)
    cache_dir: str | Path | None = Field(default=None)
    scripts_dir: str | Path | None = Field(default=None)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )

    @model_validator(mode='before')
    @classmethod
    def calculate_field(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Assign dataset, cache, and scripts dirs based on warehouse dir."""
        if "warehouse_dir" in values and values["warehouse_dir"] is not None:
            values['dataset_dir'] = \
                Path(values["warehouse_dir"]) / const.DATASET_DIR
            values['cache_dir'] = \
                Path(values["warehouse_dir"]) / const.CACHE_DIR
            values['scripts_dir'] = \
                Path(values["warehouse_dir"]) / const.SCRIPTS_DIR
        return values


class RemoteCatalog(PydanticBaseModel):
    """Base model for remote catalog configuration."""

    warehouse_dir: str | Path | None = Field(default=const.WAREHOUSE_S3_PATH)
    catalog_name: str | None = Field(default="iceberg")
    namespace_name: str | None = Field(default="teehr")
    catalog_type: str | None = Field(default="rest")
    catalog_uri: str | None = Field(default=const.CATALOG_REST_URI)

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )
