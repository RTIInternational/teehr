"""Base model for Evaluation class."""
import os
from pathlib import Path
from typing import Dict, Any

from pydantic import BaseModel as PydanticBaseModel, ConfigDict, Field, model_validator

from teehr import const


class EvaluationBaseModel:
    """Base model for the Evaluation.

    Just to solve circular import issues?

    Could be used to validate Evaluation fields?
    """

    pass


class LocalCatalog(PydanticBaseModel):
    """Base model for local catalog configuration."""

    warehouse_dir: str | Path | None = Field(default=None)
    catalog_name: str | None = Field(default=const.LOCAL_CATALOG_NAME)
    namespace_name: str | None = Field(default=const.LOCAL_NAMESPACE_NAME)
    catalog_type: str | None = Field(default=const.LOCAL_CATALOG_TYPE)
    cache_dir: str | Path | None = Field(default=None)

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
            values['cache_dir'] = \
                Path(values["warehouse_dir"]) / const.CACHE_DIR
        return values


class RemoteCatalog(PydanticBaseModel):
    """Base model for remote catalog configuration."""

    # These three are read live from the environment at instantiation time
    # (default_factory), not bound once at teehr.const's import time, so a
    # value set/changed after import (tests, notebooks, broker-session flows)
    # is reflected — matching the same env-var-at-call-time fix already
    # applied in Evaluation.create_spark_session()/get_dataset().
    warehouse_dir: str | Path | None = Field(
        default_factory=lambda: os.environ.get("REMOTE_WAREHOUSE_IDENTIFIER", "")
    )
    catalog_name: str | None = Field(default=const.REMOTE_CATALOG_NAME)
    namespace_name: str | None = Field(default=const.REMOTE_NAMESPACE_NAME)
    catalog_type: str | None = Field(
        default_factory=lambda: os.environ.get("REMOTE_CATALOG_TYPE", "rest")
    )
    catalog_uri: str | None = Field(
        default_factory=lambda: os.environ.get("REMOTE_CATALOG_REST_URI", "")
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )
