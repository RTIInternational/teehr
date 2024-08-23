"""Classes for bootstrapping sampling methods."""
from typing import Union, Callable, List

from arch.typing import ArrayLike
from numpy.random import RandomState

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field

import teehr.metrics.bootstrap_udfs as bootstrap_udfs
from teehr.models.dataset.table_enums import (
    JoinedTimeseriesFields
)


class BootstrapBasemodel(PydanticBaseModel):
    """Bootstrap Basemodel configuration."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        # extra='forbid'  # raise an error if extra fields are passed
    )


# TODO: Extend an abstract base class for bootstrapping classes to ensure
# that the necessary fields are included?  For Metrics too?
class GumBootsModel(BootstrapBasemodel):
    """Model for GumBoots bootstrapping."""

    create_func: Callable = bootstrap_udfs.create_gumboots_udf
    reps: int = 1000
    seed: int = 42
    random_state: RandomState | None = None
    quantiles: List[float] = [0.05, 0.5, 0.95]
    args: ArrayLike | None = []
    kwargs: ArrayLike | None = None
    # waterYearMonth = 10,
    # startYear = None,
    # endYear = None,
    # minDays = 100,
    # minYears = 10
    additional_fields: Union[List[JoinedTimeseriesFields], None] = Field(
        default=["value_time"]
    )


class CircularBlockModel(BootstrapBasemodel):
    """Model for arch CircularBlock bootstrapping."""

    create_func: Callable = bootstrap_udfs.create_circularblock_udf
    seed: int = 42
    random_state: RandomState | None = None
    reps: int = 1000
    block_size: int = 365
    quantiles: List[float] = [0.05, 0.5, 0.95]
    additional_fields: Union[List[JoinedTimeseriesFields], None] = None
    # args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap  # noqa
    # kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap  # noqa


class StationaryModel(BootstrapBasemodel):
    """Model for arch Stationary bootstrapping."""

    create_func: Callable = bootstrap_udfs.create_stationary_udf
    seed: int = 42
    random_state: RandomState | None = None
    reps: int = 1000
    block_size: int = 365
    quantiles: List[float] = [0.05, 0.5, 0.95]
    additional_fields: Union[List[JoinedTimeseriesFields], None] = None
    # args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap  # noqa
    # kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap  # noqa


class Bootstrappers:
    """Container class for bootstrap sampling classes."""

    GumBoots = GumBootsModel
    CircularBlock = CircularBlockModel
    Stationary = StationaryModel
