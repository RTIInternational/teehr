"""Classes for bootstrapping sampling methods."""
from typing import Callable, List

from arch.typing import ArrayLike
from numpy.random import RandomState
from pydantic import Field

import teehr.metrics.bootstrap_funcs as bootstrap_funcs
from teehr.models.dataset.table_enums import (
    JoinedTimeseriesFields
)
from teehr.models.metrics.metric_models import MetricsBasemodel


# TODO: Extend an abstract base class for bootstrapping classes to ensure
# that the necessary fields are included?  For Metrics too?
class GumBootsModel(MetricsBasemodel):
    """Model for GumBoots bootstrapping."""

    func: Callable = bootstrap_funcs.create_gumboots_func
    reps: int = 1000
    seed: int = 42
    block_size: int = 365
    random_state: RandomState | None = None
    quantiles: List[float] = [0.05, 0.5, 0.95]
    args: ArrayLike | None = []
    kwargs: ArrayLike | None = None
    # waterYearMonth = 10,
    # startYear = None,
    # endYear = None,
    # minDays = 100,
    # minYears = 10
    time_field_name: JoinedTimeseriesFields = Field(
        default="value_time"
    )


class CircularBlockModel(MetricsBasemodel):
    """Model for arch CircularBlock bootstrapping."""

    func: Callable = bootstrap_funcs.create_circularblock_func
    seed: int = 42
    random_state: RandomState | None = None
    reps: int = 1000
    block_size: int = 365
    quantiles: List[float] = [0.05, 0.5, 0.95]
    # args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap  # noqa
    # kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap  # noqa


class StationaryModel(MetricsBasemodel):
    """Model for arch Stationary bootstrapping."""

    func: Callable = bootstrap_funcs.create_stationary_func
    seed: int = 42
    random_state: RandomState | None = None
    reps: int = 1000
    block_size: int = 365
    quantiles: List[float] = [0.05, 0.5, 0.95]
    # args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap  # noqa
    # kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap  # noqa


class Bootstrappers:
    """Container class for bootstrap sampling classes."""

    GumBoots = GumBootsModel
    CircularBlock = CircularBlockModel
    Stationary = StationaryModel
