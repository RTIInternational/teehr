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
    """GumBoots bootstrapping.

    Parameters
    ----------
    func : Callable
        The wrapper to generate the bootstrapping function.
    seed : int
        The seed for the random number generator.
    random_state : RandomState, optional
        The random state for the random number generator.
    reps : int
        The number of bootstrap replications.
    block_size : int
        The block size for the GumBootsBootstrap.
    quantiles : List[float]
        The quantiles to calculate from the bootstrap results
    """

    func: Callable = bootstrap_funcs.create_gumboots_func
    reps: int = 1000
    seed: int = 42
    block_size: int = 365
    random_state: RandomState | None = None
    quantiles: List[float] = [0.05, 0.5, 0.95]
    name: str = Field(default="GumBoots")
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
    """CircularBlock bootstrapping from the arch package.

    Parameters
    ----------
    func : Callable
        The wrapper to generate the bootstrapping function.
    seed : int
        The seed for the random number generator.
    random_state : RandomState, optional
        The random state for the random number generator.
    reps : int
        The number of bootstrap replications.
    block_size : int
        The block size for the CircularBlockBootstrap.
    quantiles : List[float]
        The quantiles to calculate from the bootstrap results
    """

    func: Callable = bootstrap_funcs.create_circularblock_func
    seed: int = 42
    random_state: RandomState | None = None
    reps: int = 1000
    block_size: int = 365
    quantiles: List[float] = [0.05, 0.5, 0.95]
    name: str = Field(default="CircularBlock")
    # args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap  # noqa
    # kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap  # noqa


class StationaryModel(MetricsBasemodel):
    """Stationary bootstrapping from the arch package.

    Parameters
    ----------
    func : Callable
        The wrapper to generate the bootstrapping function.
    seed : int
        The seed for the random number generator.
    random_state : RandomState, optional
        The random state for the random number generator.
    reps : int
        The number of bootstrap replications.
    block_size : int
        The block size for the StationaryBootstrap.
    quantiles : List[float]
        The quantiles to calculate from the bootstrap results
    """

    func: Callable = bootstrap_funcs.create_stationary_func
    seed: int = 42
    random_state: RandomState | None = None
    reps: int = 1000
    block_size: int = 365
    quantiles: List[float] = [0.05, 0.5, 0.95]
    name: str = Field(default="Stationary")
    # args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap  # noqa
    # kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap  # noqa


class Bootstrappers:
    """Container class for bootstrap sampling classes."""

    GumBoots = GumBootsModel
    CircularBlock = CircularBlockModel
    Stationary = StationaryModel
