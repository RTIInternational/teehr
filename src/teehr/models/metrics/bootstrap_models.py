"""Classes for bootstrapping sampling methods."""
from typing import Callable, List, Union
from pathlib import Path

from numpy.random import RandomState
from pydantic import Field

import teehr.metrics.bootstrap_funcs as bootstrap_funcs
from teehr.models.metrics.metric_models import MetricsBasemodel


# TODO: Extend an abstract base class for bootstrapping classes to ensure
# that the necessary fields are included?  For Metrics too?
class GumbootModel(MetricsBasemodel):
    """Gumboot bootstrapping.

    This is a partial implementation of the Gumboot R package, a
    non-overlapping bootstrap method where blocks are defined by water
    years. Synthetic timeseries are constructed by randomly resampling water
    years from the input timeseries with replacement. The specified performance
    metric is calculated for each synthetic timeseries for a number of
    bootstrap replications (reps). The quantiles of the bootstrap metric
    results are calculated and returned.

    If the quantile values are not specified or are set to None, the array
    of metric values is returned (dimensions: [reps, 1]). Otherwise the
    specified quantiles of the metric values are returned as a dictionary.

    See Also:  Clark et al. (2021), "The abuse of popular performance metrics
      in hydrologic modeling", Water Resources Research,
      <doi:10.1029/2020WR029001>

      https://cran.r-project.org/web/packages/gumboot/gumboot.pdf

    Parameters
    ----------
    reps : int
        The number of bootstrap replications. Default value is 1000.
    seed : Union[int, None]
        The seed for the random number generator. Setting a seed value can be
        used to provide reproducible results. Default value is None.
    quantiles : Union[List[float], None]
        The quantiles to calculate from the bootstrap metric results. The
        default value is None.
    boot_year_file : Union[str, Path, None]
        The file path to the boot year csv file. The default value is None.
    water_year_month : int
        The month specifying the start of the water year. Default value is 10.
    name : str
        The name of the bootstrap method. Currently only used in
        logging. Default value is "Gumboot".
    include_value_time : bool, fixed
        Whether to include the value_time series in the bootstrapping
        function. Default value is True.
    func : Callable, fixed
        The wrapper to generate the bootstrapping function. Default value is
        bootstrap_funcs.create_gumboot_func.

    """

    reps: int = 1000
    seed: Union[int, None] = None
    quantiles: Union[List[float], None] = None
    boot_year_file: Union[str, Path, None] = None
    water_year_month: int = 10
    name: str = Field(default="Gumboot")
    include_value_time: bool = Field(True, frozen=True)
    func: Callable = Field(bootstrap_funcs.create_gumboot_func, frozen=True)


class CircularBlockModel(MetricsBasemodel):
    """CircularBlock bootstrapping from the arch python package.

    Parameters
    ----------
    seed : Union[int, None]
        The seed for the random number generator. Default value is None.
    random_state : RandomState, optional
        The random state for the random number generator.
    reps : int
        The number of bootstrap replications.
    block_size : int
        The block size for the CircularBlockBootstrap.
    quantiles : List[float]
        The quantiles to calculate from the bootstrap results. Default
        value is None.
    name : str
        The name of the bootstrap method. Currently only used in
        logging. Default value is "CircularBlock".
    include_value_time : bool, fixed
        Whether to include the value_time series in the bootstrapping
        function. Default value is True.
    func : Callable, fixed
        The wrapper to generate the bootstrapping function.
    """

    seed: Union[int, None] = None
    random_state: Union[RandomState, None] = None
    reps: int = 1000
    block_size: int = 365
    quantiles: Union[List[float], None] = None
    name: str = Field(default="CircularBlock")
    include_value_time: bool = Field(False, frozen=True)
    func: Callable = Field(
        bootstrap_funcs.create_circularblock_func,
        frozen=True
    )


class StationaryModel(MetricsBasemodel):
    """Stationary bootstrapping from the arch python package.

    Parameters
    ----------
    seed : Union[int, None]
        The seed for the random number generator. Default value is 42.
    random_state : RandomState, optional
        The random state for the random number generator.
    reps : int
        The number of bootstrap replications.
    block_size : int
        The block size for the StationaryBootstrap.
    quantiles : List[float]
        The quantiles to calculate from the bootstrap results. Default
        value is None.
    name : str
        The name of the bootstrap method. Currently only used in
        logging. Default value is "Stationary".
    include_value_time : bool, fixed
        Whether to include the value_time series in the bootstrapping
        function. Default value is True.
    func : Callable, fixed
        The wrapper to generate the bootstrapping function.
    """

    seed: Union[int, None] = None
    random_state: Union[RandomState, None] = None
    reps: int = 1000
    block_size: int = 365
    quantiles: Union[List[float], None] = None
    name: str = Field(default="Stationary")
    include_value_time: bool = Field(False, frozen=True)
    func: Callable = Field(
        bootstrap_funcs.create_stationary_func,
        frozen=True
    )


class Bootstrappers:
    """Container class for bootstrap sampling classes."""

    Gumboot = GumbootModel
    CircularBlock = CircularBlockModel
    Stationary = StationaryModel
