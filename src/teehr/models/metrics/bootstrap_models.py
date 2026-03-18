"""Classes for bootstrapping sampling methods."""
from typing import Callable, Union
from pathlib import Path

from numpy.random import RandomState
from pydantic import Field

import teehr.metrics.bootstrap_funcs as bootstrap_funcs
from teehr.models.metrics.basemodels import BootstrapBasemodel


class Gumboot(BootstrapBasemodel):
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
    boot_year_file : Union[str, Path, None]
        The file path to the boot year csv file. The default value is None.
    water_year_month : int
        The month specifying the start of the water year. Default value is 10.
    """

    boot_year_file: Union[str, Path, None] = None
    water_year_month: int = 10
    name: str = Field(default="Gumboot")
    include_value_time: bool = Field(True, frozen=True)
    func: Callable = Field(bootstrap_funcs.create_gumboot_func, frozen=True)


class CircularBlock(BootstrapBasemodel):
    """CircularBlock bootstrapping from the arch python package.

    Parameters
    ----------
    random_state : RandomState, optional
        The random state for the random number generator.
    block_size : int
        The block size for the CircularBlockBootstrap.
        Default value is 365.
    """

    random_state: Union[RandomState, None] = None
    block_size: int = 365
    name: str = Field(default="CircularBlock")
    include_value_time: bool = Field(False, frozen=True)
    func: Callable = Field(
        bootstrap_funcs.create_circularblock_func,
        frozen=True
    )


class Stationary(BootstrapBasemodel):
    """Stationary bootstrapping from the arch python package.

    Parameters
    ----------
    random_state : RandomState, optional
        The random state for the random number generator.
    block_size : int
        The block size for the StationaryBootstrap.
        Default value is 365.
    """

    random_state: Union[RandomState, None] = None
    block_size: int = 365
    name: str = Field(default="Stationary")
    include_value_time: bool = Field(False, frozen=True)
    func: Callable = Field(
        bootstrap_funcs.create_stationary_func,
        frozen=True
    )


class Bootstrappers:
    """Container class for bootstrap sampling classes.

    Notes
    -----
    Bootstrapping is a resampling method used to estimate uncertainty
    in metric results. The bootstrapping methods available in TEEHR
    include:

    - Gumboot
    - CircularBlock
    - Stationary
    """

    Gumboot = Gumboot
    CircularBlock = CircularBlock
    Stationary = Stationary
