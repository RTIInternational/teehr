"""Bootstrap classes."""
from typing import Any, Callable, Union
from collections.abc import Generator as PyGenerator
from pathlib import Path
import logging

from arch.bootstrap import IIDBootstrap
from arch.bootstrap.base import (
    # _loo_jackknife, # leave one out jackknife
    _add_extra_kwargs
)
from arch.typing import ArrayLike, Int64Array, Float64Array
from numpy.random import Generator, RandomState
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# FILL_VALUE = -9999  # JAB


class GumbootBootstrap(IIDBootstrap):
    """Python implementation of the Gumboot R package."""

    def __init__(
        self,
        *args: ArrayLike,
        value_time: pd.Series,
        water_year_month: Union[int, None],
        boot_year_file: Union[str, Path, None],
        seed: Union[int, Generator, RandomState, None],
        **kwargs: ArrayLike,
    ) -> None:
        """Initialize the Gumboot class, inheriting from IIDBootstrap."""
        super().__init__(*args, seed=seed, **kwargs)

        # JAB
        # self.boot_year_array: Union[np.array, None]

        logger.debug("Initializing the Gumboot water years.")

        value_time = pd.to_datetime(value_time)
        water_years = np.where(
            value_time.dt.month >= water_year_month,
            value_time.dt.year + 1,
            value_time.dt.year
        )  # (iyWater)

        self.unique_water_years = np.unique(water_years)
        self.num_water_years = self.unique_water_years.size
        self.water_year_array = water_years

        if boot_year_file:
            self.user_defined_boot_years = True
            self.boot_year_array = np.genfromtxt(
                boot_year_file, delimiter=",", dtype=int
            )
            if np.in1d(
                np.unique(self.boot_year_array),
                self.unique_water_years
            ).all() is False:
                logger.error(
                    "Invalid boot year file: The provided boot year file "
                    "contains years outside of the range of the timeseries "
                    "data."
                )
        else:
            self.user_defined_boot_years = False

    def update_indices(
        self,
        rep: int
    ) -> Int64Array:  # type: ignore
        """
        Gumboot resampling implementation.

        Notes
        -----
        This requires an extra field (value_time) to create indices based on
        water year (time).
        """
        logger.debug(f"Resampling the Gumboot water years. rep: {rep}")

        if rep == 0:
            jx_valid = np.arange(len(self._args[0]))
        else:
            if self.user_defined_boot_years:
                years = self.boot_year_array[:, rep - 1]
            else:
                year_indexes = self._generator.integers(
                    self.num_water_years, size=self.num_water_years
                )
                years = np.array(self.unique_water_years)[year_indexes]
                # self.boot_year_array[:, rep - 1] = years  # JAB

            # Get the indexes of values corresponding to the specified
            # water years, including duplicate indexes for repeating years.
            inds = [
                np.where(self.water_year_array == year)[0] for year in years
            ]
            jx_valid = np.hstack(inds)

        return jx_valid

    def apply(
        self,
        func: Callable[..., ArrayLike],
        reps: int,
        extra_kwargs: dict[str, Any] | None = None,
    ) -> Float64Array:  # type: ignore
        """
        Apply a function to bootstrap replicated data.

        Parameters
        ----------
        func : callable
            Function the computes parameter values.  See Notes for requirements
        reps : int, default 1000
            Number of bootstrap replications
        extra_kwargs : dict, default None
            Extra keyword arguments to use when calling func.  Must not
            conflict with keyword arguments used to initialize bootstrap

        Returns
        -------
        ndarray
            reps by nparam array of computed function values where each row
            corresponds to a bootstrap iteration
        """
        # JAB:
        # if not self.user_defined_boot_years:
        #     self.boot_year_array = np.full(
        #         (self.num_water_years, reps), FILL_VALUE, np.int32
        #     )

        logger.debug(f"Apply the Gumboot bootstrap method over {reps} reps.")

        reps = reps + 1

        kwargs = _add_extra_kwargs(self._kwargs, extra_kwargs)
        base = func(*self._args, **kwargs)
        try:
            num_params = base.shape[0]
        except (IndexError, AttributeError):
            num_params = 1
        results = np.zeros((reps, num_params))
        count = 0
        for pos_data, kw_data in self.bootstrap(reps):
            kwargs = _add_extra_kwargs(kw_data, extra_kwargs)
            results[count] = func(*pos_data, **kwargs)
            count += 1

        # JAB calculations here?

        return results

    def bootstrap(
        self, reps: int
    ) -> PyGenerator[
            tuple[tuple[ArrayLike, ...], dict[str, ArrayLike]],
            None,
            None
    ]:
        """
        Create iterator for use when bootstrapping.

        Parameters
        ----------
        reps : int
            Number of bootstrap replications

        Returns
        -------
        generator
            Generator to iterate over in bootstrap calculations

        Notes
        -----
        The iterator returns a tuple containing the data entered in positional
        arguments as a tuple and the data entered using keywords as a
        dictionary
        """
        for rep in range(reps):
            self._index = self.update_indices(rep)
            yield self._resample()
