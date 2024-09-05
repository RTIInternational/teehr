"""Bootstrap classes."""
from typing import Any, Callable
from collections.abc import Generator as PyGenerator

# from arch.bootstrap import IIDBootstrap
from arch.typing import ArrayLike, Int64Array, Float64Array
from numpy.random import Generator, RandomState
import numpy as np
import pandas as pd


def _get_random_integers(
    prng: Generator | RandomState, upper: int, *, size: int = 1
) -> Int64Array:  # type: ignore
    """From the arch package."""
    if isinstance(prng, Generator):
        return prng.integers(upper, size=size, dtype=np.int64)
    else:
        assert isinstance(prng, RandomState)
        return prng.randint(upper, size=size, dtype=np.int64)


def _add_extra_kwargs(
    kwargs: dict[str, Any], extra_kwargs: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Safely add additional keyword arguments to an existing dictionary.

    Parameters
    ----------
    kwargs : dict
        Keyword argument dictionary
    extra_kwargs : dict, default None
        Keyword argument dictionary to add

    Returns
    -------
    dict
        Keyword dictionary with added keyword arguments

    Notes
    -----
    There is no checking for duplicate keys
    """
    if extra_kwargs is None:
        return kwargs
    else:
        kwargs_copy = kwargs.copy()
        kwargs_copy.update(extra_kwargs)
        return kwargs_copy


class GumbootBootstrap:
    """Python implementation the Gumboot R package."""

    def __init__(
        self,
        block_size: int,
        p: pd.Series,
        s: pd.Series,
        reps: int = 1000,
        water_year_month: int = 10,
        start_year: int = None,
        end_year: int = None,
        min_days: int = 100,
        min_years: int = 10,
        return_samples: bool = False,
        boot_year_file: str = None,
        *args: ArrayLike,
        random_state: RandomState | None = None,
        seed: None | int | Generator | RandomState = None,
        **kwargs: ArrayLike,
    ) -> None:
        """Initialize the Gumboot class."""
        self.block_size: int = block_size
        self._parameters = [block_size]
        self.reps: int = reps

        # Define water years.
        # Get valid number of years, num_valid_years (ie, 19)
        # self.num_valid_years = num_valid_years

        # Get indices where obs and sim values are valid

    def update_bootstrap_indices(
        self,
        time_field_name: str
    ) -> Int64Array:  # type: ignore
        """
        TODO: This will be the Gumboot implementation.

        Notes
        -----
        This requires an extra field (value_time) to create indices based on
        water year (time).
        """


        # m_sample = self.reps + 1
        # Loop through sample





        pass


    def update_jackknife_indices(
        self,
        time_field_name: str
    ) -> Int64Array:  # type: ignore
        """
        TODO: This will be the Gumboot implementation.

        Notes
        -----
        This requires an extra field (value_time) to create indices based on
        water year (time).
        """

        # m_sample = self.num_valid_years
        # Loop through sample

        pass

    def apply(
        self,
        func: Callable[..., ArrayLike],
        reps: int = 1000,
        time_field_name: str = "value_time",
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
        kwargs = _add_extra_kwargs(self._kwargs, extra_kwargs)
        base = func(*self._args, **kwargs)
        try:
            num_params = base.shape[0]
        except (IndexError, AttributeError):
            num_params = 1
        results = np.zeros((reps, num_params))
        count = 0
        for pos_data, kw_data in self.bootstrap(reps, time_field_name):
            kwargs = _add_extra_kwargs(kw_data, extra_kwargs)
            results[count] = func(*pos_data, **kwargs)
            count += 1
        return results

    # def bootstrap(
    #     self, reps: int, time_field_name: str = "value_time"
    # ) -> PyGenerator[
    #         tuple[tuple[ArrayLike, ...], dict[str, ArrayLike]],
    #         None,
    #         None
    # ]:
    #     """
    #     Create iterator for use when bootstrapping.

    #     Parameters
    #     ----------
    #     reps : int
    #         Number of bootstrap replications

    #     Returns
    #     -------
    #     generator
    #         Generator to iterate over in bootstrap calculations

    #     Notes
    #     -----
    #     The iterator returns a tuple containing the data entered in positional
    #     arguments as a tuple and the data entered using keywords as a
    #     dictionary
    #     """
    #     for _ in range(reps):
    #         self._index = self.update_indices(time_field_name)
    #         yield self._resample()
