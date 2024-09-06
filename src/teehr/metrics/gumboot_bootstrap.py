"""Bootstrap classes."""
from typing import Any, Callable, cast, Union
from collections.abc import Generator as PyGenerator
from pathlib import Path

from arch.bootstrap import IIDBootstrap
from arch.typing import ArrayLike, Int64Array, Float64Array, NDArray
from numpy.random import Generator, RandomState
import numpy as np
import pandas as pd

ZERO_VAL = -1e-10


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


class GumbootBootstrap(IIDBootstrap):
    """Python implementation the Gumboot R package."""

    def __init__(
        self,
        *args: ArrayLike,
        value_time: pd.Series,
        water_year_month: Union[int, None] = None,
        start_year: Union[int, None] = None,
        end_year: Union[int, None] = None,
        min_days: Union[int, None] = None,
        min_years: Union[int, None] = None,
        boot_year_file: Union[str, Path, None] = None,
        random_state: Union[RandomState, None] = None,
        seed: Union[int, Generator, RandomState, None] = None,
        **kwargs: ArrayLike,
    ) -> None:
        """Initialize the Gumboot class, inheriting from IIDBootstrap."""
        super().__init__(*args, random_state=random_state, seed=seed, **kwargs)

        p, s = self._args  # for preprocessing

        self.boot_year_array: Union[np.array, None] = None

        # Define water years.
        flows_df = pd.DataFrame(
            {
                "primary_value": p,
                "secondary_value": s,
                "value_time": pd.to_datetime(value_time),
            }
        )

        # NOTE: Do we want all this pre-processing here?
        flows_df["year"] = flows_df.value_time.dt.year
        flows_df["month"] = flows_df.value_time.dt.month
        flows_df["day"] = flows_df.value_time.dt.day

        # unique_years = np.unique(flows_df.year)

        water_years = np.where(
            flows_df.month >= water_year_month,
            flows_df.year + 1,
            flows_df.year
        ) # (iyWater)
        # num_water_years = np.unique(water_years).size # (nYears)

        flows_df["water_year"] = water_years

        # Get indices where primary and secondary values are valid (ixValid).
        valid_value_indices = np.where((p > ZERO_VAL) & (s > ZERO_VAL))[0]

        # Need to get a list of water years that contain the minimum
        #  number of valid values, and the total number of valid years.
        valid_flows_df = flows_df.iloc[valid_value_indices].copy()

        if not start_year:
            start_year = valid_flows_df.water_year.min()
        if not end_year:
            end_year = valid_flows_df.water_year.max()

        # TODO: What about hourly?
        # For each year, make sure it contains the minimum number of days,
        #  and is within the specified range of start and end years.
        gp = valid_flows_df.groupby("water_year")
        valid_df_list = []
        valid_water_years = []
        for yr, df in gp:
            if (df.value_time.dt.day_of_year.unique().size >= min_days) & \
                    (yr >= start_year) & (yr <= end_year):
                valid_df_list.append(df)
                valid_water_years.append(yr)

        valid_water_years_df = pd.concat(valid_df_list)
        num_valid_years = len(valid_water_years)  # (nyValid)

        if num_valid_years < min_years:
            raise ValueError(
                "Not enough valid years to perform bootstrapping."
            )

        # Store the values.
        self.valid_water_years = valid_water_years
        self.valid_value_indices = valid_value_indices
        self.num_valid_years = num_valid_years
        self.water_year_array = valid_water_years_df.water_year.values

        if boot_year_file:
            self.boot_year_array = np.genfromtxt(
                boot_year_file, delimiter=",", dtype=int
            )

    def update_indices(
        self,
        rep: int
    ) -> Int64Array:  # type: ignore
        """
        The Gumboot resampling implementation.

        Notes
        -----
        This requires an extra field (value_time) to create indices based on
        water year (time).
        """
        rng = np.random.default_rng()

        if rep == 0:
            jx_valid = self.valid_value_indices
        else:
            if self.boot_year_array is not None:
                years = self.boot_year_array[:, rep - 1]
            else:
                year_indexes = rng.integers(
                    self.num_valid_years, size=self.num_valid_years
                )
                years = np.array(self.valid_water_years)[year_indexes]

            # Get the indexes of values corresponding to the randomly selected
            # water years, including duplicate indexes for repeating years.
            inds = [np.where(self.water_year_array == year)[0] for year in years]
            jx_valid = np.hstack(inds)

        return jx_valid


    # def update_jackknife_indices(
    #     self,
    #     time_field_name: str
    # ) -> Int64Array:  # type: ignore
    #     """
    #     TODO: This will be the Gumboot implementation.

    #     Notes
    #     -----
    #     This requires an extra field (value_time) to create indices based on
    #     water year (time).
    #     """

    #     # m_sample = self.num_valid_years
    #     # Loop through sample

    #     pass

    def apply(
        self,
        func: Callable[..., ArrayLike],
        reps: int = 1000,
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
