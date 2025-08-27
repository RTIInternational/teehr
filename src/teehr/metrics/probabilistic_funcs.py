"""Functions for probabilistic metric calculations in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np

from teehr.models.metrics.basemodels import MetricsBasemodel
# from teehr.metrics.deterministic_funcs import _transform

logger = logging.getLogger(__name__)


def _pivot_by_member(
    p: pd.Series,
    s: pd.Series,
    members: pd.Series
) -> Dict:
    """Pivot the timeseries data by members.

    Notes
    -----
    prim_arr: A 1-D array of observations at each time step in the forecast.

    sec_arr: A 2-D array of simulations at each time step in the forecast.
            The first dimension should be the time step, and the second
            dimension should be the ensemble member.
    """
    if members.isna().all():  # No ensemble members
        return {
            "primary": p.values,
            "secondary": s.values
        }
    unique_members, member_indices = np.unique(
        members.values, return_inverse=True
    )
    if unique_members.size == 1:  # Only one ensemble member
        return {
            "primary": p.values,
            "secondary": s.values
        }
    # Assumes all members are same length.
    forecast_length = member_indices[member_indices == member_indices[0]].size
    n_members = unique_members.size
    secondary_arr = np.full((forecast_length, n_members), np.nan)
    for i in range(n_members):
        mask = (member_indices == i)
        secondary_arr[:, i] = s[mask]

    return {
        "primary": p.values[member_indices == 0],
        "secondary": secondary_arr
    }


def ensemble_crps(model: MetricsBasemodel) -> Callable:
    """Create the CRPS ensemble metric function."""
    logger.debug("Building the CRPS ensemble metric func.")

    def ensemble_crps_inner(
        p: pd.Series,
        s: pd.Series,
        members: pd.Series,
    ) -> float:
        """Create a wrapper around scoringrules crps_ensemble.

        Parameters
        ----------
        p : pd.Series
            The primary values.
        s : pd.Series
            The secondary values.
        members : pd.Series
            The member IDs.

        Returns
        -------
        float
            The mean Continuous Ranked Probability Score (CRPS) for the
            ensemble, either as a single value or array of values.
        """
        # lazy load scoringrules
        import scoringrules as sr

        # p, s, value_time = _transform(p, s, model, value_time)
        # pivoted_dict = _pivot_by_value_time(p, s, value_time)
        pivoted_dict = _pivot_by_member(p, s, members)

        if model.summary_func is not None:
            return model.summary_func(
                sr.crps_ensemble(
                    pivoted_dict["primary"],
                    pivoted_dict["secondary"],
                    estimator=model.estimator,
                    backend=model.backend
                )
            )
        else:
            return sr.crps_ensemble(
                pivoted_dict["primary"],
                pivoted_dict["secondary"],
                estimator=model.estimator,
                backend=model.backend
            )

    return ensemble_crps_inner
