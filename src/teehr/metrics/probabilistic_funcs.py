"""Functions for probabilistic metric calculations in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np
import scoringrules as sr

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.metrics.deterministic_funcs import _transform

logger = logging.getLogger(__name__)


def _pivot_by_value_time(
    p: pd.Series,
    s: pd.Series,
    value_time: pd.Series
) -> Dict:
    """Pivot the timeseries data by value_time.

    Notes
    -----
    prim_arr: A 1-D array of observations at each time step in the forecast.

    sec_arr: A 2-D array of simulations at each time step in the forecast.
            The first dimension should be the time step, and the second
            dimension should be the ensemble member.
    """
    unique_value_times = value_time.unique()
    n_timesteps = len(unique_value_times)
    inds = np.array([np.where(value_time == unique_value_times[i])[0] for i in range(n_timesteps)])

    # If the number of ensemble members is 1, we need to
    # flatten the array to a 1-D array.
    # Otherwise, we need to keep it as a 2-D array.
    if inds.shape[1] == 1:
        return {
            "primary": p.values[inds[:, 0]],
            "secondary": s.values[inds].ravel()
        }
    return {
        "primary": p.values[inds[:, 0]],
        "secondary": s.values[inds]
    }


def create_crps_func(model: MetricsBasemodel) -> Callable:
    """Create the CRPS ensemble metric function."""
    logger.debug("Building the CRPS ensemble metric func.")

    def ensemble_crps(
        p: pd.Series,
        s: pd.Series,
        value_time: pd.Series,
    ) -> float:
        """Create a wrapper around scoringrules crps_ensemble.

        Parameters
        ----------
        p : pd.Series
            The primary values.
        s : pd.Series
            The secondary values.
        value_time : pd.Series
            The value time.

        Returns
        -------
        float
            The mean Continuous Ranked Probability Score (CRPS) for the
            ensemble, either as a single value or array of values.
        """
        p, s, value_time = _transform(p, s, model, value_time)
        pivoted_dict = _pivot_by_value_time(p, s, value_time)

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

    return ensemble_crps
