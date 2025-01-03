"""Functions for probabilistic metric calculations in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np
import scoringrules as sr

from teehr.models.metrics.basemodels import MetricsBasemodel

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
    # TODO: Probably a better way to do this?
    primary = []
    secondary = []
    for vt in value_time.unique():
        vt_index = value_time[value_time == vt].index
        primary.append(p[vt_index].values[0])
        secondary.append(s[vt_index].values)
    return {"primary": np.array(primary), "secondary": np.array(secondary)}


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
