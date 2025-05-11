"""Functions for probabilistic metric calculations in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np
import scoringrules as sr

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.metrics.deterministic_funcs import _transform

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
    unique_members = members.unique()
    n_members = len(unique_members)
    # Get the indices of the unique member IDs
    inds = np.array([np.where(members == unique_members[i])[0] for i in range(n_members)]).T
    if members.isna().all():  # No ensemble members
        return {
            "primary": p.values,
            "secondary": s.values
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
        p, s, members = _transform(p, s, model, members)
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

    return ensemble_crps
