"""Functions for probabilistic metric calculations in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np
import scoringrules as sr

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.metrics.basemodels import TransformEnum

logger = logging.getLogger(__name__)


def _transform(
        p: pd.Series,
        s: pd.Series,
        t: pd.Series | None,
        model: MetricsBasemodel
) -> tuple:
    """Apply timeseries transform for metrics calculations."""
    # Apply transform
    if model.transform is not None:
        match model.transform:
            case TransformEnum.log:
                logger.debug("Applying log transform")
                p = np.log(p)
                s = np.log(s)
            case TransformEnum.sqrt:
                logger.debug("Applying square root transform")
                p = np.sqrt(p)
                s = np.sqrt(s)
            case TransformEnum.square:
                logger.debug("Applying square transform")
                p = np.square(p)
                s = np.square(s)
            case TransformEnum.cube:
                logger.debug("Applying cube transform")
                p = np.power(p, 3)
                s = np.power(s, 3)
            case TransformEnum.exp:
                logger.debug("Applying exponential transform")
                p = np.exp(p)
                s = np.exp(s)
            case TransformEnum.inv:
                logger.debug("Applying inverse transform")
                p = 1.0 / p
                s = 1.0 / s
            case TransformEnum.abs:
                logger.debug("Applying absolute value transform")
                p = np.abs(p)
                s = np.abs(s)
            case _:
                raise ValueError(
                    f"Unsupported transform: {model.transform}"
                )
    else:
        logger.debug("No transform specified, using original values")

    # Remove invalid values and align series
    if t is not None:
        valid_mask = np.isfinite(p) & np.isfinite(s)
        p = p[valid_mask]
        s = s[valid_mask]
        t = t[valid_mask]
    else:
        valid_mask = np.isfinite(p) & np.isfinite(s)
        p = p[valid_mask]
        s = s[valid_mask]

    if t is not None:
        return p, s, t
    else:
        return p, s


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
        p, s, value_time = _transform(p, s, value_time, model)
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
