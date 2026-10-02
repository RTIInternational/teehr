"""Functions for probabilistic metric calculations in Spark queries."""
from typing import Dict, Callable
import logging

import pandas as pd
import numpy as np

from teehr.metrics.models.base import (
    MetricsBasemodel,
    SUMMARY_STATISTIC_FUNCS,
)

logger = logging.getLogger(__name__)


def _pivot_by_member(
    p: pd.Series,
    s: pd.Series,
    members: pd.Series,
    reference_time: pd.Series,
    value_time: pd.Series,
) -> Dict:
    """Pivot the timeseries data by members.

    Rows are aligned on (reference_time, value_time), since Spark does not
    guarantee row order within a group.

    Notes
    -----
    prim_arr: A 1-D array of observations at each time step in the forecast.

    sec_arr: A 2-D array of simulations at each time step in the forecast.
            The first dimension should be the time step, and the second
            dimension should be the ensemble member.
    """
    if members.isna().all() or members.nunique() == 1:
        # No ensemble members, or only one
        return {
            "primary": p.values,
            "secondary": s.values
        }
    # Hash-based factorize is much faster here than np.unique or df.pivot.
    m_idx, m_uniques = pd.factorize(
        members.values, sort=True, use_na_sentinel=False
    )
    rt_idx, _ = pd.factorize(
        reference_time.values, sort=True, use_na_sentinel=False
    )
    vt_idx, vt_uniques = pd.factorize(
        value_time.values, sort=True, use_na_sentinel=False
    )
    t_idx, t_uniques = pd.factorize(
        rt_idx.astype("int64") * vt_uniques.size + vt_idx, sort=True
    )
    n_m = m_uniques.size
    if np.bincount(t_idx * n_m + m_idx).max() > 1:
        raise ValueError(
            "Duplicate (reference_time, value_time, member) rows in group."
        )
    secondary = np.full((t_uniques.size, n_m), np.nan)
    secondary[t_idx, m_idx] = s.values
    primary = np.full(t_uniques.size, np.nan)
    primary[t_idx] = p.values
    return {
        "primary": primary,
        "secondary": secondary
    }


def _summarize(model: MetricsBasemodel, scores: np.ndarray):
    """Summarize per-time-step scores per the model's summary settings."""
    if model.summary_func is not None:
        return model.summary_func(scores)
    if model.summary_statistic is None:
        return scores
    return SUMMARY_STATISTIC_FUNCS[model.summary_statistic](scores)


def ensemble_crps(model: MetricsBasemodel) -> Callable:
    """Create the CRPS ensemble metric function."""
    logger.debug("Building the CRPS ensemble metric func.")

    def ensemble_crps_inner(
        p: pd.Series,
        s: pd.Series,
        members: pd.Series,
        reference_time: pd.Series,
        value_time: pd.Series,
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
        reference_time : pd.Series
            The reference times, used to align members.
        value_time : pd.Series
            The value times, used to align members.

        Returns
        -------
        float
            The mean Continuous Ranked Probability Score (CRPS) for the
            ensemble, either as a single value or array of values.
        """
        # lazy load scoringrules
        import scoringrules as sr

        pivoted_dict = _pivot_by_member(
            p, s, members, reference_time, value_time
        )
        obs = pivoted_dict["primary"]
        fct = pivoted_dict["secondary"]

        if fct.ndim == 1:
            # CRPS of a deterministic forecast is the absolute error
            return _summarize(model, np.abs(fct - obs))
        return _summarize(
            model,
            sr.crps_ensemble(
                obs, fct, estimator=model.estimator, backend=model.backend
            )
        )

    return ensemble_crps_inner


def _get_brier_score_inputs(pivoted_dict: dict,
                            threshold: float) -> dict:
    """Obtain inputs for scoringrules.brier_score from pivoted dict."""
    # get quantile flow
    p = pivoted_dict['primary']
    q_threshold = np.quantile(p, threshold)

    # get binary outcomes of observed exceeding threshold
    binary_p = np.where(p >= q_threshold, 1, 0)

    # get fraction of ensemble members exceeding threshold for each time step
    s = pivoted_dict['secondary']
    binary_s = np.where(s >= q_threshold, 1, 0)
    if len(binary_s.shape) == 1:
        # only one ensemble member
        frac_exceeds_s = binary_s
    else:
        frac_exceeds_s = np.mean(binary_s, axis=1)

    # assemble inputs dict
    brier_score_inputs = {
        'primary': binary_p,
        'secondary': frac_exceeds_s
    }

    return brier_score_inputs


def ensemble_brier_score(model: MetricsBasemodel) -> Callable:
    """Create the Brier Score ensemble metric function."""
    logger.debug("Building the Brier Score ensemble metric func.")

    def ensemble_brier_score_inner(
        p: pd.Series,
        s: pd.Series,
        members: pd.Series,
        reference_time: pd.Series,
        value_time: pd.Series,
    ) -> float:
        """Create a wrapper around scoringrules brier_score.

        Parameters
        ----------
        p : pd.Series
            The primary values.
        s : pd.Series
            The secondary values.
        members : pd.Series
            The member IDs.
        reference_time : pd.Series
            The reference times, used to align members.
        value_time : pd.Series
            The value times, used to align members.

        Returns
        -------
        float
            The mean Brier Score for the ensemble, either as a single value
            or array of values.
        """
        # lazy load scoringrules
        import scoringrules as sr

        pivoted_dict = _pivot_by_member(
            p, s, members, reference_time, value_time
        )
        bs_inputs = _get_brier_score_inputs(pivoted_dict, model.threshold)
        return _summarize(
            model,
            sr.brier_score(
                bs_inputs["primary"],
                bs_inputs["secondary"],
                backend=model.backend
            )
        )

    return ensemble_brier_score_inner
