"""Classes for probabilistic metric calculation methods."""
from typing import Callable, Dict

from pydantic import Field

from teehr.models.metrics.basemodels import ProbabilisticBasemodel
from teehr.metrics import probabilistic_funcs
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.basemodels import CRPSEstimators


class CRPS(ProbabilisticBasemodel):
    """Continous Ranked Probability Score - Ensemble.

    Parameters
    ----------
    estimator : str
        CRPS estimator, can be ("pwm", "nrg", or "fair"). Default is "pwm".
    output_field_name : str
        The output field name, by default "mean_crps_ensemble".
    func : Callable
        The function to apply to the data, by default
        :func:`probabilistic_funcs.ensemble_crps`.
    attrs : Dict
        The static attributes for the metric.
    reference_configuration : str
        The reference configuration for skill score calculation, by default None.
        If specified skill score will be included in the results.
    """

    estimator: CRPSEstimators = Field(default="pwm")
    output_field_name: str = Field(default="mean_crps_ensemble")
    func: Callable = Field(probabilistic_funcs.ensemble_crps, frozen=True)
    attrs: Dict = Field(default=tma.CRPS_ENSEMBLE_ATTRS, frozen=True)


class BrierScore(ProbabilisticBasemodel):
    """Brier Score for ensemble probabilistic forecasts.

    Parameters
    ----------
    threshold : float
        The threshold to use for binary event definition.
    output_field_name : str
        The output field name, by default "mean_brier_score".
    func : Callable
        The function to apply to the data, by default
        :func:`probabilistic_funcs.ensemble_brier_score`.
    attrs : Dict
        The static attributes for the metric.
    reference_configuration : str
        The reference configuration for skill score calculation, by default None.
        If specified skill score will be included in the results.
    """

    threshold: float = Field(default=0.75)
    output_field_name: str = Field(default="mean_brier_score")
    func: Callable = Field(probabilistic_funcs.ensemble_brier_score, frozen=True)
    attrs: Dict = Field(default=tma.BS_ENSEMBLE_ATTRS, frozen=True)


class ProbabilisticMetrics:
    """Define and customize probalistic metrics.

    Notes
    -----
    Probabilistic metrics compare a value against a distribution of predicted
    values, such as ensemble forecasts. Available probabilistic metrics
    include:

    - CRPS (Continuous Ranked Probability Score)
    - Brier Score

    Skill score metrics (CRPSS and BSS) are enabled by setting the ``reference_configuration``
    attribute to a valid reference forecast, such as "climatology" or "persistence".

    See :class:`teehr.evaluation.generate.GeneratedTimeseries` for more information
    on generating reference forecasts.
    """

    CRPS = CRPS
    BrierScore = BrierScore
