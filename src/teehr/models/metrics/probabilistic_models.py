"""Classes for probabilistic metric calculation methods."""
from typing import Callable, List, Union, Dict

from pydantic import Field

from teehr.models.metrics.basemodels import ProbabilisticBasemodel
from teehr.metrics import probabilistic_funcs
import teehr.models.metrics.metric_attributes as tma
from teehr.models.metrics.basemodels import CRPSEstimators
from teehr.models.str_enum import StrEnum


class CRPS(ProbabilisticBasemodel):
    """Continous Ranked Probability Score - Ensemble.

    Parameters
    ----------
    estimator : str
        CRPS estimator, can be ("pwm", "nrg", or "fair"). Default is "pwm".
    backend : str
        The backend to use, by default "numba". Can be ("numba" or "numpy").
    summary_func : Callable
        The function to apply to the results, by default np.mean.
    output_field_name : str
        The output field name, by default "mean_crps_ensemble".
    func : Callable
        The function to apply to the data, by default
        :func:`probabilistic_funcs.ensemble_crps`.
    input_field_names : Union[str, StrEnum, List[Union[str, StrEnum]]]
        The input field names, by default
        ["primary_value", "secondary_value", "value_time"].
    attrs : Dict
        The static attributes for the metric.
    """

    estimator: CRPSEstimators = Field(default="pwm")
    backend: str = Field(default="numba")
    output_field_name: str = Field(default="mean_crps_ensemble")
    func: Callable = Field(probabilistic_funcs.create_crps_func, frozen=True)
    summary_func: Union[Callable, None] = Field(default=None)
    input_field_names: Union[str, StrEnum, List[Union[str, StrEnum]]] = Field(
        default=["primary_value", "secondary_value", "value_time"]
    )
    attrs: Dict = Field(default=tma.CRPS_ENSEMBLE_ATTRS, frozen=True)


class ProbabilisticMetrics:
    """Define and customize probalistic metrics.

    Notes
    -----
    Probabilistic metrics compare a value against a distribution of predicted
    values, such as ensemble forecasts. Available probabilistic metrics include:

    - CRPS (Continuous Ranked Probability Score)
    """

    CRPS = CRPS
