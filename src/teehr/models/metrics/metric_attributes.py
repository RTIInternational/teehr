"""Dictionaries defining static metric attributes."""
from teehr.models.metrics.basemodels import MetricCategories as mc


ME_ATTRS = {
    "short_name": "ME",
    "display_name": "Mean Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

RBIAS_ATTRS = {
    "short_name": "RelBias",
    "display_name": "Relative Bias",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

MULTBIAS_ATTRS = {
    "short_name": "MultBias",
    "display_name": "Multiplicative Bias",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

MSE_ATTRS = {
    "short_name": "MSE",
    "display_name": "Mean Square Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

RMSE_ATTRS = {
    "short_name": "RMSE",
    "display_name": "Root Mean Square Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

MAE_ATTRS = {
    "short_name": "MAE",
    "display_name": "Mean Absolute Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

RMAE_ATTRS = {
    "short_name": "RelMAE",
    "display_name": "Mean Absolute Relative Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

PEARSON_ATTRS = {
    "short_name": "r",
    "display_name": "Pearson Correlation",
    "category": mc.Deterministic,
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

VR_ATTRS = {
    "short_name": "VR",
    "display_name": "Variance Ratio",
    "category": mc.Deterministic,
    "value_range": [0.0, None],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

R2_ATTRS = {
    "short_name": "r2",
    "display_name": "Coefficient of Determination",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

NSE_ATTRS = {
    "short_name": "NSE",
    "display_name": "Nash-Sutcliffe Efficiency",
    "category": mc.Deterministic,
    "value_range": [None, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

NNSE_ATTRS = {
    "short_name": "NNSE",
    "display_name": "Normalized Nash-Sutcliffe Efficiency",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

KGE_ATTRS = {
    "short_name": "KGE",
    "display_name": "Kling-Gupta Efficiency - original",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

KGE1_ATTRS = {
    "short_name": "KGE_mod1",
    "display_name": "Kling-Gupta Efficiency - modified 1 (2012)",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

KGE2_ATTRS = {
    "short_name": "KGE_mod2",
    "display_name": "Kling-Gupta Efficiency - modified 2 (2021)",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

COUNT_ATTRS = {
    "short_name": "count",
    "display_name": "Count",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

MINIMUM_ATTRS = {
    "short_name": "minimum",
    "display_name": "Minimum",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

MAXIMUM_ATTRS = {
    "short_name": "maximum",
    "display_name": "Maximum",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

AVERAGE_ATTRS = {
    "short_name": "average",
    "display_name": "Average",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

SUM_ATTRS = {
    "short_name": "sum",
    "display_name": "Sum",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

VARIANCE_ATTRS = {
    "short_name": "variance",
    "display_name": "Variance",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

MAX_VALUE_DELTA_ATTRS = {
    "short_name": "max_value_delta",
    "display_name": "Max Value Delta",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

MAX_VAL_TIME_ATTRS = {
    "short_name": "max_val_time",
    "display_name": "Max Value Time",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

MAX_VALUE_TIMEDELTA_ATTRS = {
    "short_name": "max_value_time_delta",
    "display_name": "Max Value Time Delta",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "units": "seconds",
    "requires_threshold_field": False,
}

ANNUAL_PEAK_RBIAS_ATTRS = {
    "short_name": "annual_peak_relative_bias",
    "display_name": "Annual Peak Relative Bias",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": False,
}

SPEARMAN_R_ATTRS = {
    "short_name": "spearman_correlation",
    "display_name": "Spearman Rank Correlation Coefficient",
    "category": mc.Deterministic,
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0,
    "requires_threshold_field": False,
}

RSR_ATTRS = {
    "short_name": "root_mean_standard_deviation_ratio",
    "display_name": "Root Mean Standard Deviation Ratio",
    "category": mc.Deterministic,
    "value_range": [0.0, None],
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

CRPS_ENSEMBLE_ATTRS = {
    "short_name": "crps_ensemble",
    "display_name": "Continuous Ranked Probability Score - Ensemble",
    "category": mc.Probabilistic,
    "value_range": [None, None],
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

BS_ENSEMBLE_ATTRS = {
    "short_name": "brier_score_ensemble",
    "display_name": "Brier Score - Ensemble",
    "category": mc.Probabilistic,
    "value_range": [0.0, 1.0],
    "optimal_value": 0.0,
    "requires_threshold_field": False,
}

FDC_SLOPE_ATTRS = {
    "short_name": "fdc_slope",
    "display_name": "Flow Duration Curve Slope",
    "category": mc.Signature,
    "value_range": [None, None],
    "optimal_value": None,
    "requires_threshold_field": False,
}

CM_ATTRS = {
    "short_name": "confusion_matrix",
    "display_name": "Confusion Matrix",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": True,
}

FAR_ATTRS = {
    "short_name": "false_alarm_ratio",
    "display_name": "False Alarm Ratio",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": True,
}

POD_ATTRS = {
    "short_name": "probability_of_detection",
    "display_name": "Probability of Detection",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": True,
}

POFD_ATTRS = {
    "short_name": "probability_of_false_detection",
    "display_name": "Probability of False Detection",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": True,
}

CSI_ATTRS = {
    "short_name": "critical_success_index",
    "display_name": "Critical Success Index",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "requires_threshold_field": True,
}
