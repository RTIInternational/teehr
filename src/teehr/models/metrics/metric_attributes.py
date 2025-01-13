"""Dictionaries defining static metric attributes."""
from teehr.models.metrics.basemodels import MetricCategories as mc


ME_ATTRS = {
    "short_name": "ME",
    "display_name": "Mean Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
}

RBIAS_ATTRS = {
    "short_name": "RelBias",
    "display_name": "Relative Bias",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
}

MULTBIAS_ATTRS = {
    "short_name": "MultBias",
    "display_name": "Multiplicative Bias",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 1.0,
}

MSE_ATTRS = {
    "short_name": "MSE",
    "display_name": "Mean Square Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
}

RMSE_ATTRS = {
    "short_name": "RMSE",
    "display_name": "Root Mean Square Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
}

MAE_ATTRS = {
    "short_name": "MAE",
    "display_name": "Mean Absolute Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
}

RMAE_ATTRS = {
    "short_name": "RelMAE",
    "display_name": "Mean Absolute Relative Error",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": 0.0,
}

PEARSON_ATTRS = {
    "short_name": "r",
    "display_name": "Pearson Correlation",
    "category": mc.Deterministic,
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0,
}

R2_ATTRS = {
    "short_name": "r2",
    "display_name": "Coefficient of Determination",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
}

NSE_ATTRS = {
    "short_name": "NSE",
    "display_name": "Nash-Sutcliffe Efficiency",
    "category": mc.Deterministic,
    "value_range": [None, 1.0],
    "optimal_value": 1.0,
}

NNSE_ATTRS = {
    "short_name": "NNSE",
    "display_name": "Normalized Nash-Sutcliffe Efficiency",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
}

KGE_ATTRS = {
    "short_name": "KGE",
    "display_name": "Kling-Gupta Efficiency - original",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
}

KGE1_ATTRS = {
    "short_name": "KGE_mod1",
    "display_name": "Kling-Gupta Efficiency - modified 1 (2012)",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
}

KGE2_ATTRS = {
    "short_name": "KGE_mod2",
    "display_name": "Kling-Gupta Efficiency - modified 2 (2021)",
    "category": mc.Deterministic,
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
}

COUNT_ATTRS = {
    "short_name": "count",
    "display_name": "Count",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

MINIMUM_ATTRS = {
    "short_name": "minimum",
    "display_name": "Minimum",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

MAXIMUM_ATTRS = {
    "short_name": "maximum",
    "display_name": "Maximum",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

AVERAGE_ATTRS = {
    "short_name": "average",
    "display_name": "Average",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

SUM_ATTRS = {
    "short_name": "sum",
    "display_name": "Sum",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

VARIANCE_ATTRS = {
    "short_name": "variance",
    "display_name": "Variance",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

MAX_VALUE_DELTA_ATTRS = {
    "short_name": "max_value_delta",
    "display_name": "Max Value Delta",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
}

MAX_VAL_TIME_ATTRS = {
    "short_name": "max_val_time",
    "display_name": "Max Value Time",
    "category": mc.Signature,
    "value_range": None,
    "optimal_value": None,
}

MAX_VALUE_TIMEDELTA_ATTRS = {
    "short_name": "max_value_time_delta",
    "display_name": "Max Value Time Delta",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
    "units": "seconds"
}

ANNUAL_PEAK_RBIAS_ATTRS = {
    "short_name": "annual_peak_relative_bias",
    "display_name": "Annual Peak Relative Bias",
    "category": mc.Deterministic,
    "value_range": None,
    "optimal_value": None,
}

SPEARMAN_R_ATTRS = {
    "short_name": "spearman_correlation",
    "display_name": "Spearman Rank Correlation Coefficient",
    "category": mc.Deterministic,
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0,
}

RSR_ATTRS = {
    "short_name": "root_mean_standard_deviation_ratio",
    "display_name": "Root Mean Standard Deviation Ratio",
    "category": mc.Deterministic,
    "value_range": [0.0, None],
    "optimal_value": 0.0,
}

CRPS_ENSEMBLE_ATTRS = {
    "short_name": "crps_ensemble",
    "display_name": "Continuous Ranked Probability Score - Ensemble",
    "category": mc.Probabilistic,
    "value_range": [None, None],
    "optimal_value": 0.0,
}
