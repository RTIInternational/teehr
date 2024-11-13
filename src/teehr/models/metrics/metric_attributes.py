"""Dictionaries defining static metric attributes."""


ME_ATTRS = {
    "short_name": "ME",
    "display_name": "Mean Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0,
    "return_type": "float"
}

RBIAS_ATTRS = {
    "short_name": "RelBias",
    "display_name": "Relative Bias",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0,
    "return_type": "float"
}

MULTBIAS_ATTRS = {
    "short_name": "MultBias",
    "display_name": "Multiplicative Bias",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 1.0,
    "return_type": "float"
}

MSE_ATTRS = {
    "short_name": "MSE",
    "display_name": "Mean Square Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0,
    "return_type": "float"
}

RMSE_ATTRS = {
    "short_name": "RMSE",
    "display_name": "Root Mean Square Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0,
    "return_type": "float"
}

MAE_ATTRS = {
    "short_name": "MAE",
    "display_name": "Mean Absolute Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0,
    "return_type": "float"
}

RMAE_ATTRS = {
    "short_name": "RelMAE",
    "display_name": "Mean Absolute Relative Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0,
    "return_type": "float"
}

PEARSON_ATTRS = {
    "short_name": "r",
    "display_name": "Pearson Correlation",
    "category": "Deterministic",
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

R2_ATTRS = {
    "short_name": "r2",
    "display_name": "Coefficient of Determination",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

NSE_ATTRS = {
    "short_name": "NSE",
    "display_name": "Nash-Sutcliffe Efficiency",
    "category": "Deterministic",
    "value_range": [None, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

NNSE_ATTRS = {
    "short_name": "NNSE",
    "display_name": "Normalized Nash-Sutcliffe Efficiency",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

KGE_ATTRS = {
    "short_name": "KGE",
    "display_name": "Kling-Gupta Efficiency - original",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

KGE1_ATTRS = {
    "short_name": "KGE_mod1",
    "display_name": "Kling-Gupta Efficiency - modified 1 (2012)",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

KGE2_ATTRS = {
    "short_name": "KGE_mod2",
    "display_name": "Kling-Gupta Efficiency - modified 2 (2021)",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

COUNT_ATTRS = {
    "short_name": "count",
    "display_name": "Count",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

MINIMUM_ATTRS = {
    "short_name": "minimum",
    "display_name": "Minimum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

MAXIMUM_ATTRS = {
    "short_name": "maximum",
    "display_name": "Maximum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

AVERAGE_ATTRS = {
    "short_name": "average",
    "display_name": "Average",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

SUM_ATTRS = {
    "short_name": "sum",
    "display_name": "Sum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

VARIANCE_ATTRS = {
    "short_name": "variance",
    "display_name": "Variance",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

MAX_VALUE_DELTA_ATTRS = {
    "short_name": "max_value_delta",
    "display_name": "Max Value Delta",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

MAX_VAL_TIME_ATTRS = {
    "short_name": "max_val_time",
    "display_name": "Max Value Time",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "timestamp"
}

MAX_VALUE_TIMEDELTA_ATTRS = {
    "short_name": "max_value_time_delta",
    "display_name": "Max Value Time Delta",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float",
    "units": "seconds"
}

ANNUAL_PEAK_RBIAS_ATTRS = {
    "short_name": "annual_peak_relative_bias",
    "display_name": "Annual Peak Relative Bias",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None,
    "return_type": "float"
}

SPEARMAN_R_ATTRS = {
    "short_name": "spearman_correlation",
    "display_name": "Spearman Rank Correlation Coefficient",
    "category": "Deterministic",
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0,
    "return_type": "float"
}

RSR_ATTRS = {
    "short_name": "root_mean_standard_deviation_ratio",
    "display_name": "Root Mean Standard Deviation Ratio",
    "category": "Deterministic",
    "value_range": [0.0, None],
    "optimal_value": 0.0,
    "return_type": "float"
}
