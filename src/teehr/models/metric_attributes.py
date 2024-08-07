"""Dictionaries defining static metric attributes."""
ME_ATTRS = {
    "short_name": "ME",
    "display_name": "Mean Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0
}

RBIAS_ATTRS = {
    "short_name": "RelBias",
    "display_name": "Relative Bias",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0
}

MULTBIAS_ATTRS = {
    "short_name": "MultBias",
    "display_name": "Multiplicative Bias",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 1.0
}

MSE_ATTRS = {
    "short_name": "MSE",
    "display_name": "Mean Square Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0
}

RMSE_ATTRS = {
    "short_name": "RMSE",
    "display_name": "Root Mean Square Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0
}

MAE_ATTRS = {
    "short_name": "MAE",
    "display_name": "Mean Absolute Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0
}

RMAE_ATTRS = {
    "short_name": "RelMAE",
    "display_name": "Mean Absolute Relative Error",
    "category": "Deterministic",
    "value_range": None,
    "optimal_value": 0.0
}

PEARSON_ATTRS = {
    "short_name": "r",
    "display_name": "Pearson Correlation",
    "category": "Deterministic",
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0
}

R2_ATTRS = {
    "short_name": "r2",
    "display_name": "Coefficient of Determination",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0
}

NSE_ATTRS = {
    "short_name": "NSE",
    "display_name": "Nash-Sutcliffe Efficiency",
    "category": "Deterministic",
    "value_range": [None, 1.0],
    "optimal_value": 1.0
}

NNSE_ATTRS = {
    "short_name": "NNSE",
    "display_name": "Normalized Nash-Sutcliffe Efficiency",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0
}

KGE_ATTRS = {
    "short_name": "KGE",
    "display_name": "Kling-Gupta Efficiency - original",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0
}

KGE1_ATTRS = {
    "short_name": "KGE_mod1",
    "display_name": "Kling-Gupta Efficiency - modified 1 (2012)",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0
}

KGE2_ATTRS = {
    "short_name": "KGE_mod2",
    "display_name": "Kling-Gupta Efficiency - modified 2 (2021)",
    "category": "Deterministic",
    "value_range": [0.0, 1.0],
    "optimal_value": 1.0
}

PRIMARY_COUNT_ATTRS = {
    "short_name": "primary_count",
    "display_name": "Primary Count",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_COUNT_ATTRS = {
    "short_name": "secondary_count",
    "display_name": "Secondary Count",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

PRIMARY_MINIMUM_ATTRS = {
    "short_name": "primary_minimum",
    "display_name": "Primary Minimum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_MINIMUM_ATTRS = {
    "short_name": "secondary_count",
    "display_name": "Secondary Count",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

PRIMARY_MAXIMUM_ATTRS = {
    "short_name": "primary_maximum",
    "display_name": "Primary Maximum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_MAXIMUM_ATTRS = {
    "short_name": "secondary_maximum",
    "display_name": "Secondary Maximum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

PRIMARY_AVERAGE_ATTRS = {
    "short_name": "primary_average",
    "display_name": "Primary Average",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_AVERAGE_ATTRS = {
    "short_name": "secondary_average",
    "display_name": "Secondary Average",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

PRIMARY_SUM_ATTRS = {
    "short_name": "primary_sum",
    "display_name": "Primary Sum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_SUM_ATTRS = {
    "short_name": "secondary_sum",
    "display_name": "Secondary Sum",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

PRIMARY_VARIANCE_ATTRS = {
    "short_name": "primary_variance",
    "display_name": "Primary Variance",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_VARIANCE_ATTRS = {
    "short_name": "secondary_variance",
    "display_name": "Secondary Variance",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

MAX_VALUE_DELTA_ATTRS = {
    "short_name": "max_value_delta",
    "display_name": "Max Value Delta",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

PRIMARY_MAX_VAL_TIME_ATTRS = {
    "short_name": "primary_max_val_time",
    "display_name": "Primary Max Value Time",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SECONDARY_MAX_VAL_TIME_ATTRS = {
    "short_name": "secondary_max_val_time",
    "display_name": "Secondary Max Value Time",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

MAX_VALUE_TIMEDELTA_ATTRS = {
    "short_name": "max_value_time_delta",
    "display_name": "Max Value Time Delta",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

ANNUAL_PEAK_RBIAS_ATTRS = {
    "short_name": "annual_peak_relative_bias",
    "display_name": "Annual Peak Relative Bias",
    "category": "Simple",
    "value_range": None,
    "optimal_value": None
}

SPEARMAN_R_ATTRS = {
    "short_name": "spearman_correlation",
    "display_name": "Spearman Rank Correlation Coefficient",
    "category": "Deterministic",
    "value_range": [-1.0, 1.0],
    "optimal_value": 1.0
}
