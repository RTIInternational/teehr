"""Functions for formatting metrics for querying."""
from typing import List, Union, Dict
import logging

import pandas as pd
from pyspark.sql import GroupedData
from pyspark.sql.functions import pandas_udf

from teehr.models.metrics.metrics import MetricsBasemodel

logger = logging.getLogger(__name__)

# TEST
from typing import Callable

import numpy as np
import pyspark.sql.types as T

# @pandas_udf( MapType(StringType(), FloatType()) )
# def bs_kling_gupta_efficiency(p: pd.Series, s: pd.Series) -> float:

#     bs = CircularBlockBootstrap(365, p, s, seed=1234)
#     results = bs.apply(kling_gupta_efficiency, 1000)
#     quantiles = (0.05, 0.50, 0.95)
#     values = np.quantile(results, quantiles)
#     quantiles = [f"KGE_{str(i)}" for i in quantiles]
#     d = dict(zip(quantiles,values))
#     return d


def build_bootstrap_udf(model: MetricsBasemodel) -> Callable:
    """Build a wrapper for the bootstrap UDF."""
    def bootstrap_udf(p: pd.Series, s: pd.Series) -> Dict:
        """Bootstrap UDF."""
        bs = model.bootstrap.bootstrapper(
            model.bootstrap.block_size,
            p,
            s,
            **model.bootstrap.kwargs
        )
        results = bs.apply(model.func, model.bootstrap.reps)
        quantiles = (0.05, 0.50, 0.95)
        values = np.quantile(results, quantiles)
        quantiles = [f"KGE_{str(i)}" for i in quantiles]
        d = dict(zip(quantiles, values))
        return d
    return bootstrap_udf

# END TEST


def apply_aggregation_metrics(
    df: GroupedData,
    include_metrics: Union[
        List[MetricsBasemodel],
        str
    ] = None
) -> pd.DataFrame:
    """Apply metrics to a PySpark DataFrame."""
    if not isinstance(include_metrics, List):
        include_metrics = [include_metrics]

    # validate the metric models?

    func_list = []
    for model in include_metrics:

        # Get the alias for the metric
        alias = model.attrs["short_name"]

        if model.bootstrap:
            logger.debug(f"Applying metric: {alias} with bootstrapping")
            func_pd = pandas_udf(build_bootstrap_udf(model), T.MapType(T.StringType(), T.FloatType()))

        else:
            logger.debug(f"Applying metric: {alias}")
            func_pd = pandas_udf(model.func, model.attrs["return_type"])

        func_list.append(
            func_pd(*model.input_field_names).alias(alias)
        )

    df = df.agg(*func_list)

    df.show()
    return df
