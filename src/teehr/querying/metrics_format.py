"""Functions for formatting metrics for querying."""
from typing import List, Union
import logging

import pandas as pd
from pyspark.sql import GroupedData
from pyspark.sql.functions import pandas_udf

from teehr.models.metrics.metrics import MetricsBasemodel

logger = logging.getLogger(__name__)


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
        alias = model.output_field_name

        logger.debug(f"Applying metric: {alias}")

        func_pd = pandas_udf(model.func, model.attrs["return_type"])

        func_list.append(
            func_pd(*model.input_field_names).alias(alias)
        )

    df = df.agg(*func_list)

    return df
