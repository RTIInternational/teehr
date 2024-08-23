"""Functions for formatting metrics for querying."""
from typing import List, Union
import logging

import pandas as pd
from pyspark.sql import GroupedData
from pyspark.sql.functions import pandas_udf
from pyspark.sql import types as T

from teehr.models.metrics.metrics_models import MetricsBasemodel

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
        alias = model.attrs["short_name"]

        if model.bootstrap:
            logger.debug(f"Applying metric: {alias} with bootstrapping")
            func_pd = pandas_udf(
                model.bootstrap.create_func(model),
                T.MapType(T.StringType(), T.FloatType())  # is this affected by number of quantiles?
            )
            if model.bootstrap.additional_fields:
                func_list.append(
                    func_pd(
                        *model.input_field_names.extend(
                            model.bootstrap.additional_fields
                        )
                    ).alias(alias)
                )
            else:
                func_list.append(
                    func_pd(*model.input_field_names).alias(alias)
                )
        else:
            logger.debug(f"Applying metric: {alias}")
            func_pd = pandas_udf(model.func, model.attrs["return_type"])

            func_list.append(
                func_pd(*model.input_field_names).alias(alias)
            )

        # Collect the metric attributes here and attach them to the DataFrame?

    df = df.agg(*func_list)

    # df.show()
    return df
