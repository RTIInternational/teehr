"""Functions for formatting metrics for querying."""
from typing import List, Union
import logging

import pandas as pd
from pyspark.sql import GroupedData
from pyspark.sql.functions import pandas_udf
from pyspark.sql import types as T

from teehr.models.metrics.metric_models import MetricsBasemodel
from teehr.querying.utils import validate_fields_exist, parse_fields_to_list

logger = logging.getLogger(__name__)

ARRAY_TYPE = T.ArrayType(T.DoubleType())  # Array results.
DICT_TYPE = T.MapType(T.StringType(), T.FloatType())  # Quantile results.


def apply_aggregation_metrics(
    gp: GroupedData,
    include_metrics: List[MetricsBasemodel] = None
) -> pd.DataFrame:
    """Apply metrics to a PySpark DataFrame."""
    if not isinstance(include_metrics, List):
        include_metrics = [include_metrics]

    func_list = []
    for model in include_metrics:

        input_field_names = parse_fields_to_list(model.input_field_names)
        validate_fields_exist(gp._df.columns, input_field_names)

        alias = model.output_field_name

        if "bootstrap" in model.model_dump() and model.bootstrap is not None:
            logger.debug(
                f"Applying metric: {alias} with {model.bootstrap.name}"
                " bootstrapping"
            )
            if model.bootstrap.quantiles is None:
                return_type = ARRAY_TYPE
            else:
                return_type = DICT_TYPE

            func_pd = pandas_udf(
                model.bootstrap.func(model),
                return_type
            )
            if (model.bootstrap.include_value_time) and \
               ("value_time" not in input_field_names):
                input_field_names.append("value_time")
        else:
            logger.debug(f"Applying metric: {alias}")
            func_pd = pandas_udf(model.func, model.attrs["return_type"])

        func_list.append(
            func_pd(*input_field_names).alias(alias)
        )

        # Collect the metric attributes here and attach them to the DataFrame?

    df = gp.agg(*func_list)

    return df
