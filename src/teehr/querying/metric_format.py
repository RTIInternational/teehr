"""Functions for formatting metrics for querying."""
from typing import List
import logging

import pandas as pd
from pyspark.sql import GroupedData
from pyspark.sql.functions import pandas_udf

from teehr.models.metrics.basemodels import MetricsBasemodel
from teehr.models.metrics.basemodels import MetricCategories as mc
from teehr.querying.utils import validate_fields_exist, parse_fields_to_list

logger = logging.getLogger(__name__)


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

            func_pd = pandas_udf(
                model.bootstrap.func(model),
                model.bootstrap.return_type
            )
            if (model.bootstrap.include_value_time) and \
               ("value_time" not in input_field_names):
                input_field_names.append("value_time")
        else:
            logger.debug(f"Applying metric: {alias}")
            if model.attrs["category"] == mc.Probabilistic:
                func_pd = pandas_udf(model.func(model), model.return_type)
            else:
                func_pd = pandas_udf(model.func, model.return_type)

        func_list.append(
            func_pd(*input_field_names).alias(alias)
        )

    sdf = gp.agg(*func_list)

    # Note: Test exploding multiple columns
    for model in include_metrics:
        if model.unpack_results:
            sdf = model.unpack_function(sdf, model.output_field_name)

    return sdf
