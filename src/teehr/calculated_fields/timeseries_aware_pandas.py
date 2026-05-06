"""Pandas/applyInPandas executors for calculated fields.

These helpers centralize the python/pandas execution path so model classes can
remain declarative while planners choose execution engines.
"""

from collections.abc import Sequence

import numpy as np
import pandas as pd
import pyspark.sql as ps
import pyspark.sql.types as T
from scipy.stats import rankdata


def _with_group_defaults(group_by: Sequence[str] | str | None, defaults: list[str]) -> Sequence[str] | str:
    """Return configured grouping columns, falling back to defaults when unset."""
    if group_by is None:
        return defaults
    return group_by


def _add_event_ids(
    sdf: ps.DataFrame,
    output_field: str,
    input_field: str,
    time_field: str,
    group_by: Sequence[str] | str,
) -> ps.DataFrame:
    """Add contiguous event segment IDs in startdate-enddate format."""
    input_schema = sdf.schema
    output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, T.StringType(), True)])

    def event_ids(pdf: pd.DataFrame, input_field: str, time_field: str, output_field: str) -> pd.DataFrame:
        pdf["segment"] = (pdf[input_field] != pdf[input_field].shift()).cumsum()

        segment_ranges = pdf.groupby("segment").agg(
            startdate=(time_field, "min"),
            enddate=(time_field, "max"),
        ).reset_index()

        pdf = pdf.merge(segment_ranges[["segment", "startdate", "enddate"]], on="segment", how="left")
        pdf[output_field] = pdf.apply(lambda row: f"{row['startdate']}-{row['enddate']}", axis=1)
        pdf.drop(columns=["segment", "startdate", "enddate"], inplace=True)
        return pdf

    return sdf.orderBy(*group_by, time_field).groupby(group_by).applyInPandas(
        lambda pdf: event_ids(pdf, input_field, time_field, output_field),
        schema=output_schema,
    )


def _add_percentile_value(
    sdf: ps.DataFrame,
    output_field: str,
    input_field: str,
    quantile: float,
    group_by: Sequence[str] | str,
) -> ps.DataFrame:
    """Add per-group quantile value as a repeated column."""
    input_schema = sdf.schema
    output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, T.DoubleType(), True)])

    def compute_quantile(pdf: pd.DataFrame, input_field: str, quantile: float, output_field: str) -> pd.DataFrame:
        percentile = pdf[input_field].quantile(quantile)
        pdf[output_field] = percentile
        return pdf

    return sdf.groupby(group_by).applyInPandas(
        lambda pdf: compute_quantile(pdf, input_field, quantile, output_field),
        schema=output_schema,
    )


def _add_percentile_event(
    sdf: ps.DataFrame,
    output_field: str,
    input_field: str,
    quantile: float,
    group_by: Sequence[str] | str,
    is_above: bool,
) -> ps.DataFrame:
    """Add per-row percentile event booleans for each group."""
    input_schema = sdf.schema
    output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, T.BooleanType(), True)])

    def is_event(
        pdf: pd.DataFrame,
        input_field: str,
        quantile: float,
        output_field: str,
        is_above: bool,
    ) -> pd.DataFrame:
        pvs = pdf[input_field]
        percentile = pvs.quantile(quantile)
        pdf[output_field] = pvs > percentile if is_above else pvs < percentile
        return pdf

    return sdf.groupby(group_by).applyInPandas(
        lambda pdf: is_event(pdf, input_field, quantile, output_field, is_above),
        schema=output_schema,
    )


def _add_threshold_event(
    sdf: ps.DataFrame,
    output_field: str,
    input_field: str,
    threshold_field: str,
    group_by: Sequence[str] | str,
    is_above: bool,
) -> ps.DataFrame:
    """Add per-row threshold event booleans for each group."""
    input_schema = sdf.schema
    output_schema = T.StructType(input_schema.fields + [T.StructField(output_field, T.BooleanType(), True)])

    def is_event(
        pdf: pd.DataFrame,
        input_field: str,
        threshold_field: str,
        output_field: str,
        is_above: bool,
    ) -> pd.DataFrame:
        pvs = pdf[input_field]
        threshold_value = float(pdf[threshold_field].iloc[0])
        pdf[output_field] = pvs > threshold_value if is_above else pvs < threshold_value
        return pdf

    return sdf.groupby(group_by).applyInPandas(
        lambda pdf: is_event(pdf, input_field, threshold_field, output_field, is_above),
        schema=output_schema,
    )


def apply_percentile_event_detection_pandas(
    sdf: ps.DataFrame,
    *,
    value_field_name: str,
    value_time_field_name: str,
    quantile: float,
    output_event_field_name: str,
    output_event_id_field_name: str,
    output_quantile_field_name: str,
    add_quantile_field: bool,
    skip_event_id: bool,
    uniqueness_fields: Sequence[str] | str | None,
    default_uniqueness_fields: list[str],
    is_above: bool,
) -> ps.DataFrame:
    """Run percentile event detection via grouped pandas execution."""
    group_by = _with_group_defaults(uniqueness_fields, default_uniqueness_fields)

    if add_quantile_field:
        sdf = _add_percentile_value(
            sdf=sdf,
            input_field=value_field_name,
            quantile=quantile,
            output_field=output_quantile_field_name,
            group_by=group_by,
        )

    sdf = _add_percentile_event(
        sdf=sdf,
        input_field=value_field_name,
        quantile=quantile,
        output_field=output_event_field_name,
        group_by=group_by,
        is_above=is_above,
    )

    if not skip_event_id:
        sdf = _add_event_ids(
            sdf=sdf,
            input_field=output_event_field_name,
            time_field=value_time_field_name,
            output_field=output_event_id_field_name,
            group_by=group_by,
        )

    return sdf


def apply_threshold_event_detection_pandas(
    sdf: ps.DataFrame,
    *,
    value_field_name: str,
    value_time_field_name: str,
    threshold_field_name: str,
    output_event_field_name: str,
    output_event_id_field_name: str,
    skip_event_id: bool,
    uniqueness_fields: Sequence[str] | str | None,
    default_uniqueness_fields: list[str],
    is_above: bool,
) -> ps.DataFrame:
    """Run threshold event detection via grouped pandas execution."""
    group_by = _with_group_defaults(uniqueness_fields, default_uniqueness_fields)

    sdf = _add_threshold_event(
        sdf=sdf,
        input_field=value_field_name,
        threshold_field=threshold_field_name,
        output_field=output_event_field_name,
        group_by=group_by,
        is_above=is_above,
    )

    if not skip_event_id:
        sdf = _add_event_ids(
            sdf=sdf,
            input_field=output_event_field_name,
            time_field=value_time_field_name,
            output_field=output_event_id_field_name,
            group_by=group_by,
        )

    return sdf


def apply_exceedance_probability_pandas(
    sdf: ps.DataFrame,
    *,
    as_percentile: bool,
    output_field_name: str,
    value_field_name: str,
    value_time_field_name: str,
    uniqueness_fields: Sequence[str] | str | None,
    default_uniqueness_fields: list[str],
) -> ps.DataFrame:
    """Run exceedance probability via grouped pandas execution."""
    group_by = _with_group_defaults(uniqueness_fields, default_uniqueness_fields)

    input_schema = sdf.schema
    output_schema = T.StructType(input_schema.fields + [T.StructField(output_field_name, T.DoubleType(), True)])

    def exceedance_probability(
        pdf: pd.DataFrame,
        input_field: str,
        output_field: str,
        as_percentile: bool,
    ) -> pd.DataFrame:
        values = pdf[input_field].values
        ranks = rankdata(-values, method="ordinal")
        n = len(values)
        if as_percentile:
            pdf[output_field] = (ranks / (n + 1)) * 100
        else:
            pdf[output_field] = ranks / (n + 1)
        return pdf

    return sdf.orderBy(*group_by, value_time_field_name).groupby(group_by).applyInPandas(
        lambda pdf: exceedance_probability(pdf, value_field_name, output_field_name, as_percentile),
        schema=output_schema,
    )


def apply_baseflow_period_detection_pandas(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    value_field_name: str,
    baseflow_field_name: str | None,
    event_threshold: float,
    output_baseflow_period_field_name: str,
    output_baseflow_period_id_field_name: str,
    uniqueness_fields: Sequence[str] | str | None,
    default_uniqueness_fields: list[str],
) -> ps.DataFrame:
    """Run baseflow period detection via grouped pandas execution."""
    group_by = _with_group_defaults(uniqueness_fields, default_uniqueness_fields)

    if baseflow_field_name is None:
        raise ValueError("baseflow_field_name must be specified.")

    input_schema = sdf.schema

    baseflow_schema = T.StructType(
        input_schema.fields + [T.StructField(output_baseflow_period_field_name, T.BooleanType(), True)]
    )

    def is_baseflow_period(
        pdf: pd.DataFrame,
        input_field: str,
        baseflow_field: str,
        event_threshold: float,
        output_field: str,
    ) -> pd.DataFrame:
        streamflows = pdf[input_field]
        baseflows = pdf[baseflow_field]
        quickflows = streamflows - baseflows
        quickflows_adj = quickflows * event_threshold
        pdf[output_field] = baseflows > quickflows_adj
        return pdf

    sdf = sdf.groupby(group_by).applyInPandas(
        lambda pdf: is_baseflow_period(
            pdf,
            value_field_name,
            baseflow_field_name,
            event_threshold,
            output_baseflow_period_field_name,
        ),
        schema=baseflow_schema,
    )

    id_schema = T.StructType(
        sdf.schema.fields + [T.StructField(output_baseflow_period_id_field_name, T.StringType(), True)]
    )

    def baseflow_period_id(pdf: pd.DataFrame, input_field: str, time_field: str, output_field: str) -> pd.DataFrame:
        pdf["segment"] = (pdf[input_field] != pdf[input_field].shift()).cumsum()
        segments = pdf[pdf[input_field]]
        segment_ranges = segments.groupby("segment").agg(
            startdate=(time_field, "min"),
            enddate=(time_field, "max"),
        ).reset_index()
        pdf = pdf.merge(segment_ranges[["segment", "startdate", "enddate"]], on="segment", how="left")
        pdf[output_field] = pdf.apply(
            lambda row: f"{row['startdate']}-{row['enddate']}" if pd.notnull(row["startdate"]) else None,
            axis=1,
        )
        pdf.drop(columns=["segment", "startdate", "enddate"], inplace=True)
        return pdf

    return sdf.orderBy(*group_by, value_time_field_name).groupby(group_by).applyInPandas(
        lambda pdf: baseflow_period_id(
            pdf,
            output_baseflow_period_field_name,
            value_time_field_name,
            output_baseflow_period_id_field_name,
        ),
        schema=id_schema,
    )


def apply_baseflow_separation_pandas(
    sdf: ps.DataFrame,
    *,
    method: str,
    output_field_name: str,
    value_field_name: str,
    value_time_field_name: str,
    beta: float,
    params: dict[str, float | None] | None,
    uniqueness_fields: Sequence[str] | str | None,
    default_uniqueness_fields: list[str],
) -> ps.DataFrame:
    """Run grouped pandas baseflow separation for supported BYU methods."""
    group_by = _with_group_defaults(uniqueness_fields, default_uniqueness_fields)
    method = method.lower()
    params = params or {}

    input_schema = sdf.schema
    output_schema = T.StructType(input_schema.fields + [T.StructField(output_field_name, T.DoubleType(), True)])

    def compute_baseflow(
        pdf: pd.DataFrame,
        input_field: str,
        time_field: str,
        output_field: str,
        beta: float,
        method: str,
        params: dict[str, float | None],
    ) -> pd.DataFrame:
        # lazy load BYU-baseflow only when this execution path is used
        from baseflow.utils import clean_streamflow
        from baseflow.methods import LH

        pdf[output_field] = None
        input_streamflow = pd.Series(pdf[input_field].values, index=pd.to_datetime(pdf[time_field]))

        if len(input_streamflow) < 120:
            raise ValueError("Input streamflow series must have at least 120 timesteps.")

        date, flow = clean_streamflow(input_streamflow)

        if method == "lyne_hollick":
            from baseflow.methods import LH as LH_method

            result = LH_method(Q=flow, beta=beta, return_exceed=False)
            pdf[output_field] = pd.DataFrame(result, index=date).iloc[:, 0].values
            return pdf

        b_lh = LH(Q=flow, beta=beta, return_exceed=False)

        if method == "ukih":
            from baseflow.methods import UKIH

            result = UKIH(Q=flow, b_LH=b_lh, return_exceed=False)
            pdf[output_field] = pd.DataFrame(result, index=date).iloc[:, 0].values
            return pdf

        if method in {"chapman", "chapman_maxwell", "boughton", "furey", "eckhardt", "willems"}:
            from baseflow.comparision import strict_baseflow
            from baseflow.param_estimate import recession_coefficient

            strict_filter = strict_baseflow(Q=flow, ice=None)
            a = params.get("a")
            if not a:
                a = recession_coefficient(Q=flow, strict=strict_filter)
        else:
            a = params.get("a")

        if method == "chapman":
            from baseflow.methods import Chapman

            result = Chapman(Q=flow, b_LH=b_lh, a=a, return_exceed=False)
        elif method == "chapman_maxwell":
            from baseflow.methods import CM

            result = CM(Q=flow, b_LH=b_lh, a=a, return_exceed=False)
        elif method == "boughton":
            from baseflow.methods import Boughton
            from baseflow.param_estimate import param_calibrate

            c = params.get("c")
            if not c:
                param_range = np.arange(0.0001, 0.1, 0.0001)
                c = param_calibrate(param_range=param_range, method=Boughton, Q=flow, b_LH=b_lh, a=a)
            result = Boughton(Q=flow, b_LH=b_lh, a=a, C=c, return_exceed=False)
        elif method == "furey":
            from baseflow.methods import Furey
            from baseflow.param_estimate import param_calibrate

            c = params.get("c")
            if not c:
                param_range = np.arange(0.01, 10, 0.01)
                c = param_calibrate(param_range=param_range, method=Furey, Q=flow, b_LH=b_lh, a=a)
            result = Furey(Q=flow, b_LH=b_lh, a=a, A=c, return_exceed=False)
        elif method == "eckhardt":
            from baseflow.methods import Eckhardt
            from baseflow.param_estimate import param_calibrate

            bfimax = params.get("BFImax")
            if not bfimax:
                param_range = np.arange(0.001, 1, 0.001)
                bfimax = param_calibrate(param_range=param_range, method=Eckhardt, Q=flow, b_LH=b_lh, a=a)
            result = Eckhardt(Q=flow, b_LH=b_lh, a=a, BFImax=bfimax, return_exceed=False)
        elif method == "ewma":
            from baseflow.methods import EWMA
            from baseflow.param_estimate import param_calibrate

            e = params.get("e")
            if not e:
                param_range = np.arange(0.0001, 0.1, 0.0001)
                e = param_calibrate(param_range=param_range, method=EWMA, Q=flow, b_LH=b_lh, a=None)
            result = EWMA(Q=flow, b_LH=b_lh, a=None, e=e, return_exceed=False)
        elif method == "willems":
            from baseflow.methods import Willems
            from baseflow.param_estimate import param_calibrate

            w = params.get("w")
            if not w:
                param_range = np.arange(0.001, 1, 0.001)
                w = param_calibrate(param_range=param_range, method=Willems, Q=flow, b_LH=b_lh, a=a)
            result = Willems(Q=flow, b_LH=b_lh, a=a, w=w, return_exceed=False)
        else:
            raise ValueError(
                "Unsupported baseflow method. Expected one of: lyne_hollick, chapman, "
                "chapman_maxwell, boughton, furey, eckhardt, ewma, willems, ukih."
            )

        pdf[output_field] = pd.DataFrame(result, index=date).iloc[:, 0].values
        return pdf

    return sdf.orderBy(*group_by, value_time_field_name).groupby(group_by).applyInPandas(
        lambda pdf: compute_baseflow(
            pdf,
            value_field_name,
            value_time_field_name,
            output_field_name,
            beta,
            method,
            params,
        ),
        schema=output_schema,
    )
