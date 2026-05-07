"""Spark-native row-level calculated-field executors."""

from __future__ import annotations

from datetime import timedelta
from typing import Union

import pandas as pd
import pyspark.sql as ps
import pyspark.sql.functions as F


def apply_month(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add month number from a timestamp column."""
    return sdf.withColumn(output_field_name, F.month(F.col(input_field_name)))


def apply_year(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add year from a timestamp column."""
    return sdf.withColumn(output_field_name, F.year(F.col(input_field_name)))


def apply_water_year(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add water year based on October boundary."""
    return sdf.withColumn(
        output_field_name,
        (
            F.year(F.col(input_field_name))
            + F.when(F.month(F.col(input_field_name)) >= 10, F.lit(1)).otherwise(F.lit(0))
        ).cast("int"),
    )


def apply_normalized_flow(
    sdf: ps.DataFrame,
    *,
    primary_value_field_name: str,
    drainage_area_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Normalize flow by drainage area."""
    return sdf.withColumn(
        output_field_name,
        F.try_divide(
            F.col(primary_value_field_name).cast("double"),
            F.col(drainage_area_field_name).cast("double"),
        ).cast("float"),
    )


def apply_seasons(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    season_months: dict,
    output_field_name: str,
) -> ps.DataFrame:
    """Map month values to season labels."""
    month_col = F.month(F.col(value_time_field_name))
    expr = None
    for season, months in season_months.items():
        cond = month_col.isin([int(m) for m in months])
        expr = F.when(cond, F.lit(season)) if expr is None else expr.when(cond, F.lit(season))
    if expr is None:
        expr = F.lit(None).cast("string")
    else:
        expr = expr.otherwise(F.lit(None).cast("string"))
    return sdf.withColumn(output_field_name, expr)


def apply_forecast_lead_time(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    reference_time_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add lead time as value time minus reference time."""
    return sdf.withColumn(output_field_name, F.col(value_time_field_name) - F.col(reference_time_field_name))


def _to_pd_timedelta(value, field_name: str, context: str) -> pd.Timedelta:
    if isinstance(value, pd.Timedelta):
        return value
    if isinstance(value, timedelta):
        return pd.Timedelta(value)
    if isinstance(value, str):
        try:
            temp = pd.Timedelta(value)
            if temp < pd.Timedelta(seconds=1) and temp != pd.Timedelta(0):
                raise ValueError("Timedelta must be at least 1 second")
            return temp
        except ValueError as e:
            raise ValueError(
                f"{context} '{field_name}' has invalid timedelta string: '{value}'. Error: {e}"
            )
    raise TypeError(
        f"{context} '{field_name}' must be pd.Timedelta, datetime.timedelta, "
        f"or a valid timedelta string, got {type(value)}"
    )


def validate_forecast_lead_time_bin_size(
    bin_size: Union[pd.Timedelta, timedelta, str, list, dict],
) -> Union[pd.Timedelta, list[tuple[pd.Timedelta, pd.Timedelta, str | None]]]:
    """Validate and normalize forecast lead time bin configuration."""
    if isinstance(bin_size, (pd.Timedelta, timedelta, str)):
        return _to_pd_timedelta(bin_size, "bin_size", "bin_size")

    if isinstance(bin_size, list):
        if not bin_size:
            raise ValueError("bin_size list cannot be empty")

        normalized = []
        for i, bin_dict in enumerate(bin_size):
            if not isinstance(bin_dict, dict):
                raise TypeError(f"Item {i} in bin_size list must be a dict")
            required_keys = {"start_inclusive", "end_exclusive"}
            if not required_keys.issubset(bin_dict.keys()):
                raise ValueError(f"Item {i} missing required keys. Must have: {required_keys}")

            start = _to_pd_timedelta(bin_dict["start_inclusive"], "start_inclusive", f"Item {i}")
            end = _to_pd_timedelta(bin_dict["end_exclusive"], "end_exclusive", f"Item {i}")
            normalized.append((start, end, None))
        return normalized

    if isinstance(bin_size, dict):
        if not bin_size:
            raise ValueError("bin_size dict cannot be empty")

        normalized = []
        for custom_id, bin_dict in bin_size.items():
            if not isinstance(custom_id, str):
                raise TypeError(f"Dict keys must be strings (custom bin IDs), got {type(custom_id)}")
            if not isinstance(bin_dict, dict):
                raise TypeError("Dict values must be dicts with bin specification")
            required_keys = {"start_inclusive", "end_exclusive"}
            if not required_keys.issubset(bin_dict.keys()):
                raise ValueError(f"Bin '{custom_id}' missing required keys. Must have: {required_keys}")

            start = _to_pd_timedelta(bin_dict["start_inclusive"], "start_inclusive", f"Bin '{custom_id}'")
            end = _to_pd_timedelta(bin_dict["end_exclusive"], "end_exclusive", f"Bin '{custom_id}'")
            normalized.append((start, end, custom_id))
        return normalized

    raise TypeError(
        "bin_size must be pd.Timedelta, datetime.timedelta, a valid timedelta string, "
        "list of dicts, or dict of dicts"
    )


def _timedelta_to_iso_duration(td: pd.Timedelta) -> str:
    iso_str = td.isoformat()
    iso_str = iso_str.replace("0M0S", "").replace("0S", "").replace("0M", "")
    if iso_str.endswith("T"):
        iso_str = iso_str[:-1] + "T0S"
    return iso_str


def _seconds_to_iso_expr(sec_col: F.Column) -> F.Column:
    """Convert a seconds Column to an ISO 8601 duration string Column."""
    sec = sec_col.cast("long")
    d = (sec / F.lit(86400)).cast("long")
    rem1 = sec - d * F.lit(86400)
    h = (rem1 / F.lit(3600)).cast("long")
    rem2 = rem1 - h * F.lit(3600)
    m = (rem2 / F.lit(60)).cast("long")
    s = rem2 - m * F.lit(60)

    t_with_days = F.when(
        (m == 0) & (s == 0), F.concat(F.lit("T"), h.cast("string"), F.lit("H"))
    ).when(
        s == 0,
        F.concat(F.lit("T"), h.cast("string"), F.lit("H"), m.cast("string"), F.lit("M")),
    ).otherwise(
        F.concat(
            F.lit("T"), h.cast("string"), F.lit("H"),
            m.cast("string"), F.lit("M"),
            s.cast("string"), F.lit("S"),
        )
    )

    t_no_days = F.when(
        (h == 0) & (m == 0) & (s == 0), F.lit("T0S")
    ).when(
        (m == 0) & (s == 0), F.concat(F.lit("T"), h.cast("string"), F.lit("H"))
    ).when(
        s == 0,
        F.concat(F.lit("T"), h.cast("string"), F.lit("H"), m.cast("string"), F.lit("M")),
    ).otherwise(
        F.concat(
            F.lit("T"), h.cast("string"), F.lit("H"),
            m.cast("string"), F.lit("M"),
            s.cast("string"), F.lit("S"),
        )
    )

    return F.when(
        d > 0,
        F.concat(F.lit("P"), d.cast("string"), F.lit("D"), t_with_days),
    ).otherwise(
        F.concat(F.lit("P"), t_no_days)
    )


def apply_forecast_lead_time_bins(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    reference_time_field_name: str,
    lead_time_field_name: str,
    output_field_name: str,
    bin_size: Union[pd.Timedelta, timedelta, str, list, dict],
) -> ps.DataFrame:
    """Add forecast lead-time bin IDs with Spark-native binning logic."""
    normalized_bin_size = validate_forecast_lead_time_bin_size(bin_size)

    if lead_time_field_name not in sdf.columns:
        sdf = apply_forecast_lead_time(
            sdf,
            value_time_field_name=value_time_field_name,
            reference_time_field_name=reference_time_field_name,
            output_field_name=lead_time_field_name,
        )

    lead_sec = (
        F.unix_timestamp(F.col(value_time_field_name))
        - F.unix_timestamp(F.col(reference_time_field_name))
    ).cast("long")

    if isinstance(normalized_bin_size, pd.Timedelta):
        bin_size_sec = F.lit(int(normalized_bin_size.total_seconds()))
        bin_num = (lead_sec / bin_size_sec).cast("long")
        start_sec = bin_num * bin_size_sec
        end_sec = (bin_num + F.lit(1)) * bin_size_sec
        bin_id_expr = F.concat(
            _seconds_to_iso_expr(start_sec),
            F.lit("_"),
            _seconds_to_iso_expr(end_sec),
        )
    else:
        bins_to_use = []
        for start_td, end_td, bin_id in normalized_bin_size:
            final_id = (
                bin_id
                if bin_id is not None
                else f"{_timedelta_to_iso_duration(start_td)}_{_timedelta_to_iso_duration(end_td)}"
            )
            bins_to_use.append(
                (int(start_td.total_seconds()), int(end_td.total_seconds()), final_id)
            )

        last_end_sec = bins_to_use[-1][1]

        first_start_s, first_end_s, first_bid = bins_to_use[0]
        first_cond = (lead_sec >= F.lit(first_start_s)) & (lead_sec < F.lit(first_end_s))
        bin_id_expr = F.when(first_cond, F.lit(first_bid))

        for i in range(1, len(bins_to_use)):
            start_s, end_s, bid = bins_to_use[i]
            cond = (lead_sec >= F.lit(start_s)) & (lead_sec < F.lit(end_s))
            bin_id_expr = bin_id_expr.when(cond, F.lit(bid))

        bin_id_expr = bin_id_expr.otherwise(
            F.when(lead_sec >= F.lit(last_end_sec), F.lit("overflow")).otherwise(
                F.lit(None).cast("string")
            )
        )

    return sdf.withColumn(output_field_name, bin_id_expr)


def apply_threshold_value_exceeded(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    threshold_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add boolean flag for value > threshold."""
    return sdf.withColumn(
        output_field_name,
        F.coalesce(
            F.col(input_field_name).cast("double") > F.col(threshold_field_name).cast("double"),
            F.lit(False),
        ).cast("boolean"),
    )


def apply_threshold_value_not_exceeded(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    threshold_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add boolean flag for value <= threshold."""
    return sdf.withColumn(
        output_field_name,
        F.coalesce(
            F.col(input_field_name).cast("double") <= F.col(threshold_field_name).cast("double"),
            F.lit(False),
        ).cast("boolean"),
    )


def apply_day_of_year(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add leap-year-adjusted day-of-year index."""
    dt_col = F.col(input_field_name)
    year_col = F.year(dt_col)
    month_col = F.month(dt_col)
    day_col = F.dayofmonth(dt_col)
    doy_col = F.dayofyear(dt_col)

    is_leap = (((year_col % 4) == 0) & (((year_col % 100) != 0) | ((year_col % 400) == 0)))
    adjusted = (
        F.when(is_leap & (month_col == 2) & (day_col == 29), F.lit(59))
        .when(is_leap & (month_col > 2), doy_col - F.lit(1))
        .otherwise(doy_col)
    )
    return sdf.withColumn(output_field_name, adjusted.cast("int"))


def apply_hour_of_year(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add leap-year-adjusted hour-of-year index."""
    dt_col = F.col(input_field_name)
    year_col = F.year(dt_col)
    month_col = F.month(dt_col)
    day_col = F.dayofmonth(dt_col)
    doy_col = F.dayofyear(dt_col)
    hour_col = F.hour(dt_col)

    is_leap = (((year_col % 4) == 0) & (((year_col % 100) != 0) | ((year_col % 400) == 0)))
    adjusted = (
        F.when(is_leap & (month_col == 2) & (day_col == 29), F.lit(58 * 24) + hour_col)
        .when(is_leap & (month_col > 2), (doy_col - F.lit(2)) * F.lit(24) + hour_col)
        .otherwise((doy_col - F.lit(1)) * F.lit(24) + hour_col)
    )
    return sdf.withColumn(output_field_name, adjusted.cast("int"))


def apply_generic_sql(sdf: ps.DataFrame, *, output_field_name: str, sql_statement: str) -> ps.DataFrame:
    """Add column computed from a Spark SQL expression."""
    return sdf.withColumn(output_field_name, F.expr(sql_statement))
