"""Row-level calculated-field executors.

This module hosts execution logic for row-level calculated fields so model
classes can remain declarative containers.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Union

import numpy as np
import pandas as pd
import pyspark.sql as ps
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import pandas_udf


def apply_month(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add month number from a timestamp column."""
    return sdf.withColumn(output_field_name, F.month(F.col(input_field_name)))


def apply_month_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add month number via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.IntegerType())
    def _month(ts: pd.Series) -> pd.Series:
        return pd.to_datetime(ts).dt.month

    return sdf.withColumn(output_field_name, _month(input_field_name))


def apply_year(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add year from a timestamp column."""
    return sdf.withColumn(output_field_name, F.year(F.col(input_field_name)))


def apply_year_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add year via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.IntegerType())
    def _year(ts: pd.Series) -> pd.Series:
        return pd.to_datetime(ts).dt.year

    return sdf.withColumn(output_field_name, _year(input_field_name))


def apply_water_year(sdf: ps.DataFrame, *, input_field_name: str, output_field_name: str) -> ps.DataFrame:
    """Add water year based on October boundary."""
    return sdf.withColumn(
        output_field_name,
        (
            F.year(F.col(input_field_name))
            + F.when(F.month(F.col(input_field_name)) >= 10, F.lit(1)).otherwise(F.lit(0))
        ).cast("int"),
    )


def apply_water_year_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add water year via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.IntegerType())
    def _water_year(ts: pd.Series) -> pd.Series:
        dt = pd.to_datetime(ts)
        return (dt.dt.year + (dt.dt.month >= 10).astype(int)).astype("int32")

    return sdf.withColumn(output_field_name, _water_year(input_field_name))


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


def apply_normalized_flow_pandas(
    sdf: ps.DataFrame,
    *,
    primary_value_field_name: str,
    drainage_area_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Normalize flow by drainage area via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.FloatType())
    def _norm(v: pd.Series, a: pd.Series) -> pd.Series:
        v = pd.to_numeric(v, errors="coerce")
        a = pd.to_numeric(a, errors="coerce")
        out = np.divide(v, a, out=np.full(len(v), np.nan, dtype="float64"), where=(a != 0))
        return pd.Series(out, index=v.index).astype("float32")

    return sdf.withColumn(
        output_field_name,
        _norm(primary_value_field_name, drainage_area_field_name),
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


def apply_seasons_pandas(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    season_months: dict,
    output_field_name: str,
) -> ps.DataFrame:
    """Map month to season label via pandas execution (opt-in path)."""
    month_to_season = {}
    for season, months in season_months.items():
        for month in months:
            month_to_season[int(month)] = season

    @pandas_udf(returnType=T.StringType())
    def _seasons(ts: pd.Series) -> pd.Series:
        dt = pd.to_datetime(ts)
        return dt.dt.month.map(month_to_season)

    return sdf.withColumn(output_field_name, _seasons(value_time_field_name))


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


def apply_forecast_lead_time_bins(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    reference_time_field_name: str,
    lead_time_field_name: str,
    output_field_name: str,
    bin_size: Union[pd.Timedelta, timedelta, str, list, dict],
) -> ps.DataFrame:
    """Add forecast lead-time bin IDs with configurable binning."""
    normalized_bin_size = validate_forecast_lead_time_bin_size(bin_size)

    if lead_time_field_name not in sdf.columns:
        sdf = apply_forecast_lead_time(
            sdf,
            value_time_field_name=value_time_field_name,
            reference_time_field_name=reference_time_field_name,
            output_field_name=lead_time_field_name,
        )

    @pandas_udf(returnType=T.StringType())
    def func(lead_time: pd.Series) -> pd.Series:
        if isinstance(normalized_bin_size, pd.Timedelta):
            bin_size_seconds = normalized_bin_size.total_seconds()
            bin_numbers = (lead_time.dt.total_seconds() // bin_size_seconds).astype(int)
            bin_ids = pd.Series("", index=lead_time.index)

            for bin_num in bin_numbers.unique():
                bin_mask = bin_numbers == bin_num
                if bin_mask.any():
                    start_td = pd.Timedelta(seconds=bin_num * bin_size_seconds)
                    end_td = pd.Timedelta(seconds=(bin_num + 1) * bin_size_seconds)
                    bin_id = f"{_timedelta_to_iso_duration(start_td)}_{_timedelta_to_iso_duration(end_td)}"
                    bin_ids[bin_mask] = bin_id
            return bin_ids

        bin_ids = pd.Series("", index=lead_time.index)
        lead_time_seconds = lead_time.dt.total_seconds()
        bins_to_use = []

        for start_td, end_td, bin_id in normalized_bin_size:
            if bin_id is None:
                final_bin_id = f"{_timedelta_to_iso_duration(start_td)}_{_timedelta_to_iso_duration(end_td)}"
            else:
                final_bin_id = bin_id
            bins_to_use.append((start_td, end_td, final_bin_id))

        max_lead_time = lead_time.max()
        last_bin_end = normalized_bin_size[-1][1]
        if max_lead_time >= last_bin_end:
            overflow_start = last_bin_end
            overflow_end = max_lead_time
            if normalized_bin_size[-1][2] is None:
                overflow_bin_id = (
                    f"{_timedelta_to_iso_duration(overflow_start)}_"
                    f"{_timedelta_to_iso_duration(overflow_end)}"
                )
            else:
                overflow_bin_id = "overflow"
            bins_to_use.append((overflow_start, overflow_end, overflow_bin_id))

        for i, (start_td, end_td, bin_id) in enumerate(bins_to_use):
            start_seconds = start_td.total_seconds()
            end_seconds = end_td.total_seconds()
            is_last_bin = i == len(bins_to_use) - 1
            if is_last_bin:
                mask = lead_time_seconds >= start_seconds
            else:
                mask = (lead_time_seconds >= start_seconds) & (lead_time_seconds < end_seconds)
            if mask.any():
                bin_ids[mask] = bin_id

        return bin_ids

    return sdf.withColumn(output_field_name, func(lead_time_field_name))


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


def apply_threshold_value_exceeded_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    threshold_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add value > threshold via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.BooleanType())
    def _thr(v: pd.Series, t: pd.Series) -> pd.Series:
        v = pd.to_numeric(v, errors="coerce")
        t = pd.to_numeric(t, errors="coerce")
        return (v > t).fillna(False)

    return sdf.withColumn(output_field_name, _thr(input_field_name, threshold_field_name))


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


def apply_threshold_value_not_exceeded_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    threshold_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add value <= threshold via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.BooleanType())
    def _thr(v: pd.Series, t: pd.Series) -> pd.Series:
        v = pd.to_numeric(v, errors="coerce")
        t = pd.to_numeric(t, errors="coerce")
        return (v <= t).fillna(False)

    return sdf.withColumn(output_field_name, _thr(input_field_name, threshold_field_name))


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


def apply_day_of_year_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add leap-year-adjusted day-of-year via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.IntegerType())
    def _doy(ts: pd.Series) -> pd.Series:
        dt = pd.to_datetime(ts)
        year = dt.dt.year
        month = dt.dt.month
        day = dt.dt.day
        doy = dt.dt.dayofyear
        is_leap = ((year % 4 == 0) & ((year % 100 != 0) | (year % 400 == 0)))
        out = np.where(
            is_leap & (month == 2) & (day == 29),
            59,
            np.where(is_leap & (month > 2), doy - 1, doy),
        )
        return pd.Series(out, index=ts.index).astype("int32")

    return sdf.withColumn(output_field_name, _doy(input_field_name))


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


def apply_hour_of_year_pandas(
    sdf: ps.DataFrame,
    *,
    input_field_name: str,
    output_field_name: str,
) -> ps.DataFrame:
    """Add leap-year-adjusted hour-of-year via pandas execution (opt-in path)."""

    @pandas_udf(returnType=T.IntegerType())
    def _hoy(ts: pd.Series) -> pd.Series:
        dt = pd.to_datetime(ts)
        year = dt.dt.year
        month = dt.dt.month
        day = dt.dt.day
        doy = dt.dt.dayofyear
        hour = dt.dt.hour
        is_leap = ((year % 4 == 0) & ((year % 100 != 0) | (year % 400 == 0)))
        out = np.where(
            is_leap & (month == 2) & (day == 29),
            58 * 24 + hour,
            np.where(is_leap & (month > 2), (doy - 2) * 24 + hour, (doy - 1) * 24 + hour),
        )
        return pd.Series(out, index=ts.index).astype("int32")

    return sdf.withColumn(output_field_name, _hoy(input_field_name))


def apply_generic_sql(sdf: ps.DataFrame, *, output_field_name: str, sql_statement: str) -> ps.DataFrame:
    """Add column computed from a Spark SQL expression."""
    return sdf.withColumn(output_field_name, F.expr(sql_statement))
