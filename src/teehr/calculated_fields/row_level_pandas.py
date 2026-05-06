"""Pandas-UDF row-level calculated-field executors."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyspark.sql as ps
import pyspark.sql.types as T
from pyspark.sql.functions import pandas_udf

from teehr.calculated_fields.row_level_spark import (
    _timedelta_to_iso_duration,
    apply_forecast_lead_time,
    validate_forecast_lead_time_bin_size,
)


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


def apply_forecast_lead_time_bins_pandas(
    sdf: ps.DataFrame,
    *,
    value_time_field_name: str,
    reference_time_field_name: str,
    lead_time_field_name: str,
    output_field_name: str,
    bin_size,
) -> ps.DataFrame:
    """Add forecast lead-time bin IDs with configurable binning via pandas UDF."""
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
