"""Signature functions."""
import pandas as pd
import numpy as np

from teehr.metrics.models.base import MetricsBasemodel
from teehr.metrics.models.base import TransformEnum
from typing import Callable, Optional
import logging
logger = logging.getLogger(__name__)

EPSILON = 1e-6  # Small constant to avoid division by zero


def _transform(
        p: pd.Series,
        model: MetricsBasemodel,
        t: Optional[pd.Series] = None
) -> tuple:
    """Apply timeseries transform for metrics calculations."""
    # Apply transform
    if model.transform is not None:
        match model.transform:
            case TransformEnum.log:
                if model.add_epsilon:
                    logger.debug("Adding epsilon to avoid log(0)")
                    p = p + EPSILON
                logger.debug("Applying log transform")
                p = np.log(p)
            case TransformEnum.sqrt:
                logger.debug("Applying square root transform")
                p = np.sqrt(p)
            case TransformEnum.square:
                logger.debug("Applying square transform")
                p = np.square(p)
            case TransformEnum.cube:
                logger.debug("Applying cube transform")
                p = np.power(p, 3)
            case TransformEnum.exp:
                logger.debug("Applying exponential transform")
                p = np.exp(p)
            case TransformEnum.inv:
                if model.add_epsilon:
                    logger.debug("Adding epsilon to avoid division by zero")
                    p = p + EPSILON
                logger.debug("Applying inverse transform")
                p = 1.0 / p
            case TransformEnum.abs:
                logger.debug("Applying absolute value transform")
                p = np.abs(p)
            case _:
                raise ValueError(
                    f"Unsupported transform: {model.transform}"
                )
    else:
        logger.debug("No transform specified, using original values")

    # Remove invalid values and align series
    if t is not None:
        if isinstance(t, pd.Series):
            valid_mask = np.isfinite(p)
            p = p[valid_mask]
            t = t[valid_mask]
        else:
            raise TypeError(
                f"Invalid type for t: {type(t)}. Expected pd.Series."
            )
    else:
        valid_mask = np.isfinite(p)
        p = p[valid_mask]

    if t is not None:
        return p, t
    else:
        return p


def max_value_time(model: MetricsBasemodel) -> Callable:
    """Create max_value_time metric function."""
    logger.debug("Building the max_value_time metric function")

    def max_value_time_inner(
        p: pd.Series,
        value_time: pd.Series
    ) -> pd.Timestamp:
        """Max value time."""
        p, value_time = _transform(p, model, value_time)
        return value_time[p.idxmax()]

    return max_value_time_inner


def variance(model: MetricsBasemodel) -> Callable:
    """Create variance metric function."""
    logger.debug("Building the variance metric function")

    def variance_inner(p: pd.Series) -> float:
        """Variance."""
        p = _transform(p, model)
        return np.var(p)

    return variance_inner


def count(model: MetricsBasemodel) -> Callable:
    """Create count metric function."""
    logger.debug("Building the count metric function")

    def count_inner(p: pd.Series) -> float:
        """Count."""
        p = _transform(p, model)
        return len(p)

    return count_inner


def minimum(model: MetricsBasemodel) -> Callable:
    """Create minimum metric function."""
    logger.debug("Building the minimum metric function")

    def minimum_inner(p: pd.Series) -> float:
        """Minimum."""
        p = _transform(p, model)
        return np.min(p)

    return minimum_inner


def maximum(model: MetricsBasemodel) -> Callable:
    """Create maximum metric function."""
    logger.debug("Building the maximum metric function")

    def maximum_inner(p: pd.Series) -> float:
        """Maximum."""
        p = _transform(p, model)
        return np.max(p)

    return maximum_inner


def average(model: MetricsBasemodel) -> Callable:
    """Create average metric function."""
    logger.debug("Building the average metric function")

    def average_inner(p: pd.Series) -> float:
        """Average."""
        p = _transform(p, model)
        return np.mean(p)

    return average_inner


def sum(model: MetricsBasemodel) -> Callable:
    """Create sum metric function."""
    logger.debug("Building the sum metric function")

    def sum_inner(p: pd.Series) -> float:
        """Sum."""
        p = _transform(p, model)
        return np.sum(p)

    return sum_inner


def flow_duration_curve_slope(model: MetricsBasemodel) -> Callable:
    """Create flow duration curve slope metric function."""
    logger.debug("Building the flow duration curve slope metric function")

    def flow_duration_curve_slope_inner(
        p: pd.Series,
    ) -> float:
        """Flow duration curve slope."""
        # ensure percentiles are within valid range
        if not (0 <= model.lower_quantile <= 1):
            raise ValueError(
                "Lower quantile must be between 0 and 1"
                )
        if not (0 <= model.upper_quantile <= 1):
            raise ValueError(
                "Upper quantile must be between 0 and 1"
                )

        # ensure lower quantile is less than upper quantile
        if model.lower_quantile >= model.upper_quantile:
            raise ValueError(
                "Lower quantile must be less than upper quantile"
                )

        # apply any specified transform
        p = _transform(p, model)

        # sort the streamflow values in descending order
        p_sorted = p.sort_values(ascending=False).reset_index(drop=True)

        # calculate exceedance probabilities
        n = len(p_sorted)
        fdc_probs = (p_sorted.index/(n+1))

        # determine indices for the specified quantiles
        lower_idx = np.argmin(np.abs(fdc_probs - model.lower_quantile))
        upper_idx = np.argmin(np.abs(fdc_probs - model.upper_quantile))

        # check for as_percentile flag
        if model.as_percentile:
            fdc_probs = fdc_probs * 100

        # calculate slope between the two quantiles
        if model.add_epsilon:
            slope = (p_sorted.iloc[upper_idx] - p_sorted.iloc[lower_idx]) / ((
                fdc_probs[upper_idx] - fdc_probs[lower_idx]
            ) + EPSILON)
        else:
            slope = (p_sorted.iloc[upper_idx] - p_sorted.iloc[lower_idx]) / (
                fdc_probs[upper_idx] - fdc_probs[lower_idx]
            )

        return slope

    return flow_duration_curve_slope_inner


def _sort_inputs(
    p: pd.Series,
    value_time: pd.Series
) -> pd.Series:
    """Sorts value_time for center_of_timing_inner()."""
    # ensure ordinates are ordered by value_time
    temp_df = pd.DataFrame({'primary_value': p, 'value_time': value_time})
    sorted_df = temp_df.sort_values(by='value_time').reset_index(drop=True)
    return sorted_df['primary_value'], sorted_df['value_time']


def _ensure_daily_timestep(
    p: pd.Series,
    value_time: pd.Series
) -> pd.Series:
    """Ensure inputs are in daily timestep for center_of_timing_inner()."""
    # determine unique, non-NaT timestep sizes
    unique_timesteps = value_time.diff().unique()
    unique_timesteps = [ts for ts in unique_timesteps if pd.notna(ts)]

    # return original series' if already in daily timestep
    if (len(unique_timesteps) == 1) & (unique_timesteps[0] == pd.Timedelta(days=1)):
        return p.reset_index(drop=True), value_time.reset_index(drop=True)
    else:
        # assemble dataframe and resample to daily
        df = pd.DataFrame({'primary_value': p.values}, index=value_time.values)

        # calculate mean for day if at least one sub-day value is available
        resampled_df = df.resample('D').apply(lambda x: x.mean() if x.notna().any() else np.nan)

        # remove NaNs to preserve original index
        resampled_df = resampled_df.dropna()

        # normalize indices, return values
        p_d = resampled_df['primary_value'].reset_index(drop=True)
        value_time_d = resampled_df.index.to_series().reset_index(drop=True)
        return p_d, value_time_d


def _get_water_year(dates: pd.Series) -> pd.Series:
    """Get water year from dates (year + 1 if month >= October)."""
    return dates.apply(lambda x: x.year + 1 if x.month >= 10 else x.year)


def _get_water_year_length(wy: int) -> int:
    """Get length of water year (365 or 366 based on leap year).

    Water year ends in September, so Feb is in year (wy).
    """
    import calendar
    return 366 if calendar.isleap(wy) else 365


def _get_day_of_wy(
    dates: pd.Series
) -> pd.Series:
    """Obtain the numeric day-of-water-year for center_of_timing_inner()."""
    import datetime
    # obtain the water year for each date
    water_years = _get_water_year(dates)

    # get the start date of the corresponding water year for each date
    water_year_starts = pd.to_datetime([datetime.date(wy - 1, 10, 1) for wy in water_years])

    # get the difference in days and add 1 (since day 1 is the start date)
    days_of_WY = (dates - water_year_starts).dt.days + 1

    return days_of_WY


def center_of_timing(model: MetricsBasemodel) -> Callable:
    """Create center_of_timing metric function."""
    logger.debug("Building the center_of_timing metric function")

    def center_of_timing_inner(
        p: pd.Series,
        value_time: pd.Series
    ) -> float:
        """Perform center_of_timing calculation.

        Computes CT for each water year separately (with leap-year handling),
        then returns the mean across all water years.

        Returns
        -------
        float
            Mean CT across all water years in the input data.
        """
        # apply transform if specified
        p, value_time = _transform(p, model, value_time)

        # ensure inputs are sorted by value_time
        p, value_time = _sort_inputs(p, value_time)

        # ensure input data is a daily timestep
        p_d, value_time_d = _ensure_daily_timestep(p, value_time)

        # Determine water years present
        water_years = _get_water_year(value_time_d)
        unique_wys = water_years.unique()

        # Group by water year and compute CT for each
        ct_values = []

        for wy in unique_wys:
            wy_mask = water_years == wy
            p_wy = p_d[wy_mask]
            value_time_wy = value_time_d[wy_mask]

            # Get water year length for leap year handling
            year_len = _get_water_year_length(wy)

            # Check missing data threshold
            if len(value_time_wy) < (1 - model.missing_day_threshold) * year_len:
                continue

            # Obtain day of water year
            WY_days = _get_day_of_wy(value_time_wy)

            # Filter to non-zero flow steps
            non_zero = p_wy > 0
            p_nz = p_wy[non_zero]
            WY_days_nz = WY_days[non_zero]

            # Calculate CT
            if len(p_nz) == 0:
                continue

            sum_p_nz = np.sum(p_nz)
            if sum_p_nz > 0:
                CT = np.sum(p_nz * WY_days_nz) / sum_p_nz
                ct_values.append(CT)

        # Return mean across all valid water years
        if len(ct_values) == 0:
            return None
        return np.mean(ct_values)

    return center_of_timing_inner


def standard_deviation_of_timing(model: MetricsBasemodel) -> Callable:
    """Create standard_deviation_of_timing metric function."""
    logger.debug("Building the standard_deviation_of_timing metric function")

    def standard_deviation_of_timing_inner(
        p: pd.Series,
        value_time: pd.Series
    ) -> float:
        """Perform standard_deviation_of_timing calculation.

        Computes SDoT for each water year separately (with leap-year handling),
        then returns the mean across all water years.

        Returns
        -------
        float
            Mean SDoT across all water years in the input data.
        """
        # apply transform if specified
        p, value_time = _transform(p, model, value_time)

        # ensure inputs are sorted by value_time
        p, value_time = _sort_inputs(p, value_time)

        # ensure input data is a daily timestep
        p_d, value_time_d = _ensure_daily_timestep(p, value_time)

        # Determine water years present
        water_years = _get_water_year(value_time_d)
        unique_wys = water_years.unique()

        # Group by water year and compute SDoT for each
        sdot_values = []

        for wy in unique_wys:
            wy_mask = water_years == wy
            p_wy = p_d[wy_mask]
            value_time_wy = value_time_d[wy_mask]

            # Get water year length for leap year handling
            year_len = _get_water_year_length(wy)

            # Check missing data threshold
            if len(value_time_wy) < (1 - model.missing_day_threshold) * year_len:
                continue

            # Obtain day of water year
            WY_days = _get_day_of_wy(value_time_wy)

            # Filter to non-zero flow steps
            non_zero = p_wy > 0
            p_nz = p_wy[non_zero]
            WY_days_nz = WY_days[non_zero]

            # Calculate CT first (needed for SDoT)
            if len(p_nz) <= 1:
                continue

            sum_p_nz = np.sum(p_nz)
            if sum_p_nz == 0:
                continue

            CT = np.sum(p_nz * WY_days_nz) / sum_p_nz

            # Normalize CT to [1, year_len] range
            if not np.isnan(CT):
                CT = ((CT - 1) % year_len) + 1

            # Get number of non-zero flow steps
            n_prime = len(p_nz)

            # Calculate SDoT
            SDoT_numerator = np.sum(p_nz * (WY_days_nz - CT)**2)
            SDoT_denominator = (sum_p_nz * (n_prime - 1))/n_prime

            if model.add_epsilon:
                SDoT = np.sqrt(SDoT_numerator / (SDoT_denominator + EPSILON))
            else:
                if SDoT_denominator == 0:
                    continue
                SDoT = np.sqrt(SDoT_numerator / SDoT_denominator)

            sdot_values.append(SDoT)

        # Return mean across all valid water years
        if len(sdot_values) == 0:
            return None
        return np.mean(sdot_values)

    return standard_deviation_of_timing_inner
