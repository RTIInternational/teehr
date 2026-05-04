"""Contains functions for bootstrap calculations for use in Spark queries."""
from typing import Dict, Callable, List, Optional, Tuple
import logging

import pandas as pd
import numpy as np

from teehr.models.metrics.basemodels import MetricsBasemodel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared-bootstrap helpers
# ---------------------------------------------------------------------------

def bootstrap_group_key(metric: MetricsBasemodel) -> Optional[tuple]:
    """Return a hashable key that identifies identical bootstrap configs.

    Two metrics with the same key can share a single set of bootstrap samples.
    Returns ``None`` for metrics without a bootstrap configuration or when the
    quantile-sharing path is not applicable (``quantiles=None``).
    """
    boot = getattr(metric, "bootstrap", None)
    if boot is None or boot.quantiles is None:
        return None

    # The input fields the UDF will receive must also match.
    if hasattr(metric, "get_input_field_names"):
        fields = tuple(metric.get_input_field_names())
    else:
        fields = tuple(metric.input_field_names or [])

    if boot.include_value_time and "value_time" not in fields:
        fields = fields + ("value_time",)

    # Build key from every config field that affects which samples are drawn.
    boot_cls = type(boot).__name__
    base = (
        boot_cls,
        boot.reps,
        boot.seed,
        tuple(sorted(boot.quantiles)),
        boot.include_value_time,
        fields,
    )

    # Method-specific extra fields
    if boot_cls in ("CircularBlock", "Stationary"):
        extra = (getattr(boot, "block_size", None),)
    elif boot_cls == "Gumboot":
        extra = (
            getattr(boot, "water_year_month", None),
            str(getattr(boot, "boot_year_file", None)),
        )
    else:
        extra = ()

    return base + extra


def partition_metrics_by_bootstrap(
    metrics: List[MetricsBasemodel],
) -> Tuple[List[MetricsBasemodel], Dict[tuple, List[MetricsBasemodel]]]:
    """Split metrics into non-bootstrap and bootstrap-sharing groups.

    Returns
    -------
    no_boot : list
        Metrics without a bootstrap config (or with ``quantiles=None``).
    boot_groups : dict
        Mapping of group key → list of metrics that can share samples.
        Singleton groups (len==1) are included so callers can treat all
        bootstrap metrics uniformly.
    """
    no_boot: List[MetricsBasemodel] = []
    boot_groups: Dict[tuple, List[MetricsBasemodel]] = {}

    for metric in metrics:
        key = bootstrap_group_key(metric)
        if key is None:
            no_boot.append(metric)
        else:
            boot_groups.setdefault(key, []).append(metric)

    return no_boot, boot_groups


def _make_bs_object(boot, args):
    """Instantiate the correct bootstrap object for a given config."""
    boot_cls = type(boot).__name__
    if boot_cls == "CircularBlock":
        from arch.bootstrap import CircularBlockBootstrap
        return CircularBlockBootstrap(
            boot.block_size,
            *args,
            seed=boot.seed,
            random_state=boot.random_state,
        )
    elif boot_cls == "Stationary":
        from arch.bootstrap import StationaryBootstrap
        return StationaryBootstrap(
            boot.block_size,
            *args,
            seed=boot.seed,
            random_state=boot.random_state,
        )
    elif boot_cls == "Gumboot":
        from teehr.metrics.gumboot_bootstrap import GumbootBootstrap
        vt = args[-1]
        metric_args = args[:-1]
        return GumbootBootstrap(
            *metric_args,
            value_time=vt,
            seed=boot.seed,
            water_year_month=boot.water_year_month,
            boot_year_file=boot.boot_year_file,
        )
    else:
        raise ValueError(f"Unsupported bootstrap class: {boot_cls}")


def create_shared_bootstrap_func(
    metrics: List[MetricsBasemodel],
) -> Callable:
    """Create a single bootstrap UDF that evaluates multiple metrics per draw.

    All metrics in *metrics* must share the same bootstrap configuration
    (same class, reps, seed, block_size, quantiles, and input fields).

    Returns a function that, when called as a pandas UDF, returns a
    ``MapType(StringType, FloatType)`` dict whose keys cover every metric's
    quantile columns — identical in format to calling each metric's own
    bootstrap UDF and merging the dicts.
    """
    # Reference bootstrap config from the first metric (all are equivalent).
    ref_boot = metrics[0].bootstrap

    # Build per-metric inner functions once at UDF-creation time.
    metric_funcs = [m.func(m) for m in metrics]
    quantiles = ref_boot.quantiles
    output_names = [m.output_field_name for m in metrics]

    def shared_bootstrap_func(*args: pd.Series) -> Dict:
        bs = _make_bs_object(ref_boot, args)

        # Each draw: evaluate ALL metric functions and return a list.
        def combined_func(*draw_args):
            # arch.bootstrap.apply expects a scalar or NumPy array output.
            # Returning a Python list/tuple can trigger shape inference issues.
            return np.asarray([fn(*draw_args) for fn in metric_funcs], dtype=float)

        # results shape: (reps, N_metrics)
        results = bs.apply(combined_func, ref_boot.reps)

        combined_dict: Dict[str, float] = {}
        for i, name in enumerate(output_names):
            combined_dict.update(
                _calculate_quantiles(name, results[:, i], quantiles)
            )
        return combined_dict

    return shared_bootstrap_func


def _calculate_quantiles(
    output_field_name: str,
    results: np.ndarray,
    quantiles: list
) -> Dict:
    """Calculate quantile values of the bootstrap results."""
    values = np.quantile(results, quantiles)
    quantiles = [f"{output_field_name}_{str(i)}" for i in quantiles]
    d = dict(zip(quantiles, values))
    return d


def create_circularblock_func(model: MetricsBasemodel) -> Callable:
    """Create the CircularBlock bootstrap function."""
    logger.debug("Building the Circular Block bootstrap func.")

    # lazy import to improve performance
    from arch.bootstrap import CircularBlockBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        bs = CircularBlockBootstrap(
            model.bootstrap.block_size,
            *args,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )

        results = bs.apply(
            model.func(model),
            model.bootstrap.reps
        )

        if model.bootstrap.quantiles is not None:
            return _calculate_quantiles(
                model.output_field_name,
                results,
                model.bootstrap.quantiles,
            )
        else:
            return results.ravel()

    return bootstrap_func


def create_gumboot_func(model: MetricsBasemodel) -> Callable:
    """Create the Gumboot bootstrap function."""
    logger.debug("Building the Gumboot bootstrap func.")

    # lazy import to improve performance
    from teehr.metrics.gumboot_bootstrap import GumbootBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        # value_time is always appended last when required by bootstrap config.
        vt = args[-1]
        metric_args = args[:-1]
        bs = GumbootBootstrap(
            *metric_args,
            value_time=vt,
            seed=model.bootstrap.seed,
            water_year_month=model.bootstrap.water_year_month,
            boot_year_file=model.bootstrap.boot_year_file
        )

        results = bs.apply(
            model.func(model),
            model.bootstrap.reps
        )

        if model.bootstrap.quantiles is not None:
            return _calculate_quantiles(
                model.output_field_name,
                results,
                model.bootstrap.quantiles,
            )
        else:
            return results.ravel()

    return bootstrap_func


def create_stationary_func(model: MetricsBasemodel) -> Callable:
    """Create the Stationary bootstrap function."""
    logger.debug("Building the Stationary bootstrap func.")

    # lazy import to improve performance
    from arch.bootstrap import StationaryBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        bs = StationaryBootstrap(
            model.bootstrap.block_size,
            *args,
            seed=model.bootstrap.seed,
            random_state=model.bootstrap.random_state
        )

        results = bs.apply(
            model.func(model),
            model.bootstrap.reps
        )

        if model.bootstrap.quantiles is not None:
            return _calculate_quantiles(
                model.output_field_name,
                results,
                model.bootstrap.quantiles,
            )
        else:
            return results.ravel()

    return bootstrap_func
