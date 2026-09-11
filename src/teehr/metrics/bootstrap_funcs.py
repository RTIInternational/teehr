"""Contains functions for bootstrap calculations for use in Spark queries."""
import os
from typing import Any, Dict, Callable, List, Optional, Tuple
import logging

import pandas as pd
import numpy as np

from teehr.metrics.models.base import MetricsBasemodel
from teehr.metrics.vectorized_bootstrap_funcs import (
    VECTORIZED_BOOTSTRAP_METHODS,
    assemble_bootstrap_output,
    compute_vectorized_shared_bootstrap,
    is_vectorized_metric,
)
from teehr.querying.utils import bootstrap_quantile_key, parse_fields_to_list

logger = logging.getLogger(__name__)

# Engine selector for the shared-bootstrap path. The vectorized engine is now
# the default; set TEEHR_BOOTSTRAP_ENGINE=legacy to fall back to the per-rep
# loop. The escape hatch exists because the two are meant to be numerically
# identical -- if they ever are not, switching back should be one env var, not
# a release.
#
# Read dynamically (not cached at import time) so it can be toggled at runtime
# (e.g. before creating a Spark session) and in tests via monkeypatch. On a
# Spark cluster it must be set on the EXECUTORS, not just the driver --
# spark.executorEnv.TEEHR_BOOTSTRAP_ENGINE -- since the check runs inside the
# pandas UDF.
def _vectorized_engine_enabled() -> bool:
    engine = os.environ.get("TEEHR_BOOTSTRAP_ENGINE", "vectorized")
    engine = engine.strip().lower()
    if engine not in ("vectorized", "legacy"):
        # Don't silently pick an engine for a typo. Which way a misspelling
        # falls is invisible in the results -- the two engines agree
        # numerically -- so the only symptom would be an unexplained 8x
        # slowdown, or none at all.
        logger.warning(
            "Unrecognized TEEHR_BOOTSTRAP_ENGINE=%r; expected 'vectorized' "
            "or 'legacy'. Using the default (vectorized).",
            engine,
        )
        return True
    return engine == "vectorized"


def _vectorized_engine_available(ref_boot, metrics) -> bool:
    """Group-level preconditions for the vectorized path.

    These are properties of the bootstrap object and the group's input fields,
    so they genuinely cannot be decided per metric:

    - the resampler must expose arch's ``update_indices()`` contract. Gumboot
      takes a ``rep`` argument and returns ragged per-water-year index blocks,
      so no ``(reps, n)`` matrix exists for it at all.
    - ``include_value_time`` adds a third positional arg that the kernels have
      no slot for, and ``np.asarray(..., dtype=float)`` on datetimes is lossy.
    - the field count must match the kernel contract: ``args[0]`` primary and
      an optional ``args[1]`` secondary. This one is not redundant with the
      registry -- e.g. ``Signatures.Average(secondary_field_name=...)`` yields
      a two-field group whose scalar closure takes one argument, where legacy
      raises TypeError but a kernel would quietly ignore ``s_mat`` and
      succeed. Rejecting keeps the two paths' error behavior identical.
    """
    if type(ref_boot).__name__ not in VECTORIZED_BOOTSTRAP_METHODS:
        return False
    if ref_boot.include_value_time:
        return False

    ref = metrics[0]
    if hasattr(ref, "get_input_field_names"):
        n_fields = len(parse_fields_to_list(ref.get_input_field_names()))
    else:
        n_fields = len(parse_fields_to_list(ref.input_field_names))
    expects_secondary = getattr(ref, "secondary_field_name", None) is not None
    return n_fields == (2 if expects_secondary else 1)


def _can_use_vectorized_engine(ref_boot, metrics: List[MetricsBasemodel]) -> bool:
    """Whether the vectorized path can help this bootstrap/metric group.

    ``any``, not ``all``: metrics without a kernel are evaluated by their own
    scalar closure inside ``compute_vectorized_shared_bootstrap``, on the same
    draws as the kernels. Requiring every metric to be covered meant one
    uncovered metric cost the whole group an ~8x slowdown, and
    ``bootstrap_group_key`` excludes metric class precisely so that unrelated
    metrics *do* share a group.

    A group with no covered metrics returns False and takes the untouched
    ``bs.apply`` path, so nothing changes for it.
    """
    if not _vectorized_engine_enabled():
        return False
    if not _vectorized_engine_available(ref_boot, metrics):
        return False
    return any(is_vectorized_metric(m) for m in metrics)


def _optimal_block_size(data: np.ndarray, method: str = "stationary") -> int:
    """Estimate the optimal block size for block bootstrap methods.

    Implements robust estimation with data normalization, matching the approach
    from nwm-explorer to improve numerical stability across diverse data ranges.

    Parameters
    ----------
    data:
        1-D array of the primary metric input (e.g. primary values).
    method:
        ``"stationary"`` uses the ``b_sb`` column from
        ``arch.bootstrap.optimal_block_length``;
        ``"circular"`` uses the ``b_cb`` column.

    Returns
    -------
    int
        Estimated block size (at least 2).
    """
    from arch.bootstrap import optimal_block_length

    clean = np.asarray(data, dtype=float).ravel()
    clean = clean[np.isfinite(clean)]

    # arch.bootstrap.optimal_block_length can fail on very short or invalid
    # arrays (e.g., length <= 2 after dropping NaN/inf).
    if clean.size <= 2:
        logger.warning(
            "Insufficient finite samples (%s) for optimal block estimation; "
            "falling back to block_size=2",
            int(clean.size),
        )
        return 2

    # Normalize data to stable range [0, 1] before estimation.
    # This improves robustness across diverse data magnitudes.
    max_value = np.max(np.abs(clean))
    if max_value > 0:
        normalized = clean / (max_value * 1.01)
    else:
        normalized = clean

    try:
        result = optimal_block_length(normalized)
    except Exception as exc:
        logger.warning(
            "optimal_block_length failed (%s: %s); falling back to block_size=2",
            type(exc).__name__,
            str(exc),
        )
        return 2

    col_candidates = (
        ("b_sb", "stationary") if method == "stationary"
        else ("b_cb", "circular")
    )

    for col in col_candidates:
        if col in result.columns:
            value = result[col].iloc[0]
            if value is None or not np.isfinite(value):
                continue
            block_size = int(np.ceil(float(value)))
            return max(block_size, 2)

    # Fallback for unexpected arch return schema.
    logger.warning(
        "optimal_block_length columns %s did not include expected %s; "
        "falling back to block_size=2",
        list(result.columns),
        list(col_candidates),
    )
    return 2


# ---------------------------------------------------------------------------
# Shared-bootstrap helpers
# ---------------------------------------------------------------------------

def bootstrap_group_key(metric: MetricsBasemodel) -> Optional[tuple]:
    """Return a hashable key that identifies identical bootstrap configs.

    Two metrics with the same key can share a single set of bootstrap samples.
    Returns ``None`` for metrics without a bootstrap configuration.
    """
    boot = getattr(metric, "bootstrap", None)
    if boot is None:
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
    quantile_mode = "quantile" if boot.quantiles is not None else "raw"
    quantile_key = tuple(sorted(boot.quantiles)) if boot.quantiles is not None else ()

    base = (
        boot_cls,
        boot.reps,
        boot.seed,
        quantile_mode,
        quantile_key,
        boot.include_value_time,
        fields,
        # The guards must be part of the key. create_shared_bootstrap_func
        # reads them from metrics[0].bootstrap, so two configs differing only
        # in a guard would otherwise share a group and the first metric's
        # thresholds would silently apply to the rest.
        boot.minimum_sample_size,
        boot.minimum_mean,
        boot.minimum_variance,
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
        Metrics without a bootstrap config.
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
        block_size = boot.block_size
        if block_size is None:
            block_size = _optimal_block_size(
                np.asarray(args[0], dtype=float), method="circular"
            )
            logger.debug(f"CircularBlock: auto block_size={block_size}")
        return CircularBlockBootstrap(
            block_size,
            *args,
            seed=boot.seed,
            random_state=boot.random_state,
        )
    elif boot_cls == "Stationary":
        from arch.bootstrap import StationaryBootstrap
        block_size = boot.block_size
        if block_size is None:
            block_size = _optimal_block_size(
                np.asarray(args[0], dtype=float), method="stationary"
            )
            logger.debug(f"Stationary: auto block_size={block_size}")
        return StationaryBootstrap(
            block_size,
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
    (same class, reps, seed, block_size, quantiles, guards, and input fields).

    Parameters
    ----------
    metrics : List[MetricsBasemodel]
        Metrics sharing the same bootstrap config.

    Returns
    -------
    Callable
        UDF returning dict with per-metric quantiles or raw bootstrap arrays.
    """
    # Reference bootstrap config from the first metric (all are equivalent).
    ref_boot = metrics[0].bootstrap

    # Quality guards come from the bootstrap config rather than this call:
    # they describe the resampling, and bootstrap_group_key already keys
    # groups on the config, so metrics in a group necessarily share them.
    minimum_sample_size = ref_boot.minimum_sample_size
    minimum_mean = ref_boot.minimum_mean
    minimum_variance = ref_boot.minimum_variance

    # Build per-metric inner functions once at UDF-creation time.
    metric_funcs = [m.func(m) for m in metrics]
    quantiles = ref_boot.quantiles
    output_names = [m.output_field_name for m in metrics]

    def shared_bootstrap_func(*args: pd.Series) -> Dict[str, Any]:
        # Validate series quality before attempting bootstrap (nwm-explorer pattern).
        primary_series = np.asarray(args[0], dtype=float)
        if len(primary_series) < minimum_sample_size:
            logger.debug(
                "Sample size %s < minimum %s; skipping bootstrap.",
                len(primary_series),
                minimum_sample_size,
            )
            return {name: None for name in output_names}

        mean_val = np.nanmean(primary_series)
        if mean_val < minimum_mean:
            logger.debug(
                "Mean %.6e < minimum %.6e; skipping bootstrap.",
                mean_val,
                minimum_mean,
            )
            return {name: None for name in output_names}

        var_val = np.nanvar(primary_series)
        if var_val < minimum_variance:
            logger.debug(
                "Variance %.6e < minimum %.6e; skipping bootstrap.",
                var_val,
                minimum_variance,
            )
            return {name: None for name in output_names}

        bs = _make_bs_object(ref_boot, args)

        if _can_use_vectorized_engine(ref_boot, metrics):
            return compute_vectorized_shared_bootstrap(
                metrics, metric_funcs, args, bs, ref_boot.reps, quantiles
            )

        # Each draw: evaluate ALL metric functions and return a list.
        def combined_func(*draw_args):
            # arch.bootstrap.apply expects a scalar or NumPy array output.
            # Returning a Python list/tuple can trigger shape inference issues.
            return np.asarray([fn(*draw_args) for fn in metric_funcs], dtype=float)

        # results shape: (reps, N_metrics)
        results = bs.apply(combined_func, ref_boot.reps)

        return assemble_bootstrap_output(output_names, results, quantiles)

    return shared_bootstrap_func


def _calculate_quantiles(
    output_field_name: str,
    results: np.ndarray,
    quantiles: list
) -> Dict:
    """Calculate quantile values of the bootstrap results."""
    values = np.quantile(results, quantiles)
    quantiles = [bootstrap_quantile_key(output_field_name, i) for i in quantiles]
    d = dict(zip(quantiles, values))
    return d


def create_circularblock_func(model: MetricsBasemodel) -> Callable:
    """Create the CircularBlock bootstrap function.

    If ``model.bootstrap.block_size`` is ``None``, the block size is estimated
    using ``arch.bootstrap.optimal_block_length`` (``b_cb`` column).

    Not used by the aggregation pipeline. ``format.py`` routes every bootstrap
    group -- including groups of one -- through
    ``create_shared_bootstrap_func``, so this is retained as the public
    ``Bootstrappers.*.func`` field default (frozen, see
    ``models/bootstrap.py``), for the autodoc page, and as an independent
    reference implementation the equivalence tests compare against. It has no
    sample-size/mean/variance quality guards, unlike the shared path.
    """
    logger.debug("Building the Circular Block bootstrap func.")

    # lazy import to improve performance
    from arch.bootstrap import CircularBlockBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        block_size = model.bootstrap.block_size
        if block_size is None:
            block_size = _optimal_block_size(
                np.asarray(args[0], dtype=float), method="circular"
            )
            logger.debug(
                f"CircularBlock: auto block_size={block_size}"
            )
        bs = CircularBlockBootstrap(
            block_size,
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
    """Create the Gumboot bootstrap function.

    Not used by the aggregation pipeline. ``format.py`` routes every bootstrap
    group -- including groups of one -- through
    ``create_shared_bootstrap_func``, so this is retained as the public
    ``Bootstrappers.*.func`` field default (frozen, see
    ``models/bootstrap.py``), for the autodoc page, and as an independent
    reference implementation the equivalence tests compare against. It has no
    sample-size/mean/variance quality guards, unlike the shared path.
    """
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
    """Create the Stationary bootstrap function.

    If ``model.bootstrap.block_size`` is ``None``, the block size is estimated
    using ``arch.bootstrap.optimal_block_length`` (``b_sb`` column).

    Not used by the aggregation pipeline. ``format.py`` routes every bootstrap
    group -- including groups of one -- through
    ``create_shared_bootstrap_func``, so this is retained as the public
    ``Bootstrappers.*.func`` field default (frozen, see
    ``models/bootstrap.py``), for the autodoc page, and as an independent
    reference implementation the equivalence tests compare against. It has no
    sample-size/mean/variance quality guards, unlike the shared path.
    """
    logger.debug("Building the Stationary bootstrap func.")

    # lazy import to improve performance
    from arch.bootstrap import StationaryBootstrap

    def bootstrap_func(*args: pd.Series) -> Dict:
        """Bootstrap function."""
        block_size = model.bootstrap.block_size
        if block_size is None:
            block_size = _optimal_block_size(
                np.asarray(args[0], dtype=float), method="stationary"
            )
            logger.debug(
                f"Stationary: auto block_size={block_size}"
            )
        bs = StationaryBootstrap(
            block_size,
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
