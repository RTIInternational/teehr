.. _metrics:

*******
Metrics
*******

TEEHR provides comprehensive metrics for evaluating hydrologic model performance.
The :meth:`query() <teehr.evaluation.tables.base_table.BaseTable.query>` method on tables and views computes metrics across grouped data,
with support for bootstrapping, transforms, and multiple metric categories.

Using the Query Method
======================

The :meth:`query() <teehr.evaluation.tables.base_table.BaseTable.query>` method is available on all Table and View objects. It computes
specified metrics grouped by selected fields:

.. code-block:: python

    import teehr
    from teehr.metrics import DeterministicMetrics

    ev = teehr.LocalReadWriteEvaluation(dir_path="/path/to/evaluation")

    # Basic metrics query
    metrics_df = ev.table("joined_timeseries").query(
        include_metrics=[
            DeterministicMetrics.KlingGuptaEfficiency(),
            DeterministicMetrics.NashSutcliffeEfficiency(),
        ],
        group_by=["primary_location_id"],
    ).to_pandas()

Query Parameters
----------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Parameter
     - Description
   * - ``include_metrics``
     - List of metric instances to compute
   * - ``group_by``
     - Fields to group by (e.g., location, configuration, month)
   * - ``order_by``
     - Fields to sort results by
   * - ``filters``
     - List of filter conditions to apply before computing metrics
   * - ``include_geometry``
     - Whether to include location geometry in results (for GeoDataFrame output)

Group By Fields
---------------

The ``group_by`` parameter controls how metrics are aggregated. Common groupings:

.. code-block:: python

    import teehr.models.calculated_fields.row_level as rcf

    # Group by location only
    jt.query(include_metrics=[...], group_by=["primary_location_id"])

    # Group by location and configuration
    jt.query(include_metrics=[...], group_by=["primary_location_id", "configuration_name"])

    # Group by calculated fields
    jt = ev.joined_timeseries_view().add_calculated_fields([
        rcf.Month(),
        rcf.WaterYear(),
    ])
    jt.query(include_metrics=[...], group_by=["primary_location_id", "water_year", "month"])


Using Metrics
=============

Import metric classes and instantiate them:

.. code-block:: python

    from teehr.metrics import DeterministicMetrics, Signatures, ProbabilisticMetrics

    # Deterministic metrics
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    nse = DeterministicMetrics.NashSutcliffeEfficiency()
    rmse = DeterministicMetrics.RootMeanSquareError()

    # Signatures (single field statistics)
    avg = Signatures.Average()
    fdc = Signatures.FlowDurationCurveSlope()

    # Probabilistic metrics (ensemble forecasts)
    crps = ProbabilisticMetrics.CRPS()


Transforms
----------

Apply mathematical transformations before computing metrics:

.. code-block:: python

    from teehr.models.metrics.basemodels import TransformEnum

    # Log-transformed RMSE
    rmse = DeterministicMetrics.RootMeanSquareError()
    rmse.transform = TransformEnum.log
    rmse.add_epsilon = True  # Avoid log(0)

Available transforms: ``log``, ``sqrt``, ``square``, ``cube``, ``exp``, ``inv``, ``abs``


Bootstrapping
-------------

Compute confidence intervals using bootstrap resampling:

.. code-block:: python

    from teehr.models.metrics.bootstrap_models import Bootstrappers

    # Configure bootstrap
    boot = Bootstrappers.CircularBlock(
        reps=1000,
        block_size=365,
        seed=42,
        quantiles=[0.05, 0.5, 0.95]
    )

    # Apply to metric
    kge = DeterministicMetrics.KlingGuptaEfficiency()
    kge.bootstrap = boot
    kge.unpack_results = True  # Separate columns for quantiles

    metrics_df = jt.query(
        include_metrics=[kge],
        group_by=["primary_location_id"],
    ).to_pandas()

    # Results: kling_gupta_efficiency_0.05, _0.5, _0.95

See also: :class:`Bootstrappers <teehr.Bootstrappers>`


Complete Example
----------------

.. code-block:: python

    import teehr
    from teehr.metrics import DeterministicMetrics, Signatures
    import teehr.models.calculated_fields.row_level as rcf

    ev = teehr.LocalReadWriteEvaluation(dir_path="/path/to/evaluation")

    # Build view with calculated fields
    metrics_df = (
        ev.joined_timeseries_view(add_attrs=True)
        .add_calculated_fields([rcf.WaterYear(), rcf.Seasons()])
        .filter("water_year >= 2015")
        .query(
            include_metrics=[
                DeterministicMetrics.KlingGuptaEfficiency(),
                DeterministicMetrics.RelativeBias(),
                Signatures.Average(),
            ],
            group_by=["primary_location_id", "season"],
            order_by=["primary_location_id", "season"],
        )
        .to_pandas()
    )

    print(metrics_df.head())
    ev.spark.stop()


=================
Available Metrics
=================

The metrics currently built into TEEHR are listed in the tables below.
Please note that some are still in development and planned for inclusion in future versions.

Signatures
==========

Signatures operate on a single field to characterize timeseries properties.

.. list-table::
   :header-rows: 1
   :class: metrics-table

   * - Available
     - Description
     - Short Name
     - Equation
     - API Reference
   * - :material-regular:`check;1.5em;sd-text-success`
     - Average
     - :math:`Average`
     - :math:`\frac{\sum(prim)}{count}`
     - :class:`Average <teehr.Signatures.Average>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Count
     - :math:`Count`
     - :math:`count`
     - :class:`Count <teehr.Signatures.Count>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Flow Duration Curve Slope
     - :math:`FDC\ Slope`
     - :math:`\frac{q85-q25}{p85-p25}`
     - :class:`Flow Duration Curve Slope <teehr.Signatures.FlowDurationCurveSlope>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Max Value Time
     - :math:`Max\ Value\ Time`
     - :math:`peak\ time_{prim}`
     - :class:`Max Value Time <teehr.Signatures.MaxValueTime>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Maximum
     - :math:`Max`
     - :math:`max(prim)`
     - :class:`Maximum <teehr.Signatures.Maximum>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Minimum
     - :math:`Min`
     - :math:`min(prim)`
     - :class:`Minimum <teehr.Signatures.Minimum>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Sum
     - :math:`Sum`
     - :math:`\sum(prim)`
     - :class:`Sum <teehr.Signatures.Sum>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Variance
     - :math:`Variance`
     - :math:`\sigma^2_{prim}`
     - :class:`Variance <teehr.Signatures.Variance>`


Deterministic Metrics
=====================

Deterministic metrics compare two timeseries, typically primary ("observed") vs. secondary ("modeled") values.

.. list-table::
   :header-rows: 1
   :class: metrics-table

   * - Available
     - Description
     - Short Name
     - Equation
     - API Reference
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Error
     - :math:`Mean\ Error`
     - :math:`\frac{\sum(sec-prim)}{count}`
     - :class:`Mean Error <teehr.DeterministicMetrics.MeanError>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Relative Bias
     - :math:`Relative\ Bias`
     - :math:`\frac{\sum(sec-prim)}{\sum(prim)}`
     - :class:`Relative Bias <teehr.DeterministicMetrics.RelativeBias>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Multiplicative Bias
     - :math:`Mult.\ Bias`
     - :math:`\frac{\mu_{sec}}{\mu_{prim}}`
     - :class:`Multiplicative Bias <teehr.DeterministicMetrics.MultiplicativeBias>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Square Error
     - :math:`MSE`
     - :math:`\frac{\sum(sec-prim)^2}{count}`
     - :class:`Mean Square Error <teehr.DeterministicMetrics.MeanSquareError>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Root Mean Square Error
     - :math:`RMSE`
     - :math:`\sqrt{\frac{\sum(sec-prim)^2}{count}}`
     - :class:`Root Mean Square Error <teehr.DeterministicMetrics.RootMeanSquareError>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Absolute Error
     - :math:`MAE`
     - :math:`\frac{\sum|sec-prim|}{count}`
     - :class:`Mean Absolute Error <teehr.DeterministicMetrics.MeanAbsoluteError>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Absolute Relative Error
     - :math:`Relative\ MAE`
     - :math:`\frac{\sum|sec-prim|}{\sum(prim)}`
     - :class:`Mean Absolute Relative Error <teehr.DeterministicMetrics.MeanAbsoluteRelativeError>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Pearson Correlation Coefficient
     - :math:`r`
     - :math:`r(sec, prim)`
     - :class:`Pearson Correlation Coefficient <teehr.DeterministicMetrics.PearsonCorrelationCoefficient>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Coefficient of Determination
     - :math:`r^2`
     - :math:`r(sec, prim)^2`
     - :class:`Coefficient of Determination <teehr.DeterministicMetrics.RSquared>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Nash-Sutcliffe Efficiency
     - :math:`NSE`
     - :math:`1-\frac{\sum(prim-sec)^2}{\sum(prim-\mu_{prim}^2)}`
     - :class:`Nash-Sutcliffe Efficiency <teehr.DeterministicMetrics.NashSutcliffeEfficiency>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Normalized Nash-Sutcliffe Efficiency
     - :math:`NNSE`
     - :math:`\frac{1}{(2-NSE)}`
     - :class:`Normalized Nash-Sutcliffe Efficiency <teehr.DeterministicMetrics.NormalizedNashSutcliffeEfficiency>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - original
     - :math:`KGE`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}}{\sigma_{prim}}-1)^2+(\frac{\mu_{sec}}{\mu_{sec}/\mu_{prim}}-1)^2}`
     - :class:`Kling Gupta Efficiency - original <teehr.DeterministicMetrics.KlingGuptaEfficiency>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - modified 1 (2012)
     - :math:`KGE'`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}/\mu_{sec}}{\sigma_{prim}/\mu_{prim}}-1)^2+(\frac{\mu_{sec}}{\mu_{sec}/\mu_{prim}}-1)^2}`
     - :class:`Kling Gupta Efficiency - modified 1 <teehr.DeterministicMetrics.KlingGuptaEfficiencyMod1>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - modified 2 (2021)
     - :math:`KGE''`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}}{\sigma_{prim}}-1)^2+\frac{(\mu_{sec}-\mu_{prim})^2}{\sigma_{prim}^2}}`
     - :class:`Kling Gupta Efficiency - modified 2 <teehr.DeterministicMetrics.KlingGuptaEfficiencyMod2>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Annual Peak Relative Bias
     - :math:`Ann\ PF\ Bias`
     - :math:`\frac{\sum(ann.\ peak_{sec}-ann.\ peak_{prim})}{\sum(ann.\ peak_{prim})}`
     - :class:`Annual Peak Relative Bias <teehr.DeterministicMetrics.AnnualPeakRelativeBias>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Spearman Rank Correlation Coefficient
     - :math:`r_s`
     - :math:`1-\frac{6*\sum|rank_{prim}-rank_{sec}|^2}{count(count^2-1)}`
     - :class:`Spearman Rank Correlation Coefficient <teehr.DeterministicMetrics.SpearmanCorrelation>`
   * - `Coming Soon`
     - Flow Duration Curve Slope Error
     - :math:`Slope\ FDC\ Error`
     - :math:`\frac{q66_{sec}-q33_{sec}}{33}-\frac{q66_{prim}-q33_{prim}}{33}`
     - `N/A`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Event Peak Flow Relative Bias
     - :math:`Peak\ Bias`
     - :math:`\frac{\sum(peak_{sec}-peak_{prim})}{\sum(peak_{prim})}`
     - `N/A`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Event Peak Flow Timing Error
     - :math:`Peak\ Time\ Error`
     - :math:`\frac{\sum(peak\ time_{sec}-peak\ time_{prim})}{count}`
     - `N/A`
   * - `Coming Soon`
     - Baseflow Index Error
     - :math:`BFI\ Error`
     - :math:`\frac{\frac{\mu(baseflow_{sec})}{\mu(sec)}-\frac{\mu(baseflow_{prim})}{\mu(prim)}}{\frac{\mu(baseflow_{prim})}{\mu(prim)}}`
     - `N/A`
   * - `Coming Soon`
     - Rising Limb Density Error
     - :math:`RLD\ Error`
     - :math:`\frac{count(rising\ limb\ events_{sec})}{count(rising\ limb\ timesteps_{sec})}-\frac{count(rising\ limb\ events_{prim})}{count(rising\ limb\ timesteps_{prim})}`
     - `N/A`
   * - `Coming Soon`
     - Mean Square Error Skill Score (generalized reference)
     - :math:`MSESS`
     - :math:`1-\frac{\sum(prim-sec)^2}{\sum(prim-reference)^2}`
     - `N/A`
   * - `Coming Soon`
     - Runoff Ratio Error
     - :math:`RR\ Error`
     - :math:`abs\left\|\frac{\mu(volume_{sec})}{\mu(precip\ volume)}-\frac{\mu(volume_{prim})}{\mu(precip\ volume)}\right\|`
     - `N/A`
   * - :material-regular:`check;1.5em;sd-text-success`
     - False Alarm Ratio
     - :math:`FAR`
     - :math:`\frac{n_{FP}}{n_{TP}+n_{FP}}`
     - :class:`False Alarm Ratio <teehr.DeterministicMetrics.FalseAlarmRatio>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Probability of Detection
     - :math:`POD`
     - :math:`\frac{n_{TP}}{n_{TP}+n_{FN}}`
     - :class:`Probability of Detection <teehr.DeterministicMetrics.ProbabilityOfDetection>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Probability of False Detection
     - :math:`POFD`
     - :math:`\frac{n_{FP}}{n_{TN}+n_{FP}}`
     - :class:`Probability of False Detection <teehr.DeterministicMetrics.ProbabilityOfFalseDetection>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Critical Success Index (Threat Score)
     - :math:`CSI`
     - :math:`\frac{n_{TP}}{n_{TP}+n_{FN}+n_{FP}}`
     - :class:`Critical Success Index <teehr.DeterministicMetrics.CriticalSuccessIndex>`


Probabilistic Metrics
=====================

Probabilistic metrics compare a value against a distribution of predicted values, such as ensemble forecasts.

.. list-table::
   :header-rows: 1
   :class: metrics-table

   * - Available
     - Description
     - Short Name
     - Equation
     - API Reference
   * - :material-regular:`check;1.5em;sd-text-success`
     - Continuous Ranked Probability Score
     - :math:`CRPS`
     - :math:`\int_{-\infty}^{\infty} (F(x) - \mathbf{1}_{x \geq y})^2 dx`
     - :class:`Continuous Ranked Probability Score <teehr.ProbabilisticMetrics.ContinuousRankedProbabilityScore>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Brier Score
     - :math:`BS`
     - :math:`\frac{\sum(sec\ ensemble\ prob-prim\ outcome)^2}{n}`
     - :class:`Brier Score <teehr.ProbabilisticMetrics.BrierScore>`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Brier Skill Score
     - :math:`BSS`
     - :math:`1-\frac{BS}{BS_{ref}}`
     - `N/A`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Continuous Ranked Probability Skill Score
     - :math:`CRPSS`
     - :math:`1-\frac{CRPS}{CRPS_{ref}}`
     - `N/A`
