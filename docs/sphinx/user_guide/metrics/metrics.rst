=================
Available Metrics
=================

The metrics currently built into TEEHR are listed in the tables below.
Please note that some are still in development and planned for inclusion in future versions.

Signature Metrics
=================

Signature metrics operate on a single field to characterize timeseries properties.

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
     - `Average <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.Average>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Count
     - :math:`Count`
     - :math:`count`
     - `Count <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.Count>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Flow Duration Curve Slope
     - :math:`FDC\ Slope`
     - :math:`\frac{q85-q25}{p85-p25}`
     - `Flow Duration Curve Slope <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.FlowDurationCurveSlope>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Max Value Time
     - :math:`Max\ Value\ Time`
     - :math:`peak\ time_{prim}`
     - `Max Value Time <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.MaxValueTime>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Maximum
     - :math:`Max`
     - :math:`max(prim)`
     - `Maximum <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.Maximum>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Minimum
     - :math:`Min`
     - :math:`min(prim)`
     - `Minimum <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.Minimum>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Sum
     - :math:`Sum`
     - :math:`\sum(prim)`
     - `Sum <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.Sum>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Variance
     - :math:`Variance`
     - :math:`\sigma^2_{prim}`
     - `Variance <https://rtiinternational.github.io/teehr/api/generated/teehr.SignatureMetrics.html#teehr.SignatureMetrics.Variance>`__


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
     - `Mean Error <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.MeanError>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Relative Bias
     - :math:`Relative\ Bias`
     - :math:`\frac{\sum(sec-prim)}{\sum(prim)}`
     - `Relative Bias <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.RelativeBias>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Multiplicative Bias
     - :math:`Mult.\ Bias`
     - :math:`\frac{\mu_{sec}}{\mu_{prim}}`
     - `Multiplicative Bias <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.MultiplicativeBias>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Square Error
     - :math:`MSE`
     - :math:`\frac{\sum(sec-prim)^2}{count}`
     - `Mean Square Error <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.MeanSquareError>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Root Mean Square Error
     - :math:`RMSE`
     - :math:`\sqrt{\frac{\sum(sec-prim)^2}{count}}`
     - `Root Mean Square Error <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.RootMeanSquareError>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Absolute Error
     - :math:`MAE`
     - :math:`\frac{\sum|sec-prim|}{count}`
     - `Mean Absolute Error <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.MeanAbsoluteError>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Absolute Relative Error
     - :math:`Relative\ MAE`
     - :math:`\frac{\sum|sec-prim|}{\sum(prim)}`
     - `Mean Absolute Relative Error <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.MeanAbsoluteRelativeError>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Pearson Correlation Coefficient
     - :math:`r`
     - :math:`r(sec, prim)`
     - `Pearson Correlation Coefficient <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.PearsonCorrelationCoefficient>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Coefficient of Determination
     - :math:`r^2`
     - :math:`r(sec, prim)^2`
     - `Coefficient of Determination <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.RSquared>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Nash-Sutcliffe Efficiency
     - :math:`NSE`
     - :math:`1-\frac{\sum(prim-sec)^2}{\sum(prim-\mu_{prim}^2)}`
     - `Nash-Sutcliffe Efficiency <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.NashSutcliffeEfficiency>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Normalized Nash-Sutcliffe Efficiency
     - :math:`NNSE`
     - :math:`\frac{1}{(2-NSE)}`
     - `Normalized Nash-Sutcliffe Efficiency <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.NormalizedNashSutcliffeEfficiency>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - original
     - :math:`KGE`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}}{\sigma_{prim}}-1)^2+(\frac{\mu_{sec}}{\mu_{sec}/\mu_{prim}}-1)^2}`
     - `Kling Gupta Efficiency - original <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.KlingGuptaEfficiency>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - modified 1 (2012)
     - :math:`KGE'`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}/\mu_{sec}}{\sigma_{prim}/\mu_{prim}}-1)^2+(\frac{\mu_{sec}}{\mu_{sec}/\mu_{prim}}-1)^2}`
     - `Kling Gupta Efficiency - modified 1 <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.KlingGuptaEfficiencyMod1>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - modified 2 (2021)
     - :math:`KGE''`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}}{\sigma_{prim}}-1)^2+\frac{(\mu_{sec}-\mu_{prim})^2}{\sigma_{prim}^2}}`
     - `Kling Gupta Efficiency - modified 2 <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.KlingGuptaEfficiencyMod2>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Annual Peak Relative Bias
     - :math:`Ann\ PF\ Bias`
     - :math:`\frac{\sum(ann.\ peak_{sec}-ann.\ peak_{prim})}{\sum(ann.\ peak_{prim})}`
     - `Annual Peak Relative Bias <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.AnnualPeakRelativeBias>`__
   * - :material-regular:`check;1.5em;sd-text-success`
     - Spearman Rank Correlation Coefficient
     - :math:`r_s`
     - :math:`1-\frac{6*\sum|rank_{prim}-rank_{sec}|^2}{count(count^2-1)}`
     - `Spearman Rank Correlation Coefficient <https://rtiinternational.github.io/teehr/api/generated/teehr.DeterministicMetrics.html#teehr.DeterministicMetrics.SpearmanCorrelation>`__
   * - `Coming Soon`
     - Flow Duration Curve Slope Error
     - :math:`Slope\ FDC\ Error`
     - :math:`\frac{q66_{sec}-q33_{sec}}{33}-\frac{q66_{prim}-q33_{prim}}{33}`
     - `N/A`
   * - `Coming Soon`
     - Event Peak Flow Relative Bias
     - :math:`Peak\ Bias`
     - :math:`\frac{\sum(peak_{sec}-peak_{prim})}{\sum(peak_{prim})}`
     - `N/A`
   * - `Coming Soon`
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
   * - `Coming Soon`
     - False Alarm Ratio
     - :math:`FAR`
     - :math:`\frac{n_{FP}}{n_{TP}+n_{FP}}`
     - `N/A`
   * - `Coming Soon`
     - Probability of Detection
     - :math:`POD`
     - :math:`\frac{n_{TP}}{n_{TP}+n_{FN}}`
     - `N/A`
   * - `Coming Soon`
     - Probability of False Detection
     - :math:`POFD`
     - :math:`\frac{n_{FP}}{n_{TN}+n_{FP}}`
     - `N/A`
   * - `Coming Soon`
     - Critical Success Index (Threat Score)
     - :math:`CSI`
     - :math:`\frac{n_{TP}}{n_{TP}+n_{FN}+n_{FP}}`
     - `N/A`


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
     - `Continuous Ranked Probability Score <https://rtiinternational.github.io/teehr/api/generated/teehr.ProbabilisticMetrics.html#teehr.ProbabilisticMetrics.ContinuousRankedProbabilityScore>`__
   * - `Coming Soon`
     - Brier Score
     - :math:`BS`
     - :math:`\frac{\sum(sec\ ensemble\ prob-prim\ outcome)^2}{n}`
     - `N/A`
   * - `Coming Soon`
     - Brier Skill Score
     - :math:`BSS`
     - :math:`1-\frac{BS}{BS_{ref}}`
     - `N/A`
   * - `Coming Soon`
     - Continuous Ranked Probability Skill Score
     - :math:`CRPSS`
     - :math:`1-\frac{CRPS}{CRPS_{ref}}`
     - `N/A`
