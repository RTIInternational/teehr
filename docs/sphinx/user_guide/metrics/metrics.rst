=======
Metrics
=======

The metrics currently built into TEEHR are listed in the table below.
Please note that some are still in development and planned for inclusion in future versions.
To download a pdf of the equations corresponding to the metrics listed in the table, see:
:download:`pdf <TEEHR Metrics_27May2024.pdf>`


.. list-table::
   :header-rows: 1
   :class: metrics-table

   * - Available
     - Description
     - Short Name
     - Equation
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Error
     - :math:`Mean\ Error`
     - :math:`\frac{\sum(sec-prim)}{count}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Relative Bias
     - :math:`Relative\ Bias`
     - :math:`\frac{\sum(sec-prim)}{\sum(prim)}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Multiplicative Bias
     - :math:`Mult.\ Bias`
     - :math:`\frac{\mu_{sec}}{\mu_{prim}}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Square Error
     - :math:`MSE`
     - :math:`\frac{\sum(sec-prim)^2}{count}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Root Mean Square Error
     - :math:`RMSE`
     - :math:`\sqrt{\frac{\sum(sec-prim)^2}{count}}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Absolute Error
     - :math:`MAE`
     - :math:`\frac{\sum|sec-prim|}{count}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Mean Absolute Relative Error
     - :math:`Relative\ MAE`
     - :math:`\frac{\sum|sec-prim|}{\sum(prim)}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Pearson Correlation Coefficient
     - :math:`r`
     - :math:`r(sec, prim)`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Coefficient of Determination
     - :math:`r^2`
     - :math:`r(sec, prim)^2`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Nash-Sutcliffe Efficiency
     - :math:`NSE`
     - :math:`1-\frac{\sum(prim-sec)^2}{\sum(prim-\mu_{prim}^2)}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Normalized Nash-Sutcliffe Efficiency
     - :math:`NNSE`
     - :math:`\frac{1}{(2-NSE)}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - original
     - :math:`KGE`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}}{\sigma_{prim}}-1)^2+(\frac{\mu_{sec}}{\mu_{sec}/\mu_{prim}}-1)^2}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - modified 1 (2012)
     - :math:`KGE'`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}/\mu_{sec}}{\sigma_{prim}/\mu_{prim}}-1)^2+(\frac{\mu_{sec}}{\mu_{sec}/\mu_{prim}}-1)^2}`
   * - :material-regular:`check;1.5em;sd-text-success`
     - Kling Gupta Efficiency - modified 2 (2021)
     - :math:`KGE''`
     - :math:`1-\sqrt{(r(sec, prim)-1)^2+(\frac{\sigma_{sec}}{\sigma_{prim}}-1)^2+\frac{(\mu_{sec}-\mu_{prim})^2}{\sigma_{prim}^2}}`
   * - `Coming Soon`
     - Nash-Sutcliffe Efficiency of Log Flows
     - :math:`NSE(log)`
     - :math:`1-\frac{\sum(log(prim)-log(sec))^2}{\sum(log(prim)-\mu(log(prim)))^2}`
   * - `Coming Soon`
     - Annual Peak Flow Relative Bias
     - :math:`Ann\ PF\ Bias`
     - :math:`\frac{\sum(ann.\ peak_{sec}-ann.\ peak_{prim})}{\sum(ann.\ peak_{prim})}`
   * - `Coming Soon`
     - Spearman Rank Correlation Coefficient
     - :math:`r_s`
     - :math:`1-\frac{6*\sum|rank_{prim}-rank_{sec}|^2}{count(count^2-1)}`
   * - `Coming Soon`
     - Flow Duration Curve Slope Error
     - :math:`Slope\ FDC\ Error`
     - :math:`\frac{q66_{sec}-q33_{sec}}{33}-\frac{q66_{prim}-q33_{prim}}{33}`
   * - `Coming Soon`
     - Event Peak Flow Relative Bias
     - :math:`Peak\ Bias`
     - :math:`\frac{\sum(peak_{sec}-peak_{prim})}{\sum(peak_{prim})}`
   * - `Coming Soon`
     - Event Peak Flow Timing Error
     - :math:`Peak\ Time\ Error`
     - :math:`\frac{\sum(peak\ time_{sec}-peak\ time_{prim})}{count}`
   * - `Coming Soon`
     - Baseflow Index Error
     - :math:`BFI\ Error`
     - :math:`\frac{\frac{\mu(baseflow_{sec})}{\mu(sec)}-\frac{\mu(baseflow_{prim})}{\mu(prim)}}{\frac{\mu(baseflow_{prim})}{\mu(prim)}}`
   * - `Coming Soon`
     - Rising Limb Density Error
     - :math:`RLD\ Error`
     - :math:`\frac{count(rising\ limb\ events_{sec})}{count(rising\ limb\ timesteps_{sec})}-\frac{count(rising\ limb\ events_{prim})}{count(rising\ limb\ timesteps_{prim})}`
   * - `Coming Soon`
     - Mean Square Error Skill Score (generalized reference)
     - :math:`MSESS`
     - :math:`1-\frac{\sum(prim-sec)^2}{\sum(prim-reference)^2}`
   * - `Coming Soon`
     - Runoff Ratio Error
     - :math:`RR\ Error`
     - :math:`abs\left\|\frac{\mu(volume_{sec})}{\mu(precip\ volume)}-\frac{\mu(volume_{prim})}{\mu(precip\ volume)}\right\|`
   * - `Coming Soon`
     - False Alarm Ratio
     - :math:`FAR`
     - :math:`\frac{n_{FP}}{n_{TP}+n_{FP}}`
   * - `Coming Soon`
     - Probability of Detection
     - :math:`POD`
     - :math:`\frac{n_{TP}}{n_{TP}+n_{FN}}`
   * - `Coming Soon`
     - Probability of False Detection
     - :math:`POFD`
     - :math:`\frac{n_{FP}}{n_{TN}+n_{FP}}`
   * - `Coming Soon`
     - Critical Success Index (Threat Score)
     - :math:`CSI`
     - :math:`\frac{n_{TP}}{n_{TP}+n_{FN}+n_{FP}}`
   * - `Coming Soon`
     - Brier Score
     - :math:`BS`
     - :math:`\frac{\sum(sec\ ensemble\ prob-prim\ outcome)^2}{n}`
   * - `Coming Soon`
     - Brier Skill Score
     - :math:`BSS`
     - :math:`1-\frac{BS}{BS_{ref}}`
   * - `Coming Soon`
     - Continuous Ranked Probability Skill Score
     - :math:`CRPSS`
     - :math:`1-\frac{CRPS}{CRPS_{ref}}`










