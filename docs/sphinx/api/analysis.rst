==================
Analysis & Metrics
==================

The Metrics Class
-----------------

Methods related to metric calculations.

.. currentmodule:: teehr.evaluation.metrics

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   Metrics

.. currentmodule:: teehr.evaluation.metrics.Metrics

.. autosummary::
   :nosignatures:

   query
   add_calculated_fields
   to_pandas
   to_geopandas
   to_sdf
   write

Metric Functions
----------------

Functions for calculating metrics.

.. currentmodule:: teehr.metrics

.. autosummary::
   :toctree: generated
   :template: custom-module-template.rst
   :recursive:
   :nosignatures:

   deterministic_funcs
   signature_funcs
   probabilistic_funcs

Metric and Bootstrap Models
---------------------------

Classes for defining and customizing metrics and bootstrap models.

.. currentmodule:: teehr

.. autosummary::
   :toctree: generated
   :template: custom-class-template.rst
   :nosignatures:

   DeterministicMetrics
   SignatureMetrics
   ProbabilisticMetrics
   Bootstrappers

Calculated Field Models
-----------------------

Classes for defining and customizing user-defined field models.

.. currentmodule:: teehr.models.calculated_fields

.. autosummary::
   :toctree: generated
   :template: custom-class-template.rst
   :recursive:
   :nosignatures:

   row_level.RowLevelCalculatedFields
   timeseries_aware.TimeseriesAwareCalculatedFields