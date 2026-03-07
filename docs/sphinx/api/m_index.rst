Metrics and Analysis
====================


Metrics Class
-------------

.. note::
   The ``metrics`` property on Evaluation is deprecated. Use the ``query()`` method
   directly on tables or views with the ``include_metrics`` argument instead.

.. currentmodule:: teehr.evaluation.metrics

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   Metrics

Calculated Fields
-----------------
.. currentmodule:: teehr.models.calculated_fields

.. autosummary::
   :toctree: generated
   :template: custom-class-template-no-inheritance.rst
   :recursive:
   :nosignatures:

   row_level.RowLevelCalculatedFields
   timeseries_aware.TimeseriesAwareCalculatedFields


Metric Functions
----------------
.. currentmodule:: teehr.metrics

.. autosummary::
   :toctree: generated
   :template: custom-module-template.rst
   :recursive:
   :nosignatures:

   deterministic_funcs
   signature_funcs
   probabilistic_funcs
   bootstrap_funcs


Metric Models
-------------
.. currentmodule:: teehr

.. autosummary::
   :toctree: generated
   :template: custom-class-template-no-inheritance.rst
   :nosignatures:

   DeterministicMetrics
   Signatures
   ProbabilisticMetrics
   Bootstrappers
   Operators


Generated Timeseries
--------------------
.. currentmodule:: teehr.evaluation.generate

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   GeneratedTimeseries


Generated Timeseries Models
---------------------------
.. currentmodule:: teehr.models.generate.timeseries_generator_models

.. autosummary::
   :template: custom-class-template-no-inheritance.rst
   :toctree: generated
   :nosignatures:

   Normals
   ReferenceForecast
   Persistence