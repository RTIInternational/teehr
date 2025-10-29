.. _api:

#############
API reference
#############

This page provides an auto-generated summary of TEEHR's API. For more details
and examples, refer to the User Guide part of the documentation.

.. toctree::
   :maxdepth: 2
   :caption: The Evaluation Dataset
   :hidden:

   ev_dataset
   ev_tables

.. toctree::
   :maxdepth: 2
   :caption: Data I/O
   :hidden:

   io_fetch
   io_load
   io_extract
   io_read
   io_validate
   io_write

.. toctree::
   :maxdepth: 2
   :caption: Analysis & Metrics
   :hidden:

   m_metrics
   m_functions
   m_models
   m_calculated_fields
   m_generate

.. toctree::
   :maxdepth: 2
   :caption: Visualization
   :hidden:

   visualization

.. currentmodule:: teehr


The Evaluation Dataset
----------------------

The top-level class for interacting with and exploring a TEEHR Evaluation.

.. include:: ev_dataset.rst

Classes for creating, describing, and querying the Evaluation dataset tables.

.. include:: ev_tables.rst

Data I/O
--------

Methods related to data input and output.

.. include:: io_fetch.rst
.. include:: io_load.rst
.. include:: io_extract.rst
.. include:: io_read.rst
.. include:: io_validate.rst
.. include:: io_write.rst


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
-------------------------

Classes for defining and customizing user-defined field models.

.. currentmodule:: teehr.models.calculated_fields

.. autosummary::
   :toctree: generated
   :template: custom-class-template.rst
   :recursive:
   :nosignatures:


   row_level.RowLevelCalculatedFields
   timeseries_aware.TimeseriesAwareCalculatedFields


Visualization
-------------

Methods for visualizing Evaluation data.

.. currentmodule:: teehr.visualization.dataframe_accessor

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   TEEHRDataFrameAccessor
