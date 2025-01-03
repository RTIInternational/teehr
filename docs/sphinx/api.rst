.. currentmodule:: teehr

.. _api:

#############
API reference
#############

This page provides an auto-generated summary of TEEHR's API. For more details
and examples, refer to the User Guide part of the
documentation.


The Evaluation Class
--------------------

The top-level class for interacting with and exploring a TEEHR Evaluation.

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated

   Evaluation


Creating and Managing an Evaluation
------------------------------------

Methods for creating, cloning, and configuring an Evaluation.

.. autosummary::

   Evaluation.clone_template
   Evaluation.list_s3_evaluations
   Evaluation.clone_from_s3
   Evaluation.clean_cache
   Evaluation.enable_logging


The Evaluation Dataset
----------------------

Classes for creating, describing, and querying the Evaluation dataset tables.

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated

   BaseTable
   UnitTable
   VariableTable
   AttributeTable
   ConfigurationTable
   LocationTable
   LocationAttributeTable
   LocationCrosswalkTable
   PrimaryTimeseriesTable
   SecondaryTimeseriesTable
   JoinedTimeseriesTable


Fetching NWM and USGS data
--------------------------

Methods for fetching NWM and USGS data from external sources.

.. autosummary::
   :toctree: generated

   Fetch.usgs_streamflow
   Fetch.nwm_retrospective_points
   Fetch.nwm_forecast_points
   Fetch.nwm_retrospective_grids
   Fetch.nwm_forecast_grids


Metric Queries
--------------

Methods for querying and calculating metrics.

.. autosummary::
   :toctree: generated

   evaluation.metrics.Metrics
   deterministic_funcs
   signature_funcs
   probabilistic_funcs


Metric and Bootstrap Models
---------------------------

Classes for defining and customizing metrics and bootstrap models.

.. autosummary::
   :toctree: generated

   deterministic_models
   signature_models
   probabilistic_models
   bootstrap_models


Visualization
-------------

Methods for visualizing Evaluation data.

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated

   TEEHRDataFrameAccessor
