.. currentmodule:: teehr

.. _api:

#############
API reference
#############

This page provides an auto-generated summary of TEEHR's API. For more details
and examples, refer to the User Guide part of the
documentation.


The Evaluation Class
====================

The top-level class for interacting with and exploring a TEEHR Evaluation.

.. autosummary::
   :template: custom-class-template.rst

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

   Fetch.usgs_streamflow
   Fetch.nwm_retrospective_points
   Fetch.nwm_forecast_points
   Fetch.nwm_retrospective_grids
   Fetch.nwm_forecast_grids


Metric Queries
--------------

Methods for querying and calculating metrics.

.. autosummary::
   :template: custom-class-template.rst

   evaluation.metrics.Metrics


Metric and Bootstrap Models
---------------------------

Classes for defining and customizing metrics and bootstrap models.

.. autosummary::
   :template: custom-module-template.rst

   metric_models
   bootstrap_models
   metric_funcs


Visualization
-------------

Methods for visualizing Evaluation data.

.. autosummary::
   :template: custom-class-template.rst

   TEEHRDataFrameAccessor


.. toctree::
   :maxdepth: 2
   :hidden:

   api_evaluation_index
   api_dataset_index
   api_fetch_index
   api_metrics_index
   api_visualization_index