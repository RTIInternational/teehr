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

.. autosummary::
   :toctree: generated/
   :template: custom-class-template.rst

   Evaluation


Creating and Managing an Evaluation
------------------------------------

.. autosummary::
   :toctree: generated/

   Evaluation.clone_template
   Evaluation.clone_study
   Evaluation.clean_cache
   Evaluation.enable_logging


The Evaluation Dataset
----------------------

.. autosummary::
   :toctree: generated/
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

.. autosummary::
   :toctree: generated/
   :recursive:

   Fetch.usgs_streamflow
   Fetch.nwm_retrospective_points
   Fetch.nwm_forecast_points
   Fetch.nwm_retrospective_grids
   Fetch.nwm_forecast_grids


Metric Queries
--------------

.. autosummary::
   :toctree: generated/
   :template: custom-class-template.rst

   teehr.evaluation.metrics.Metrics


Metric and Bootstrap Models
---------------------------

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst

   teehr.models.metrics.metric_models
   teehr.models.metrics.bootstrap_models


Visualization
-------------

.. autosummary::
   :toctree: generated/
   :template: custom-class-template.rst

   TEEHRDataFrameAccessor
