.. _api:

#############
API reference
#############


.. toctree::
   :maxdepth: 2
   :hidden:

   ev_index

.. toctree::
   :maxdepth: 2
   :hidden:

   io_index

.. toctree::
   :maxdepth: 2
   :hidden:

   m_index

.. currentmodule:: teehr


The Evaluation Dataset
----------------------

The top-level classes for interacting with and managing a TEEHR Evaluation.

Evaluation Classes
^^^^^^^^^^^^^^^^^^
*  :class:`Evaluation <teehr.Evaluation>` - Main local read-write evaluation class
*  :class:`LocalReadWriteEvaluation <teehr.LocalReadWriteEvaluation>` - Explicit local read-write class
*  :class:`RemoteReadOnlyEvaluation <teehr.RemoteReadOnlyEvaluation>` - Read-only access to remote catalogs
*  :class:`RemoteReadWriteEvaluation <teehr.RemoteReadWriteEvaluation>` - Read-write access to remote catalogs

Tables
^^^^^^
*  :class:`Attributes Table <teehr.evaluation.tables.attribute_table.AttributeTable>`
*  :class:`Configurations Table <teehr.evaluation.tables.configuration_table.ConfigurationTable>`
*  :class:`Units Table <teehr.evaluation.tables.unit_table.UnitTable>`
*  :class:`Variables Table <teehr.evaluation.tables.variable_table.VariableTable>`
*  :class:`Location Attribute Table <teehr.evaluation.tables.location_attribute_table.LocationAttributeTable>`
*  :class:`Location Crosswalk Table <teehr.evaluation.tables.location_crosswalk_table.LocationCrosswalkTable>`
*  :class:`Locations Table <teehr.evaluation.tables.location_table.LocationTable>`
*  :class:`Primary Timeseries Table <teehr.evaluation.tables.primary_timeseries_table.PrimaryTimeseriesTable>`
*  :class:`Secondary Timeseries Table <teehr.evaluation.tables.secondary_timeseries_table.SecondaryTimeseriesTable>`
*  :class:`Base Table <teehr.evaluation.tables.base_table.BaseTable>`

Views
^^^^^
*  :class:`Base View <teehr.evaluation.views.base_view.View>`
*  :class:`Joined Timeseries View <teehr.evaluation.views.joined_timeseries_view.JoinedTimeseriesView>`
*  :class:`Location Attributes View <teehr.evaluation.views.location_attributes_view.LocationAttributesView>`
*  :class:`Primary Timeseries View <teehr.evaluation.views.primary_timeseries_view.PrimaryTimeseriesView>`
*  :class:`Secondary Timeseries View <teehr.evaluation.views.secondary_timeseries_view.SecondaryTimeseriesView>`

Data I/O
--------

Classes and methods related to data input and output.

* :class:`Fetching USGS and NWM Data <teehr.evaluation.fetch.Fetch>`
* :class:`Reading Data <teehr.evaluation.read.Read>`
* :class:`Writing Data <teehr.evaluation.write.Write>`
* :class:`Loading Data <teehr.evaluation.load.Load>`
* :class:`Extracting Data <teehr.evaluation.extract.Extract>`
* :class:`Validating Data <teehr.evaluation.validate.Validate>`
* :class:`Downloading Data from TEEHR Warehouse <teehr.evaluation.download.Download>`

Analysis & Metrics
------------------

Tools for calculating metrics and generating synthetic timeseries.

Metrics
^^^^^^^
* :class:`The Metrics Class <teehr.evaluation.metrics.Metrics>` *(deprecated - use query() on tables/views)*
* :class:`Row-Level Calculated Fields <teehr.models.calculated_fields.row_level.RowLevelCalculatedFields>`
* :class:`Timeseries-Aware Calculated Fields <teehr.models.calculated_fields.timeseries_aware.TimeseriesAwareCalculatedFields>`
* :mod:`Deterministic Functions <teehr.metrics.deterministic_funcs>`
* :mod:`Signature Functions <teehr.metrics.signature_funcs>`
* :mod:`Probabilistic Functions <teehr.metrics.probabilistic_funcs>`
* :mod:`Bootstrap Functions <teehr.metrics.bootstrap_funcs>`
* :class:`Deterministic Metrics Models <teehr.DeterministicMetrics>`
* :class:`Probabilistic Metrics Models <teehr.ProbabilisticMetrics>`
* :class:`Signature Models <teehr.Signatures>`
* :class:`Bootstrappers <teehr.Bootstrappers>`
* :class:`Operators <teehr.Operators>`

Generated Timeseries
^^^^^^^^^^^^^^^^^^^^
* :class:`Generated Timeseries <teehr.evaluation.generate.GeneratedTimeseries>`
* :class:`Streamflow Normals <teehr.models.generate.timeseries_generator_models.Normals>`
* :class:`Reference Forecast Model <teehr.models.generate.timeseries_generator_models.ReferenceForecast>`
* :class:`Persistence Forecast Model <teehr.models.generate.timeseries_generator_models.Persistence>`