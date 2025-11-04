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

The top-level class and tables for interacting with and managing a TEEHR Evaluation.

*  :class:`The Evaluation Class <teehr.Evaluation>`
*  :class:`Attributes Table <teehr.evaluation.tables.attribute_table.AttributeTable>`
*  :class:`Configurations Table <teehr.evaluation.tables.configuration_table.ConfigurationTable>`
*  :class:`Units Table <teehr.evaluation.tables.unit_table.UnitTable>`
*  :class:`Variables Table <teehr.evaluation.tables.variable_table.VariableTable>`
*  :class:`Location Attribute Table <teehr.evaluation.tables.location_attribute_table.LocationAttributeTable>`
*  :class:`Location Crosswalk Table <teehr.evaluation.tables.location_crosswalk_table.LocationCrosswalkTable>`
*  :class:`Locations Table <teehr.evaluation.tables.location_table.LocationTable>`
*  :class:`Primary Timeseries Table <teehr.evaluation.tables.primary_timeseries_table.PrimaryTimeseriesTable>`
*  :class:`Secondary Timeseries Table <teehr.evaluation.tables.secondary_timeseries_table.SecondaryTimeseriesTable>`
*  :class:`Joined Timeseries Table <teehr.evaluation.tables.joined_timeseries_table.JoinedTimeseriesTable>`

Data I/O
--------

Classes and methods related to data input and output.

* :class:`Fetching USGS and NWM Data <teehr.evaluation.fetch.Fetch>`
* :class:`Reading Data <teehr.evaluation.read.Read>`
* :class:`Writing Data <teehr.evaluation.write.Write>`
* :class:`Loading Data <teehr.evaluation.load.Load>`
* :class:`Extracting Data <teehr.evaluation.extract.Extract>`
* :class:`Validating Data <teehr.evaluation.validate.Validate>`

Analysis & Metrics
------------------

Tools for calculating metrics and generating synthetic timeseries.

* :class:`The Metrics Class <teehr.evaluation.metrics.Metrics>`
* :class:`Row-Level Calculated Fields <teehr.models.calculated_fields.row_level.RowLevelCalculatedFields>`
* :class:`Timeseries-Aware Calculated Fields <teehr.models.calculated_fields.timeseries_aware.TimeseriesAwareCalculatedFields>`
* :mod:`Deterministic Functions <teehr.metrics.deterministic_funcs>`
* :mod:`Signature Functions <teehr.metrics.signature_funcs>`
* :mod:`Probabilistic Functions <teehr.metrics.probabilistic_funcs>`
* :class:`Deterministic Metrics Models <teehr.DeterministicMetrics>`
* :class:`Probabilistic Metrics Models <teehr.ProbabilisticMetrics>`
* :class:`Signature Models <teehr.SignatureMetrics>`
* :class:`Generated Timeseries <teehr.evaluation.generate.GeneratedTimeseries>`
* :class:`Reference Forecast Model <teehr.models.generate.timeseries_generator_models.ReferenceForecast>`
