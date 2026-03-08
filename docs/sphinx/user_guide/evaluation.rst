.. _evaluation:

==============
The Evaluation
==============

Creating an Evaluation is the first step to working with TEEHR. An Evaluation represents
both a Python class and a directory structure that contains your data, configuration,
and analysis results.

Evaluation Classes
==================

TEEHR provides several Evaluation classes for different use cases:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Class
     - Description
   * - ``Evaluation`` / ``LocalReadWriteEvaluation``
     - Full read-write access to a local evaluation directory. Use this when working
       locally or managing your own data.
   * - ``RemoteReadOnlyEvaluation``
     - Read-only access to a remote TEEHR catalog. Use this for querying data in
       TEEHR-Hub without local storage.
   * - ``RemoteReadWriteEvaluation``
     - Read-write access to a remote TEEHR catalog. Requires appropriate AWS credentials.


Creating a Local Evaluation
===========================

The most common way to work with TEEHR is to create a local evaluation:

.. code-block:: python

   import teehr

   # Create a new evaluation (creates directory if it doesn't exist)
   ev = teehr.Evaluation(
       dir_path="/path/to/my_evaluation",
       create_dir=True
   )

   # Or open an existing evaluation
   ev = teehr.Evaluation(dir_path="/path/to/existing_evaluation")


The ``Evaluation`` class is an alias for ``LocalReadWriteEvaluation``. Both work identically:

.. code-block:: python

   # These are equivalent
   ev = teehr.Evaluation(dir_path="./my_eval", create_dir=True)
   ev = teehr.LocalReadWriteEvaluation(dir_path="./my_eval", create_dir=True)


Parameters
----------

``dir_path`` : str or Path
    The directory path for the evaluation. This is where the Iceberg warehouse
    and cache directories will be created.

``create_dir`` : bool, default False
    Whether to create the directory if it doesn't exist. Set to ``True`` when
    creating a new evaluation.

``check_evaluation_version`` : bool, default True
    Whether to verify the evaluation version is compatible with the current
    TEEHR version. Set to ``False`` to bypass version checking (use with caution).

``spark`` : SparkSession, optional
    An existing SparkSession to use. If not provided, TEEHR creates a new
    session with default configuration.


Directory Structure
-------------------

When you create an evaluation, TEEHR sets up the following directory structure:

.. code-block:: text

   my_evaluation/
   └── local/              # Iceberg warehouse directory
       ├── teehr/          # Namespace containing tables
       │   ├── attributes/
       │   ├── configurations/
       │   ├── locations/
       │   ├── location_attributes/
       │   ├── location_crosswalks/
       │   ├── primary_timeseries/
       │   ├── secondary_timeseries/
       │   ├── units/
       │   └── variables/
       ├── cache/          # Temporary files
       └── version         # Version file


Remote Evaluations (TEEHR-Hub)
==============================

When running in the TEEHR-Hub environment, you can access remote catalogs directly:

Read-Only Access
----------------

.. code-block:: python

   import teehr

   # Read-only access to remote catalog
   ev = teehr.RemoteReadOnlyEvaluation()

   # Query data without local storage
   df = ev.primary_timeseries.filter("location_id LIKE 'usgs%'").to_pandas()

This creates a temporary directory for caching and sets the active catalog to remote.

Read-Write Access
-----------------

.. code-block:: python

   import teehr

   # Read-write access (requires AWS credentials with write permissions)
   ev = teehr.RemoteReadWriteEvaluation()

.. note::

   Remote evaluation classes are currently only available within the TEEHR-Hub
   environment. For local access to TEEHR warehouse data, use the ``Download``
   class with a local evaluation.


Upgrading Existing Evaluations
==============================

If you have an evaluation created with an earlier version of TEEHR (pre-v0.6),
you'll need to migrate it to the new Iceberg-based format.

When opening an older evaluation, TEEHR will detect the version mismatch and
provide instructions:

.. code-block:: python

   ev = teehr.Evaluation(dir_path="./old_evaluation")
   # Raises ValueError with migration instructions

To migrate:

.. code-block:: python

   from teehr.utilities.convert_to_iceberg import convert_evaluation

   # Convert the evaluation in-place
   convert_evaluation("/path/to/old_evaluation")

   # Now you can open it
   ev = teehr.Evaluation(dir_path="/path/to/old_evaluation")


Apache Spark Configuration
==========================

TEEHR uses Apache Spark with Apache Iceberg for scalable data processing. By default,
TEEHR creates an optimized Spark session automatically.

Default Configuration
---------------------

The default configuration:

- Runs in local mode using all available cores
- Sets driver memory to 75% of available system memory
- Configures Iceberg catalogs (local JDBC + remote REST)
- Includes required packages (Iceberg, Sedona for geospatial, AWS S3)

Custom Spark Session
--------------------

For advanced use cases, you can create a custom Spark session:

.. code-block:: python

   from teehr.evaluation.spark_session_utils import create_spark_session
   import teehr

   # Create a custom Spark session
   spark = create_spark_session(
       app_name="My TEEHR Analysis",
       driver_memory="16g",
       driver_max_result_size="8g"
   )

   # Use it with your evaluation
   ev = teehr.Evaluation(
       dir_path="./my_eval",
       create_dir=True,
       spark=spark
   )


Spark Cluster Mode (Kubernetes)
-------------------------------

In TEEHR-Hub or other Kubernetes environments, you can start executor pods:

.. code-block:: python

   spark = create_spark_session(
       start_spark_cluster=True,
       executor_instances=4,
       executor_memory="4g",
       executor_cores=2
   )


Logging and Debugging
---------------------

Enable logging to troubleshoot issues:

.. code-block:: python

   ev = teehr.Evaluation(dir_path="./my_eval")
   ev.enable_logging()  # Writes to teehr.log in evaluation directory

   # View current Spark configuration
   ev.log_spark_config()


Cleaning Up
===========

Clean temporary cache files:

.. code-block:: python

   ev.clean_cache()

Always stop the Spark session when done (especially in scripts):

.. code-block:: python

   ev.spark.stop()


Common Operations
=================

Once you have an evaluation, you can access its components:

.. code-block:: python

   import teehr

   ev = teehr.Evaluation(dir_path="./my_eval", create_dir=True)

   # Access tables
   ev.locations          # LocationTable
   ev.primary_timeseries # PrimaryTimeseriesTable
   ev.secondary_timeseries # SecondaryTimeseriesTable
   # ... and more

   # Access views
   ev.joined_timeseries_view()  # Computed join of primary/secondary

   # Access I/O components
   ev.fetch      # Fetch from external sources (USGS, NWM)
   ev.download   # Download from TEEHR warehouse
   ev.load       # Load local files
   ev.write      # Write query results

   # Run SQL queries directly
   df = ev.sql("SELECT * FROM primary_timeseries LIMIT 10")

   # List available tables
   ev.list_tables()
