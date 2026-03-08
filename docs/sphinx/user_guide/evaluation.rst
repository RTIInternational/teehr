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
   * - ``LocalReadWriteEvaluation`` / ``Evaluation``
     - Full read-write access to a local evaluation directory. Use this when working
       locally or managing your own data.  ``Evaluation`` is an alias for ``LocalReadWriteEvaluation``
       and is provided for backwards compatibility.  It will be removed in a future release,
       so please switch to using ``LocalReadWriteEvaluation``.
   * - ``RemoteReadOnlyEvaluation``
     - Read-only access to a remote TEEHR catalog. Use this for querying data in
       TEEHR-Hub without local storage.
   * - ``RemoteReadWriteEvaluation``
     - Read-write access to a remote TEEHR catalog. This will primarily be used by remote catalog administrators.
       Requires appropriate AWS credentials.

.

Creating a Local Evaluation
===========================

If you are working with TEEHR locally and or want to create your own evaluation dataset,
the most common way to work with TEEHR is to create a local evaluation:

.. code-block:: python

   import teehr

   # Create a new evaluation (creates directory if it doesn't exist)
   ev = teehr.LocalReadWriteEvaluation(
       dir_path="/path/to/my_evaluation",
       create_dir=True
   )

   # Or open an existing evaluation
   ev = teehr.LocalReadWriteEvaluation(dir_path="/path/to/existing_evaluation")


The ``Evaluation`` class is an alias for ``LocalReadWriteEvaluation``. Both work identically
but you should use ``LocalReadWriteEvaluation`` moving forward since ``Evaluation`` will be deprecated
in a future release.:

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


Remote Evaluations (TEEHR-HUB)
==============================

When running in the TEEHR-HUB environment, you can access remote catalogs directly to
access the data that is in the TEEHR warehouse without needing to download it. This evaluation type is
only available in TEEHR-HUB where the necessary permissions and configurations are in place.

Read-Only Access
----------------

.. code-block:: python

   import teehr

   # Read-only access to remote catalog
   ev = teehr.RemoteReadOnlyEvaluation()

   # Query data without local storage
   df = ev.primary_timeseries.filter("location_id = 'usgs-02424000'").to_pandas()

This evaluation provides read-only access to the TEEHR Data Warehouse. It is ideal for users who
want to query and analyze data in TEEHR-HUB without needing to manage local storage. It does not
require AWS credentials when used in TEEHR-HUB since it only accesses the remote catalog in
read-only mode. For users who need temporary storage for caching and logging, it creates a
temporary directory and sets the active catalog to remote.

Read-Write Access
-----------------

.. code-block:: python

   import teehr

   # Read-write access (requires AWS credentials with write permissions)
   ev = teehr.RemoteReadWriteEvaluation()

.. note::

   This evaluation creates a read-write remote evaluation. This is primarily intended for TEEHR-HUB
   administrators who need to manage the remote catalogs. It requires appropriate AWS credentials with
   write permissions to the remote catalogs. Use with caution, as changes will affect all users accessing
   the remote catalogs. For typical TEEHR-HUB users, the ``RemoteReadOnlyEvaluation`` is recommended to
   safely access data without risking unintended modifications. For local access to TEEHR warehouse data,
   use the ``Download`` class with a local evaluation.


Upgrading Existing Evaluations
==============================

If you have an evaluation created with an earlier version of TEEHR (pre-v0.6),
you'll need to migrate it to the new Iceberg-based format.

When opening an older evaluation, TEEHR will detect the version mismatch and
provide instructions for migrating your data to the new format. The migration process involves converting
your existing evaluation in-place to the new structure. You can use the provided utility function to perform
the conversion.:

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

Note this will leave your existing data intact and create the necessary Iceberg t
ables and structure. However, it's always a good idea to back up your data before
performing the conversion, just in case.  Once you are convinced the new format is
working well for you, you can delete the backup of the old evaluation and the old
version files in the evaluation directory.


Apache Spark Configuration
==========================

TEEHR uses Apache Spark with Apache Iceberg for scalable data processing. By default,
TEEHR automatically creates an optimized Spark session for local usage on your machine
or within TEEHR-HUB.

Default Configuration
---------------------

The default configuration:

- Runs in local mode using all available cores
- Sets driver memory to 75% of available system memory
- Configures Iceberg catalogs (local JDBC + remote REST)
- Includes required packages (Iceberg, Sedona for geospatial, AWS S3)

Custom Spark Session
--------------------

For advanced use cases, you can create a custom Spark session that will allow your analyics
to scale beyond your local machine or to customize Spark settings. You can create a Spark
session using the ``create_spark_session`` utility function and pass it to your evaluation:

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

In TEEHR-HUB or other Kubernetes environments, you can start executor pods:

.. code-block:: python

   spark = create_spark_session(
       start_spark_cluster=True,
       executor_instances=4,
       executor_memory="4g",
       executor_cores=2
   )

.. note::
   When running in cluster mode, the Spark UI will be accessible through the TEEHR-HUB interface,
   and logs will be written to the temporary evaluation directory for easier debugging.

.. warning::
   It is very important to stop your Spark session when running in cluster mode to avoid
   leaving orphaned executor pods running in your Kubernetes cluster. Always call ``spark.stop()``
   when you are done with your analysis, especially in scripts or notebooks that may be left open.

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

Once you have an evaluation, you can access its components.  The following
are common operations you can perform with an Evaluation instance:

.. code-block:: python

   import teehr

   ev = teehr.LocalReadWriteEvaluation(dir_path="./my_eval", create_dir=True)

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
