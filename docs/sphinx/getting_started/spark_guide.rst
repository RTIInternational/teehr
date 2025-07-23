.. _spark_guide:

=====================
Introduction to Spark
=====================

Introduction
------------
Apache Spark is a unified analytics engine for large-scale data processing, with built-in modules for SQL, machine
learning, and graph processing. It is designed to be fast and general-purpose, and it can handle both
batch and real-time data processing. Spark is particularly well-suited for big data applications, as it can process
large datasets across a distributed cluster of computers. Spark provides a programming model that allows developers
to write applications in Java, Scala, Python, and R.

Our TEEHR framework leverages PySpark, the Python API for Apache Spark, to enable scalable and efficient data processing
for hydrologic model evaluations. By integrating PySpark into TEEHR, users can efficiently process, analyze, and evaluate
large volumes of hydrologic model outputs, enabling robust and scalable workflows for water resources research and operational
applications.

While this introduction provides a high-level overview of Spark, users are encouraged to refer to the official
`Apache Spark documentation <https://spark.apache.org/docs/latest/api/python/index.html#>`_ for more detailed information.

Creating a Spark Session
------------------------
To use PySpark, you need to create a Spark session. This session is the entry point for using Spark functionality.
When creating your TEEHR evaluation, a Spark session is automatically created for you.  An example of creating the
Spark session using TEEHR's default configuration is shown below:

.. code-block:: python

    import teehr
    from pathlib import Path

    # Create a TEEHR evaluation
    evaluation = teehr.Evaluation(dir_path=Path("path/to/your/evaluation"),
                                  create_dir=True)

    # Access the Spark session
    spark = evaluation.spark


Within the TEEHR framework, the Spark session is automatically configured with the necessary settings to work with
the TEEHR data model. The default spark configuration created by TEEHR dynamically updates the driver memory and
max result size to equal 75% and 50% of you on-board system memory, respectively. Advanced TEEHR users can also define
their own Spark session configuration by passing a custom configuration to the `teehr.Evaluation` constructor. For example,
if the user encountered a memory management issue, they could create a custom Spark session with increased memory settings
as follows:

.. code-block:: python

    import teehr
    from pathlib import Path
    from pyspark.sql import SparkSession
    from pyspark import SparkConf

    # Create your custom spark configuration
    conf = (
        SparkConf()
        .setAppName("TEEHR")
        .setMaster("local[*]")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.driver.memory", "32g")
        .set("spark.driver.maxResultSize", "24g")
        )
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

    # Create a TEEHR evaluation with the custom Spark session
    evaluation = teehr.Evaluation(dir_path=Path("path/to/your/evaluation"),
                                  create_dir=True,
                                  spark=spark_session)

For additional information on configuring Spark sessions, refer to the official
`Spark Session documentation  <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html>`_.

Common Spark Warnings
---------------------
By default, TEEHR sets the default log level to "WARN" -- meaning only warnings will be shown in the notebook/terminal output while running a script.
When using Spark, you may encounter some common warnings that are not necessarily errors but are worth noting:

1. **SparkSession Already Exists**: If you try to create a Spark session when one already exists, you may see a warning indicating that the session
already exists. This is normal behavior in PySpark and can be ignored if you are not trying to create multiple sessions.

2. **Native Hadoop Library**: You may see a warning about the native Hadoop library not being found. This is common when running Spark on certain systems
and can usually be ignored.

3. **SparkStringUtils**: You may see warnings related to `SparkStringUtils` when using certain string operations. These warnings are typically related to
performance optimizations and can be ignored unless you are experiencing performance issues.

4. **Arrow Optimization**: If you see warnings related to Arrow optimization, it may indicate that certain operations are not fully optimized for performance.
These warnings can usually be ignored unless you are experiencing performance issues.

5. **Broadcasting Large Task**: If you see warnings about broadcasting large tasks, it may indicate that you are trying to broadcast a large dataset
to all nodes in the cluster. This can lead to performance issues and may require you to increase the memory allocated to the Spark driver if you encounter
errors following the warning. You can adjust the memory settings in your Spark configuration to handle larger datasets.

6. **Memory Management**: You may see warnings related to memory management, such as "Task not serializable" or "Out of memory". These warnings
can indicate that your Spark job is using too much memory or that there are issues with serialization of objects. You may need to adjust your Spark configuration
to allocate more memory or optimize your code to reduce memory usage.
