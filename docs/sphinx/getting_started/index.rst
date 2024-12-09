.. _getting_started:

===============
Getting started
===============


Installation
------------
TEEHR requires the following dependencies:

* Python 3.10 or later

* Java 8 or later for Spark (we use 17)


The easiest way to install TEEHR is from PyPI using `pip`.
If using `pip` to install TEEHR, we recommend installing TEEHR in a virtual environment.
The code below creates a new virtual environment and installs TEEHR in it.

.. code-block:: python

   # Create directory for your code and create a new virtual environment.
   mkdir teehr_examples
   cd teehr_examples
   python3 -m venv .venv
   source .venv/bin/activate

   # Install using pip.
   # Starting with version 0.4.1 TEEHR is available in PyPI
   pip install teehr

   # Download the required JAR files for Spark to interact with AWS S3.
   python -m teehr.utils.install_spark_jars

Or, if you do not want to install TEEHR in your own virtual environment, you can use Docker:

.. code-block:: bash

   docker build -t teehr:v0.4.5 .
   docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:v0.4.5 jupyter lab --ip 0.0.0.0 $HOME

Project Objectives
------------------

* Easy integration into research workflows

* Use of modern and efficient data structures and computing platforms

* Scalable for rapid execution of large-domain/large-sample evaluations

* Simplified exploration of performance trends and potential drivers (e.g., climate, time-period, regulation, and basin characteristics)

* Inclusion of common and emergent evaluation methods (e.g., error statistics, skill scores, categorical metrics, hydrologic signatures, uncertainty quantification, and graphical methods)

* Open source and community-extensible development


Why TEEHR?
----------
TEEHR is a python package that provides a framework for the evaluation of hydrologic model performance.
It is designed to enable iterative and exploratory analysis of hydrologic data, and facilitates this through:

* Scalability - TEEHR's computational engine is built on PySpark, allowing it to take advantage of your available compute resources.

* Data Integrity - TEEHR's internal data model (:doc:`teehr_framework`) makes it easier to work with and validate the various data making up your evaluation, such as model outputs, observations, location attributes, and more.

* Flexibility - TEEHR is designed to be flexible and extensible, allowing you to easily customize metrics, add bootstrapping, and group and filter your data in a variety of ways.


TEEHR Evaluation Example
------------------------
The following is an example of initializing a TEEHR Evaluation, cloning a dataset from the TEEHR S3 bucket,
and calculating two versions of KGE (one with bootstrap uncertainty and one without).

.. code-block:: python

   import teehr
   from pathlib import Path

   # Initialize an Evaluation object
   ev = teehr.Evaluation(
      dir_path=Path(Path().home(), "temp", "quick_start_example"),
      create_dir=True
   )

   # Clone the example data from S3
   ev.clone_from_s3("e0_2_location_example")

   # Define a bootstrapper with custom parameters.
   boot = teehr.Bootstrappers.CircularBlock(
      seed=50,
      reps=500,
      block_size=10,
      quantiles=[0.05, 0.95]
   )
   kge = teehr.Metrics.KlingGuptaEfficiency(bootstrap=boot)
   kge.output_field_name = "BS_KGE"

   include_metrics = [kge, teehr.Metrics.KlingGuptaEfficiency()]

   # Get the currently available fields to use in the query.
   flds = ev.joined_timeseries.field_enum()

   metrics_df = ev.metrics.query(
      include_metrics=include_metrics,
      group_by=[flds.primary_location_id],
      order_by=[flds.primary_location_id]
   ).to_pandas()

   metrics_df


For a full list of metrics currently available in TEEHR, see the :doc:`/user_guide/metrics/metrics` documentation.

.. toctree::
    :maxdepth: 2
    :hidden:

    TEEHR Framework <teehr_framework>