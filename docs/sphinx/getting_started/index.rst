.. _getting_started:

===============
Getting started
===============

Installation
------------
There are several methods currently available for installing TEEHR.

You can install from github:

.. code-block:: python

   # Using pip
   pip install 'teehr @ git+https://github.com/RTIInternational/teehr@[BRANCH_TAG]'

   # Using poetry
   poetry add git+https://github.com/RTIInternational/teehr.git#[BRANCH TAG]

You can use Docker:

.. code-block:: bash

   docker build -t teehr:[RELEASE TAG] .
   docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:[RELEASE TAG] jupyter lab --ip 0.0.0.0 $HOME

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

* Data Integrity - TEEHR's internal data model makes it easier to work with and validate the various data making up your evaluation, such as model outputs, observations, location attributes, and more.

* Flexibility - TEEHR is designed to be flexible and extensible, allowing you to easily customize metrics, add bootstrapping, and group and filter your data in a variety of ways.


TEEHR Evaluation Example
------------------------
The following is an example of initializing a TEEHR Evaluation on an existing dataset,
and calculating two versions of KGE (one including bootstrapping and one without).

.. code-block:: python

    import teehr

    # Initialize an Evaluation object
    ev = teehr.Evaluation(dir_path="test_data/test_study")

    # Enable logging
    ev.enable_logging()

    # Define a bootstrapper with custom parameters.
    boot = teehr.Bootstrappers.CircularBlock(
       seed=50,
       reps=500,
       block_size=10,
       quantiles=[0.05, 0.95]
    )
    kge = teehr.Metrics.KlingGuptaEfficiency(bootstrap=boot)
    kge.output_field_name = "BS_KGE"

    include_metrics = [kge, metrics.KlingGuptaEfficiency()]

    # Get the currently available fields to use in the query.
    flds = ev.joined_timeseries.field_enum()

    metrics_df = ev.metrics.query(
       include_metrics=include_metrics,
       group_by=[flds.primary_location_id],
       order_by=[flds.primary_location_id]
    ).to_geopandas()


For a full list of metrics currently available in TEEHR, see the :doc:`/user_guide/metrics/metrics` documentation.

.. toctree::
    :maxdepth: 2
    :hidden:

    TEEHR Framework <teehr_framework>