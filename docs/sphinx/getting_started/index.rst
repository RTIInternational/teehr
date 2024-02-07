.. _getting_started:

===============
Getting started
===============

Installation
------------
There are several methods for installing TEEHR.

You can install from PyPI using pip [TODO]:

.. code-block:: python

   pip install teehr

You can install from github:

.. code-block:: python

   # Using pip
   pip install 'teehr @ git+https://github.com/RTIInternational/teehr@[BRANCH_TAG]'

   # Using poetry
   poetry add git+https://github.com/RTIInternational/teehr.git#[BRANCH TAG]

You can use Docker:

.. code-block:: bash

   docker build -t teehr:v0.3.2 .
   docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:v0.3.2 jupyter lab --ip 0.0.0.0 $HOME


API Overview
------------
TEEHR is comprised of several submodules having specific functionality related to loading, storing,
processing, and visualizing hydrologic data.

* **Loading**: For fetching and formatting data (ie, NWM forecasts or USGS streamflow records).
* **Queries**: For querying data from cached parquet files or databases and for generating metrics.
* **Database**: For building and querying data using a persistent database.
* **API**: For enabling web-based analysis of a TEEHR database.
* **Utilities**: Helper scripts for common analysis tasks.

Each submodule can be imported independently:

.. ipython:: python

   # To fetch and format NWM point data.
   import teehr.loading.nwm.nwm_points as tlp

   # For querying cached parquet files.
   import teehr.queries.duckdb as tqd


.. note::

   Add note about using Dask (many functions, are designed to take advantage of Dask,
   especially loading, and you should see performance improvements by starting a local Dask cluster)

For example:

.. code-block:: python

   import os
   from dask.distributed import Client

   n_workers = max(os.cpu_count() - 1, 1)
   client = Client(n_workers=n_workers)


Examples
--------

Fetching NWM Data
^^^^^^^^^^^^^^^^^

An example of using TEEHR to fetch retrospective NWM v2.0 data and format into the TEEHR data model
is shown here.

.. code-block:: python

   # Import the packages.
   import teehr.loading.nwm.retrospective as nwm_retro
   from pathlib import Path
   from datetime import datetime

   # Define the import variables.
   NWM_VERSION = "nwm20"
   VARIABLE_NAME = "streamflow"
   START_DATE = datetime(2000, 1, 1)
   END_DATE = datetime(2000, 1, 2, 23)
   LOCATION_IDS = [7086109, 7040481]

   OUTPUT_ROOT = Path(Path().home(), "temp")
   OUTPUT_DIR = Path(OUTPUT_ROOT, "nwm20_retrospective")

   # Fetch and format
   nwm_retro.nwm_retro_to_parquet(
      nwm_version=NWM_VERSION,
      variable_name=VARIABLE_NAME,
      start_date=START_DATE,
      end_date=END_DATE,
      location_ids=LOCATION_IDS,
      output_parquet_dir=OUTPUT_DIR
   )

TEEHR Database
^^^^^^^^^^^^^^

Once the data adheres to the TEEHR data model, we can use the `TEEHRDatasetDB` class
to create a persisent database, allowing for efficient exploration and metric queries.

.. ipython:: python

   from pathlib import Path

   from teehr.database.teehr_dataset import TEEHRDatasetDB

   # Define file paths the test data
   PRIMARY_FILEPATH = "../../tests/data/test_study/timeseries/*short_obs.parquet"
   SECONDARY_FILEPATH = "../../tests/data/test_study/timeseries/*_fcast.parquet"
   CROSSWALK_FILEPATH = "../../tests/data/test_study/geo/crosswalk.parquet"
   DATABASE_FILEPATH = Path("../../tests/data/temp/temp_test.db")


Example notebooks
^^^^^^^^^^^^^^^^^

See the :doc:`Loading <../user_guide/notebooks/loading_examples_index>` and
:doc:`Query <../user_guide/notebooks/queries_examples_index>` notebooks for more in-depth examples.

Data Model
----------

Link to the data model documentation: :ref:`data_model`


Queries
-------

Link to the queries documentation: :ref:`queries`

.. toctree::
    :maxdepth: 2
    :hidden:

    data_model
    queries
