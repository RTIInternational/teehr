.. _getting_started:

===============
Getting started
===============

Installation
------------
.. code-block::

   # Install from GitHub
   Using pip
   $ pip install 'teehr @ git+https://github.com/RTIInternational/teehr@[BRANCH_TAG]'

   # Using poetry
   $ poetry add git+https://github.com/RTIInternational/teehr.git#[BRANCH TAG]


Can we publish this on PyPI?

API Overview
------------
A descriptive high-level summary of the API components.

* Loading
* Queries
* Database
* Utilities
* Web App
* ...

Examples
--------
Link to, or parse the example notebooks here?

.. code-block:: python

   import teehr.loading.nwm_common.retrospective as nwm_retro

   nwm_retro.nwm_retro_to_parquet(
      nwm_version=NWM_VERSION,
      variable_name=VARIABLE_NAME,
      start_date=START_DATE,
      end_date=END_DATE,
      location_ids=LOCATION_IDS,
      output_parquet_dir=OUTPUT_DIR
   )


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
