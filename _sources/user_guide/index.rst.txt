.. _user_guide:

==========
User Guide
==========

Run through several "use-cases" here, start to finish

NWM retro
---------

* Fetch NWM retro and USGS data
* Load into a database
* Run some queries
* Visualize some output


NWM forecasts
-------------
* Fetch NWM operational forecasts and USGS data
* Load into a database
* Run some queries
* Visualize some output


Gridded/MAP analysis
--------------------
* ...


Web interface
-------------
* ...


TEEHR-Hub
---------
* ... ?


Some Tests
----------

ipython example:

.. ipython:: python

   import pandas as pd

   s = pd.Series([1, 3, 5, np.nan, 6, 8])
   s

:download:`Download this example notebook (right-click, Save-As) <notebooks/loading/grid_loading_example.ipynb>`

.. toctree::
   :maxdepth: 2
   :hidden:

   notebooks/loading_examples_index
   notebooks/queries_examples_index


.. include: grid_loading_example.ipynb
.. :parser: myst_nb.docutils_
