.. _fetching:

========================
Fetching and Downloading
========================

TEEHR provides methods to fetch data from external sources and download data from
the TEEHR warehouse. This section covers both approaches.


Fetching External Data
======================

The ``fetch`` component retrieves data from USGS and the National Water Model (NWM),
validates it, and loads it directly into your evaluation.


USGS Streamflow Data
--------------------

Fetch observed streamflow data from USGS gages:

.. code-block:: python

   import teehr
   from datetime import datetime

   ev = teehr.LocalReadWriteEvaluation(dir_path="./my_eval", create_dir=True)

   # First, load your USGS gage locations
   ev.locations.load_spatial("./data/usgs_gages.geojson")

   # Fetch streamflow data for all locations in the locations table
   ev.fetch.usgs_streamflow(
       start_date=datetime(2024, 1, 1),
       end_date=datetime(2024, 1, 31),
       service="iv",              # "iv" = hourly instantaneous, "dv" = daily
       filter_to_hourly=True,     # Drop 15-minute data
       convert_to_si=True         # Convert from ft³/s to m³/s
   )

See also: :meth:`Fetch.usgs_streamflow() <teehr.evaluation.fetch.Fetch.usgs_streamflow>`

The data is automatically:

1. Fetched from USGS NWIS
2. Converted to the TEEHR schema
3. Validated
4. Loaded into the ``primary_timeseries`` table

Parameters
^^^^^^^^^^

``start_date``, ``end_date``
    Time range to fetch (inclusive start, exclusive end).

``service``
    - ``"iv"`` - Instantaneous values (15-min or hourly)
    - ``"dv"`` - Daily mean values

``chunk_by``
    Process data in chunks to manage memory:
    ``"location_id"``, ``"day"``, ``"week"``, ``"month"``, ``"year"``, or ``None``

``filter_to_hourly``
    When True, keeps only values on the hour (drops 15-minute data).

``convert_to_si``
    Convert from ft³/s to m³/s.


Edge Cases: Sub-Locations
^^^^^^^^^^^^^^^^^^^^^^^^^

Some USGS sites have multiple measurement points. To handle these, use
the underlying function directly with a dictionary:

.. code-block:: python

   from teehr.fetching.usgs.usgs import usgs_to_parquet

   # Site with sub-locations
   sites = [
       {"site_no": "USGS-02449838", "description": "Main Gage"},
       "USGS-01234567"  # Regular site
   ]

   usgs_to_parquet(
       sites=sites,
       start_date="2024-01-01",
       end_date="2024-01-31",
       output_parquet_dir="./cache/usgs"
   )

   # Then load the resulting Parquet files into TEEHR
   ev.primary_timeseries.load_parquet("./cache/usgs/*.parquet")


NWM Retrospective Data (Points)
-------------------------------

Fetch NWM retrospective streamflow simulations at point locations:

.. code-block:: python

   # First, set up crosswalks mapping USGS to NWM location IDs
   ev.location_crosswalks.load_csv("./data/usgs_nwm_crosswalk.csv")

   # Fetch NWM v3.0 retrospective data
   ev.fetch.nwm_retrospective_points(
       nwm_version="nwm30",
       variable_name="streamflow",
       start_date=datetime(2020, 1, 1),
       end_date=datetime(2020, 12, 31),
       chunk_by="month"
   )

See also: :meth:`Fetch.nwm_retrospective_points() <teehr.evaluation.fetch.Fetch.nwm_retrospective_points>`

Supported NWM Versions
^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Version
     - Date Range
   * - ``nwm20``
     - 1993-01-01 to 2018-12-31
   * - ``nwm21``
     - 1979-01-01 to 2020-12-31
   * - ``nwm30``
     - 1979-02-01 to 2023-01-31

Supported Variables
^^^^^^^^^^^^^^^^^^^

- ``streamflow`` - Channel streamflow (m³/s)
- ``velocity`` - Channel velocity (m/s)


NWM Operational Data (Points)
-----------------------------

Fetch real-time NWM forecast data:

.. code-block:: python

   ev.fetch.nwm_operational_points(
       nwm_version="nwm30",
       forecast_configuration="analysis_assim",
       variable_name="streamflow",
       start_date=datetime(2024, 1, 1),
       end_date=datetime(2024, 1, 7)
   )

See also: :meth:`Fetch.nwm_operational_points() <teehr.evaluation.fetch.Fetch.nwm_operational_points>`

Forecast Configurations
^^^^^^^^^^^^^^^^^^^^^^^

- ``analysis_assim`` - Analysis and assimilation
- ``short_range`` - Short-range forecast (0-18 hours)
- ``medium_range`` - Medium-range forecast (0-10 days)


NWM Gridded Data
----------------

Fetch gridded NWM data (e.g., forcing variables) and compute zonal statistics:

.. code-block:: python

   # Fetch gridded precipitation and compute zonal means
   ev.fetch.nwm_retrospective_grids(
       nwm_version="nwm30",
       variable_name="RAINRATE",
       start_date=datetime(2020, 6, 1),
       end_date=datetime(2020, 6, 30),
       calculate_zonal_weights=True,  # Compute weights first time
       domain="CONUS"
   )

See also: :meth:`Fetch.nwm_retrospective_grids() <teehr.evaluation.fetch.Fetch.nwm_retrospective_grids>`

Zonal Weights
^^^^^^^^^^^^^

When ``calculate_zonal_weights=True``, TEEHR computes area-weighted averages
for each location's drainage basin. The weights are cached for subsequent calls.

For custom polygons, provide a weights file:

.. code-block:: python

   ev.fetch.nwm_retrospective_grids(
       nwm_version="nwm30",
       variable_name="RAINRATE",
       start_date="2020-06-01",
       end_date="2020-06-30",
       zonal_weights_filepath="./data/custom_weights.parquet"
   )

Gridded Variables
^^^^^^^^^^^^^^^^^

Forcing variables available:

- ``RAINRATE`` - Precipitation rate
- ``T2D`` - 2-meter temperature
- ``LWDOWN`` - Longwave radiation
- ``SWDOWN`` - Shortwave radiation
- ``Q2D`` - Specific humidity
- ``U2D``, ``V2D`` - Wind components
- ``PSFC`` - Surface pressure


Tuning NWM Fetching Performance
-------------------------------

The defaults are derived from the machine TEEHR is running on, so start by
leaving them alone. If a fetch is slower than expected, these are the knobs:

``io_concurrency``
    How many remote files are read at once. Defaults to 48; raise it on a fast
    network, and lower it when several fetches run at the same time, since the
    budgets multiply.

``cpu_workers``
    How many files are processed at once. Defaults to the number of CPUs
    available; setting it higher than that measures slower, not faster.

``process_by_z_hour``
    ``True`` (the default) puts one forecast cycle in each chunk, so a
    ``short_range`` chunk holds 18 files. ``False`` uses ``stepsize`` instead.

``stepsize``
    How many files go in each chunk when ``process_by_z_hour=False``. Defaults
    to 100.

Two things that are easy to miss:

- Each chunk is read in one go, so an ``io_concurrency`` above the chunk size
  does nothing for the read step. To make a higher value reachable, set
  ``process_by_z_hour=False`` with a larger ``stepsize``.
- Larger chunks need more memory, because every file in a chunk is held at once.

Not every method takes both: ``nwm_operational_points`` and
``nwm_operational_grids`` accept ``io_concurrency`` and ``cpu_workers``,
``nwm_retrospective_grids`` accepts only ``cpu_workers``, and
``process_by_z_hour`` and ``stepsize`` apply to ``nwm_operational_points``.

To set the concurrency once for a whole session rather than per call:

.. code-block:: python

    from teehr.utils.concurrency import set_concurrency

    set_concurrency(io=24, cpu=8)


Downloading from TEEHR Warehouse
================================

The ``download`` component retrieves pre-processed data from the TEEHR data warehouse
via the TEEHR-HUB REST API. This is useful for quickly setting up evaluations with
curated datasets.  Starting in May 2026 the API now requires authentication through the
use of API keys or bearer tokens, which can be obtained by contacting the TEEHR team.

Configure the API
-----------------

Authentication credentials and the API base URL are read automatically from
environment variables at startup, so calling ``configure()`` is optional for
common cases:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Environment variable
     - Purpose
   * - ``TEEHR_DOWNLOAD_API_BASE_URL``
     - Override the default API base URL
   * - ``TEEHR_DOWNLOAD_API_KEY``
     - API key sent as ``x-api-key``
   * - ``TEEHR_DOWNLOAD_BEARER_TOKEN``
     - Bearer token sent as ``Authorization: Bearer <token>``

Set these in your shell or ``.env`` file rather than embedding secrets in code:

.. code-block:: bash

   export TEEHR_DOWNLOAD_API_BASE_URL="some-alternative-api-endpoint"
   export TEEHR_DOWNLOAD_API_KEY="your-api-key-here"

Then use the download component without passing credentials in Python:

.. code-block:: python

   # No configure() call needed — credentials are read from env vars
   ev.download.locations(prefix="usgs", load=True)

   # configure() can still override individual settings explicitly
   ev.download.configure(
       api_base_url="https://api.teehr.rtiamanzi.org",
       verify_ssl=False,
       timeout=120
   )

See also: :meth:`Download.configure() <teehr.evaluation.download.Download.configure>`


Download Locations
------------------

.. code-block:: python

   # Preview available locations
   locs_df = ev.download.locations(prefix="usgs", limit=100)
   print(locs_df.head())

   # Download and load directly into evaluation
   ev.download.locations(prefix="usgs", load=True)

   # Filter by bounding box
   ev.download.locations(
       prefix="usgs",
       bbox=[-85, 30, -80, 35],  # [minx, miny, maxx, maxy]
       load=True
   )

   # Include attributes in response (for preview only)
   locs_with_attrs = ev.download.locations(
       prefix="usgs",
       include_attributes=True
   )

See also: :meth:`Download.locations() <teehr.evaluation.download.Download.locations>`


Download Domain Data
--------------------

.. code-block:: python

   # Download all configurations
   ev.download.configurations(load=True)

   # Download specific unit
   ev.download.units(name="m3/s", load=True)

   # Download variables
   ev.download.variables(load=True)

   # Download attributes (definitions)
   ev.download.attributes(type="continuous", load=True)

See also: :meth:`Download.configurations() <teehr.evaluation.download.Download.configurations>`,
:meth:`Download.units() <teehr.evaluation.download.Download.units>`,
:meth:`Download.variables() <teehr.evaluation.download.Download.variables>`,
:meth:`Download.attributes() <teehr.evaluation.download.Download.attributes>`


Download Location Attributes
----------------------------

.. code-block:: python

   # Download attributes for specific locations
   ev.download.location_attributes(
       location_id=["usgs-01010000", "usgs-01020000"],
       load=True
   )

   # Download all attributes for locations with a prefix
   ev.download.location_attributes(load=True)

See also: :meth:`Download.location_attributes() <teehr.evaluation.download.Download.location_attributes>`


Download Crosswalks
-------------------

.. code-block:: python

   # Download crosswalks for NWM v3.0
   ev.download.location_crosswalks(
       secondary_location_id_prefix="nwm30",
       load=True
   )

See also: :meth:`Download.location_crosswalks() <teehr.evaluation.download.Download.location_crosswalks>`


Download Timeseries
-------------------

.. code-block:: python

   # Download primary (observed) timeseries
   ev.download.primary_timeseries(
       primary_location_id=["usgs-01010000"],
       start_date="2020-01-01",
       end_date="2020-12-31",
       load=True
   )

   # Download secondary (simulated) timeseries
   ev.download.secondary_timeseries(
       configuration_name="nwm30_retrospective",
       primary_location_id=["usgs-01010000"],
       start_date="2020-01-01",
       end_date="2020-12-31",
       load=True
   )

See also: :meth:`Download.primary_timeseries() <teehr.evaluation.download.Download.primary_timeseries>`,
:meth:`Download.secondary_timeseries() <teehr.evaluation.download.Download.secondary_timeseries>`


Complete Workflow Examples
==========================

Example 1: New Evaluation with TEEHR Warehouse Data
---------------------------------------------------

.. code-block:: python

   import teehr

   # Create evaluation
   ev = teehr.LocalReadWriteEvaluation(dir_path="./nwm_eval", create_dir=True)

   # Download curated data from warehouse
   ev.download.configure()

   # Get domain tables
   ev.download.units(load=True)
   ev.download.variables(load=True)
   ev.download.configurations(load=True)
   ev.download.attributes(load=True)

   # Get locations and crosswalks
   ev.download.locations(primary_location_id=["usgs-01010000"], load=True)
   ev.download.location_attributes(primary_location_id=["usgs-01010000"], load=True)
   ev.download.location_crosswalks(secondary_location_id_prefix="nwm30", load=True)

   # Get timeseries data
   ev.download.primary_timeseries(
        configuration_name="usgs_observations",
        primary_location_id=["usgs-01010000"],
        start_date="2020-01-01",
        end_date="2020-12-31",
        load=True
   )
   ev.download.secondary_timeseries(
        configuration_name="nwm30_retrospective",
        primary_location_id=["usgs-01010000"],
        start_date="2020-01-01",
        end_date="2020-12-31",
        load=True
   )


Example 2: Fresh Data from USGS and NWM
---------------------------------------

.. code-block:: python

   import teehr
   from datetime import datetime

   ev = teehr.LocalReadWriteEvaluation(dir_path="./fresh_eval", create_dir=True)

   # Load your location data
   ev.locations.load_spatial("./data/my_gages.geojson")
   ev.location_crosswalks.load_csv("./data/my_crosswalk.csv")

   # Fetch fresh USGS data
   ev.fetch.usgs_streamflow(
       start_date=datetime(2024, 1, 1),
       end_date=datetime(2024, 3, 31)
   )

   # Fetch corresponding NWM retrospective data
   ev.fetch.nwm_retrospective_points(
       nwm_version="nwm30",
       variable_name="streamflow",
       start_date=datetime(2020, 1, 1),
       end_date=datetime(2022, 12, 31),
       chunk_by="year"
   )
