.. _visualization:

*************
Visualization
*************

TEEHR integrates with HoloViews/hvPlot and GeoViews for interactive visualization
of timeseries and spatial data. This section demonstrates common visualization
patterns for TEEHR evaluation results.

.. note::

    This section is under development. Additional examples and interactive
    visualizations will be added in future releases.


Prerequisites
=============

TEEHR includes visualization dependencies (installed with the main package):

- **HoloViews/hvPlot**: Interactive plotting for timeseries data
- **GeoViews**: Geographic visualization with Bokeh backend
- **matplotlib**: Static plotting support

.. code-block:: python

    import holoviews as hv
    import hvplot.pandas
    import geoviews as gv
    from bokeh.io import output_notebook

    # Enable notebook output
    hv.extension('bokeh')
    output_notebook()


Basic Timeseries Plot
=====================

Plot observed vs. simulated timeseries for a single location:

.. code-block:: python

    import teehr
    import hvplot.pandas

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Get joined timeseries for a specific location
    df = ev.joined_timeseries_view().filter(
        "primary_location_id = 'usgs-01184000'"
    ).to_pandas()

    # Plot primary (observed) vs secondary (simulated)
    plot = df.hvplot.line(
        x='value_time',
        y=['primary_value', 'secondary_value'],
        title='Streamflow: Observed vs. Simulated',
        xlabel='Time',
        ylabel='Streamflow (cfs)',
        legend='top',
        width=800,
        height=400
    )

    plot


Forecast Timeseries
-------------------

Plot ensemble forecast with spread:

.. code-block:: python

    # Filter for forecast data with multiple members
    df = ev.secondary_timeseries.filter(
        "configuration_name = 'nwm30_medium_range'"
    ).to_pandas()

    # Plot ensemble members
    plot = df.hvplot.line(
        x='value_time',
        y='value',
        by='member',
        title='Ensemble Forecast',
        xlabel='Time',
        ylabel='Streamflow (cfs)',
        width=800,
        height=400
    )

    plot


Basic Map
=========

Display locations with attributes:

.. code-block:: python

    import geoviews as gv
    import geoviews.tile_sources as gts

    gv.extension('bokeh')

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Get locations as GeoDataFrame
    gdf = ev.locations.to_geopandas()

    # Create point layer
    points = gv.Points(gdf).opts(
        size=8,
        color='blue',
        tools=['hover'],
    )

    # Add basemap tiles
    tiles = gts.OSM

    # Combine layers
    map_plot = tiles * points
    map_plot.opts(width=700, height=500, title='Evaluation Locations')


Metrics on a Map
================

Visualize performance metrics spatially:

.. code-block:: python

    import geoviews as gv
    from bokeh.palettes import RdYlGn11

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Calculate metrics with geometry
    from teehr.metrics import DeterministicMetrics

    metrics_gdf = ev.joined_timeseries_view().query(
        include_metrics=[DeterministicMetrics.KlingGuptaEfficiency()],
        group_by=["primary_location_id"],
        include_geometry=True
    ).to_geopandas()

    # Create colored points based on KGE
    points = gv.Points(
        metrics_gdf,
        kdims=['geometry'],
        vdims=['kling_gupta_efficiency', 'primary_location_id']
    ).opts(
        size=10,
        color='kling_gupta_efficiency',
        cmap='RdYlGn',
        clim=(0, 1),
        colorbar=True,
        tools=['hover'],
    )

    # Add basemap
    tiles = gv.tile_sources.OSM

    map_plot = tiles * points
    map_plot.opts(
        width=700,
        height=500,
        title='Kling-Gupta Efficiency by Location'
    )


Hydrograph Comparison
=====================

Compare observed and simulated hydrographs with metrics overlay:

.. code-block:: python

    import holoviews as hv
    import hvplot.pandas

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Get timeseries
    df = ev.joined_timeseries_view().filter(
        "primary_location_id = 'usgs-01184000'"
    ).to_pandas()

    # Calculate metrics
    from teehr.metrics import DeterministicMetrics

    metrics = ev.joined_timeseries_view().filter(
        "primary_location_id = 'usgs-01184000'"
    ).query(
        include_metrics=[
            DeterministicMetrics.KlingGuptaEfficiency(),
            DeterministicMetrics.NashSutcliffeEfficiency(),
        ],
        group_by=["primary_location_id"]
    ).to_pandas()

    kge = metrics['kling_gupta_efficiency'].values[0]
    nse = metrics['nash_sutcliffe_efficiency'].values[0]

    # Create hydrograph plot
    obs_line = df.hvplot.line(
        x='value_time', y='primary_value',
        label='Observed', color='blue', line_width=1.5
    )
    sim_line = df.hvplot.line(
        x='value_time', y='secondary_value',
        label='Simulated', color='red', line_width=1.5, alpha=0.7
    )

    # Add metrics as text annotation
    title = f'Hydrograph Comparison | KGE: {kge:.3f}, NSE: {nse:.3f}'

    plot = (obs_line * sim_line).opts(
        title=title,
        xlabel='Time',
        ylabel='Streamflow (cfs)',
        legend_position='top_right',
        width=900,
        height=400
    )

    plot


Additional Resources
====================

For more visualization examples, see:

- `HoloViews Documentation <https://holoviews.org/>`_
- `hvPlot Documentation <https://hvplot.holoviz.org/>`_
- `GeoViews Documentation <https://geoviews.org/>`_

Example notebooks demonstrating TEEHR visualizations are available in the
:ref:`examples` section.
