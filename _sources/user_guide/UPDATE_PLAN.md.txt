I want to redo the User Guide section of the docs. New section should roughly follow the outline below, but should be arranged to flow ( move away from notebooks in the user guide except an examples section). It should include lots of code examples that can be copy pasted and edited as needed.  Still want plenty of description and narrative too.

# The Evaluation
- Creating an evaluation is the first step to working with TEEHR.
 -Cover the different evaluation classes and what they are used for. Local vs Remote
- For Local Evaluations cover upgrading if you have an Evaluation that was created with an earlier version.  Starting an evaluation should check for this and provide instructions.
- Apache Spark and how it is used in TEEHR including the create spark session utility, default options plus starting executors.
# The Basic Tables (domain tables, timseries tables, location data, etc.) that make up the core evaluation 
- The schema
- The Table class and its core methods
- Loading methods when using a Local Evaluation. Cover the different tables and the methods that exist for loading.
- Method chaining
- Should maybe include a section on the TeehrDataframeBase…not sure if it is needed.
- The different ways to get a table reference ev.primary_timeseries, ev.table(“primary_timeseries)
# Fetching and Downloading
Getting data for your Local Evaluation from external sources both the TEEHR warehouse and non-TEEHR sources (usgs, nws, etc.)
- USGS
  - Edge cases (dict vs. list)
- NWM
  - Retro
  - Operational
  - Points vs. Grids (weights calculation)
- Using the download method to fetch data from the TEEHR warehouse
# The Views
- Basic methods on all views
- The joined timseries view including the existing figures of how the join works
- Describe the other views and what they do.  They are basically convenience views 
- Adding user defined fields
- Row Level and Timeseries Aware
- Event detection
# The Metrics (in the query method)
- Reference this in the other query method sections  here for deeper discussion of include metrics. Specifically, the group by and include metrics.  Use the diagrams and discussion of the group by and include metrics
- Cover signature, deterministic, ensemble metrics, etc 
- Bootstrapping discussion, how to use it
- Transforms
- Table of available metrics
- Examples of how to chain queries together the calculate more complex metrics.
# Generating data 
- Signature
- Benchmark
# Place holder for some examples of how to generate maps and plots using holoviews from the TEEHR query data (I have some examples for this). 
- Basic Map
- Basic plot showing observed and retrospective simulated timeseries for a simple location and configuration. Similar example for forecast timeseries.
- Basic metrics shown on a map
# Example Notebooks (only place where Notebooks will be used in docs)
- Accessing a remote warehouse.  This is currently only possible when running in TEEHR HUB. Set up a new evaluation and run some queries.
- Creating a new local evaluation and populating with some data from the TEEHR warehouse using the download class through the TEEHR API
- Starting new Local Evaluation from scratch and load some data, etc.
- This section will grow