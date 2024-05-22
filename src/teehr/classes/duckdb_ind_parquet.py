"""Class based queries based on individual parquet files."""
import teehr.models.queries_database as tmqd
import teehr.queries.utils as tqu
from teehr.classes.duckdb_base import DuckDBBase
from teehr.models.queries import MetricEnum


import duckdb
import geopandas as gpd
import pandas as pd


from pathlib import Path
from typing import Any, Dict, List, Union, Optional


class DuckDBJoinedParquet(DuckDBBase):
    pass