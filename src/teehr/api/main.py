"""Module for database queries via FastAPI."""
import json

from yaml import load  # , dump
try:
    from yaml import CLoader as Loader  # , CDumper as Dumper
except ImportError:
    from yaml import Loader  # , Dumper

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from typing import Union, Dict, List
# from pydantic import validator
from pathlib import Path

# from teehr.queries.duckdb import get_metrics
from teehr.classes.duckdb_database_api import DuckDBDatabaseAPI
from teehr.models.queries import (
    # JoinedFilterFieldEnum,
    # JoinedFilter,
    # BaseModel,
    FilterOperatorEnum,
    MetricEnum
)
from teehr.models.queries_database import (
    MetricQuery,
    TimeseriesQuery,
    TimeseriesCharQuery,
    JoinedTimeseriesFieldName
)
import pandas as pd
import geopandas as gpd
# import duckdb

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


with open(Path(Path(__file__).resolve().parent, "data.yaml")) as f:
    datasets = load(f.read(), Loader)


def format_response(df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> List[Dict]:
    """Convert the query response format from the dataframe to a dict.

    Parameters
    ----------
    df : Union[gpd.GeoDataFrame, pd.DataFrame]
        Dataframe or Geodataframe.

    Returns
    -------
    List[Dict]
        A list of dictionaries.
    """
    # print(df.info())
    if isinstance(df, gpd.GeoDataFrame):
        # convert datetime/duration to string
        for col in df.columns:
            if df[col].dtype in ["datetime64[ns]", "timedelta64[ns]"]:
                df[col] = df[col].astype(str)
        return json.loads(df.to_json())
    elif isinstance(df, pd.DataFrame):
        return df.to_dict(orient="records", index=True)
    else:
        return df


@app.get("/")
def read_root():
    """Root message."""
    return {"msg": "Welcome to TEEHR"}


@app.get("/datasets/")
async def get_datasets():
    """Get datasets."""
    return datasets["datasets"]


@app.get("/datasets/{dataset_id}")
async def read_dataset_by_id(dataset_id: str) -> Dict:
    """Get a dataset by its ID.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.

    Returns
    -------
    Dict
        Dataset filepaths.
    """
    return datasets["datasets"][dataset_id]


@app.get("/datasets/{dataset_id}/get_metric_fields")
async def get_metric_fields(
    dataset_id: str,
) -> List[str]:
    """Get metric fields.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.

    Returns
    -------
    List
        List of available metrics.
    """
    return list(MetricEnum)


@app.get("/datasets/{dataset_id}/get_data_fields")
async def get_data_fields(
    dataset_id: str,
) -> List[Dict]:
    """Get data fields and datatypes.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.

    Returns
    -------
    List
        A list of dictionaries of database fields and their types.
    """
    config = datasets["datasets"][dataset_id]
    tds = DuckDBDatabaseAPI(config["database_filepath"])
    fields = tds.get_joined_timeseries_schema()
    fields.rename(
        columns={
            "column_name": "name",
            "column_type": "type"
        },
        inplace=True
    )
    return fields[["name", "type"]].to_dict(orient="records")


@app.get("/datasets/{dataset_id}/get_filter_operators")
async def get_filter_operators(dataset_id: str) -> Dict:
    """Get filter operators.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.

    Returns
    -------
    Dict
        A dictionary of filter operator keywords and their symbols.
    """
    return {item.name: item.value for item in FilterOperatorEnum}


# @app.get("/datasets/{dataset_id}/get_group_by_fields")
# async def get_metric_fields(
#     dataset_id: str,
# ):

#     db = TEEHRDataset(filepath="/some/file/path/to/database.db")
#     # df = db.get_database_fields()
#     df = db.get_metrics(APIMetricQuery)

#     return {"fields": df}


# @app.post("/datasets/{dataset_id}/get_field_values")
# async def get_metric_fields(
#     dataset_id: str,
#     field_name: str
# ):

#     db = TEEHRDataset(filepath="/some/file/path/to/database.db")
#     list = db.get_distinct_values("field_name")

#     return {"fields": df}


@app.post("/datasets/{dataset_id}/get_metrics")
async def get_metrics_by_query(
    dataset_id: str,
    api_metrics_query: MetricQuery
) -> List[Dict]:
    """Get metrics by query.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.
    api_metrics_query : MetricQuery
        Pydantic model of query variables.

    Returns
    -------
    List[Dict]
        A list of dictionaries of query results.
    """
    config = datasets["datasets"][dataset_id]
    tds = DuckDBDatabaseAPI(config["database_filepath"])
    df = tds.get_metrics(api_metrics_query)

    return format_response(df)


@app.post("/datasets/{dataset_id}/get_timeseries")
async def get_timeseries_by_query(
    dataset_id: str,
    api_timeseries_query: TimeseriesQuery
) -> List[Dict]:
    """Get timeseries by query.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.
    api_timeseries_query : TimeseriesQuery
        Pydantic model of query variables.

    Returns
    -------
    List[Dict]
        A list of dictionaries of query results.
    """
    config = datasets["datasets"][dataset_id]
    tds = DuckDBDatabaseAPI(config["database_filepath"])
    df = tds.get_timeseries(api_timeseries_query)

    return format_response(df)


@app.post("/datasets/{dataset_id}/get_timeseries_chars")
async def get_timeseries_chars_by_query(
    dataset_id: str,
    api_timeseries_char_query: TimeseriesCharQuery
) -> List[Dict]:
    """Get time series characteristics by query.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.
    api_timeseries_char_query : TimeseriesCharQuery
        Pydantic model of query variables.

    Returns
    -------
    List[Dict]
        A list of dictionaries of query results.
    """
    config = datasets["datasets"][dataset_id]
    tds = DuckDBDatabaseAPI(config["database_filepath"])
    df = tds.get_timeseries_chars(api_timeseries_char_query)

    return format_response(df)


@app.post("/datasets/{dataset_id}/get_unique_field_values")
async def get_unique_field_vals(
    dataset_id: str,
    api_field_name: JoinedTimeseriesFieldName
) -> List[Dict]:
    """Get unique field names in the joined_timeseries table.

    Parameters
    ----------
    dataset_id : str
        Dataset ID.
    api_field_name : JoinedTimeseriesFieldName
        Pydantic model of query variables.

    Returns
    -------
    List[Dict]
        Pydantic model of query variables.
    """
    config = datasets["datasets"][dataset_id]
    tds = DuckDBDatabaseAPI(config["database_filepath"])
    df = tds.get_unique_field_values(api_field_name)

    return format_response(df)
