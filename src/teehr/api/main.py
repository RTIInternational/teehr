import json

from yaml import load  # , dump
try:
    from yaml import CLoader as Loader  # , CDumper as Dumper
except ImportError:
    from yaml import Loader  # , Dumper

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from typing import List, Optional, Union
from pydantic import validator
from pathlib import Path

from teehr.queries.duckdb import get_metrics
from teehr.models.queries import (
    JoinedFilterFieldEnum,
    JoinedFilter,
    BaseModel,
    MetricEnum
)
import pandas as pd
import geopandas as gpd
import duckdb

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


class APIMetricQuery(BaseModel):
    group_by: List[JoinedFilterFieldEnum]
    order_by: List[JoinedFilterFieldEnum]
    include_metrics: Union[List[str], str]
    filters: Optional[List[JoinedFilter]] = []
    return_query: bool
    include_geometry: bool

    @validator("filters")
    def filter_must_be_list(cls, v):
        if v is None:
            return []
        return v


def format_response(resp: Union[gpd.GeoDataFrame, pd.DataFrame]) -> dict:
    if isinstance(resp, gpd.GeoDataFrame):
        # convert datetime/duration to string
        for col in resp.columns:
            if resp[col].dtype in ["datetime64[ns]", "timedelta64[ns]"]:
                resp[col] = resp[col].astype(str)
        return json.loads(resp.to_json())
    elif isinstance(resp, pd.DataFrame):
        return resp.to_dict(orient="records")
    else:
        return resp


@app.get("/")
def read_root():
    return {"msg": "Welcome to TEEHR"}


@app.get("/datasets/")
async def get_datasets():
    return datasets["datasets"]


@app.get("/datasets/{dataset_id}")
async def read_dataset_by_id(dataset_id: str):
    return datasets["datasets"][dataset_id]


@app.get("/datasets/{dataset_id}/get_metric_fields")
async def get_metric_fields(
    dataset_id: str,
):
    return list(MetricEnum)


@app.get("/datasets/{dataset_id}/get_group_by_fields")
async def get_group_by_fields(
    dataset_id: str,
):
    return list(JoinedFilterFieldEnum)


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
    api_metrics_query: APIMetricQuery
):

    # once we have a DataSet approach implemented, get Dataset here
    config = datasets["datasets"][dataset_id]

    # converting to dict and then unpacking is bad
    # need to chang func sig to take MetricQuery
    df = get_metrics(**{**api_metrics_query.dict(), **config})

    return format_response(df)


# @app.post("/datasets/{dataset_id}/get_timeseries")
# async def get_metrics_by_query(dataset_id: str, api_metrics_query: APIMetricQuery):

#     # once we have a DataSet approach implemented, get Dataset here
#     config = datasets["datasets"][dataset_id]

#     # converting to dict and then unpacking is bad
#     # need to chang func sig to take MetricQuery
#     resp = get_metrics(**{**api_metrics_query.dict(), **config})

#     return format_response(resp)


# @app.post("/datasets/{dataset_id}/get_timeseries_chars")
# async def get_metrics_by_query(dataset_id: str, api_metrics_query: APIMetricQuery):

#     # once we have a DataSet approach implemented, get Dataset here
#     config = datasets["datasets"][dataset_id]

#     # converting to dict and then unpacking is bad
#     # need to chang func sig to take MetricQuery
#     resp = get_metrics(**{**api_metrics_query.dict(), **config})

#     return format_response(resp)


# @app.post("/datasets/{dataset_id}/get_joined_timeseries")
# async def get_metrics_by_query(dataset_id: str, api_metrics_query: APIMetricQuery):

#     # once we have a DataSet approach implemented, get Dataset here
#     config = datasets["datasets"][dataset_id]

#     # converting to dict and then unpacking is bad
#     # need to chang func sig to take MetricQuery
#     resp = get_metrics(**{**api_metrics_query.dict(), **config})

#     return format_response(resp)