import json

from yaml import load  # , dump
try:
    from yaml import CLoader as Loader  # , CDumper as Dumper
except ImportError:
    from yaml import Loader  # , Dumper

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from typing import Union
# from pydantic import validator
from pathlib import Path

# from teehr.queries.duckdb import get_metrics
from teehr.database.teehr_dataset import TEEHRDatasetAPI
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


def format_response(df: Union[gpd.GeoDataFrame, pd.DataFrame]) -> dict:
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


@app.get("/datasets/{dataset_id}/get_data_fields")
async def get_data_fields(
    dataset_id: str,
):
    config = datasets["datasets"][dataset_id]
    tds = TEEHRDatasetAPI(config["database_filepath"])
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
async def get_filter_operators(dataset_id: str):
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
):
    config = datasets["datasets"][dataset_id]
    tds = TEEHRDatasetAPI(config["database_filepath"])
    df = tds.get_metrics(api_metrics_query)

    return format_response(df)


@app.post("/datasets/{dataset_id}/get_timeseries")
async def get_timeseries_by_query(
    dataset_id: str,
    api_timeseries_query: TimeseriesQuery
):

    config = datasets["datasets"][dataset_id]
    tds = TEEHRDatasetAPI(config["database_filepath"])
    df = tds.get_timeseries(api_timeseries_query)

    return format_response(df)


@app.post("/datasets/{dataset_id}/get_timeseries_chars")
async def get_timeseries_chars_by_query(
    dataset_id: str,
    api_timeseries_char_query: TimeseriesCharQuery
):

    config = datasets["datasets"][dataset_id]
    tds = TEEHRDatasetAPI(config["database_filepath"])
    df = tds.get_timeseries_characteristics(api_timeseries_char_query)

    return format_response(df)


@app.post("/datasets/{dataset_id}/get_unique_field_values")
async def get_unique_field_vals(
    dataset_id: str,
    api_field_name: JoinedTimeseriesFieldName
):
    config = datasets["datasets"][dataset_id]
    tds = TEEHRDatasetAPI(config["database_filepath"])
    df = tds.get_unique_field_values(api_field_name)

    return format_response(df)