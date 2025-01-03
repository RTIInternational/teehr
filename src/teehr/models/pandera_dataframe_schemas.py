import pandera.pyspark as ps
import pandera as pa
import pyspark.sql.types as T
import pandas as pd
from typing import List, Union

# Domains
# PySpark Pandera Schemas

def desc_no_commas(pyspark_obj) -> bool:
    """Ensure description column does not contain commas."""
    return pyspark_obj.filter(pyspark_obj["description"].contains(",")).count() == 0


def ln_no_commas(pyspark_obj) -> bool:
    """Ensure description column does not contain commas."""
    return pyspark_obj.filter(pyspark_obj["long_name"].contains(",")).count() == 0


def valid_name(pyspark_obj) -> bool:
    """Ensure name column matches ^[a-zA-Z0-9_]+$."""
    return pyspark_obj.filter(pyspark_obj["name"].rlike(r"^[a-zA-Z0-9_]+$")).count() == pyspark_obj.count()


def valid_unit_name(pyspark_obj) -> bool:
    """Ensure name column matches ^[a-zA-Z0-9_^/]+$."""
    return pyspark_obj.filter(pyspark_obj["name"].rlike(r"^[a-zA-Z0-9_^/]+$")).count() == pyspark_obj.count()

def format_datetime64(s: pd.Series) -> pd.Series:
    # Convert to UTC.
    # if s.dt.tz is not None:
    #     s = s.dt.tz_convert("UTC")
    # Drop timezone information.
    s = s.dt.tz_localize(None)
    return s.astype("datetime64[ms]")

def configuration_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "name": pa.Column(
                    pa.String,
                    unique=True,
                    checks=[
                        pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                    ],
                    regex=r"^[a-zA-Z0-9_]+$"
                ),
                "type": pa.Column(
                    pa.String,
                    checks=pa.Check.isin(
                        ["primary", "secondary"]
                    )
                ),
                "description": pa.Column(
                    pa.String,
                    checks=pa.Check(lambda s: not s.str.contains(",").any())
                ),
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "name": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(valid_name, error="`name` column contains special characters")
                ),
                "type": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check.isin(["primary", "secondary"])
                ),
                "description": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(desc_no_commas, error="`description` column contains commas")
                ),
            },
            strict=True,
            unique=["name"],
            coerce=True
        )

def unit_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "name": pa.Column(
                    pa.String,
                    unique=True,
                    checks=[
                        pa.Check.str_matches(r"^[a-zA-Z0-9_^/]+$")
                    ]
                ),
                "long_name": pa.Column(
                    pa.String,
                    checks=pa.Check(lambda s: not s.str.contains(",").any())
                ),
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "name": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(valid_unit_name, error="`name` column contains special characters")
                ),
                "long_name": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(ln_no_commas, error="`long_name` column contains commas")
                ),
            },
            strict=True,
            unique=["name"],
            coerce=True
        )

def variable_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "name": pa.Column(
                    pa.String,
                    unique=True,
                    checks=[
                        pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                    ],
                    regex=r"^[a-zA-Z0-9_]+$"
                ),
                "long_name": pa.Column(
                    pa.String,
                    checks=pa.Check(lambda s: not s.str.contains(",").any())
                ),
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "name": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(valid_name, error="`name` column contains special characters")
                ),
                "long_name": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(ln_no_commas, error="`long_name` column contains commas")
                ),
            },
            strict=True,
            unique=["name"],
            coerce=True
        )

def attribute_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "name": pa.Column(
                    pa.String,
                    unique=True,
                    checks=[
                        pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                    ],
                ),
                "type": pa.Column(
                    pa.String,
                    checks=pa.Check.isin(
                        ["categorical", "continuous"]
                    )
                ),
                "description": pa.Column(
                    pa.String,
                    checks=pa.Check(lambda s: not s.str.contains(",").any())
                ),
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "name": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(valid_name, error="`name` column contains special characters")
                ),
                "type": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check.isin(["categorical", "continuous"])
                ),
                "description": ps.Column(
                    T.StringType(),
                    nullable=False,
                    checks=ps.Check(desc_no_commas, error="`description` column contains commas")
                ),
            },
            strict=True,
            unique=["name"],
            coerce=True
        )

# Locations
# PySpark Pandera Models
def locations_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "id": pa.Column(str, coerce=True),
                "name": pa.Column(str, coerce=True),
                "geometry": pa.Column("geometry")
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "id": ps.Column(T.StringType(), nullable=False),
                "name": ps.Column(T.StringType(), nullable=False),
                "geometry": ps.Column(T.BinaryType(), nullable=False),
            },
            strict=True,
            unique=["id"],
            coerce=True
        )

def location_attributes_schema(
        location_ids: List[str] = None,
        attr_names: List[str] = None,
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "location_id": pa.Column(
                    pa.String,
                    # pa.Check.isin(location_ids),
                    coerce=True
                ),
                "attribute_name": pa.Column(
                    pa.String,
                    # pa.Check.isin(attr_names),
                    coerce=True
                ),
                "value": pa.Column(
                    pa.String,
                    coerce=True
                )
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "location_id": ps.Column(
                    T.StringType(),
                    ps.Check.isin(location_ids),
                    nullable=False
                ),
                "attribute_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(attr_names),
                    nullable=False
                ),
                "value": ps.Column(
                    T.StringType(),
                    nullable=False,
                )
            },
            strict=True,
            coerce=True
        )

def location_crosswalks_schema(
        location_ids: List[str] = None,
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "primary_location_id": pa.Column(
                    pa.String,
                    # pa.Check.isin(location_ids),
                    coerce=True
                ),
                "secondary_location_id": pa.Column(
                    pa.String,
                    coerce=True
                )
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "primary_location_id": ps.Column(
                    T.StringType(),
                    ps.Check.isin(location_ids),
                    nullable=False,
                ),
                "secondary_location_id": ps.Column(
                    T.StringType(),
                    nullable=False,
                )
            },
            strict=True,
            coerce=True
        )

# Timeseries
pandas_value_type = pa.Float32()
pyspark_value_type = T.FloatType()

# PySpark Pandera Models
def primary_timeseries_schema(
        location_ids: List[str] = None,
        variable_names: List[str] = None,
        configuration_names: List[str] = None,
        unit_names: List[str] = None,
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "reference_time": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "value_time": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=False,
                    coerce=True,
                ),
                "value": pa.Column(
                    pandas_value_type,
                    nullable=False,
                    coerce=True,
                ),
                "variable_name": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(variable_names),
                    nullable=False
                ),
                "configuration_name": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(configuration_names),
                    nullable=False
                ),
                "unit_name": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(unit_names),
                    nullable=False
                ),
                "location_id": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(location_ids),
                    nullable=False
                )
            },
            strict="filter",
            coerce=True
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "reference_time": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True
                ),
                "value_time": ps.Column(
                    T.TimestampNTZType(),
                    nullable=False
                ),
                "value": ps.Column(
                    pyspark_value_type,
                    nullable=False,
                    coerce=True,
                ),
                "variable_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(variable_names),
                    nullable=False
                ),
                "configuration_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(configuration_names),
                    nullable=False
                ),
                "unit_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(unit_names),
                    nullable=False
                ),
                "location_id": ps.Column(
                    T.StringType(),
                    ps.Check.isin(location_ids),
                    nullable=False
                )
            },
            strict=True,
            coerce=True
        )


def secondary_timeseries_schema(
        location_ids: List[str] = None,
        variable_names: List[str] = None,
        configuration_names: List[str] = None,
        unit_names: List[str] = None,
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "reference_time": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True
                ),
                "value_time": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=False
                ),
                "value": pa.Column(
                    pandas_value_type,
                    nullable=False,
                    coerce=True,
                ),
                "variable_name": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(variable_names),
                    nullable=False
                ),
                "configuration_name": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(configuration_names),
                    nullable=False
                ),
                "unit_name": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(unit_names),
                    nullable=False
                ),
                "location_id": pa.Column(
                    pa.String,
                    # checks=pa.Check.isin(location_ids),
                    nullable=False
                ),
                "member": pa.Column(
                    pa.String,
                    nullable=True
                )
            },
            strict="filter",
            coerce=True,
            add_missing_columns=True
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "reference_time": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True
                ),
                "value_time": ps.Column(
                    T.TimestampNTZType(),
                    nullable=False
                ),
                "value": ps.Column(
                    pyspark_value_type,
                    nullable=False,
                    coerce=True,
                ),
                "variable_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(variable_names),
                    nullable=False
                ),
                "configuration_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(configuration_names),
                    nullable=False
                ),
                "unit_name": ps.Column(
                    T.StringType(),
                    ps.Check.isin(unit_names),
                    nullable=False
                ),
                "location_id": ps.Column(
                    T.StringType(),
                    ps.Check.isin(location_ids),
                    nullable=False
                ),
                "member": ps.Column(
                    T.StringType(),
                    nullable=True,

                )
            },
            strict=True,
            coerce=True
        )

def joined_timeseries_schema(
    type: str = "pyspark",
) -> ps.DataFrameSchema:
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "reference_time": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True
                ),
                "value_time": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=False
                ),
                "primary_value": pa.Column(
                    pandas_value_type,
                    nullable=False,
                ),
                "secondary_value": pa.Column(
                    pandas_value_type,
                    nullable=False,
                ),
                "variable_name": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "configuration_name": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "unit_name": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "primary_location_id": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "secondary_location_id": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "member": pa.Column(
                    pa.String,
                    nullable=True
                )
            }
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "reference_time": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True
                ),
                "value_time": ps.Column(
                    T.TimestampNTZType(),
                    nullable=False
                ),
                "primary_value": ps.Column(
                    pyspark_value_type,
                    nullable=False,
                    coerce=True,
                ),
                "secondary_value": ps.Column(
                    pyspark_value_type,
                    nullable=False,
                    coerce=True,
                ),
                "variable_name": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "configuration_name": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "unit_name": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "primary_location_id": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "secondary_location_id": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "member": ps.Column(
                    T.StringType(),
                    nullable=True
                )
            },
            coerce=True
        )