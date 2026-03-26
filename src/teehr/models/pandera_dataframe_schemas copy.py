"""Pandera DataFrame Schemas for TEEHR Tables."""
import pandera.pyspark as ps
import pandera.pandas as pa
import pyspark.sql.types as T
import pandas as pd
import pyarrow


# Domains
# PySpark Pandera Schemas

def ln_no_commas(pyspark_obj) -> bool:
    """Ensure long_name column does not contain commas."""
    return pyspark_obj.filter(pyspark_obj["long_name"].contains(",")).count() == 0


def valid_name(pyspark_obj) -> bool:
    """Ensure name column matches ^[a-zA-Z0-9_]+$."""
    return pyspark_obj.filter(pyspark_obj["name"].rlike(r"^[a-zA-Z0-9_]+$")).count() == pyspark_obj.count()


def valid_unit_name(pyspark_obj) -> bool:
    """Ensure name column matches ^[a-zA-Z0-9_^/]+$."""
    return pyspark_obj.filter(pyspark_obj["name"].rlike(r"^[a-zA-Z0-9_^/]+$")).count() == pyspark_obj.count()


def format_datetime64(s: pd.Series) -> pd.Series:
    """Format a pandas Series to datetime64[ms] in UTC."""
    # Convert to UTC.
    # if s.dt.tz is not None:
    #     s = s.dt.tz_convert("UTC")
    # Drop timezone information.
    s = s.dt.tz_localize(None)
    return s.astype("datetime64[ms]")


def configuration_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    """Return the schema for configuration data."""
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "name": pa.Column(
                    pa.String,
                    unique=True,
                    checks=[
                        pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                    ]
                ),
                "type": pa.Column(
                    pa.String,
                    checks=pa.Check.isin(
                        ["primary", "secondary"]
                    )
                ),
                "description": pa.Column(
                    pa.String
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                )
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
                    nullable=False
                ),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True
        )
    if type == "arrow":
        return pyarrow.schema([
            ("name", pyarrow.string()),
            ("type", pyarrow.string()),
            ("description", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
        ])


def unit_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    """Return the schema for unit data."""
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
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                )
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
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True
        )
    if type == "arrow":
        return pyarrow.schema([
            ("name", pyarrow.string()),
            ("long_name", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
        ])


def variable_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    """Return the schema for variable data."""
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "name": pa.Column(
                    pa.String,
                    unique=True,
                    checks=[
                        pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                    ]
                ),
                "long_name": pa.Column(
                    pa.String,
                    checks=pa.Check(lambda s: not s.str.contains(",").any())
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                )
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
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True
        )
    if type == "arrow":
        return pyarrow.schema([
            ("name", pyarrow.string()),
            ("long_name", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
        ])


def attribute_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    """Return the schema for attribute data."""
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
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                )
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
                    nullable=False
                ),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True
        )
    if type == "arrow":
        return pyarrow.schema([
            ("name", pyarrow.string()),
            ("type", pyarrow.string()),
            ("description", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
        ])


# Locations
# PySpark Pandera Models
def locations_schema(type: str = "pyspark") -> ps.DataFrameSchema:
    """Return the schema for location data."""
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "id": pa.Column(str, coerce=True),
                "name": pa.Column(str, coerce=True),
                "geometry": pa.Column("geometry"),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "properties": pa.Column(
                    dict,
                    coerce=True,
                    nullable=True,
                )
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "id": ps.Column(T.StringType(), nullable=False),
                "name": ps.Column(T.StringType(), nullable=False),
                "geometry": ps.Column(T.BinaryType(), nullable=False),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "properties": ps.Column(
                    T.MapType(T.StringType(), T.StringType()),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True
        )
    if type == "arrow":
        return pyarrow.schema([
            ("id", pyarrow.string()),
            ("name", pyarrow.string()),
            ("geometry", pyarrow.binary()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
            ("properties", pyarrow.map_(pyarrow.string(), pyarrow.string())),
        ])


def location_attributes_schema(
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    """Return the schema for location attribute data."""
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "location_id": pa.Column(
                    pa.String,
                    coerce=True
                ),
                "attribute_name": pa.Column(
                    pa.String,
                    coerce=True
                ),
                "value": pa.Column(
                    pa.String,
                    coerce=True
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "properties": pa.Column(
                    dict,
                    coerce=True,
                    nullable=True,
                )
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "location_id": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "attribute_name": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "value": ps.Column(
                    T.StringType(),
                    nullable=False,
                ),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "properties": ps.Column(
                    T.MapType(T.StringType(), T.StringType()),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True,
        )
    if type == "arrow":
        return pyarrow.schema([
            ("location_id", pyarrow.string()),
            ("attribute_name", pyarrow.string()),
            ("value", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
            ("properties", pyarrow.map_(pyarrow.string(), pyarrow.string())),
        ])


def location_crosswalks_schema(
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    """Return the schema for location crosswalk data."""
    if type == "pandas":
        return pa.DataFrameSchema(
            columns={
                "primary_location_id": pa.Column(
                    pa.String,
                    coerce=True
                ),
                "secondary_location_id": pa.Column(
                    pa.String,
                    coerce=True
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "properties": pa.Column(
                    dict,
                    coerce=True,
                    nullable=True,
                )
            },
            strict="filter"
        )
    if type == "pyspark":
        return ps.DataFrameSchema(
            columns={
                "primary_location_id": ps.Column(
                    T.StringType(),
                    nullable=False,
                ),
                "secondary_location_id": ps.Column(
                    T.StringType(),
                    nullable=False,
                ),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "properties": ps.Column(
                    T.MapType(T.StringType(), T.StringType()),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True,
        )
    if type == "arrow":
        return pyarrow.schema([
            ("primary_location_id", pyarrow.string()),
            ("secondary_location_id", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
            ("properties", pyarrow.map_(pyarrow.string(), pyarrow.string())),
        ])


def weights_file_schema() -> pa.DataFrameSchema:
    """Return the schema for a weights file."""
    return pa.DataFrameSchema(
        columns={
            "row": pa.Column(
                pa.Int32,
                nullable=False,
            ),
            "col": pa.Column(
                pa.Int32,
                nullable=False,
            ),
            "weight": pa.Column(
                pa.Float32,
                nullable=False,
            ),
            "location_id": pa.Column(
                pa.String,
                nullable=False,
            )
        },
        strict="filter",
        coerce=True
    )


# Timeseries
pandas_value_type = pa.Float32()
pyspark_value_type = T.FloatType()
pyarrow_value_type = pyarrow.float32()


# PySpark Pandera Models
def primary_timeseries_schema(
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    """Return the schema for primary timeseries data."""
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
                "location_id": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
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
                "location_id": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True,
        )
    if type == "arrow":
        return pyarrow.schema([
            ("reference_time", pyarrow.timestamp("ms")),
            ("value_time", pyarrow.timestamp("ms")),
            ("value", pyarrow_value_type),
            ("variable_name", pyarrow.string()),
            ("configuration_name", pyarrow.string()),
            ("unit_name", pyarrow.string()),
            ("location_id", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
        ])


def secondary_timeseries_schema(
        type: str = "pyspark",
) -> ps.DataFrameSchema:
    """Return the schema for secondary timeseries data."""
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
                "location_id": pa.Column(
                    pa.String,
                    nullable=False
                ),
                "member": pa.Column(
                    pa.String,
                    nullable=True
                ),
                "created_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                ),
                "updated_at": pa.Column(
                    pa.DateTime,
                    parsers=pa.Parser(format_datetime64),
                    nullable=True,
                    coerce=True,
                )
            },
            strict="filter",
            coerce=True,
            add_missing_columns=True,
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
                "location_id": ps.Column(
                    T.StringType(),
                    nullable=False
                ),
                "member": ps.Column(
                    T.StringType(),
                    nullable=True,
                ),
                "created_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                ),
                "updated_at": ps.Column(
                    T.TimestampNTZType(),
                    nullable=True,
                )
            },
            strict=True,
            coerce=True,
        )
    if type == "arrow":
        return pyarrow.schema([
            ("reference_time", pyarrow.timestamp("ms")),
            ("value_time", pyarrow.timestamp("ms")),
            ("value", pyarrow_value_type),
            ("variable_name", pyarrow.string()),
            ("configuration_name", pyarrow.string()),
            ("unit_name", pyarrow.string()),
            ("location_id", pyarrow.string()),
            ("member", pyarrow.string()),
            ("created_at", pyarrow.timestamp("ms")),
            ("updated_at", pyarrow.timestamp("ms")),
        ])
