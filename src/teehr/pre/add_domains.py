from pathlib import Path
from typing import Union, List
import duckdb
import pandas as pd
from teehr.models.dataset import Configuration, Unit


def add_configuration(
    database_path: Union[Path, str],
    configuration: Union[Configuration, List[Configuration]]
):
    """Insert a configuration into the database."""
    conn = duckdb.connect()

    filepath = f"{database_path}/configurations/configurations.csv"
    org_df = pd.read_csv(filepath, sep="|")
    org_df = org_df[[key for key in Configuration.model_fields]]
    conn.register("org_df", org_df)

    if isinstance(configuration, Configuration):
        configuration = [configuration]

    new_df = pd.DataFrame([c.model_dump() for c in configuration])
    new_df = new_df[[key for key in Configuration.model_fields]]
    conn.register("new_df", new_df)

    conn.sql("""
        CREATE TABLE IF NOT EXISTS configurations (
            name VARCHAR PRIMARY KEY,
            type VARCHAR,
            description VARCHAR
        );
    """)
    conn.sql("""
        INSERT INTO configurations
        SELECT * FROM org_df;
    """)
    conn.sql("""
        INSERT INTO configurations
        SELECT * FROM new_df;
    """)
    conn.sql(f"""
        COPY (
            SELECT * FROM configurations
        ) TO '{filepath}' WITH (HEADER, DELIMITER '|');
    """)


def add_unit(
    database_path: Union[Path, str],
    unit: Union[Unit, List[Unit]]
):
    """Insert a configuration into the database."""
    conn = duckdb.connect()

    filepath = f"{database_path}/units/units.csv"
    org_df = pd.read_csv(filepath, sep="|")
    org_df = org_df[[key for key in Unit.model_fields]]
    conn.register("org_df", org_df)

    if isinstance(unit, Unit):
        unit = [unit]

    new_df = pd.DataFrame([u.model_dump() for u in unit])
    new_df = new_df[[key for key in Unit.model_fields]]
    conn.register("new_df", new_df)

    conn.sql("""
        CREATE TABLE IF NOT EXISTS units (
            name VARCHAR PRIMARY KEY,
            long_name VARCHAR,
            aliases VARCHAR[],
        );
    """)
    conn.sql("""
        INSERT INTO units
        SELECT * FROM org_df;
    """)
    conn.sql("""
        INSERT INTO units
        SELECT * FROM new_df;
    """)
    conn.sql(f"""
        COPY (
            SELECT * FROM units
        ) TO '{filepath}' WITH (HEADER, DELIMITER '|');
    """)
