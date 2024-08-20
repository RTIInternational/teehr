"""Functions to add domain data to the dataset."""
from pathlib import Path
from typing import Union, List
import duckdb
import pandas as pd
from teehr.models.dataset.table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
import teehr.const as const
import logging
from teehr.loading.duckdb_sql import create_database_tables

logger = logging.getLogger(__name__)


def add_configuration(
    dataset_path: Union[Path, str],
    configuration: Union[Configuration, List[Configuration]]
):
    """Insert a configuration into the dataset."""
    conn = duckdb.connect()

    filepath = Path(
        dataset_path, const.CONFIGURATIONS_DIR, const.CONFIGURATIONS_FILE
    )
    logger.debug(f"Adding configuration to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Configuration.model_fields]]
    conn.register("org_df", org_df)

    if isinstance(configuration, Configuration):
        configuration = [configuration]

    new_df = pd.DataFrame([c.model_dump() for c in configuration])
    new_df = new_df[[key for key in Configuration.model_fields]]
    conn.register("new_df", new_df)

    create_database_tables(conn)

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
        ) TO '{filepath}' WITH (HEADER);
    """)


def add_unit(
    dataset_path: Union[Path, str],
    unit: Union[Unit, List[Unit]]
):
    """Insert a unit into the dataset."""
    conn = duckdb.connect()

    filepath = Path(dataset_path, const.UNITS_DIR, const.UNITS_FILE)
    logger.debug(f"Adding unit to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Unit.model_fields]]
    conn.register("org_df", org_df)

    if isinstance(unit, Unit):
        unit = [unit]

    new_df = pd.DataFrame([u.model_dump() for u in unit])
    new_df = new_df[[key for key in Unit.model_fields]]
    conn.register("new_df", new_df)

    create_database_tables(conn)

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
        ) TO '{filepath}' WITH (HEADER);
    """)


def add_variable(
    dataset_path: Union[Path, str],
    variable: Union[Variable, List[Variable]]
):
    """Insert a variable into the dataset."""
    conn = duckdb.connect()

    filepath = Path(dataset_path, const.VARIABLES_DIR, const.VARIABLES_FILE)
    logger.debug(f"Adding variable to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Variable.model_fields]]
    conn.register("org_df", org_df)

    if isinstance(variable, Variable):
        variable = [variable]

    new_df = pd.DataFrame([u.model_dump() for u in variable])
    new_df = new_df[[key for key in Variable.model_fields]]
    conn.register("new_df", new_df)

    create_database_tables(conn)

    conn.sql("""
        INSERT INTO variables
        SELECT * FROM org_df;
    """)
    conn.sql("""
        INSERT INTO variables
        SELECT * FROM new_df;
    """)
    conn.sql(f"""
        COPY (
            SELECT * FROM variables
        ) TO '{filepath}' WITH (HEADER);
    """)


def add_attribute(
    dataset_path: Union[Path, str],
    attribute: Union[Attribute, List[Attribute]]
):
    """Insert an attribute into the dataset."""
    conn = duckdb.connect()

    filepath = Path(dataset_path, const.ATTRIBUTES_DIR, const.ATTRIBUTES_FILE)
    logger.debug(f"Adding attribute to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Attribute.model_fields]]
    conn.register("org_df", org_df)

    if isinstance(attribute, Attribute):
        attribute = [attribute]

    new_df = pd.DataFrame([u.model_dump() for u in attribute])
    new_df = new_df[[key for key in Attribute.model_fields]]
    conn.register("new_df", new_df)

    create_database_tables(conn)

    conn.sql("""
        INSERT INTO attributes
        SELECT * FROM org_df;
    """)
    conn.sql("""
        INSERT INTO attributes
        SELECT * FROM new_df;
    """)
    conn.sql(f"""
        COPY (
            SELECT * FROM attributes
        ) TO '{filepath}' WITH (HEADER);
    """)
