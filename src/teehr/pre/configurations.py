from pathlib import Path
from typing import Union
import duckdb


def insert_configuration(
    database_path: Union[Path, str],
    configuration_name: str,
    configuration_description: str,
    configuration_type: str,
):
    """Insert a configuration into the database."""
    conn = duckdb.connect()

    filepath = f"{database_path}/configurations/configurations.csv"

    conn.sql("""
        CREATE TABLE IF NOT EXISTS configurations (
                name VARCHAR PRIMARY KEY,
                type VARCHAR,
                description VARCHAR
        );
    """)
    conn.sql(f"""
    INSERT INTO configurations SELECT *
    FROM read_csv(
        '{filepath},
        delim = '|',
        header = true,
        columns = {{
            'name': 'VARCHAR',
            'type': 'VARCHAR',
            'description': 'VARCHAR'
        }}
    );
    """)
    conn.sql(f"""
        INSERT INTO configurations
        VALUES (
            '{configuration_name}',
            '{configuration_type}',
            '{configuration_description}'
        );
    """)
    conn.sql(f"""
        COPY (
            SELECT * FROM configurations
        ) TO '{filepath}' WITH (HEADER, DELIMITER '|');
    """)