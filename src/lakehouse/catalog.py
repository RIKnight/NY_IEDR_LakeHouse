# src/lakehouse/catalog.py
# Some of the code in this file was originated by making a Google (gemini-lite) query:
#   "how to make data catalog for duckdb database"
from __future__ import annotations
import duckdb
import getpass
from .config import CONFIG
from .__init__ import __version__ as VERSION

# get the username
USERNAME = getpass.getuser()


def add_table_to_catalog(con: duckdb.DuckDBPyConnection,
                         table_name: str,
                         description: str,
                         source_system: str) -> None:
    """
    Inputs
    ------
    con: duckdb.DuckDBPyConnection
        the duckdb connection to the lakehouse
    table_name: String
        schema.table name of the new table
    description: String
        description of the new table
    source_system: String
        the system that created the table,
        try using the function name from etl.py

    """
    con.execute(
        f"""
        INSERT INTO {CONFIG.catalog}.tables (table_name, description, source_system, lakehouse_version, created_by, last_updated)
        VALUES ('{table_name}', '{description}', '{source_system}', '{VERSION}', '{USERNAME}', NOW())
        """
    )


def create_table_catalog(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Create catalog for new tables and new columns

    Note: column tracking is not yet fully implemented

    """
    created: list[str] = []

    # metadata for Lakehouse tables
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {CONFIG.catalog}.tables (
            table_name VARCHAR,
            description VARCHAR,
            source_system VARCHAR,
            lakehouse_version VARCHAR,
            created_by VARCHAR,
            last_updated TIMESTAMP
        )
        """
    )
    created.append(f"{CONFIG.catalog}.tables")

    # metadata for Lakehouse columns
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {CONFIG.catalog}.columns (
            table_name VARCHAR,
            column_name VARCHAR,
            data_type VARCHAR,
            description VARCHAR,
            lakehouse_version VARCHAR,
            is_primary_key BOOLEAN
        )
        """
    )
    created.append(f"{CONFIG.catalog}.columns")

    # Self-document creation of Data Catalog tables
    # Note: this meta-metadata may be too much. (TBD)
    add_table_to_catalog(con, f"{CONFIG.catalog}.tables", "Metadata for Lakehouse Tables", "Data Catalog")
    add_table_to_catalog(con, f"{CONFIG.catalog}.tables", "Metadata for Lakehouse Tables", "Data Catalog")

    return(created)
