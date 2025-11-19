# src/lakehouse/catalog.py
# Some of the code in this file was originated by making a Google (gemini-lite) query:
#   "how to make data catalog for duckdb database"
from __future__ import annotations
import duckdb
import getpass
from .config import CONFIG

# get the username
USERNAME = getpass.getuser()

def create_table_catalog(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Create catalog for new tables

    """
    created: list[str] = []

    # metadata for Lakehouse tables
    con.execute(
        f"""
        CREATE TABLE {CONFIG.catalog}.tables (
            table_name VARCHAR,
            description VARCHAR,
            source_system VARCHAR,
            created_by VARCHAR,
            last_updated TIMESTAMP
        )
        """
    )
    created.append(f"{CONFIG.catalog}.tables")

    # metadata for Lakehouse columns
    con.execute(
        f"""
        CREATE TABLE {CONFIG.catalog}.columns (
            table_name VARCHAR,
            column_name VARCHAR,
            data_type VARCHAR,
            description VARCHAR,
            is_primary_key BOOLEAN
        )
        """
    )
    created.append(f"{CONFIG.catalog}.columns")

    # Self-document creation of Data Catalog tables
    # Note: this meta-metadata may be too much. (TBD)
    con.execute(
        f"""
        INSERT INTO {CONFIG.catalog}.tables (table_name, description, source_system, created_by, last_updated)
        VALUES ('{CONFIG.catalog}.tables', 'Metadata for Lakehouse Tables', 'Data Catalog', '{USERNAME}', NOW())
        """
    )
    con.execute(
        f"""
        INSERT INTO {CONFIG.catalog}.tables (table_name, description, source_system, created_by, last_updated)
        VALUES ('{CONFIG.catalog}.columns', 'Metadata for Lakehouse Columns', 'Data Catalog', '{USERNAME}', NOW())
        """
    )

    return(created)
    