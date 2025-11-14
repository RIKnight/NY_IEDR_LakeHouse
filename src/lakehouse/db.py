# src/lakehouse/db.py
from __future__ import annotations
import duckdb
from .config import CONFIG

def connect(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    path = str(CONFIG.db_path if db_path is None else db_path)
    return duckdb.connect(path)

def ensure_schemas(con: duckdb.DuckDBPyConnection) -> None:
    for schema in (CONFIG.bronze, CONFIG.silver, CONFIG.gold, CONFIG.platinum):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
