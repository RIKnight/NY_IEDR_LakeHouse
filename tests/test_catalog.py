# tests/test_catalog.py
from pathlib import Path
import duckdb
from lakehouse.db import ensure_schemas
from lakehouse.config import CONFIG
from lakehouse.catalog import create_table_catalog

def test_catalog_exists(tmp_path: Path):
    dbfile = tmp_path / "test.duckdb"
    con = duckdb.connect(str(dbfile))
    try:
        ensure_schemas(con)
        created = create_table_catalog(con)
        rows = con.execute(
            f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = '{CONFIG.catalog}' AND table_name IN (?, ?)
            ORDER BY table_name
            """, ['columns','tables']).fetchall()
        names = [r[0] for r in rows]
        assert names == sorted(['columns','tables'])
    finally:
        con.close()