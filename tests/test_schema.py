# tests/test_schema.py
from pathlib import Path
import duckdb
from lakehouse.db import ensure_schemas
from lakehouse.config import CONFIG

def test_schemas_exist(tmp_path: Path):
    dbfile = tmp_path / "test.duckdb"
    con = duckdb.connect(str(dbfile))
    try:
        ensure_schemas(con)
        rows = con.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN (?, ?, ?, ?, ?)
            ORDER BY schema_name
        """, [CONFIG.bronze, CONFIG.silver, CONFIG.gold, CONFIG.platinum, CONFIG.catalog]).fetchall()
        names = [r[0] for r in rows]
        assert names == sorted([CONFIG.bronze, CONFIG.silver, CONFIG.gold, CONFIG.platinum, CONFIG.catalog])
    finally:
        con.close()