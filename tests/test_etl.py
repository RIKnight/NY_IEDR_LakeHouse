# tests/test_etl.py
from pathlib import Path
import duckdb
from lakehouse.etl import bronze_ingest, silver_transform, gold_transform, platinum_transform
from lakehouse.db import ensure_schemas
from lakehouse.config import CONFIG

CUSTOMERS_CSV = """customer_id,name
1,Ada Lovelace
2,Grace Hopper
3,Linus Torvalds
"""

ORDERS_CSV = """order_id,customer_id,amount
101,1,120.00
102,1,80.50
103,2,42.00
104,3,200.00
"""

def _write_sources(tmp_ingest: Path):
    (tmp_ingest / "customers.csv").write_text(CUSTOMERS_CSV)
    (tmp_ingest / "orders.csv").write_text(ORDERS_CSV)

def test_bronze_silver_gold_platinum(tmp_path: Path):
    dbfile = tmp_path / "test.duckdb"
    ingest = tmp_path / "ingest"
    ingest.mkdir(parents=True, exist_ok=True)
    _write_sources(ingest)

    con = duckdb.connect(str(dbfile))
    try:
        ensure_schemas(con)
        b = bronze_ingest(con, ingest)
        assert f"{CONFIG.bronze}.customers" in b
        assert f"{CONFIG.bronze}.orders" in b

        s = silver_transform(con, b)
        assert f"{CONFIG.silver}.customers" in s
        assert f"{CONFIG.silver}.orders" in s

        g = gold_transform(con)
        assert f"{CONFIG.gold}.sales" in g

        p = platinum_transform(con)
        assert f"{CONFIG.platinum}.sales_by_customer" in p

        # Validate counts and simple agg
        n_sales = con.execute(f"SELECT COUNT(*) FROM {CONFIG.gold}.sales").fetchone()[0]
        assert n_sales == 4

        top = con.execute(f"""
            SELECT customer_id, total_amount
            FROM {CONFIG.platinum}.sales_by_customer
            ORDER BY total_amount DESC
            LIMIT 1
        """).fetchone()

        assert top[0] == 3  # Linus Torvalds has the largest single order (200.00)
        assert float(top[1]) == 200.0
    finally:
        con.close()