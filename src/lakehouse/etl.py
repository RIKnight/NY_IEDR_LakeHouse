# src/lakehouse/etl.py
from __future__ import annotations
from pathlib import Path
from typing import Iterable
import re
import duckdb
from .config import CONFIG
from .db import ensure_schemas

def _snake(s: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower()

def discover_sources(ingest_dir: Path) -> list[Path]:
    ingest_dir.mkdir(parents=True, exist_ok=True)
    exts = {".csv", ".parquet"}
    return sorted([p for p in ingest_dir.iterdir() if p.suffix.lower() in exts and p.is_file()])

def bronze_ingest(con: duckdb.DuckDBPyConnection, source_dir: Path | None = None) -> list[str]:
    """
    Ingests CSV/Parquet files from /ingest into bronze.<table_name>.
    Adds an _ingested_at timestamp.
    Returns the list of created/updated bronze tables.
    """
    ensure_schemas(con)
    src_dir = CONFIG.ingest_dir if source_dir is None else source_dir
    created: list[str] = []
    for path in discover_sources(src_dir):
        tname = _snake(path.stem)
        full = f"{CONFIG.bronze}.{tname}"
        if path.suffix.lower() == ".csv":
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {full} AS
                SELECT *, now()::TIMESTAMP AS _ingested_at
                FROM read_csv_auto(?, HEADER=TRUE, SAMPLE_SIZE=20000, nullstr=['NULL','null']);
                """,
                [str(path)],
            )
        else:  # parquet
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {full} AS
                SELECT *, now()::TIMESTAMP AS _ingested_at
                FROM read_parquet(?);
                """,
                [str(path)],
            )
        created.append(full)
    return created

def silver_transform(con: duckdb.DuckDBPyConnection, tables: Iterable[str] | None = None) -> list[str]:
    """
    For each bronze table, create deduplicated silver.<table_name>.
    (You can extend this with type coercions, null handling, etc.)
    """
    ensure_schemas(con)
    if tables is None:
        rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY 1
            """,
            [CONFIG.bronze],
        ).fetchall()
        bronze_tables = [r[0] for r in rows]
    else:
        bronze_tables = [t.split(".")[-1] for t in tables]

    out: list[str] = []
    for t in bronze_tables:
        #print(f"Working from table {CONFIG.bronze}.{t}:")
        if t in ['utility1_install_der','utility1_planned_der']:
            print(f"Working from table {CONFIG.bronze}.{t}:")
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.silver}.{t} AS
                SELECT DISTINCT * EXCLUDE (_ingested_at), TRY_CAST(ProjectCircuitID AS BIGINT) AS PCID_int, _ingested_at
                FROM {CONFIG.bronze}.{t};
                """
            )
        else:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.silver}.{t} AS
                SELECT DISTINCT * EXCLUDE (_ingested_at), _ingested_at
                FROM {CONFIG.bronze}.{t};
                """
            )
        # create null values from default "null"
        #column_names = con.execute(
        #    f"""
        #    SELECT column_name, data_type FROM information_schema.columns
        #    WHERE table_schema = '{CONFIG.bronze}' AND table_name = '{t}'
        #    """
        #).fetchall()
        #print(column_names)
        #for column_name, data_type in column_names:  #[(column_name[0], data_type[0]) for (column_name, data_type) in column_names]:
        #    if data_type == 'VARCHAR':
        #        con.execute(
        #            f"""
        #            UPDATE {CONFIG.silver}.{t}
        #            SET {column_name} = CASE
        #                WHEN LOWER({column_name}) = 'null' THEN NULL
        #                ELSE {column_name}
        #            END
        #            WHERE LOWER({column_name}) = 'null';
        #            """
        #        )
        out.append(f"{CONFIG.silver}.{t}")
    return out

def gold_transform(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Creates curated marts. If 'customers' and 'orders' exist in silver,
    builds a joined 'gold.sales' table as an example.
    """
    ensure_schemas(con)
    have_customers = bool(con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = ? AND table_name = 'customers'
        """,
        [CONFIG.silver],
    ).fetchone())

    have_orders = bool(con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = ? AND table_name = 'orders'
        """,
        [CONFIG.silver],
    ).fetchone())

    created: list[str] = []
    if have_customers and have_orders:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {CONFIG.gold}.sales AS
            SELECT
                o.order_id,
                o.customer_id,
                c.name AS customer_name,
                CAST(o.amount AS DOUBLE) AS amount,
                o._ingested_at
            FROM {CONFIG.silver}.orders o
            LEFT JOIN {CONFIG.silver}.customers c
              ON o.customer_id = c.customer_id;
            """
        )
        created.append(f"{CONFIG.gold}.sales")
    return created

def platinum_transform(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Aggregated/semantic layer on top of gold marts.
    """
    ensure_schemas(con)
    exists_sales = bool(con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = ? AND table_name = 'sales'
        """,
        [CONFIG.gold],
    ).fetchone())

    created: list[str] = []
    if exists_sales:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {CONFIG.platinum}.sales_by_customer AS
            SELECT
              customer_id,
              any_value(customer_name) AS customer_name,
              COUNT(*) AS orders_count,
              SUM(amount) AS total_amount,
              AVG(amount) AS avg_order_value
            FROM {CONFIG.gold}.sales
            GROUP BY customer_id
            ORDER BY total_amount DESC;
            """
        )
        created.append(f"{CONFIG.platinum}.sales_by_customer")
    return created

def run_all(db_path: str | None = None, source_dir: str | None = None) -> None:
    con = duckdb.connect(str(CONFIG.db_path if db_path is None else db_path))
    try:
        ensure_schemas(con)
        b = bronze_ingest(con, Path(source_dir) if source_dir else None)
        s = silver_transform(con, b)
        #g = gold_transform(con)
        #p = platinum_transform(con)
        con.commit()
        print("BRONZE:", b)
        print("SILVER:", s)
        #print("GOLD:", g)
        #print("PLATINUM:", p)
    finally:
        con.close()