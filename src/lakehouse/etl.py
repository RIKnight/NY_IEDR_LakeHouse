# src/lakehouse/etl.py
from __future__ import annotations
from pathlib import Path
from typing import Iterable
import re
import duckdb
from .config import CONFIG
from .db import ensure_schemas
from .catalog import create_table_catalog, add_table_to_catalog

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

    Yet to be implemented: store metadata for the files (e.g., file name, size, upload timestamp, data format, etc.)
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
        add_table_to_catalog(con,full,f"New data from file {str(path)} in bronze layer with null values","bronze_ingest")
    return created

def silver_transform(con: duckdb.DuckDBPyConnection, tables: Iterable[str] | None = None) -> list[str]:
    """
    For each bronze table, create deduplicated silver.<table_name>.
    Also cast data to better types where automatic detection did not do well.
    Returns the list of created/updated bronze tables.

    Yet to be implemented:
        Better null handling? Try COALESCE with default values?
        Record metadata about type casting?
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

    created: list[str] = []
    for t in bronze_tables:
        full = f"{CONFIG.silver}.{t}"
        #print(f"Working from table {CONFIG.bronze}.{t}:")
        if t == 'utility1_circuits':
            # adjust automatic types to numeric and timestamp with time zone and name the index
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.silver}.{t} AS
                SELECT
                    column00 AS INDEX,
                    Circuits_Phase3_CIRCUIT,
                    Circuits_Phase3_NUMPHASES,
                    Circuits_Phase3_OVERUNDER,
                    Circuits_Phase3_PHASE,
                    TRY_CAST(NYHCPV_csv_NSECTION AS BIGINT) AS NYHCPV_csv_NSECTION,
                    TRY_CAST(NYHCPV_csv_NFEEDER AS BIGINT) AS NYHCPV_csv_NFEEDER,
                    TRY_CAST(NYHCPV_csv_NVOLTAGE AS DOUBLE) AS NYHCPV_csv_NVOLTAGE,
                    TRY_CAST(NYHCPV_csv_NMAXHC AS DOUBLE) AS NYHCPV_csv_NMAXHC,
                    NYHCPV_csv_NMAPCOLOR,
                    TRY_CAST(NYHCPV_csv_FFEEDER AS BIGINT) AS NYHCPV_csv_FFEEDER,
                    TRY_CAST(NYHCPV_csv_FVOLTAGE AS DOUBLE) AS NYHCPV_csv_FVOLTAGE,
                    TRY_CAST(NYHCPV_csv_FMAXHC AS DOUBLE) AS NYHCPV_csv_FMAXHC,
                    TRY_CAST(NYHCPV_csv_FMINHC AS DOUBLE) AS NYHCPV_csv_FMINHC,
                    TRY_CAST(NYHCPV_csv_FHCADATE AS TIMESTAMPTZ) AS NYHCPV_csv_FHCADATE,
                    NYHCPV_csv_FNOTES,
                    Shape_Length,
                    _ingested_at
                FROM {CONFIG.bronze}.{t};
                """
            )
            add_table_to_catalog(con,full,f"type casting with TRY_CAST and named INDEX","silver_transform")
        elif t == 'utility2_install_der':
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.silver}.{t} AS
                SELECT
                    * EXCLUDE (_ingested_at, DER_NAMEPLATE_RATING),
                    TRY_CAST(DER_NAMEPLATE_RATING AS DOUBLE) AS DER_NAMEPLATE_RATING,
                    _ingested_at
                FROM {CONFIG.bronze}.{t};
                """
            )
            add_table_to_catalog(con,full,f"type casting with TRY_CAST","silver_transform")
        elif t == 'utility2_planned_der':
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.silver}.{t} AS
                SELECT
                    DER_TYPE,
                    TRY_CAST(DER_NAMEPLATE_RATING AS DOUBLE) AS DER_NAMEPLATE_RATING,
                    INVERTER_NAMEPLATE_RATING,
                    TRY_CAST(PLANNED_INSTALLATION_DATE AS TIMESTAMP) AS PLANNED_INSTALLATION_DATE,
                    DER_STATUS,
                    DER_STATUS_RATIONALE,
                    TRY_CAST(TOTAL_MW_FOR_SUBSTATION AS DOUBLE) AS TOTAL_MW_FOR_SUBSTATION,
                    INTERCONNECTION_QUEUE_REQUEST_ID,
                    TRY_CAST(INTERCONNECTION_QUEUE_POSITION AS TIMESTAMP) AS INTERCONNECTION_QUEUE_POSITION,
                    DER_INTERCONNECTION_LOCATION,
                    _ingested_at
                FROM {CONFIG.bronze}.{t};
                """
            )
            add_table_to_catalog(con,full,f"type casting with TRY_CAST","silver_transform")
        elif t in ['utility1_install_der','utility1_planned_der']:
            # Since utility1_circuits.Circuits_Phase3_CIRCUIT is BIGINT, need to create BIGINT columns to match
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
            add_table_to_catalog(con,full,f"copy of bronze version","silver_transform")
        # Alternative to using nullstr on bronze ingest: create null values from default "null"
        #column_names = con.execute(
        #    f"""
        #    SELECT column_name, data_type FROM information_schema.columns
        #    WHERE table_schema = '{CONFIG.bronze}' AND table_name = '{t}'
        #    """
        #).fetchall()
        ##print(column_names)
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
        #        add_table_to_catalog(con,full,f"transform 'null' and 'NULL' to null values","silver_transform")
        created.append(f"{CONFIG.silver}.{t}")
    return created

def gold_transform(con: duckdb.DuckDBPyConnection, tables: Iterable[str] | None = None) -> list[str]:
    """
    Aggregates circuit segments into circuits in utility1 tables
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
            [CONFIG.silver],
        ).fetchall()
        silver_tables = [r[0] for r in rows]
    else:
        silver_tables = [t.split(".")[-1] for t in tables]

    created: list[str] = []
    for t in silver_tables:
        full = f"{CONFIG.gold}.{t}"
        # utility1_circuits: GROUP BY Circuits_Phase3_CIRCUIT, aggregate by MODE()
        if t == "utility1_circuits":
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.gold}.{t} AS
                SELECT
                    Circuits_Phase3_CIRCUIT,
                    MODE(Circuits_Phase3_NUMPHASES) AS Circuits_Phase3_NUMPHASES,
                    MODE(Circuits_Phase3_OVERUNDER) AS Circuits_Phase3_OVERUNDER,
                    MODE(Circuits_Phase3_PHASE) AS Circuits_Phase3_PHASE,
                    MIN(NYHCPV_csv_NSECTION) AS NYHCPV_csv_NSECTION_MIN,
                    MAX(NYHCPV_csv_NSECTION) AS NYHCPV_csv_NSECTION_MAX,
                    MODE(NYHCPV_csv_NFEEDER) AS NYHCPV_csv_NFEEDER,
                    MODE(NYHCPV_csv_NVOLTAGE) AS NYHCPV_csv_NVOLTAGE,
                    MODE(NYHCPV_csv_NMAXHC) AS NYHCPV_csv_NMAXHC,
                    MODE(NYHCPV_csv_NMAPCOLOR) AS NYHCPV_csv_NMAPCOLOR,
                    MODE(NYHCPV_csv_FFEEDER) AS NYHCPV_csv_FFEEDER,
                    MODE(NYHCPV_csv_FVOLTAGE) AS NYHCPV_csv_FVOLTAGE,
                    MODE(NYHCPV_csv_FMAXHC) AS NYHCPV_csv_FMAXHC,
                    MODE(NYHCPV_csv_FMINHC) AS NYHCPV_csv_FMINHC,
                    MODE(NYHCPV_csv_FHCADATE) AS NYHCPV_csv_FHCADATE,
                    MODE(NYHCPV_csv_FNOTES) AS NYHCPV_csv_FNOTES,
                    SUM(Shape_Length) AS Shape_Length_Sum,
                    MAX(_ingested_at) AS _ingested_at
                FROM {CONFIG.silver}.{t}
                GROUP BY Circuits_Phase3_CIRCUIT;
                """
            )
            add_table_to_catalog(con,full,f"aggregation on Circuits_Phase3_CIRCUIT; mostly MODE agg","gold_transform")
        # Collect Circuits_Phase3_CIRCUIT from utility1_circuits on matching ProjectID
        elif t in ['utility1_install_der','utility1_planned_der']:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.gold}.{t} AS
                SELECT
                    DISTINCT der.* EXCLUDE (_ingested_at),
                    cir.Circuits_Phase3_CIRCUIT,
                    der._ingested_at
                FROM {CONFIG.silver}.{t} der
                JOIN {CONFIG.silver}.utility1_circuits cir
                ON der.ProjectID = cir.INDEX;
                """
            )
            add_table_to_catalog(con,full,f"JOIN circuits.Circuits_Phase3_CIRCUIT","gold_transform")
        else:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {CONFIG.gold}.{t} AS
                SELECT DISTINCT * EXCLUDE (_ingested_at), _ingested_at
                FROM {CONFIG.silver}.{t};
                """
            )
            add_table_to_catalog(con,full,f"copy of silver version","gold_transform")
        created.append(f"{CONFIG.gold}.{t}")
    return created

def platinum_transform(con: duckdb.DuckDBPyConnection) -> list[str]:
    """
    Unify data model across all utilities.
    Unify column naming convention to snake_case

    TODO: add parquet output of platinum tables to decouple dependence on duckdb?
    """
    platinum_table_names = ['circuits','install_der','planned_der']
    created: list[str] = []
    for table_name in platinum_table_names:
        full = f"{CONFIG.platinum}.{table_name}"
        if table_name == 'circuits':
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {CONFIG.platinum}.{table_name} AS
                SELECT
                    CAST(Circuits_Phase3_CIRCUIT AS VARCHAR) AS feeder_ID,
                    NYHCPV_csv_FVOLTAGE AS feeder_voltage,
                    NYHCPV_csv_FMAXHC AS feeder_max_hc,
                    NYHCPV_csv_FMINHC AS feeder_min_hc,
                    NYHCPV_csv_FHCADATE AS hca_date,
                    NYHCPV_csv_NMAPCOLOR AS color,
                    Shape_Length_Sum AS shape_length,
                    'utility1' AS utility_name,
                    _ingested_at
                FROM {CONFIG.gold}.utility1_circuits
                UNION
                SELECT
                    Master_CDF as feeder_ID,
                    feeder_voltage,
                    feeder_max_hc,
                    feeder_min_hc,
                    hca_refresh_date AS hca_date,
                    color,
                    shape_length,
                    'utility2' AS utility_name,
                    _ingested_at
                FROM {CONFIG.gold}.utility2_circuits
                """
            )
            add_table_to_catalog(con,full,f"union of utility1 and utility2 circuit tables","platinum_transform")
        elif table_name == 'install_der':
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {CONFIG.platinum}.{table_name} AS
                SELECT
                    CAST(Circuits_Phase3_CIRCUIT AS VARCHAR) AS feeder_ID,
                    ProjectCircuitID AS der_ID,
                    ProjectType AS der_type,
                    NamePlateRating AS der_nameplate_rating,
                    'utility1' AS utility_name,
                    _ingested_at
                FROM {CONFIG.gold}.utility1_install_der
                UNION
                SELECT
                    DER_INTERCONNECTION_LOCATION AS feeder_ID,
                    CAST(DER_ID AS VARCHAR) AS der_ID,
                    DER_TYPE AS der_type,
                    DER_NAMEPLATE_RATING AS der_nameplate_rating,
                    'utility2' AS utility_name,
                    _ingested_at
                FROM {CONFIG.gold}.utility2_install_der
                """
            )
            add_table_to_catalog(con,full,f"union of utility1 and utility2 install_der tables","platinum_transform")
        elif table_name == 'planned_der':
            con.execute(
                f"""
                CREATE OR REPLACE VIEW {CONFIG.platinum}.{table_name} AS
                SELECT
                    CAST(Circuits_Phase3_CIRCUIT AS VARCHAR) AS feeder_ID,
                    ProjectCircuitID AS der_ID,
                    ProjectType AS der_type,
                    NamePlateRating AS der_nameplate_rating,
                    'utility1' AS utility_name,
                    _ingested_at
                FROM {CONFIG.gold}.utility1_planned_der
                UNION
                SELECT
                    DER_INTERCONNECTION_LOCATION AS feeder_ID,
                    NULL AS der_ID,
                    DER_TYPE AS der_type,
                    DER_NAMEPLATE_RATING AS der_nameplate_rating,
                    'utility2' AS utility_name,
                    _ingested_at
                FROM {CONFIG.gold}.utility2_planned_der
                """
            )
            add_table_to_catalog(con,full,f"union of utility1 and utility2 planned_der tables","platinum_transform")
        created.append(f"{CONFIG.platinum}.{table_name}")
    return created

def run_all(db_path: str | None = None, source_dir: str | None = None) -> None:
    """
    Yet to be implemented: back up the existing duckdb file with timestamp before starting new ELT
    Possibilities for future development: time-travel style data versioning?
    """
    con = duckdb.connect(str(CONFIG.db_path if db_path is None else db_path))
    try:
        ensure_schemas(con)
        c = create_table_catalog(con)
        b = bronze_ingest(con, Path(source_dir) if source_dir else None)
        s = silver_transform(con, b)
        g = gold_transform(con, s)
        p = platinum_transform(con)
        con.commit()
        print("CATALOG:", c)
        print("BRONZE:", b)
        print("SILVER:", s)
        print("GOLD:", g)
        print("PLATINUM:", p)
    finally:
        con.close()