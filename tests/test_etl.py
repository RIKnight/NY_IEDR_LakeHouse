# tests/test_etl.py
from pathlib import Path
import duckdb
import re

from lakehouse.etl import (
    bronze_ingest,
    silver_transform,
    gold_transform,
    platinum_transform,
)
from lakehouse.db import ensure_schemas
from lakehouse.config import CONFIG


# ----------------------------
# Sample source data (CSV text)
# ----------------------------

UTILITY1_CIRCUITS_CSV = """column00,Circuits_Phase3_CIRCUIT,Circuits_Phase3_NUMPHASES,Circuits_Phase3_OVERUNDER,Circuits_Phase3_PHASE,NYHCPV_csv_NSECTION,NYHCPV_csv_NFEEDER,NYHCPV_csv_NVOLTAGE,NYHCPV_csv_NMAXHC,NYHCPV_csv_NMAPCOLOR,NYHCPV_csv_FFEEDER,NYHCPV_csv_FVOLTAGE,NYHCPV_csv_FMAXHC,NYHCPV_csv_FMINHC,NYHCPV_csv_FHCADATE,NYHCPV_csv_FNOTES,Shape_Length
1001,55555,3,over,ABC,10,1,12.47,5.5,green,2,12.47,8.8,2.2,2024-01-01T12:00:00Z,NULL,100.0
1001,55555,3,over,ABC,12,1,12.47,5.5,green,2,12.47,8.8,2.2,2024-01-01T12:00:00Z,ok,80.5
"""

UTILITY1_INSTALL_DER_CSV = """ProjectID,ProjectCircuitID,ProjectType,NamePlateRating
1001,99999,PV,1.5
"""

UTILITY1_PLANNED_DER_CSV = """ProjectID,ProjectCircuitID,ProjectType,NamePlateRating
1001,88888,BESS,2.0
"""

UTILITY2_CIRCUITS_CSV = """Master_CDF,feeder_voltage,feeder_max_hc,feeder_min_hc,hca_refresh_date,color,shape_length
FDR-77,13.2,9.9,1.1,2024-06-01,yellow,50.0
"""

UTILITY2_INSTALL_DER_CSV = """DER_INTERCONNECTION_LOCATION,DER_ID,DER_TYPE,DER_NAMEPLATE_RATING
FDR-77,222,PV,3.3
"""

UTILITY2_PLANNED_DER_CSV = """DER_TYPE,DER_NAMEPLATE_RATING,INVERTER_NAMEPLATE_RATING,PLANNED_INSTALLATION_DATE,DER_STATUS,DER_STATUS_RATIONALE,TOTAL_MW_FOR_SUBSTATION,INTERCONNECTION_QUEUE_REQUEST_ID,INTERCONNECTION_QUEUE_POSITION,DER_INTERCONNECTION_LOCATION
PV,4.4,4.0,2024-07-15 00:00:00,Planned,Queue,12.5,REQ-1,2024-07-01 00:00:00,FDR-77
"""


def _write_sources(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    (root / "utility1_circuits.csv").write_text(UTILITY1_CIRCUITS_CSV)
    (root / "utility1_install_der.csv").write_text(UTILITY1_INSTALL_DER_CSV)
    (root / "utility1_planned_der.csv").write_text(UTILITY1_PLANNED_DER_CSV)
    (root / "utility2_circuits.csv").write_text(UTILITY2_CIRCUITS_CSV)
    (root / "utility2_install_der.csv").write_text(UTILITY2_INSTALL_DER_CSV)
    (root / "utility2_planned_der.csv").write_text(UTILITY2_PLANNED_DER_CSV)


def _dtype_map(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> dict[str, str]:
    rows = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def test_bronze_silver_gold_platinum_end_to_end(tmp_path: Path):
    # Arrange: temporary DB and ingest directory
    dbfile = tmp_path / "test.duckdb"
    ingest = tmp_path / "ingest"
    _write_sources(ingest)

    con = duckdb.connect(str(dbfile))
    try:
        # ---- Bootstrap schemas
        ensure_schemas(con)

        # ---- Bronze: ingest all CSVs (nullstr=['NULL','null'] is enabled in the new ETL)
        bronze_created = bronze_ingest(con, ingest)
        expected_bronze = {
            f"{CONFIG.bronze}.utility1_circuits",
            f"{CONFIG.bronze}.utility1_install_der",
            f"{CONFIG.bronze}.utility1_planned_der",
            f"{CONFIG.bronze}.utility2_circuits",
            f"{CONFIG.bronze}.utility2_install_der",
            f"{CONFIG.bronze}.utility2_planned_der",
        }
        assert set(bronze_created) == expected_bronze

        # Sanity: 'NULL' (string) becomes actual NULL in bronze
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {CONFIG.bronze}.utility1_circuits WHERE NYHCPV_csv_FNOTES IS NULL"
        ).fetchone()[0]
        assert null_count == 1

        # ---- Silver: type coercions + dedup rules
        silver_created = silver_transform(con, bronze_created)
        expected_silver = {b.replace(f"{CONFIG.bronze}.", f"{CONFIG.silver}.") for b in expected_bronze}
        assert set(silver_created) == expected_silver

        # Type checks for utility1_circuits
        dtypes_u1c = _dtype_map(con, CONFIG.silver, "utility1_circuits")
        # Explicit casts in new ETL
        assert dtypes_u1c["NYHCPV_csv_NSECTION"] == "BIGINT"
        assert dtypes_u1c["NYHCPV_csv_FHCADATE"] in ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE")
        # Index rename applied
        assert "INDEX" in dtypes_u1c

        # Type checks for utility2_install_der
        dtypes_u2i = _dtype_map(con, CONFIG.silver, "utility2_install_der")
        assert dtypes_u2i["DER_NAMEPLATE_RATING"] in ("DOUBLE", "DOUBLE PRECISION")

        # Type checks for utility2_planned_der
        dtypes_u2p = _dtype_map(con, CONFIG.silver, "utility2_planned_der")
        assert dtypes_u2p["DER_NAMEPLATE_RATING"] in ("DOUBLE", "DOUBLE PRECISION")
        assert dtypes_u2p["PLANNED_INSTALLATION_DATE"].startswith("TIMESTAMP")
        assert dtypes_u2p["TOTAL_MW_FOR_SUBSTATION"] in ("DOUBLE", "DOUBLE PRECISION")
        assert dtypes_u2p["INTERCONNECTION_QUEUE_POSITION"].startswith("TIMESTAMP")

        # utility1_* have PCID_int BIGINT
        for t in ("utility1_install_der", "utility1_planned_der"):
            dtypes = _dtype_map(con, CONFIG.silver, t)
            assert "PCID_int" in dtypes
            assert dtypes["PCID_int"] == "BIGINT"

        # ---- Gold: aggregations and joins
        gold_created = gold_transform(con, silver_created)
        expected_gold = {s.replace(f"{CONFIG.silver}.", f"{CONFIG.gold}.") for s in expected_silver}
        assert set(gold_created) == expected_gold

        # Aggregation result for utility1_circuits
        # Two segments for circuit 55555 should collapse to one row, with Shape_Length_Sum = 180.5
        row = con.execute(
            f"""
            SELECT Circuits_Phase3_CIRCUIT, NYHCPV_csv_NSECTION_MIN, NYHCPV_csv_NSECTION_MAX, Shape_Length_Sum
            FROM {CONFIG.gold}.utility1_circuits
            """
        ).fetchone()
        assert row[0] == 55555
        assert row[1] == 10
        assert row[2] == 12
        assert float(row[3]) == 180.5

        # Join applied for utility1_install_der/planned_der ⇒ Circuits_Phase3_CIRCUIT present
        for t in ("utility1_install_der", "utility1_planned_der"):
            cols = [c[0] for c in con.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                """,
                [CONFIG.gold, t],
            ).fetchall()]
            assert "Circuits_Phase3_CIRCUIT" in cols
            # Should match the circuit 55555 via ProjectID=INDEX join
            val = con.execute(
                f"SELECT DISTINCT Circuits_Phase3_CIRCUIT FROM {CONFIG.gold}.{t}"
            ).fetchone()[0]
            assert val == 55555

        # Default path created gold.utility2_circuits
        count_u2c_gold = con.execute(
            f"SELECT COUNT(*) FROM {CONFIG.gold}.utility2_circuits"
        ).fetchone()[0]
        assert count_u2c_gold == 1

        # ---- Platinum: unified semantic views
        platinum_created = platinum_transform(con)
        assert set(platinum_created) == {
            f"{CONFIG.platinum}.circuits",
            f"{CONFIG.platinum}.install_der",
            f"{CONFIG.platinum}.planned_der",
        }

        # Circuits view: union of utility1 (agg) + utility2 (pass-through)
        circ = con.execute(
            f"SELECT feeder_ID, feeder_max_hc, utility_name FROM {CONFIG.platinum}.circuits ORDER BY utility_name"
        ).fetchall()
        # Expect 2 rows: utility1 + utility2
        assert len(circ) == 2
        # utility1 row: feeder_ID is string '55555' and feeder_max_hc from FVOLTAGE/FMAXHC mapping (8.8)
        assert circ[0][2] == "utility1"
        assert circ[0][0] == "55555"
        assert float(circ[0][1]) == 8.8
        # utility2 row:
        assert circ[1][2] == "utility2"
        assert circ[1][0] == "FDR-77"

        # Install DER view: 2 rows (1 from each utility)
        n_install = con.execute(
            f"SELECT COUNT(*) FROM {CONFIG.platinum}.install_der"
        ).fetchone()[0]
        assert n_install == 2

        # Planned DER view: 2 rows (1 from each utility)
        n_planned = con.execute(
            f"SELECT COUNT(*) FROM {CONFIG.platinum}.planned_der"
        ).fetchone()[0]
        assert n_planned == 2

        # Ensure 'NULL' strings stayed NULL post-Silver (we had one NULL FNOTES)
        null_silver = con.execute(
            f"SELECT COUNT(*) FROM {CONFIG.silver}.utility1_circuits WHERE NYHCPV_csv_FNOTES IS NULL"
        ).fetchone()[0]
        assert null_silver == 1

    finally:
        con.close()