# NY_IEDR_LakeHouse

This package is a complete, lightweight DuckDB + Python 3.12 lakehouse you can run in Docker, with:

* Bind mounts for ingesting data from your host (./ingest) and persisting the DuckDB file (./data).
* An SQL REPL as the Docker default command (using duckcli) so docker run drops you straight into SQL.
* Python code that builds a medallion architecture: bronze, silver, gold, platinum.
* pytest unit tests for schema, tables, and ETL routines.


## Project Layout
```
NY_IDER_LakeHouse/
├─ Dockerfile
├─ pyproject.toml
├─ README.md
├─ .gitignore
├─ src/
│  └─ lakehouse/
│     ├─ __init__.py
│     ├─ __main__.py
│     ├─ catalog.py
│     ├─ config.py
│     ├─ db.py
│     └─ etl.py
└─ tests/
   ├─ test_catalog.py
   ├─ test_schema.py
   └─ test_etl.py

# host-side bind mounts you create locally (not added to image):
./ingest/    # put CSV/Parquet files here to be ingested into bronze
./data/      # the DuckDB file lives here: ./data/lakehouse.duckdb
```

## Dependencies

For Docker version:

* docker

For local install:

* python-venv
* apache-arrow


## Dockerfile

* Uses Python 3.12 slim.
* Installs duckdb, duckcli (SQL REPL), and pytest.
* Sets default command to launch SQL REPL against /data/lakehouse.duckdb.
* Binds ./ingest → /ingest and ./data → /data.


## Build
```bash
docker build -t duckdb-lakehouse:latest .
```

## Prepare bind mounts
```bash
mkdir -p ./ingest ./data
```

* Put source files to ingest in `./ingest` (CSV or Parquet).
* The DuckDB file will persist at `./data/lakehouse.duckdb`.


## Start SQL REPL (default)
```bash
docker run --rm -it \
  -v "$(pwd)/ingest:/ingest" \
  -v "$(pwd)/data:/data" \
  duckdb-lakehouse:latest
```

You’ll drop into duckcli connected to `/data/lakehouse.duckdb`.  Try executing commands:
```SQL
SELECT current_database();
SHOW ALL TABLES;
```

## Run the ETL (Bronze → Silver → Gold → Platinum)
```bash
docker run --rm -it \
  -v "$(pwd)/ingest:/ingest" \
  -v "$(pwd)/data:/data" \
  duckdb-lakehouse:latest \
  python -m lakehouse --source /ingest --db /data/lakehouse.duckdb
```


## Unit tests (pytest)

These tests create temporary data and a temporary DuckDB file so they don’t touch your persisted /data. They validate:

* Schemas exist.
* Bronze/Silver/Gold/Platinum tables & views are produced.
* ETL logic (row counts and simple aggregations).

Run inside the container with: `pytest -q`

### Run tests
```bash
docker run --rm -it \
  -v "$(pwd)/ingest:/ingest" \
  -v "$(pwd)/data:/data" \
  duckdb-lakehouse:latest \
  pytest -q
```

## Example local usage for ETL, CLI, and pytest (without Docker)

If you want to run it locally:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e . duckcli pytest
mkdir -p ingest data
python -m lakehouse --source ./ingest --db ./data/lakehouse.duckdb
duckcli ./data/lakehouse.duckdb
pytest -q
```

## Data Catalog

This package contains code for documenting and organizing information about data assets within the lakehouse.  This data catalog resides in its own schema within the duckdb database called `catalog`.  Information in the data catalog can be used for data lineage tracking, data versioning, and accountability tracing.

## Notes

* Bronze layer auto-discovers *.csv and *.parquet in /ingest.
* Silver layer deduplicates and preserves _ingested_at.
* Gold/Platinum examples kick in if customers and orders are present.
* Customize etl.py for real-world cleansing/typing/PK-FK enforcement.

## Extending the Medallion patterns (optional ideas)

* Bronze: Store file metadata (file name, size, modification time, checksum) in a control table to support idempotent loads and replay.
* Silver: Apply enforced types, null-value policies, and constraint checks (e.g., NOT NULL on business keys).
* Gold: Build star schemas using DuckDB views or materialized tables; schedule refresh via cron/k8s jobs.
* Platinum: Semantic marts for BI tools; define metrics (e.g., total_amount, orders_count, avg_order_value) as reusable views.

## Common commands (quick reference)

Default (SQL REPL):
```bash
docker run --rm -it -v "$(pwd)/ingest:/ingest" -v "$(pwd)/data:/data" duckdb-lakehouse:latest
```

Run ETL:
```bash
docker run --rm -it -v "$(pwd)/ingest:/ingest" -v "$(pwd)/data:/data" duckdb-lakehouse:latest \
  python -m lakehouse --source /ingest --db /data/lakehouse.duckdb
```

Run tests:
```bash
docker run --rm -it -v "$(pwd)/ingest:/ingest" -v "$(pwd)/data:/data" duckdb-lakehouse:latest pytest -q
```


## Acknowlegements

This architecture in this package was designed with the help of Microsoft Copilot (GPT-5).  To get started, I gave it the following prompt: "Help me design a lightweight data lakehouse based on duckDB and Python 3.12 in a Docker container.  Include a docker bind mount for ingesting data from the host and for data persistence.  Include an SQL REPL that can be launched as the Docker run command. In Python code, build a medallion architecture in the duckDB with bronze, silver, gold, and platinum layers.  Include pytest unit testing for duckDB schema, tables and ETL routines."  Later on, after crafting the particular ELT code for this project, I prompted again: "I have re-written the file etl.py.  Please re-write the file test_etl.py to test this new ETL version.  The new etl.py is attached."


## Copyright

Copyright 2025 Robert Knight

