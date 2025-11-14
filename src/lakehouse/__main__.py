# src/lakehouse/__main__.py
import argparse
from .etl import run_all

def main():
    parser = argparse.ArgumentParser(description="Lakehouse ETL runner")
    parser.add_argument("--db", dest="db_path", default=None, help="DuckDB path (default /data/lakehouse.duckdb)")
    parser.add_argument("--source", dest="source_dir", default=None, help="Ingest dir (default /ingest)")
    args = parser.parse_args()
    run_all(db_path=args.db_path, source_dir=args.source_dir)

if __name__ == "__main__":
    main()
