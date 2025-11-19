# src/lakehouse/config.py
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Config:
    db_path: Path = Path(os.getenv("LAKEHOUSE_DB", "/data/lakehouse.duckdb"))
    ingest_dir: Path = Path(os.getenv("LAKEHOUSE_INGEST", "/ingest"))

    # Medallion schemas
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str   = "gold"
    platinum: str = "platinum"

    # Data Catalog schema
    catalog: str = "catalog"

CONFIG = Config()