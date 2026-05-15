import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Create logs folder if not existed
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/ingestion.log"),
    ],
)
log = logging.getLogger(__name__)

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "postgres"),
    "port":     os.getenv("DB_PORT", "5432"),
    "dbname":   os.getenv("DWH_DB"),
    "user":     os.getenv("DWH_USER"),
    "password": os.getenv("DWH_PASSWORD"),
}

missing = [key for key, value in DB_CONFIG.items() if not value]
if missing:
    raise RuntimeError(f"Missing database config: {', '.join(missing)}")

TARGET_SCHEMA = "bronze"
TARGET_TABLE  = "raw_flights"
DEFAULT_FILE  = "data/raw/US_flights_2023.csv"
DEFAULT_CHUNK = 500_000

COLUMN_RENAME = {
    "FlightDate":       "flight_date",
    "Day_Of_Week":      "day_of_week",
    "Airline":          "airline",
    "Tail_Number":      "tail_number",
    "Dep_Airport":      "dep_airport",
    "Dep_CityName":     "dep_city",
    "DepTime_label":    "dep_time_label",
    "Dep_Delay":        "dep_delay",
    "Dep_Delay_Tag":    "dep_delay_tag",
    "Dep_Delay_Type":   "dep_delay_type",
    "Arr_Airport":      "arr_airport",
    "Arr_CityName":     "arr_city",
    "Arr_Delay":        "arr_delay",
    "Arr_Delay_Type":   "arr_delay_type",
    "Flight_Duration":  "flight_duration",
    "Distance_type":    "distance_type",
    "Delay_Carrier":    "delay_carrier",
    "Delay_Weather":    "delay_weather",
    "Delay_NAS":        "delay_nas",
    "Delay_Security":   "delay_security",
    "Delay_LastAircraft":"delay_last_aircraft",
    "Manufacturer":     "manufacturer",
    "Model":            "model",
    "Aicraft_age":      "aircraft_age",
}

def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["dbname"],
    )
    return create_engine(url, pool_pre_ping=True)

def create_bronze_table(engine):
    """Tạo bảng bronze.raw_flights nếu chưa có."""
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};
 
    CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
        flight_date         TEXT,
        day_of_week         INTEGER,
        airline             TEXT,
        tail_number         TEXT,
        dep_airport         TEXT,
        dep_city            TEXT,
        dep_time_label      TEXT,
        dep_delay           NUMERIC,
        dep_delay_tag       INTEGER,
        dep_delay_type      TEXT,
        arr_airport         TEXT,
        arr_city            TEXT,
        arr_delay           NUMERIC,
        arr_delay_type      TEXT,
        flight_duration     INTEGER,
        distance_type       TEXT,
        delay_carrier       NUMERIC,
        delay_weather       NUMERIC,
        delay_nas           NUMERIC,
        delay_security      NUMERIC,
        delay_last_aircraft NUMERIC,
        manufacturer        TEXT,
        model               TEXT,
        aircraft_age        INTEGER,
        _ingested_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        _source_file        TEXT
    );
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    log.info(f"Table {TARGET_SCHEMA}.{TARGET_TABLE} ready")


def truncate_table(engine):
    """Truncate old data before reloading"""
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE}"))
        conn.commit()
    log.info("Old table truncated. Begin to load new table.")

def transform_chunk(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Bronze layer: only rename columns and add metadata
    Transforming is not involved
    """
    df = df.rename(columns=COLUMN_RENAME)

    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_file"] = os.path.basename(source_file)

    return df

def log_summary(engine, start_time: float):
    """Print total rows loaded and runtime."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
        )
        total_rows = result.scalar()
 
    elapsed = time.time() - start_time
    log.info("=" * 50)
    log.info(f"Ingestion completed")
    log.info(f"  Total loaded rows : {total_rows:,}")
    log.info(f"  Runtimes          : {elapsed:.1f}s")
    log.info(f"  Table             : {TARGET_SCHEMA}.{TARGET_TABLE}")
    log.info("=" * 50)

#    Main

def run(file_path: str, chunksize: int):
    log.info(f"Begin ingestion: {file_path}")
    log.info(f"Chunk size: {chunksize:,} rows")
 
    engine = get_engine()
 
    # Table setup
    create_bronze_table(engine)
    truncate_table(engine)
 
    start_time = time.time()
    total_loaded = 0
    chunk_num = 0
 
    # Read CSV with chunk, avoiding OOM with 6M rows
    reader = pd.read_csv(
        file_path,
        chunksize=chunksize,
        low_memory=False,
    )
 
    for chunk in reader:
        chunk_num += 1
        rows_in_chunk = len(chunk)
 
        log.info(f"Chunk {chunk_num}: executing {rows_in_chunk:,} rows...")
 
        # Transform (Bronze: rename + add metadata)
        chunk = transform_chunk(chunk, file_path)
 
        # Load into PostgreSQL
        chunk.to_sql(
            name=TARGET_TABLE,
            schema=TARGET_SCHEMA,
            con=engine,
            if_exists="append",   
            index=False,
            method="multi",       # batch insert 
            chunksize=5_000,      # rows per INSERT statement
        )
 
        total_loaded += rows_in_chunk
        elapsed = time.time() - start_time
        log.info(
            f"Chunk {chunk_num}: done — "
            f"Total {total_loaded:,} rows / {elapsed:.1f}s"
        )
 
    log_summary(engine, start_time)
    engine.dispose()
 
 
# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest flight CSV vào Bronze layer")
    parser.add_argument(
        "--file",
        default=DEFAULT_FILE,
        help=f"Path to CSV (default: {DEFAULT_FILE})",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=DEFAULT_CHUNK,
        help=f"rows per chunk (default: {DEFAULT_CHUNK:,})",
    )
    args = parser.parse_args()
 
    # Existed file check
    if not os.path.exists(args.file):
        log.error(f"File not existed: {args.file}")
        sys.exit(1)
 
    run(file_path=args.file, chunksize=args.chunksize)
