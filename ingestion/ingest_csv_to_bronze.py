import os
import sys
import time
import logging
import argparse
from io import StringIO
from datetime import datetime, timezone

import pandas as pd
from psycopg2 import sql
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

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
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DWH_DB"),
    "user": os.getenv("DWH_USER"),
    "password": os.getenv("DWH_PASSWORD"),
}

missing = [key for key, value in DB_CONFIG.items() if not value]
if missing:
    raise RuntimeError(f"Missing database config: {', '.join(missing)}")

TARGET_SCHEMA = "bronze"
TARGET_TABLE = "raw_flights"
DEFAULT_FILE = "data/raw/US_flights_2023.csv"
DEFAULT_CHUNK = 500_000

COLUMN_RENAME = {
    "FlightDate": "flight_date",
    "Day_Of_Week": "day_of_week",
    "Airline": "airline",
    "Tail_Number": "tail_number",
    "Dep_Airport": "dep_airport",
    "Dep_CityName": "dep_city",
    "DepTime_label": "dep_time_label",
    "Dep_Delay": "dep_delay",
    "Dep_Delay_Tag": "dep_delay_tag",
    "Dep_Delay_Type": "dep_delay_type",
    "Arr_Airport": "arr_airport",
    "Arr_CityName": "arr_city",
    "Arr_Delay": "arr_delay",
    "Arr_Delay_Type": "arr_delay_type",
    "Flight_Duration": "flight_duration",
    "Distance_type": "distance_type",
    "Delay_Carrier": "delay_carrier",
    "Delay_Weather": "delay_weather",
    "Delay_NAS": "delay_nas",
    "Delay_Security": "delay_security",
    "Delay_LastAircraft": "delay_last_aircraft",
    "Manufacturer": "manufacturer",
    "Model": "model",
    "Aicraft_age": "aircraft_age",
}

TARGET_COLUMNS = [
    "flight_date",
    "day_of_week",
    "airline",
    "tail_number",
    "dep_airport",
    "dep_city",
    "dep_time_label",
    "dep_delay",
    "dep_delay_tag",
    "dep_delay_type",
    "arr_airport",
    "arr_city",
    "arr_delay",
    "arr_delay_type",
    "flight_duration",
    "distance_type",
    "delay_carrier",
    "delay_weather",
    "delay_nas",
    "delay_security",
    "delay_last_aircraft",
    "manufacturer",
    "model",
    "aircraft_age",
    "_ingested_at",
    "_source_file",
]

RAW_COLUMNS = TARGET_COLUMNS[:-2]


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
    """Create bronze.raw_flights if it does not exist."""
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

    CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
        flight_date         TEXT,
        day_of_week         TEXT,
        airline             TEXT,
        tail_number         TEXT,
        dep_airport         TEXT,
        dep_city            TEXT,
        dep_time_label      TEXT,
        dep_delay           TEXT,
        dep_delay_tag       TEXT,
        dep_delay_type      TEXT,
        arr_airport         TEXT,
        arr_city            TEXT,
        arr_delay           TEXT,
        arr_delay_type      TEXT,
        flight_duration     TEXT,
        distance_type       TEXT,
        delay_carrier       TEXT,
        delay_weather       TEXT,
        delay_nas           TEXT,
        delay_security      TEXT,
        delay_last_aircraft TEXT,
        manufacturer        TEXT,
        model               TEXT,
        aircraft_age        TEXT,
        _ingested_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        _source_file        TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

        non_text_columns = []
        for column in RAW_COLUMNS:
            data_type = conn.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema "
                    "AND table_name = :table "
                    "AND column_name = :column"
                ),
                {
                    "schema": TARGET_SCHEMA,
                    "table": TARGET_TABLE,
                    "column": column,
                },
            ).scalar_one()
            if data_type != "text":
                non_text_columns.append(column)

        if non_text_columns:
            # One-time migration: dbt recreates this view after ingestion.
            conn.execute(text("DROP VIEW IF EXISTS silver.stg_flights"))
            for column in non_text_columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {TARGET_SCHEMA}.{TARGET_TABLE} "
                        f"ALTER COLUMN {column} TYPE TEXT USING {column}::TEXT"
                    )
                )
            log.info(
                "Migrated Bronze columns to TEXT: "
                + ", ".join(non_text_columns)
            )

    log.info(f"Table {TARGET_SCHEMA}.{TARGET_TABLE} ready")


def transform_chunk(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Apply technical naming and metadata without business transformation."""
    df = df.rename(columns=COLUMN_RENAME)
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_file"] = os.path.basename(source_file)

    missing_columns = [column for column in TARGET_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Source file is missing required columns: {', '.join(missing_columns)}"
        )

    return df[TARGET_COLUMNS]


def copy_chunk(cursor, df: pd.DataFrame):
    """Bulk-load one DataFrame chunk with PostgreSQL COPY."""
    buffer = StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        na_rep=r"\N",
        lineterminator="\n",
    )
    buffer.seek(0)

    copy_statement = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    ).format(
        sql.Identifier(TARGET_SCHEMA),
        sql.Identifier(TARGET_TABLE),
        sql.SQL(", ").join(map(sql.Identifier, TARGET_COLUMNS)),
    )
    cursor.copy_expert(copy_statement.as_string(cursor), buffer)


def log_summary(engine, start_time: float):
    """Print total rows loaded and runtime."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
        )
        total_rows = result.scalar()

    elapsed = time.time() - start_time
    log.info("=" * 50)
    log.info("Ingestion completed")
    log.info(f"  Total loaded rows : {total_rows:,}")
    log.info(f"  Runtime           : {elapsed:.1f}s")
    log.info(f"  Table             : {TARGET_SCHEMA}.{TARGET_TABLE}")
    log.info("=" * 50)


def run(file_path: str, chunksize: int):
    log.info(f"Begin ingestion: {file_path}")
    log.info(f"Chunk size: {chunksize:,} rows")

    engine = get_engine()

    try:
        create_bronze_table(engine)
        connection = engine.raw_connection()
        cursor = connection.cursor()

        try:
            truncate_statement = sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(TARGET_TABLE),
            )
            cursor.execute(truncate_statement)
            log.info("Old table truncated. Begin to load new table.")

            start_time = time.time()
            total_loaded = 0

            reader = pd.read_csv(
                file_path,
                chunksize=chunksize,
                dtype=str,
                keep_default_na=False,
                low_memory=False,
            )

            for chunk_num, chunk in enumerate(reader, start=1):
                rows_in_chunk = len(chunk)
                log.info(f"Chunk {chunk_num}: loading {rows_in_chunk:,} rows...")

                chunk = transform_chunk(chunk, file_path)
                copy_chunk(cursor, chunk)

                total_loaded += rows_in_chunk
                elapsed = time.time() - start_time
                log.info(
                    f"Chunk {chunk_num}: done - "
                    f"Total {total_loaded:,} rows / {elapsed:.1f}s"
                )

            connection.commit()
        except Exception:
            connection.rollback()
            log.exception("Ingestion failed; the load transaction was rolled back.")
            raise
        finally:
            cursor.close()
            connection.close()

        log_summary(engine, start_time)
    finally:
        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest flight CSV into Bronze layer")
    parser.add_argument(
        "--file",
        default=DEFAULT_FILE,
        help=f"Path to CSV (default: {DEFAULT_FILE})",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=DEFAULT_CHUNK,
        help=f"Rows per chunk (default: {DEFAULT_CHUNK:,})",
    )
    args = parser.parse_args()

    if not os.path.exists(args.file):
        log.error(f"File does not exist: {args.file}")
        sys.exit(1)

    run(file_path=args.file, chunksize=args.chunksize)
