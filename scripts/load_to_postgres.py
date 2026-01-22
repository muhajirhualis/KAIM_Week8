# src/load_raw_to_postgres.py
import os
import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "medical_telegram_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def create_raw_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        DROP TABLE IF EXISTS raw.telegram_messages;
        CREATE TABLE raw.telegram_messages (
            message_id BIGINT,
            channel_name TEXT,
            channel_title TEXT,
            message_date TIMESTAMP,
            message_text TEXT,
            has_media BOOLEAN,
            image_path TEXT,
            views INTEGER,
            forwards INTEGER
        );
        """)
        conn.commit()
    logger.info("Created raw.telegram_messages table")

def load_json_files_to_df(base_data_dir: str, date_str: str) -> pd.DataFrame:
    messages = []
    partition_dir = Path(base_data_dir) / "raw" / "telegram_messages" / date_str
    if not partition_dir.exists():
        raise FileNotFoundError(f"No data found for date: {date_str}")
    
    for json_file in partition_dir.glob("*.json"):
        if json_file.name == "_manifest.json":
            continue
        with open(json_file, "r", encoding="utf-8") as f:
            msgs = json.load(f)
            messages.extend(msgs)
    
    df = pd.json_normalize(messages)
    # Ensure correct dtypes
    df["message_date"] = pd.to_datetime(df["message_date"])
    df["has_media"] = df["has_media"].astype(bool)
    df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0).astype(int)
    df["forwards"] = pd.to_numeric(df["forwards"], errors="coerce").fillna(0).astype(int)
    return df

def insert_df_to_postgres(df: pd.DataFrame, conn):
    tuples = [tuple(row) for row in df.to_numpy()]
    cols = ",".join(df.columns)
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO raw.telegram_messages ({cols}) VALUES %s",
            tuples
        )
        conn.commit()
    logger.info(f"Inserted {len(tuples)} rows into raw.telegram_messages")


def main():
    base_data_dir = "data"
    # today = datetime.today().strftime("%Y-%m-%d")

    date_string = "2026-01-18"
    specific_date = datetime.strptime(date_string, "%Y-%m-%d")
    today = specific_date.strftime("%Y-%m-%d")
    df = load_json_files_to_df(base_data_dir, today)
    conn = connect_db()
    create_raw_table(conn)
    insert_df_to_postgres(df, conn)
    conn.close()
    logger.info("✅ Raw data loaded to PostgreSQL")

if __name__ == "__main__":
    main()