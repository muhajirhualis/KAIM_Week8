"""
Load YOLO detection results (CSV) into PostgreSQL under schema 'enriched'.
Assumes CSV has columns: message_id, image_path, detected_objects, max_confidence, image_category
"""
import os
import json
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
from psycopg2.extras import execute_values
from load_to_postgres import connect_db  # reuse DB logic

def create_enriched_schema_and_table(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS enriched;")
        cur.execute("""
            DROP TABLE IF EXISTS enriched.yolo_detections;
            CREATE TABLE enriched.yolo_detections (
                message_id BIGINT,
                image_path TEXT,
                detected_objects TEXT,
                max_confidence FLOAT,
                image_category TEXT
            );
        """)
        conn.commit()

def main():
    today = datetime.today().strftime("%Y-%m-%d")
    csv_path = Path("data/enriched/yolo_detections") / today / "detections.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"YOLO CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    # Ensure correct types
    df["message_id"] = df["message_id"].astype(int)
    df["max_confidence"] = pd.to_numeric(df["max_confidence"], errors="coerce")

    conn = connect_db()
    create_enriched_schema_and_table(conn)

    tuples = [tuple(row) for row in df.to_numpy()]
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO enriched.yolo_detections VALUES %s",
            tuples
        )
        conn.commit()

    conn.close()
    print(f"✅ Loaded {len(df)} YOLO records into enriched.yolo_detections")

if __name__ == "__main__":
    main()