# # dagster_pipeline.py
# import sys
# import os
# import subprocess
# from pathlib import Path
# from dagster import job, op, get_dagster_logger

# PROJECT_ROOT = Path(__file__).parent.resolve()
# DBT_DIR = PROJECT_ROOT / "medical_warehouse"

# def run_in_project(script: str):
#     return subprocess.run(
#         [sys.executable, script],
#         cwd=PROJECT_ROOT,
#         env={**os.environ},
#         capture_output=True,
#         text=True
#     )

# @op
# def scrape_telegram_data():
#     logger = get_dagster_logger()
#     logger.info("⏭️ Using pre-existing sample data. Skipping scrape.")

# @op
# def load_raw_to_postgres():
#     logger = get_dagster_logger()
#     logger.info("📥 Loading raw data...")
#     result = run_in_project("scripts/load_to_postgres.py")
#     if result.returncode != 0:
#         raise Exception(f"❌ Load failed:\n{result.stderr}")

# @op
# def run_dbt_transformations():
#     logger = get_dagster_logger()
#     logger.info("⚙️ Running dbt...")
#     result = subprocess.run(
#         [sys.executable, "-m", "dbt", "run"],
#         cwd=DBT_DIR,
#         env={**os.environ},
#         capture_output=True,
#         text=True
#     )
#     if result.returncode != 0:
#         raise Exception(f"❌ dbt failed:\n{result.stderr}")

# @op
# def run_yolo_enrichment():
#     logger = get_dagster_logger()
#     logger.info("🖼️ Running YOLO...")
#     r1 = run_in_project("src/yolo_detect.py")
#     r2 = run_in_project("scripts/load_yolo_to_postgres.py")
#     if r1.returncode != 0 or r2.returncode != 0:
#         raise Exception(f"❌ YOLO failed:\n{r1.stderr}\n{r2.stderr}")

# @job
# def medical_telegram_pipeline():
#     scrape_telegram_data()
#     load_raw_to_postgres()
#     run_dbt_transformations()
#     run_yolo_enrichment()


# KAIM_Week8/pipeline.py

import sys
import os
import subprocess
from pathlib import Path
from dagster import job, op, get_dagster_logger

@op
def scrape_telegram_data():
    logger = get_dagster_logger()
    logger.info("🚀 Starting Telegram scraping...")
    result = subprocess.run([
        "python", "scripts/tg_scraper.py"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"❌ Telegram scraping failed:\n{result.stderr}")
    logger.info("✅ Telegram scraping completed.")

@op
def load_raw_to_postgres():
    logger = get_dagster_logger()
    logger.info("📥 Loading raw JSON to PostgreSQL...")
    result = subprocess.run([
        "python", "scripts/load_to_postgres.py"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"❌ Load raw failed:\n{result.stderr}")
    logger.info("✅ Raw data loaded to PostgreSQL.")

@op
def run_dbt_transformations():
    logger = get_dagster_logger()
    logger.info("⚙️ Running dbt transformations...")
    result = subprocess.run([
        "dbt", "run",
        "--project-dir", "medical_warehouse",
        "--profiles-dir", "medical_warehouse"
    ], cwd="medical_warehouse", capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"❌ dbt run failed:\n{result.stderr}")
    logger.info("✅ dbt transformations completed.")

@op
def run_yolo_enrichment():
    logger = get_dagster_logger()
    logger.info("🖼️ Running YOLO image enrichment...")
    
    # Step 1: Run YOLO detection
    result1 = subprocess.run([
        "python", "src/yolo_detect.py"
    ], capture_output=True, text=True)
    if result1.returncode != 0:
        raise Exception(f"❌ YOLO detect failed:\n{result1.stderr}")
    
    # Step 2: Load results to PostgreSQL
    result2 = subprocess.run([
        "python", "scripts/load_yolo_to_postgres.py"
    ], capture_output=True, text=True)
    if result2.returncode != 0:
        raise Exception(f"❌ YOLO load failed:\n{result2.stderr}")
    
    logger.info("✅ YOLO enrichment completed.")

@job
def medical_telegram_pipeline():
    """
    End-to-end pipeline:
    Scrape → Load Raw → Transform (dbt) → Enrich (YOLO)
    """
    yolo_out = run_yolo_enrichment(
        run_dbt_transformations(
            load_raw_to_postgres(
                scrape_telegram_data()
            )
        )
    )

# Optional: Add schedule
from dagster import ScheduleDefinition, DefaultScheduleStatus

medical_telegram_schedule = ScheduleDefinition(
    job=medical_telegram_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    default_status=DefaultScheduleStatus.RUNNING,
)