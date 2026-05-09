from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os

import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine, text


SOURCE_DATA_PATH = os.getenv("SOURCE_DATA_PATH", "/opt/airflow/data/diabetic_data.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "15000"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mlops_diabetes")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mlops_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mlops_password")

DB_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


@dag(
    dag_id="diabetes_mlops_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "diabetes", "raw-ingestion"],
)
def diabetes_mlops_pipeline():

    @task
    def validate_source_file() -> dict:
        if not os.path.exists(SOURCE_DATA_PATH):
            raise FileNotFoundError(f"No existe el archivo fuente: {SOURCE_DATA_PATH}")

        df_preview = pd.read_csv(SOURCE_DATA_PATH, nrows=5)

        required_columns = {"encounter_id", "patient_nbr", "readmitted"}
        missing_columns = required_columns - set(df_preview.columns)

        if missing_columns:
            raise ValueError(f"Faltan columnas requeridas: {missing_columns}")

        return {
            "source_path": SOURCE_DATA_PATH,
            "file_size_mb": round(os.path.getsize(SOURCE_DATA_PATH) / (1024 * 1024), 2),
        }

    @task
    def get_next_batch_window(file_info: dict) -> dict:
        engine = create_engine(DB_URI)

        with engine.begin() as conn:
            loaded_count = conn.execute(
                text("SELECT COUNT(*) FROM raw.diabetes_raw")
            ).scalar()

        df = pd.read_csv(file_info["source_path"])
        total_rows = len(df)

        start_row = int(loaded_count or 0)
        end_row = min(start_row + BATCH_SIZE, total_rows)

        if start_row >= total_rows:
            return {
                "has_new_data": False,
                "start_row": start_row,
                "end_row": end_row,
                "total_rows": total_rows,
                "message": "No hay registros nuevos para cargar.",
            }

        return {
            "has_new_data": True,
            "start_row": start_row,
            "end_row": end_row,
            "total_rows": total_rows,
        }

    @task
    def load_raw_batch(file_info: dict, batch_window: dict) -> dict:
        if not batch_window["has_new_data"]:
            return batch_window

        df = pd.read_csv(file_info["source_path"])

        start_row = batch_window["start_row"]
        end_row = batch_window["end_row"]

        batch_df = df.iloc[start_row:end_row].copy()

        if batch_df.empty:
            raise ValueError("El batch calculado está vacío. Revisa start_row y end_row.")

        batch_id = datetime.utcnow().strftime("batch_%Y%m%d_%H%M%S")
        records = []

        for _, row in batch_df.iterrows():
            row_dict = row.where(pd.notnull(row), None).to_dict()

            row_json = json.dumps(row_dict, sort_keys=True, default=str)
            row_hash = hashlib.sha256(row_json.encode("utf-8")).hexdigest()

            records.append(
                {
                    "encounter_id": int(row_dict["encounter_id"]),
                    "patient_nbr": int(row_dict["patient_nbr"]),
                    "batch_id": batch_id,
                    "source_file": "diabetic_data.csv",
                    "row_hash": row_hash,
                    "raw_data": row_json,
                }
            )

        insert_sql = text(
            """
            INSERT INTO raw.diabetes_raw (
                encounter_id,
                patient_nbr,
                batch_id,
                ingestion_timestamp,
                source_file,
                row_hash,
                raw_data
            )
            VALUES (
                :encounter_id,
                :patient_nbr,
                :batch_id,
                CURRENT_TIMESTAMP,
                :source_file,
                :row_hash,
                CAST(:raw_data AS JSONB)
            )
            """
        )

        engine = create_engine(DB_URI)

        with engine.begin() as conn:
            conn.execute(insert_sql, records)

        return {
            "batch_id": batch_id,
            "records_loaded": len(records),
            "start_row": start_row,
            "end_row": end_row,
            "total_rows": batch_window["total_rows"],
        }

    file_info = validate_source_file()
    batch_window = get_next_batch_window(file_info)
    load_raw_batch(file_info, batch_window)


diabetes_mlops_pipeline()