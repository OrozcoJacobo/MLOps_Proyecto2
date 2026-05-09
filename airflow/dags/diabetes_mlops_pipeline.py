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
    tags=["mlops", "diabetes"],
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

        return {"source_path": SOURCE_DATA_PATH}

    @task
    def get_next_batch_window(file_info: dict) -> dict:
        engine = create_engine(DB_URI)

        with engine.begin() as conn:
            loaded_count = conn.execute(
                text("SELECT COUNT(*) FROM raw.diabetes_raw")
            ).scalar()

        total_rows = len(pd.read_csv(file_info["source_path"]))

        start_row = int(loaded_count or 0)
        end_row = min(start_row + BATCH_SIZE, total_rows)

        return {
            "has_new_data": start_row < total_rows,
            "start_row": start_row,
            "end_row": end_row,
            "total_rows": total_rows,
        }

    @task
    def load_raw_batch(file_info: dict, batch_window: dict) -> dict:
        if not batch_window["has_new_data"]:
            return batch_window

        df = pd.read_csv(file_info["source_path"])
        batch_df = df.iloc[batch_window["start_row"]:batch_window["end_row"]].copy()

        if batch_df.empty:
            raise ValueError("El batch calculado está vacío.")

        batch_id = datetime.utcnow().strftime("batch_%Y%m%d_%H%M%S")
        records = []

        for _, row in batch_df.iterrows():
            row_dict = row.where(pd.notnull(row), None).to_dict()
            row_json = json.dumps(row_dict, sort_keys=True, default=str)
            row_hash = hashlib.sha256(row_json.encode("utf-8")).hexdigest()

            records.append({
                "encounter_id": int(row_dict["encounter_id"]),
                "patient_nbr": int(row_dict["patient_nbr"]),
                "batch_id": batch_id,
                "source_file": "diabetic_data.csv",
                "row_hash": row_hash,
                "raw_data": row_json,
            })

        insert_sql = text("""
            INSERT INTO raw.diabetes_raw (
                encounter_id,
                patient_nbr,
                batch_id,
                source_file,
                row_hash,
                raw_data
            )
            VALUES (
                :encounter_id,
                :patient_nbr,
                :batch_id,
                :source_file,
                :row_hash,
                CAST(:raw_data AS JSONB)
            )
        """)

        engine = create_engine(DB_URI)

        with engine.begin() as conn:
            conn.execute(insert_sql, records)

        return {"batch_id": batch_id, "records_loaded": len(records)}

    @task
    def process_raw_batch(raw_result: dict) -> dict:
        if not raw_result.get("batch_id"):
            return {"processed_records": 0, "message": "No hubo batch nuevo para procesar."}

        batch_id = raw_result["batch_id"]
        engine = create_engine(DB_URI)

        with engine.begin() as conn:
            already_processed = conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM monitoring.processed_batches
                    WHERE batch_id = :batch_id
                """),
                {"batch_id": batch_id},
            ).scalar()

            if already_processed > 0:
                return {
                    "batch_id": batch_id,
                    "processed_records": 0,
                    "message": "Batch ya procesado.",
                }

            raw_rows = conn.execute(
                text("""
                    SELECT encounter_id, patient_nbr, batch_id, raw_data
                    FROM raw.diabetes_raw
                    WHERE batch_id = :batch_id
                """),
                {"batch_id": batch_id},
            ).mappings().all()

        processed_records = []

        for row in raw_rows:
            data = row["raw_data"]

            if isinstance(data, str):
                data = json.loads(data)

            def clean_text(value):
                if value in [None, "?"]:
                    return None
                return str(value)

            def clean_int(value):
                if value in [None, "?"]:
                    return None
                return int(value)

            processed_records.append({
                "encounter_id": row["encounter_id"],
                "patient_nbr": row["patient_nbr"],
                "race": clean_text(data.get("race")),
                "gender": clean_text(data.get("gender")),
                "age": clean_text(data.get("age")),
                "time_in_hospital": clean_int(data.get("time_in_hospital")),
                "num_lab_procedures": clean_int(data.get("num_lab_procedures")),
                "num_procedures": clean_int(data.get("num_procedures")),
                "num_medications": clean_int(data.get("num_medications")),
                "number_outpatient": clean_int(data.get("number_outpatient")),
                "number_emergency": clean_int(data.get("number_emergency")),
                "number_inpatient": clean_int(data.get("number_inpatient")),
                "number_diagnoses": clean_int(data.get("number_diagnoses")),
                "diabetes_med": clean_text(data.get("diabetesMed")),
                "readmitted": clean_text(data.get("readmitted")),
                "batch_id": batch_id,
            })

        if not processed_records:
            raise ValueError(f"No se encontraron registros raw para batch_id={batch_id}")

        insert_processed_sql = text("""
            INSERT INTO processed.diabetes_processed (
                encounter_id,
                patient_nbr,
                race,
                gender,
                age,
                time_in_hospital,
                num_lab_procedures,
                num_procedures,
                num_medications,
                number_outpatient,
                number_emergency,
                number_inpatient,
                number_diagnoses,
                diabetes_med,
                readmitted,
                batch_id
            )
            VALUES (
                :encounter_id,
                :patient_nbr,
                :race,
                :gender,
                :age,
                :time_in_hospital,
                :num_lab_procedures,
                :num_procedures,
                :num_medications,
                :number_outpatient,
                :number_emergency,
                :number_inpatient,
                :number_diagnoses,
                :diabetes_med,
                :readmitted,
                :batch_id
            )
        """)

        insert_monitoring_sql = text("""
            INSERT INTO monitoring.processed_batches (batch_id)
            VALUES (:batch_id)
        """)

        with engine.begin() as conn:
            conn.execute(insert_processed_sql, processed_records)
            conn.execute(insert_monitoring_sql, {"batch_id": batch_id})

        return {"batch_id": batch_id, "processed_records": len(processed_records)}

    @task
    def build_feature_store(process_result: dict) -> dict:
        if process_result.get("processed_records", 0) == 0:
            return {"features_created": 0}

        batch_id = process_result["batch_id"]
        engine = create_engine(DB_URI)

        with engine.begin() as conn:
            processed_rows = conn.execute(
                text("""
                    SELECT *
                    FROM processed.diabetes_processed
                    WHERE batch_id = :batch_id
                """),
                {"batch_id": batch_id},
            ).mappings().all()

        feature_records = []

        for row in processed_rows:

            def safe_int(value):
                if value is None:
                    return 0
                return int(value)

            age_raw = row["age"]
            age_midpoint = None

            if age_raw:
                age_clean = age_raw.replace("[", "").replace(")", "")
                start_age, end_age = age_clean.split("-")
                age_midpoint = (int(start_age) + int(end_age)) // 2

            total_previous_visits = (
                safe_int(row["number_outpatient"])
                + safe_int(row["number_emergency"])
                + safe_int(row["number_inpatient"])
            )

            medication_intensity = (
                safe_int(row["num_medications"])
                + safe_int(row["num_lab_procedures"])
            )

            diabetes_med_flag = 1 if row["diabetes_med"] == "Yes" else 0
            readmitted_flag = 1 if row["readmitted"] in ["<30", ">30"] else 0

            feature_records.append({
                "encounter_id": row["encounter_id"],
                "patient_nbr": row["patient_nbr"],
                "age_midpoint": age_midpoint,
                "gender": row["gender"],
                "race": row["race"],
                "time_in_hospital": row["time_in_hospital"],
                "num_lab_procedures": row["num_lab_procedures"],
                "num_procedures": row["num_procedures"],
                "num_medications": row["num_medications"],
                "number_outpatient": row["number_outpatient"],
                "number_emergency": row["number_emergency"],
                "number_inpatient": row["number_inpatient"],
                "number_diagnoses": row["number_diagnoses"],
                "total_previous_visits": total_previous_visits,
                "medication_intensity": medication_intensity,
                "diabetes_med_flag": diabetes_med_flag,
                "readmitted_flag": readmitted_flag,
                "batch_id": batch_id,
            })

        insert_sql = text("""
            INSERT INTO feature_store.model_features (
                encounter_id,
                patient_nbr,
                age_midpoint,
                gender,
                race,
                time_in_hospital,
                num_lab_procedures,
                num_procedures,
                num_medications,
                number_outpatient,
                number_emergency,
                number_inpatient,
                number_diagnoses,
                total_previous_visits,
                medication_intensity,
                diabetes_med_flag,
                readmitted_flag,
                batch_id
            )
            VALUES (
                :encounter_id,
                :patient_nbr,
                :age_midpoint,
                :gender,
                :race,
                :time_in_hospital,
                :num_lab_procedures,
                :num_procedures,
                :num_medications,
                :number_outpatient,
                :number_emergency,
                :number_inpatient,
                :number_diagnoses,
                :total_previous_visits,
                :medication_intensity,
                :diabetes_med_flag,
                :readmitted_flag,
                :batch_id
            )
        """)

        with engine.begin() as conn:
            conn.execute(insert_sql, feature_records)

        return {"batch_id": batch_id, "features_created": len(feature_records)}

    file_info = validate_source_file()
    batch_window = get_next_batch_window(file_info)
    raw_result = load_raw_batch(file_info, batch_window)
    process_result = process_raw_batch(raw_result)
    build_feature_store(process_result)


diabetes_mlops_pipeline()