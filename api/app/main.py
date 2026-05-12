from __future__ import annotations

import os
import time
import uuid
from typing import Optional

import mlflow.pyfunc
import pandas as pd
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "diabetes-readmission-model")
MLFLOW_MODEL_ALIAS = os.getenv("MLFLOW_MODEL_ALIAS", "champion")

MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mlops_diabetes")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mlops_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mlops_password")

DB_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

app = FastAPI(
    title="Diabetes Readmission Prediction API",
    description="API de inferencia para modelo de readmisión hospitalaria usando MLflow.",
    version="0.1.0",
)

# Prometheus metrics
Instrumentator().instrument(app).expose(app)

model = None
model_uri = f"models:/{MLFLOW_MODEL_NAME}@{MLFLOW_MODEL_ALIAS}"


class PredictionRequest(BaseModel):
    age_midpoint: int = Field(..., example=65)
    gender: str = Field(..., example="Female")
    race: Optional[str] = Field(None, example="Caucasian")

    time_in_hospital: int = Field(..., example=4)
    num_lab_procedures: int = Field(..., example=45)
    num_procedures: int = Field(..., example=1)
    num_medications: int = Field(..., example=16)

    number_outpatient: int = Field(..., example=0)
    number_emergency: int = Field(..., example=0)
    number_inpatient: int = Field(..., example=1)
    number_diagnoses: int = Field(..., example=8)

    total_previous_visits: int = Field(..., example=1)
    medication_intensity: int = Field(..., example=61)
    diabetes_med_flag: int = Field(..., example=1)


class PredictionResponse(BaseModel):
    request_id: str
    prediction: int
    prediction_label: str
    probability: Optional[float]
    model_name: str
    model_alias: str
    response_time_ms: float


@app.on_event("startup")
def load_model() -> None:
    global model

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model = mlflow.pyfunc.load_model(model_uri)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "model_loaded": model is not None,
        "model_uri": model_uri,
    }


@app.get("/model/info")
def model_info() -> dict:
    return {
        "model_name": MLFLOW_MODEL_NAME,
        "model_alias": MLFLOW_MODEL_ALIAS,
        "model_uri": model_uri,
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(payload: PredictionRequest) -> PredictionResponse:
    if model is None:
        raise RuntimeError("El modelo no está cargado.")

    start_time = time.time()
    request_id = str(uuid.uuid4())

    input_df = pd.DataFrame([payload.model_dump()])

    prediction = model.predict(input_df)
    predicted_class = int(prediction[0])

    probability = None

    response_time_ms = round((time.time() - start_time) * 1000, 2)

    prediction_label = "readmitted" if predicted_class == 1 else "not_readmitted"

    log_prediction(
        request_id=request_id,
        input_data=payload.model_dump_json(),
        prediction=prediction_label,
        probability=probability,
        response_time_ms=response_time_ms,
    )

    return PredictionResponse(
        request_id=request_id,
        prediction=predicted_class,
        prediction_label=prediction_label,
        probability=probability,
        model_name=MLFLOW_MODEL_NAME,
        model_alias=MLFLOW_MODEL_ALIAS,
        response_time_ms=response_time_ms,
    )


def log_prediction(
    request_id: str,
    input_data: str,
    prediction: str,
    probability: Optional[float],
    response_time_ms: float,
) -> None:
    engine = create_engine(DB_URI)

    insert_sql = text("""
        INSERT INTO monitoring.inference_logs (
            request_id,
            input_data,
            prediction,
            probability,
            model_name,
            model_alias,
            response_time_ms
        )
        VALUES (
            :request_id,
            CAST(:input_data AS JSONB),
            :prediction,
            :probability,
            :model_name,
            :model_alias,
            :response_time_ms
        )
    """)

    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "request_id": request_id,
                "input_data": input_data,
                "prediction": prediction,
                "probability": probability,
                "model_name": MLFLOW_MODEL_NAME,
                "model_alias": MLFLOW_MODEL_ALIAS,
                "response_time_ms": response_time_ms,
            },
        )