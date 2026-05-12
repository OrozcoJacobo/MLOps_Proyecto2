from __future__ import annotations

import os

import requests
import streamlit as st


API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")


st.set_page_config(
    page_title="Diabetes Readmission Predictor",
    page_icon="🏥",
    layout="centered",
)

st.title("Diabetes Readmission Predictor")
st.write(
    "Interfaz de demostración para consumir el modelo registrado en MLflow "
    "a través de la API FastAPI."
)

st.divider()

with st.sidebar:
    st.header("Modelo")
    try:
        health_response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        health_data = health_response.json()

        if health_response.status_code == 200 and health_data.get("model_loaded"):
            st.success("API conectada")
            st.write(f"Modelo: `{health_data.get('model_uri')}`")
        else:
            st.warning("API disponible, pero modelo no cargado")
    except Exception as exc:
        st.error("No se pudo conectar con la API")
        st.caption(str(exc))


st.header("Datos del paciente")

age_midpoint = st.slider("Edad aproximada", min_value=5, max_value=95, value=65, step=10)

gender = st.selectbox(
    "Género",
    options=["Female", "Male", "Unknown/Invalid"],
    index=0,
)

race = st.selectbox(
    "Raza",
    options=["Caucasian", "AfricanAmerican", "Hispanic", "Asian", "Other"],
    index=0,
)

time_in_hospital = st.number_input("Tiempo en hospital", min_value=1, max_value=30, value=4)
num_lab_procedures = st.number_input("Número de procedimientos de laboratorio", min_value=0, max_value=150, value=45)
num_procedures = st.number_input("Número de procedimientos", min_value=0, max_value=20, value=1)
num_medications = st.number_input("Número de medicamentos", min_value=0, max_value=100, value=16)

number_outpatient = st.number_input("Visitas outpatient previas", min_value=0, max_value=50, value=0)
number_emergency = st.number_input("Visitas emergency previas", min_value=0, max_value=50, value=0)
number_inpatient = st.number_input("Visitas inpatient previas", min_value=0, max_value=50, value=1)

number_diagnoses = st.number_input("Número de diagnósticos", min_value=1, max_value=30, value=8)

diabetes_med_flag = st.selectbox(
    "Usa medicación para diabetes",
    options=[1, 0],
    format_func=lambda value: "Sí" if value == 1 else "No",
)

total_previous_visits = number_outpatient + number_emergency + number_inpatient
medication_intensity = num_medications + num_lab_procedures

payload = {
    "age_midpoint": age_midpoint,
    "gender": gender,
    "race": race,
    "time_in_hospital": time_in_hospital,
    "num_lab_procedures": num_lab_procedures,
    "num_procedures": num_procedures,
    "num_medications": num_medications,
    "number_outpatient": number_outpatient,
    "number_emergency": number_emergency,
    "number_inpatient": number_inpatient,
    "number_diagnoses": number_diagnoses,
    "total_previous_visits": total_previous_visits,
    "medication_intensity": medication_intensity,
    "diabetes_med_flag": diabetes_med_flag,
}

st.divider()

st.subheader("Features derivadas")
st.json(
    {
        "total_previous_visits": total_previous_visits,
        "medication_intensity": medication_intensity,
    }
)

if st.button("Generar predicción", type="primary"):
    try:
        response = requests.post(
            f"{API_BASE_URL}/predict",
            json=payload,
            timeout=20,
        )

        if response.status_code != 200:
            st.error("La API respondió con error")
            st.code(response.text)
        else:
            result = response.json()

            st.success("Predicción generada")

            prediction_label = result["prediction_label"]

            if prediction_label == "readmitted":
                st.error("Resultado: paciente con riesgo de readmisión")
            else:
                st.info("Resultado: paciente sin readmisión estimada")

            st.metric("Predicción", result["prediction"])
            st.metric("Tiempo de respuesta", f"{result['response_time_ms']} ms")

            st.json(result)

    except Exception as exc:
        st.error("No se pudo generar la predicción")
        st.caption(str(exc))