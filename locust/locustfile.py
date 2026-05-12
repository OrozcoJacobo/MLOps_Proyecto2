from __future__ import annotations

from locust import HttpUser, between, task


class DiabetesPredictionUser(HttpUser):
    wait_time = between(1, 3)

    @task(1)
    def health_check(self):
        self.client.get("/health", name="/health")

    @task(5)
    def predict_readmission(self):
        payload = {
            "age_midpoint": 65,
            "gender": "Female",
            "race": "Caucasian",
            "time_in_hospital": 4,
            "num_lab_procedures": 45,
            "num_procedures": 1,
            "num_medications": 16,
            "number_outpatient": 0,
            "number_emergency": 0,
            "number_inpatient": 1,
            "number_diagnoses": 8,
            "total_previous_visits": 1,
            "medication_intensity": 61,
            "diabetes_med_flag": 1,
        }

        with self.client.post(
            "/predict",
            json=payload,
            name="/predict",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"Status code inesperado: {response.status_code}")
                return

            try:
                data = response.json()
            except Exception:
                response.failure("La respuesta no es JSON válido")
                return

            if "prediction" not in data:
                response.failure("La respuesta no contiene prediction")
                return

            response.success()