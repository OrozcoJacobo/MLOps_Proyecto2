# MLOps Proyecto 2 - Diabetes Readmission Pipeline

Proyecto académico de MLOps para construir un flujo end-to-end de ingesta, procesamiento, feature engineering, entrenamiento, registro, despliegue e inferencia de un modelo de readmisión hospitalaria en pacientes diabéticos.

El proyecto usa Docker Compose, PostgreSQL, MinIO, MLflow, Apache Airflow, FastAPI, Streamlit y Locust. El objetivo es dejar un stack funcional en Docker para que pueda ser migrado posteriormente a Kubernetes.

---

## 1. Arquitectura general

```text
diabetic_data.csv
        ↓
Airflow DAG
        ↓
raw.diabetes_raw
        ↓
processed.diabetes_processed
        ↓
feature_store.model_features
        ↓
train_model
        ↓
MLflow Tracking / Model Registry
        ↓
MinIO artifact storage
        ↓
FastAPI
        ↓
Streamlit / Locust
        ↓
monitoring.inference_logs
```

---

## 2. Flujo funcional

1. Airflow valida la existencia del dataset.
2. Airflow carga datos incrementalmente por batches.
3. Los datos originales se almacenan en la capa RAW.
4. Los datos se limpian y normalizan en la capa PROCESSED.
5. Se construyen features para entrenamiento en la capa FEATURE STORE.
6. Se entrena un modelo `RandomForestClassifier`.
7. Se evalúa el modelo con métricas de clasificación.
8. Se registra el modelo en MLflow.
9. Se promueve como `champion` solo si iguala o mejora el mejor `f1_score` histórico.
10. FastAPI carga el modelo `champion` desde MLflow.
11. Streamlit consume FastAPI para generar predicciones visuales.
12. Locust consume FastAPI para pruebas de carga.
13. Cada inferencia se registra en PostgreSQL.

---

## 3. Servicios Docker

| Servicio | Descripción | Puerto local | Puerto contenedor |
|---|---|---:|---:|
| postgres | Base principal del proyecto, metadata de Airflow y backend store de MLflow | 5432 | 5432 |
| minio | Object storage compatible con S3 para artefactos de MLflow | 9000 / 9001 | 9000 / 9001 |
| minio-init | Inicializa el bucket de MinIO | N/A | N/A |
| mlflow | Tracking server y model registry | 5050 | 5000 |
| airflow-init | Inicialización de Airflow | N/A | N/A |
| airflow-webserver | UI de Airflow | 8081 | 8080 |
| airflow-scheduler | Scheduler de Airflow | N/A | N/A |
| api | API de inferencia con FastAPI | 8000 | 8000 |
| streamlit | Interfaz visual de inferencia | 8501 | 8501 |
| locust | Pruebas de carga | 8089 | 8089 |

---

## 4. URLs locales

```text
Airflow:   http://localhost:8081
MLflow:    http://localhost:5050
MinIO:     http://localhost:9001
FastAPI:   http://localhost:8000/docs
Streamlit: http://localhost:8501
Locust:    http://localhost:8089
```

---

## 5. Credenciales locales

### Airflow

```text
Usuario: airflow
Password: airflow
```

### MinIO

```text
Usuario: minioadmin
Password: minioadmin
```

---

## 6. Estructura esperada del proyecto

```text
.
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   └── diabetes_mlops_pipeline.py
│   ├── include/
│   └── requirements.txt
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       └── main.py
├── data/
│   └── diabetic_data.csv
├── database/
│   └── init/
│       ├── 00_create_airflow_db.sql
│       └── 01_create_schemas.sql
├── locust/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── locustfile.py
├── mlflow/
│   └── Dockerfile
├── streamlit_app/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

## 7. Variables de entorno

El proyecto usa `.env`.

Para crear el archivo:

```bash
cp .env.example .env
```

Variables principales esperadas:

```env
# =========================
# PROJECT
# =========================
PROJECT_NAME=mlops-diabetes
ENVIRONMENT=development

# =========================
# POSTGRES - PROJECT DATABASE
# =========================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=mlops_diabetes
POSTGRES_USER=mlops_user
POSTGRES_PASSWORD=mlops_password

# =========================
# AIRFLOW DATABASE
# =========================
AIRFLOW_DB=airflow_db
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=airflow_password

# =========================
# AIRFLOW
# =========================
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASSWORD=airflow
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_SECRET_KEY=mlops_diabetes_airflow_secret_key_2026

# =========================
# MLFLOW
# =========================
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_EXPERIMENT_NAME=diabetes-readmission
MLFLOW_MODEL_NAME=diabetes-readmission-model
MLFLOW_MODEL_ALIAS=champion

# =========================
# MINIO / S3
# =========================
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET_NAME=mlflow-artifacts
MLFLOW_S3_ENDPOINT_URL=http://minio:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin

# =========================
# API
# =========================
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=true

# =========================
# STREAMLIT
# =========================
STREAMLIT_SERVER_PORT=8501
API_BASE_URL=http://api:8000

# =========================
# LOCUST
# =========================
LOCUST_HOST=http://api:8000

# =========================
# DATA PIPELINE
# =========================
SOURCE_DATA_PATH=/opt/airflow/data/diabetic_data.csv
BATCH_SIZE=15000
TARGET_COLUMN=readmitted
```

---

## 8. Requisitos previos

Antes de ejecutar:

- Tener Docker Desktop instalado.
- Tener Docker Compose disponible.
- Tener el archivo `diabetic_data.csv`.
- Tener suficiente memoria asignada a Docker Desktop. Recomendado: al menos 4 GB; ideal 6 GB o más.

Verificar Docker:

```bash
docker --version
docker compose version
```

---

## 9. Preparación inicial

### 9.1. Clonar el repositorio

```bash
git clone <https://github.com/OrozcoJacobo/MLOps_Proyecto2/tree/desarrollo_docker>
cd MLOps_Proyecto2
```

### 9.2. Crear `.env`

```bash
cp .env.example .env
```

### 9.3. Agregar dataset

Crear carpeta:

```bash
mkdir -p data
```

Ubicar el dataset en:

```text
data/diabetic_data.csv
```

Importante:

- El dataset no debe versionarse en Git.
- La carpeta `data/` debe existir localmente.
- El DAG espera leer el archivo desde `/opt/airflow/data/diabetic_data.csv` dentro del contenedor.

---

## 10. Ejecución desde cero con Docker Compose

### 10.1. Construir imágenes

```bash
docker compose build --no-cache
```

### 10.2. Levantar servicios base

```bash
docker compose up -d postgres minio minio-init mlflow
```

Esperar unos segundos y validar:

```bash
docker compose ps
```

### 10.3. Inicializar Airflow

```bash
docker compose run --rm airflow-init
```

### 10.4. Levantar Airflow

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

Validar:

```bash
docker compose ps
```

### 10.5. Ejecutar el DAG de entrenamiento

Entrar a Airflow:

```text
http://localhost:8081
```

Pasos:

1. Iniciar sesión con `airflow / airflow`.
2. Buscar el DAG `diabetes_mlops_pipeline`.
3. Activar el DAG si está pausado.
4. Presionar `Trigger DAG`.
5. Esperar a que todas las tareas queden en verde.

El DAG ejecuta:

```text
validate_source_file
        ↓
get_next_batch_window
        ↓
load_raw_batch
        ↓
process_raw_batch
        ↓
build_feature_store
        ↓
train_model
```

### 10.6. Levantar API, Streamlit y Locust

Cuando el DAG haya entrenado y registrado un modelo `champion`, levantar:

```bash
docker compose up -d api streamlit locust
```

Si las imágenes no se han construido individualmente:

```bash
docker compose build --no-cache api streamlit locust
docker compose up -d api streamlit locust
```

---

## 11. Ejecución rápida cuando ya existen volúmenes

Si ya se ejecutó antes y no se borraron volúmenes:

```bash
docker compose up -d
```

Validar:

```bash
docker compose ps
```

No usar `docker compose down -v` salvo que se quiera reiniciar todo desde cero.

---

## 12. Verificaciones obligatorias

### 12.1. Verificar contenedores

```bash
docker compose ps
```

Deben aparecer en estado `Up`:

```text
mlops-postgres
mlops-minio
mlops-mlflow
mlops-airflow-webserver
mlops-airflow-scheduler
mlops-api
mlops-streamlit
mlops-locust
```

---

## 13. Validación de base de datos

### 13.1. Conteo RAW

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM raw.diabetes_raw;"
```

### 13.2. Conteo PROCESSED

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM processed.diabetes_processed;"
```

### 13.3. Conteo FEATURE STORE

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM feature_store.model_features;"
```

### 13.4. Batches procesados

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT * FROM monitoring.processed_batches ORDER BY processed_timestamp DESC LIMIT 10;"
```

### 13.5. Entrenamientos registrados

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT run_id, model_version, f1_score, promoted_to_champion, training_timestamp FROM monitoring.model_training_runs ORDER BY training_timestamp DESC LIMIT 10;"
```

Resultado esperado:

- Al menos un entrenamiento.
- Al menos una fila con `promoted_to_champion = true`.
- Una versión de modelo asociada.

---

## 14. Validación de MLflow

Abrir:

```text
http://localhost:5050
```

Validar:

1. Existe el experimento `diabetes-readmission`.
2. Hay al menos un run.
3. El run tiene métricas:
   - accuracy
   - precision
   - recall
   - f1_score
4. Entrar a `Models`.
5. Validar que existe:
   - `diabetes-readmission-model`
6. Confirmar que existe alias:
   - `champion`

El modelo activo para inferencia es:

```text
models:/diabetes-readmission-model@champion
```

---

## 15. Validación de MinIO

Abrir:

```text
http://localhost:9001
```

Credenciales:

```text
minioadmin / minioadmin
```

Validar que existe el bucket:

```text
mlflow-artifacts
```

Dentro deben aparecer artefactos de MLflow, por ejemplo:

```text
MLmodel
model.pkl
conda.yaml
python_env.yaml
requirements.txt
```

---

## 16. Validación de FastAPI

Abrir Swagger:

```text
http://localhost:8000/docs
```

### 16.1. Health check

Probar:

```bash
curl http://localhost:8000/health
```

Respuesta esperada:

```json
{
  "status": "ok",
  "model_loaded": true,
  "model_uri": "models:/diabetes-readmission-model@champion"
}
```

### 16.2. Model info

```bash
curl http://localhost:8000/model/info
```

Respuesta esperada:

```json
{
  "model_name": "diabetes-readmission-model",
  "model_alias": "champion",
  "model_uri": "models:/diabetes-readmission-model@champion"
}
```

### 16.3. Predict

Probar desde Swagger o terminal:

```bash
curl -X POST "http://localhost:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{
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
    "diabetes_med_flag": 1
  }'
```

Respuesta esperada:

```json
{
  "request_id": "...",
  "prediction": 1,
  "prediction_label": "readmitted",
  "probability": null,
  "model_name": "diabetes-readmission-model",
  "model_alias": "champion",
  "response_time_ms": 123.45
}
```

Nota: `probability` puede estar en `null` en la versión actual. La predicción de clase funciona correctamente.

### 16.4. Validar log de inferencia

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT request_id, prediction, model_name, model_alias, response_time_ms, timestamp FROM monitoring.inference_logs ORDER BY timestamp DESC LIMIT 5;"
```

---

## 17. Validación de Streamlit

Abrir:

```text
http://localhost:8501
```

Pasos:

1. Verificar que en la barra lateral aparezca `API conectada`.
2. Verificar que el modelo mostrado sea:
   ```text
   models:/diabetes-readmission-model@champion
   ```
3. Completar o dejar valores por defecto.
4. Presionar `Generar predicción`.
5. Confirmar que aparece:
   - Predicción generada
   - Resultado
   - Tiempo de respuesta
   - JSON de respuesta

Después validar en PostgreSQL:

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT request_id, prediction, response_time_ms, timestamp FROM monitoring.inference_logs ORDER BY timestamp DESC LIMIT 5;"
```

---

## 18. Validación de Locust

Abrir:

```text
http://localhost:8089
```

Configuración inicial recomendada:

```text
Number of users: 5
Ramp up: 1
Host: http://api:8000
```

Ejecutar durante 1 o 2 minutos.

Luego probar:

```text
Number of users: 10
Ramp up: 2
Host: http://api:8000
```

Validar en Locust:

- Requests totales.
- Fails.
- Average response time.
- p95.
- RPS.

Validar en PostgreSQL que las inferencias aumentaron:

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM monitoring.inference_logs;"
```

Ver últimas inferencias:

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT request_id, prediction, response_time_ms, timestamp FROM monitoring.inference_logs ORDER BY timestamp DESC LIMIT 10;"
```

Nota importante:

El endpoint `/predict` escribe cada inferencia en PostgreSQL. Bajo carga concurrente, esto puede aumentar la latencia. Para pruebas de rendimiento más fuertes se recomienda evaluar:

- múltiples workers de API;
- logging asíncrono;
- separar inferencia de escritura;
- optimización del modelo;
- recursos asignados a Docker/Kubernetes.

---

## 19. Selección de modelo champion por f1_score

El proyecto no sirve simplemente el último modelo entrenado.

La task `train_model`:

1. Entrena una nueva versión de `RandomForestClassifier`.
2. Calcula métricas:
   - accuracy
   - precision
   - recall
   - f1_score
3. Consulta el mejor `f1_score` histórico promovido como champion.
4. Registra el nuevo modelo en MLflow.
5. Promueve la nueva versión como `champion` si su `f1_score` iguala o supera el mejor champion histórico.
6. Guarda la decisión en `monitoring.model_training_runs`.

Consulta recomendada:

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT run_id, model_version, f1_score, promoted_to_champion, training_timestamp FROM monitoring.model_training_runs ORDER BY training_timestamp DESC LIMIT 10;"
```

Interpretación:

```text
promoted_to_champion = true
```

Significa que esa versión fue promovida como modelo activo de producción.

FastAPI siempre consume:

```text
models:/diabetes-readmission-model@champion
```

Por tanto, la API usa la mejor versión validada según la métrica de promoción definida.

---

## 20. Comandos de operación

### 20.1. Levantar todo

```bash
docker compose up -d
```

### 20.2. Apagar sin borrar datos

```bash
docker compose down
```

### 20.3. Apagar y borrar volúmenes

Usar solo si se quiere reiniciar completamente el proyecto:

```bash
docker compose down -v
```

Después de esto se debe volver a ejecutar:

```bash
docker compose build --no-cache
docker compose up -d postgres minio minio-init mlflow
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Y volver a correr el DAG para crear el modelo champion antes de levantar API/Streamlit/Locust.

### 20.4. Reiniciar Airflow

```bash
docker compose restart airflow-scheduler airflow-webserver
```

### 20.5. Reiniciar API

```bash
docker compose restart api
```

### 20.6. Ver logs Airflow Scheduler

```bash
docker compose logs airflow-scheduler --tail=200
```

### 20.7. Ver logs Airflow Webserver

```bash
docker compose logs airflow-webserver --tail=200
```

### 20.8. Ver logs MLflow

```bash
docker compose logs mlflow --tail=100
```

### 20.9. Ver logs API

```bash
docker compose logs api --tail=100
```

### 20.10. Ver logs Streamlit

```bash
docker compose logs streamlit --tail=100
```

### 20.11. Ver logs Locust

```bash
docker compose logs locust --tail=100
```

### 20.12. Ver recursos

```bash
docker stats
```

---

## 21. Orden recomendado para migración a Kubernetes

Para Kubernetes, respetar dependencias lógicas:

```text
1. PostgreSQL
2. MinIO
3. MinIO bucket init
4. MLflow
5. Airflow webserver/scheduler
6. Ejecutar DAG y registrar champion
7. FastAPI
8. Streamlit
9. Locust
```

La API depende lógicamente de que exista:

```text
models:/diabetes-readmission-model@champion
```

Si el modelo champion no existe en MLflow, la API puede fallar al iniciar.

---

## 22. Servicios que debe migrar el compañero a Kubernetes

```text
postgres
minio
minio-init
mlflow
airflow-webserver
airflow-scheduler
api
streamlit
locust
```

Elementos a considerar:

- ConfigMaps para variables no sensibles.
- Secrets para usuarios y contraseñas.
- PVCs para PostgreSQL y MinIO.
- Services para comunicación interna.
- Exposición externa para:
  - Airflow
  - MLflow
  - MinIO Console
  - FastAPI
  - Streamlit
  - Locust

---

## 23. Estado final de la parte Docker

La parte Docker entrega:

- Pipeline Airflow funcional.
- Base PostgreSQL con capas RAW, PROCESSED, FEATURE STORE y MONITORING.
- Entrenamiento y versionamiento con MLflow.
- Promoción de champion por `f1_score`.
- Artefactos almacenados en MinIO.
- API FastAPI consumiendo el modelo champion.
- Streamlit consumiendo la API.
- Locust listo para pruebas de carga.
- Logging de inferencias en PostgreSQL.
- Docker Compose como referencia operativa para migración a Kubernetes.

---

## 24. Limitaciones conocidas

1. `probability` puede salir `null` en la respuesta de `/predict`.
2. La API usa un solo proceso Uvicorn por defecto.
3. Cada predicción escribe síncronamente en PostgreSQL.
4. Bajo carga concurrente, `/predict` puede presentar latencias altas.
5. El modelo actual es un baseline RandomForest.
6. La optimización de rendimiento queda para la fase de pruebas y despliegue Kubernetes.

---

## 25. Nota técnica importante

El preprocesamiento está dentro del pipeline del modelo:

```text
ColumnTransformer
        ↓
SimpleImputer
        ↓
OneHotEncoder(handle_unknown="ignore")
        ↓
RandomForestClassifier
```

Esto evita que el sistema falle cuando en inferencia aparezcan categorías nuevas o cuando no estén presentes exactamente las mismas categorías vistas en entrenamiento.

Este punto es crítico para que el sistema no sea solo un notebook funcional, sino un pipeline desplegable.

---

## 26. Checklist final para entregar

Antes de entregar al compañero:

```bash
docker compose ps
```

Validar URLs:

```text
Airflow:   http://localhost:8081
MLflow:    http://localhost:5050
MinIO:     http://localhost:9001
FastAPI:   http://localhost:8000/docs
Streamlit: http://localhost:8501
Locust:    http://localhost:8089
```

Validar modelo champion:

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT run_id, model_version, f1_score, promoted_to_champion, training_timestamp FROM monitoring.model_training_runs ORDER BY training_timestamp DESC LIMIT 10;"
```

Validar API:

```bash
curl http://localhost:8000/health
```

Validar inferencias:

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT request_id, prediction, response_time_ms, timestamp FROM monitoring.inference_logs ORDER BY timestamp DESC LIMIT 5;"
```

Si todo lo anterior funciona, el entorno Docker está listo para migración a kubernetes y pruebas.
