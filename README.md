# MLOps Proyecto 2 - Diabetes Readmission Pipeline

Proyecto académico de MLOps orientado a construir un pipeline end-to-end para ingesta, procesamiento, feature engineering y entrenamiento de modelos usando Docker Compose, Airflow, PostgreSQL, MinIO y MLflow.

El objetivo del proyecto es predecir la readmisión hospitalaria de pacientes diabéticos a partir del dataset `diabetic_data.csv`.

---

# Arquitectura actual

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
PostgreSQL monitoring tables
```

---

# Servicios implementados

| Servicio | Descripción | URL / Puerto |
|---|---|---|
| PostgreSQL | Base de datos principal del proyecto y metadata de Airflow/MLflow | localhost:5432 |
| MinIO | Object storage compatible con S3 para artefactos de MLflow | localhost:9001 |
| MLflow | Tracking server y model registry | localhost:5050 |
| Airflow Webserver | Orquestación del pipeline | localhost:8081 |
| Airflow Scheduler | Ejecución de DAGs | Interno |

---

# Estructura del proyecto

```text
.
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   └── diabetes_mlops_pipeline.py
│   ├── include/
│   └── requirements.txt
├── data/
│   └── diabetic_data.csv
├── database/
│   └── init/
│       ├── 00_create_airflow_db.sql
│       └── 01_create_schemas.sql
├── mlflow/
│   └── Dockerfile
├── docker-compose.yml
├── .env
├── .env.example
├── .gitignore
└── README.md
```

---

# Componentes desarrollados hasta el momento

## 1. Configuración base con Docker Compose

Se configuró un entorno local reproducible con:

- PostgreSQL 15
- MinIO
- MLflow
- Airflow 2.9.3 con imagen custom
- Red interna Docker
- Volúmenes persistentes

También se separó la base de datos de Airflow de la base principal del proyecto para evitar conflictos de metadata.

---

## 2. PostgreSQL y arquitectura de datos

Se crearon los esquemas:

```sql
raw
processed
feature_store
monitoring
```

Tablas principales:

```text
raw.diabetes_raw
processed.diabetes_processed
feature_store.model_features
monitoring.processed_batches
monitoring.model_training_runs
```

---

## 3. Capa RAW

Tabla:

```text
raw.diabetes_raw
```

Esta capa almacena los datos originales del CSV como JSONB.

Incluye:

- encounter_id
- patient_nbr
- batch_id
- source_file
- row_hash
- raw_data
- ingestion_timestamp

---

## 4. Capa PROCESSED

Tabla:

```text
processed.diabetes_processed
```

Esta capa limpia y normaliza el dataset.

Transformaciones actuales:

- Conversión de `?` a NULL
- Conversión de variables numéricas
- Selección de variables relevantes
- Conservación de batch_id

---

## 5. Feature Store

Tabla:

```text
feature_store.model_features
```

Features creadas:

- age_midpoint
- total_previous_visits
- medication_intensity
- diabetes_med_flag
- readmitted_flag

También se conservan variables base necesarias para entrenamiento.

---

## 6. Control de batches

Tabla:

```text
monitoring.processed_batches
```

Se usa para evitar reprocesar batches ya transformados.

Esto permite:

- procesamiento incremental
- trazabilidad
- control de duplicados
- consistencia operacional

---

## 7. DAG principal de Airflow

Archivo:

```text
airflow/dags/diabetes_mlops_pipeline.py
```

Tasks actuales:

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

---

## 8. Entrenamiento inicial

La task `train_model` implementa:

- RandomForestClassifier
- Pipeline de sklearn
- ColumnTransformer
- OneHotEncoder(handle_unknown="ignore")
- MLflow Tracking
- MLflow Model Registry

La idea es que todo el preprocesamiento quede embebido dentro del pipeline para garantizar consistencia en inferencia futura.

---

# Variables de entorno

Archivo:

```text
.env
```

Variables principales:

```env
POSTGRES_DB=mlops_diabetes
POSTGRES_USER=mlops_user
POSTGRES_PASSWORD=mlops_password

AIRFLOW_DB=airflow_db
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=airflow_password

AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASSWORD=airflow

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_EXPERIMENT_NAME=diabetes-readmission
MLFLOW_MODEL_NAME=diabetes-readmission-model
MLFLOW_MODEL_ALIAS=champion

SOURCE_DATA_PATH=/opt/airflow/data/diabetic_data.csv
BATCH_SIZE=15000
```

---

# Cómo correr el proyecto

## 1. Clonar repositorio

```bash
git clone <repo-url>
cd MLOps_Proyecto2
```

---

## 2. Crear archivo .env

```bash
cp .env.example .env
```

---

## 3. Agregar dataset

Crear carpeta:

```bash
mkdir -p data
```

Ubicar:

```text
data/diabetic_data.csv
```

---

## 4. Construir imágenes

```bash
docker compose build --no-cache
```

---

## 5. Inicializar Airflow

```bash
docker compose run --rm airflow-init
```

---

## 6. Levantar servicios

```bash
docker compose up -d
```

---

## 7. Verificar servicios

```bash
docker compose ps
```

URLs:

```text
Airflow -> http://localhost:8081
MLflow  -> http://localhost:5050
MinIO   -> http://localhost:9001
```

---

# Cómo ejecutar el pipeline

1. Entrar a Airflow
2. Activar DAG `diabetes_mlops_pipeline`
3. Presionar `Trigger DAG`

---

# Verificaciones útiles

## Conteo RAW

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM raw.diabetes_raw;"
```

## Conteo PROCESSED

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM processed.diabetes_processed;"
```

## Conteo FEATURE STORE

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT COUNT(*) FROM feature_store.model_features;"
```

## Batches procesados

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT * FROM monitoring.processed_batches;"
```

## Runs de entrenamiento

```bash
docker compose exec postgres psql -U mlops_user -d mlops_diabetes -c "SELECT * FROM monitoring.model_training_runs;"
```

---

# Estado actual

Actualmente ya existe:

- Docker Compose funcional
- Airflow funcional
- PostgreSQL funcional
- MinIO funcional
- MLflow funcional
- DAG operativo
- Ingesta incremental
- RAW layer
- PROCESSED layer
- FEATURE STORE
- Pipeline sklearn
- Integración inicial con MLflow

---

# Problemas conocidos actuales

## train_model aún necesita estabilización

Actualmente se debe terminar de validar:

- creación correcta de runs en MLflow
- registro de métricas
- registro de modelos
- alias champion
- almacenamiento de artefactos
- tiempos de entrenamiento

El RandomForest actual puede ser pesado para desarrollo.

Configuración recomendada temporal:

```python
RandomForestClassifier(
    n_estimators=30,
    max_depth=10,
    random_state=42,
    class_weight="balanced",
    n_jobs=1,
)
```

---

# Próximos pasos

## 1. Estabilizar MLflow

Validar:

- runs
- métricas
- artifacts
- registry
- aliases

---

## 2. Crear FastAPI

Endpoints planeados:

```text
/health
/model/info
/predict
/predict/batch
```

---

## 3. Crear Streamlit

Objetivo:

- interfaz para demo
- formulario de predicción
- visualización de resultados
- conexión con FastAPI

---

## 4. Crear Locust

Objetivo:

- pruebas de carga
- medición de throughput
- análisis de latencia
- documentación experimental

---

## 5. Publicar imágenes en Docker Hub

Pendiente:

- API
- Streamlit
- Locust

---

## 6. Preparación para Kubernetes

Pendiente migrar:

- Docker Compose
- networking
- volúmenes
- environment variables
- servicios

A:

- Deployments
- Services
- PVCs
- ConfigMaps
- Secrets

---

# Comandos útiles

## Ver logs Airflow Scheduler

```bash
docker compose logs airflow-scheduler --tail=200
```

## Ver logs MLflow

```bash
docker compose logs mlflow --tail=100
```

## Ver recursos Docker

```bash
docker stats
```

## Apagar servicios

```bash
docker compose down
```

## Reinicio completo

```bash
docker compose down -v
```

---

# Nota técnica importante

El preprocesamiento debe formar parte del pipeline del modelo.

Por eso se utiliza:

```text
ColumnTransformer
        ↓
OneHotEncoder(handle_unknown="ignore")
        ↓
RandomForestClassifier
```

Esto evita que inferencia falle cuando aparezcan categorías nuevas o faltantes.

Ese detalle separa un notebook académico simple de un pipeline de ML robusto y desplegable.
