# MLOps Proyecto 2 — Despliegue en Kubernetes

Este directorio contiene todos los manifiestos necesarios para desplegar la arquitectura MLOps en un clúster local de Kubernetes (Docker Desktop).

---

## 1. Prerequisitos

- Docker Desktop con Kubernetes habilitado
- `kubectl` disponible en el PATH
- Imágenes publicadas en DockerHub bajo `jchapadockerhub`

Verificar que Kubernetes está activo:

```bash
kubectl cluster-info
kubectl get nodes
```
![alt text](images/k8_ready.png)
---

## 2. Imágenes en DockerHub

| Imagen | Repositorio |
|---|---|
| Airflow | `jchapadockerhub/mlops-airflow:latest` |
| API FastAPI | `jchapadockerhub/mlops-api:latest` |
| Streamlit | `jchapadockerhub/mlops-streamlit:latest` |
| Locust | `jchapadockerhub/mlops-locust:latest` |
| MLflow | `jchapadockerhub/mlops-mlflow:latest` |

![alt text](images/dockerhub-images.png)

---

## 3. Estructura de manifiestos

```text
k8s/
├── namespace.yaml
├── secrets/
│   └── secrets.yaml
├── configmaps/
│   └── configmap.yaml
├── postgres/
│   ├── pvc.yaml
│   ├── statefulset.yaml
│   ├── service.yaml
│   └── initdb-configmap.yaml
├── minio/
│   ├── pvc.yaml
│   ├── statefulset.yaml
│   ├── service.yaml
│   └── init-job.yaml
├── mlflow/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── airflow/
│   ├── configmap-dags.yaml
│   ├── init-job.yaml
│   ├── deployment.yaml
│   └── service.yaml
├── api/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── streamlit/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── locust/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── prometheus/
│   ├── configmap.yaml
│   ├── deployment.yaml
│   └── service.yaml
└── grafana/
    ├── configmap.yaml
    ├── pvc.yaml
    ├── deployment.yaml
    └── service.yaml
```

---

## 4. Despliegue completo paso a paso

### 4.1. Namespace

```bash
kubectl apply -f k8s/namespace.yaml
```

### 4.2. Secrets y ConfigMaps base

```bash
kubectl apply -f k8s/secrets/secrets.yaml
kubectl apply -f k8s/configmaps/configmap.yaml
```

![alt text](images/k8s_n-s-cm.png)

Verificar

```bash
kubectl get secrets -n mlops
kubectl get configmaps -n mlops
```
![alt text](images/k8s_n-s-cm%202.png)

### 4.3. PostgreSQL

```bash
kubectl apply -f k8s/postgres/initdb-configmap.yaml
kubectl apply -f k8s/postgres/pvc.yaml
kubectl apply -f k8s/postgres/statefulset.yaml
kubectl apply -f k8s/postgres/service.yaml
```

![alt text](images/k8s_postgres.png)

Esperar a que PostgreSQL esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=postgres -n mlops --timeout=120s
```

![alt text](images/k8s_postgrescondition.png)

Verificar

```bash
kubectl get pods -n mlops -l app=postgres
```

![alt text](images/k8s_postgresverifying.png)

### 4.4. MinIO

```bash
kubectl apply -f k8s/minio/pvc.yaml
kubectl apply -f k8s/minio/statefulset.yaml
kubectl apply -f k8s/minio/service.yaml
```

![alt text](images/k8s_minio.png)

Esperar a que MinIO esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=minio -n mlops --timeout=120s
```

![alt text](images/k8s_miniocondition.png)

Ejecutar el job de inicialización del bucket:

```bash
kubectl apply -f k8s/minio/init-job.yaml
kubectl wait --for=condition=complete job/minio-init -n mlops --timeout=120s
```

![alt text](images/k8s_minio_initbucket.png)

Subir dataset:

```bash
kubectl apply -f k8s/minio/upload-dataset-job.yaml
kubectl wait --for=condition=complete job/upload-dataset -n mlops --timeout=300s
```

![alt text](images/k8s_minio_datasetuploading.png)

Verificar que el dataset se subió correctamente:

```bash
kubectl logs -n mlops job/upload-dataset
```

![alt text](images/k8s_minio_datasetuploaded.png)

![alt text](images/k8s_minioconsole.png)

### 4.5. MLflow

```bash
kubectl apply -f k8s/mlflow/configmap.yaml
kubectl apply -f k8s/mlflow/deployment.yaml
kubectl apply -f k8s/mlflow/service.yaml
```

![alt text](images/k8s_mlflow.png)

Esperar a que MLflow esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=mlflow -n mlops --timeout=120s
```
![alt text](images/k8s_mlflow_condition.png)

![alt text](images/k8s_mlflow_console.png)

### 4.6. Airflow

```bash
kubectl apply -f k8s/airflow/configmap-dags.yaml
kubectl apply -f k8s/airflow/init-job.yaml
kubectl wait --for=condition=complete job/airflow-init -n mlops --timeout=180s
kubectl apply -f k8s/airflow/deployment.yaml
kubectl apply -f k8s/airflow/service.yaml
```

![alt text](images/k8s_airflow.png)

Esperar a que Airflow esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=airflow-webserver -n mlops --timeout=180s
```

![alt text](images/k8s_airflow_webserver_condition.png)

![alt text](images/k8s_airflow_console.png)

### 4.7. Ejecutar el DAG

1. Abrir Airflow en `http://localhost:8081`
2. Iniciar sesión con `airflow / airflow`
3. Buscar el DAG `diabetes_mlops_pipeline`
4. Activarlo y presionar `Trigger DAG`
5. Esperar a que todas las tareas queden en verde


Evidencias de ejecución exitosa

![alt text](images/k8s_airflow_dagtasks.png)

![alt text](images/k8s_airflow_dagexecution.png)

Modelo en MINIO

![alt text](images/k8s_minio_artifacts.png)

Modelo en MLFLOW

![alt text](images/k8s_mlflow_model.png)

### 4.8. API FastAPI

```bash
kubectl apply -f k8s/api/configmap.yaml
kubectl apply -f k8s/api/deployment.yaml
kubectl apply -f k8s/api/service.yaml
```

Esperar a que la API esté lista:

```bash
kubectl wait --for=condition=ready pod -l app=api -n mlops --timeout=120s
```

![alt text](images/k8s_API.png)

Verificar

```bash
curl http://localhost:8000/health
```

![alt text](images/k8s_API_health.png)

![alt text](images/k8s_API_2.png)

### 4.9. Streamlit

```bash
kubectl apply -f k8s/streamlit/configmap.yaml
kubectl apply -f k8s/streamlit/deployment.yaml
kubectl apply -f k8s/streamlit/service.yaml
```

![alt text](images/k8s_streamlit.png)

![alt text](images/k8s_streamlit_home.png)

![alt text](images/k8s_streamlit_prediction.png)

### 4.10. Locust

```bash
kubectl apply -f k8s/locust/configmap.yaml
kubectl apply -f k8s/locust/deployment.yaml
kubectl apply -f k8s/locust/service.yaml
```

![alt text](images/k8s_locust.png)

![alt text](images/k8s_locust_home.png)

### 4.11. Prometheus

```bash
kubectl apply -f k8s/prometheus/configmap.yaml
kubectl apply -f k8s/prometheus/deployment.yaml
kubectl apply -f k8s/prometheus/service.yaml
kubectl wait --for=condition=ready pod -l app=prometheus -n mlops --timeout=120s
```

![alt text](images/k8s_prometheus.png)

![alt text](images/k8s_prometheus_home.png)

### 4.12. Grafana

```bash
kubectl apply -f k8s/grafana/configmap.yaml
kubectl apply -f k8s/grafana/pvc.yaml
kubectl apply -f k8s/grafana/deployment.yaml
kubectl apply -f k8s/grafana/service.yaml
kubectl wait --for=condition=ready pod -l app=grafana -n mlops --timeout=120s
```

![alt text](images/k8s_grafana.png)

![alt text](images/k8s_grafana_dashboard.png)

---

## 5. URLs locales

| Servicio | URL |
|---|---|
| Airflow | http://localhost:8081 |
| MLflow | http://localhost:5050 |
| MinIO Console | http://localhost:9001 |
| FastAPI Swagger | http://localhost:8000/docs |
| FastAPI Metrics | http://localhost:8000/metrics |
| Streamlit | http://localhost:8501 |
| Locust | http://localhost:8089 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

---

## 6. Credenciales

| Servicio | Usuario | Contraseña |
|---|---|---|
| Airflow | airflow | airflow |
| MinIO | minioadmin | minioadmin |
| Grafana | admin | admin |

---

## 7. Verificaciones

### 7.1. Estado de todos los pods

```bash
kubectl get pods -n mlops
```

Todos los pods deben estar en estado `Running`.

![alt text](images/k8s_pods.png)

### 7.2. Estado de los servicios

```bash
kubectl get services -n mlops
```

![alt text](images/k8s_services.png)

### 7.3. Verificar métricas de la API

```bash
curl http://localhost:8000/metrics
```

![alt text](images/k8s_API_metrics.png)

### 7.4. Verificar que Prometheus recibe métricas

Abrir `http://localhost:9090/targets` y confirmar que `mlops-api` aparece en estado `UP`.

![alt text](images/k8s_prometheus_targets.png)

### 7.5. Verificar modelo champion

```bash
kubectl exec -n mlops $(kubectl get pod -n mlops -l app=postgres -o jsonpath="{.items[0].metadata.name}") -- \
  psql -U mlops_user -d mlops_diabetes -c \
  "SELECT run_id, model_version, f1_score, promoted_to_champion, training_timestamp FROM monitoring.model_training_runs ORDER BY training_timestamp DESC LIMIT 5;"
```

Se realiza verificación posterior a tres ejecuciones del DAG en Airflow.

![alt text](images/k8s_postgres_champion.png)

Tres ejecuciones exitosas en Airflow.

![alt text](images/k8s_airflow_runs.png)

Worflow de última ejecución exitosa.

![alt text](images/k8s_airflow_lastrun.png)


### 7.6. Verificar logs de inferencia

```bash
kubectl exec -n mlops $(kubectl get pod -n mlops -l app=postgres -o jsonpath="{.items[0].metadata.name}") -- \
  psql -U mlops_user -d mlops_diabetes -c \
  "SELECT request_id, prediction, response_time_ms, timestamp FROM monitoring.inference_logs ORDER BY timestamp DESC LIMIT 5;"
```

Se realizan tres predicciones de prueba desde Streamlit.

![alt text](images/k8s_postgres_inferencelog.png)

---

## 8. Prueba de carga con Locust

1. Abrir `http://localhost:8089`
2. Configurar:
   - Number of users: 10
   - Ramp up: 2
   - Host: http://api:8000
3. Iniciar la prueba
4. Observar en Grafana `http://localhost:3000` cómo cambian las métricas en tiempo real

Evidencia de pruebas:

Locust

![alt text](images/k8s_locust_test1.png)

Grafana

![alt text](images/k8s_grafana_test1.png)

---

## 9. Comandos útiles de operación

### Ver logs de un componente

```bash
kubectl logs -n mlops deployment/api --tail=100
kubectl logs -n mlops deployment/mlflow --tail=100
kubectl logs -n mlops deployment/airflow-webserver --tail=100
kubectl logs -n mlops deployment/airflow-scheduler --tail=100
```

### Reiniciar un deployment

```bash
kubectl rollout restart deployment/api -n mlops
```

### Eliminar todo el namespace

```bash
kubectl delete namespace mlops
```

### Aplicar todos los manifiestos

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets/
kubectl apply -f k8s/configmaps/
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/minio/
kubectl apply -f k8s/mlflow/
kubectl apply -f k8s/airflow/
kubectl apply -f k8s/api/
kubectl apply -f k8s/streamlit/
kubectl apply -f k8s/locust/
kubectl apply -f k8s/prometheus/
kubectl apply -f k8s/grafana/
```

---

## 10. Notas importantes

- El dataset `diabetic_data.csv` no requiere carga manual. El Job `upload-dataset` lo descarga automáticamente desde Google Drive y lo sube al bucket `mlflow-artifacts` de MinIO. El DAG incluye la tarea `download_dataset` que descarga el archivo desde MinIO al pod del scheduler antes de iniciar el pipeline. Para que esto funcione correctamente, el Job `upload-dataset` debe completarse exitosamente antes de ejecutar el DAG. Verificar con `kubectl logs -n mlops job/upload-dataset`.
- La API no carga el modelo hasta que exista un modelo con alias `champion` en MLflow. Ejecutar el DAG antes de levantar la API.
- Bajo carga concurrente con Locust, la latencia puede aumentar porque cada predicción escribe síncronamente en PostgreSQL.

## 11. Errores encontrados y soluciones
 
Esta sección documenta los errores detectados durante el despliegue en Kubernetes y cómo fueron resueltos. Sirve como referencia para futuros despliegues o migraciones.
 
---
 
### Error 1 — MinIO: `executable file not found in $PATH`
 
**Componente:** `k8s/minio/statefulset.yaml`
 
**Síntoma:**
```
failed to create shim task: OCI runtime create failed: runc create failed:
unable to start container process: error during container init:
exec: "server": executable file not found in $PATH
```
 
**Causa:**
La imagen `minio/minio:latest` tiene su propio entrypoint (`/usr/bin/docker-entrypoint.sh`). Al usar `command` en el manifiesto de Kubernetes se sobreescribe ese entrypoint y el binario `server` no existe como ejecutable directo en el PATH.
 
**Solución:**
Cambiar `command` por `args` en el StatefulSet. Con `args` se respeta el entrypoint original de la imagen y solo se pasan los argumentos:
 
```yaml
# Incorrecto
command:
  - server
  - /data
  - --console-address
  - ":9001"
 
# Correcto
args:
  - server
  - /data
  - --console-address
  - ":9001"
```
 
---
 
### Error 2 — Airflow Scheduler: `executable file not found in $PATH`
 
**Componente:** `k8s/airflow/deployment.yaml`
 
**Síntoma:**
```
exec: "scheduler": executable file not found in $PATH
```
 
**Causa:**
Mismo problema que MinIO. La imagen de Airflow tiene su propio entrypoint. El comando `scheduler` debe pasarse como argumento, no como comando directo.
 
**Solución:**
Cambiar `command` por `args` en el deployment del scheduler:
 
```yaml
# Incorrecto
command:
  - scheduler
 
# Correcto
args:
  - scheduler
```
 
---
 
### Error 3 — Airflow: variables de entorno no resueltas en `SQL_ALCHEMY_CONN`
 
**Componente:** `k8s/airflow/deployment.yaml` y `k8s/airflow/init-job.yaml`
 
**Síntoma:**
```
password authentication failed for user "$(AIRFLOW_DB_USER)"
```
 
**Causa:**
Kubernetes resuelve las referencias `$(VAR)` en el bloque `env` en orden de definición. Si `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` se define antes que `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD` y `AIRFLOW_DB`, las variables aún no tienen valor en el momento de la interpolación.
 
**Solución:**
Definir las variables dependientes antes de la variable que las referencia:
 
```yaml
# Incorrecto — SQL_ALCHEMY_CONN se define antes que sus dependencias
env:
  - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    value: postgresql+psycopg2://$(AIRFLOW_DB_USER):$(AIRFLOW_DB_PASSWORD)@postgres:5432/$(AIRFLOW_DB)
  - name: AIRFLOW_DB_USER
    valueFrom:
      secretKeyRef:
        name: airflow-secret
        key: airflow-db-user
 
# Correcto — dependencias se definen primero
env:
  - name: AIRFLOW_DB_USER
    valueFrom:
      secretKeyRef:
        name: airflow-secret
        key: airflow-db-user
  - name: AIRFLOW_DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: airflow-secret
        key: airflow-db-password
  - name: AIRFLOW_DB
    valueFrom:
      secretKeyRef:
        name: airflow-secret
        key: airflow-db
  - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    value: postgresql+psycopg2://$(AIRFLOW_DB_USER):$(AIRFLOW_DB_PASSWORD)@postgres:5432/$(AIRFLOW_DB)
```
 
---
 
### Error 4 — Airflow Init: usuario admin no creado
 
**Componente:** `k8s/airflow/init-job.yaml`
 
**Síntoma:**
```
bash: line 2: AIRFLOW_ADMIN_USER: command not found
argument -u/--username: expected one argument
```
 
**Causa:**
El bloque `command` de Kubernetes no interpola variables de entorno con la sintaxis `$(VAR)`. Esta sintaxis solo funciona dentro del bloque `env`. En un script bash dentro de `command`, las variables de entorno deben referenciarse con `$VAR` sin paréntesis.
 
**Solución:**
Usar `$VAR` en lugar de `$(VAR)` dentro del script bash del comando, y asegurarse de que las variables estén definidas en el bloque `env`:
 
```yaml
# Incorrecto
command:
  - bash
  - -c
  - |
    airflow users create \
      --username $(AIRFLOW_ADMIN_USER) \
      --password $(AIRFLOW_ADMIN_PASSWORD)
 
# Correcto
command:
  - bash
  - -c
  - |
    airflow users create \
      --username $AIRFLOW_ADMIN_USER \
      --password $AIRFLOW_ADMIN_PASSWORD
```
 
---
 
### Error 5 — Airflow Scheduler: loop recursivo en directorio de DAGs
 
**Componente:** `k8s/airflow/configmap-dags.yaml`
 
**Síntoma:**
```
RuntimeError: Detected recursive loop when walking DAG directory /opt/airflow/dags:
/opt/airflow/dags/..2026_05_17_17_17_59.3728245712 has appeared more than once.
```
 
**Causa:**
Cuando Kubernetes monta un ConfigMap como volumen, crea symlinks internos con nombres que empiezan con `..` para gestionar las actualizaciones atómicas. Airflow al escanear el directorio de DAGs interpreta estos symlinks como un loop recursivo y falla.
 
**Solución:**
Agregar un archivo `.airflowignore` al ConfigMap que le indique a Airflow que ignore archivos que empiecen con punto:
 
```yaml
data:
  .airflowignore: |
    \..*
  diabetes_mlops_pipeline.py: |
    ... (código del DAG)
```
 
---
 
### Error 6 — Grafana: dashboard no se actualiza al cambiar el ConfigMap
 
**Componente:** `k8s/grafana/configmap.yaml`
 
**Síntoma:**
El dashboard en Grafana no refleja los cambios aplicados al ConfigMap aunque se reinicie el pod.
 
**Causa:**
Grafana persiste los dashboards provisionados en su base de datos interna (SQLite dentro del PVC). Una vez que el dashboard se carga por primera vez, Grafana lo guarda en el PVC y no vuelve a leerlo del ConfigMap aunque este cambie.
 
**Solución:**
Eliminar el PVC de Grafana para forzar que arranque desde cero y lea el ConfigMap actualizado:
 
```bash
kubectl delete deployment grafana -n mlops
kubectl delete pvc grafana-pvc -n mlops
kubectl apply -f k8s/grafana/pvc.yaml
kubectl apply -f k8s/grafana/deployment.yaml
kubectl wait --for=condition=ready pod -l app=grafana -n mlops --timeout=120s
```
 
---
 
### Error 7 — Grafana: JSON del dashboard inválido por comillas sin escapar
 
**Componente:** `k8s/grafana/configmap.yaml`
 
**Síntoma:**
Algunos paneles del dashboard muestran `No data` aunque la métrica existe en Prometheus.
 
**Causa:**
El JSON del dashboard contenía comillas dobles sin escapar dentro de una cadena JSON. En la query de Prometheus para la tasa de error, las comillas alrededor del regex `"5.."` rompían el JSON:
 
```json
"expr": "sum(rate(http_requests_total{status_code=~"5.."}[1m]))"
```
 
Grafana no podía parsear el panel y lo mostraba sin datos.
 
**Solución:**
Escapar las comillas internas con `\"`:
 
```json
"expr": "sum(rate(http_requests_total{status_code=~\"5..\"}[1m]))"
```