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

---

## 2. Imágenes en DockerHub

| Imagen | Repositorio |
|---|---|
| Airflow | `jchapadockerhub/mlops-airflow:latest` |
| API FastAPI | `jchapadockerhub/mlops-api:latest` |
| Streamlit | `jchapadockerhub/mlops-streamlit:latest` |
| Locust | `jchapadockerhub/mlops-locust:latest` |
| MLflow | `jchapadockerhub/mlops-mlflow:latest` |

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

### 4.3. PostgreSQL

```bash
kubectl apply -f k8s/postgres/initdb-configmap.yaml
kubectl apply -f k8s/postgres/pvc.yaml
kubectl apply -f k8s/postgres/statefulset.yaml
kubectl apply -f k8s/postgres/service.yaml
```

Esperar a que PostgreSQL esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=postgres -n mlops --timeout=120s
```

### 4.4. MinIO

```bash
kubectl apply -f k8s/minio/pvc.yaml
kubectl apply -f k8s/minio/statefulset.yaml
kubectl apply -f k8s/minio/service.yaml
```

Esperar a que MinIO esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=minio -n mlops --timeout=120s
```

Ejecutar el job de inicialización del bucket:

```bash
kubectl apply -f k8s/minio/init-job.yaml
kubectl wait --for=condition=complete job/minio-init -n mlops --timeout=120s
```

### 4.5. MLflow

```bash
kubectl apply -f k8s/mlflow/configmap.yaml
kubectl apply -f k8s/mlflow/deployment.yaml
kubectl apply -f k8s/mlflow/service.yaml
```

Esperar a que MLflow esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=mlflow -n mlops --timeout=120s
```

### 4.6. Airflow

```bash
kubectl apply -f k8s/airflow/configmap-dags.yaml
kubectl apply -f k8s/airflow/init-job.yaml
kubectl wait --for=condition=complete job/airflow-init -n mlops --timeout=180s
kubectl apply -f k8s/airflow/deployment.yaml
kubectl apply -f k8s/airflow/service.yaml
```

Esperar a que Airflow esté listo:

```bash
kubectl wait --for=condition=ready pod -l app=airflow-webserver -n mlops --timeout=180s
```

### 4.7. Ejecutar el DAG

1. Abrir Airflow en `http://localhost:8081`
2. Iniciar sesión con `airflow / airflow`
3. Buscar el DAG `diabetes_mlops_pipeline`
4. Activarlo y presionar `Trigger DAG`
5. Esperar a que todas las tareas queden en verde

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

### 4.9. Streamlit

```bash
kubectl apply -f k8s/streamlit/configmap.yaml
kubectl apply -f k8s/streamlit/deployment.yaml
kubectl apply -f k8s/streamlit/service.yaml
```

### 4.10. Locust

```bash
kubectl apply -f k8s/locust/configmap.yaml
kubectl apply -f k8s/locust/deployment.yaml
kubectl apply -f k8s/locust/service.yaml
```

### 4.11. Prometheus

```bash
kubectl apply -f k8s/prometheus/configmap.yaml
kubectl apply -f k8s/prometheus/deployment.yaml
kubectl apply -f k8s/prometheus/service.yaml
```

### 4.12. Grafana

```bash
kubectl apply -f k8s/grafana/configmap.yaml
kubectl apply -f k8s/grafana/pvc.yaml
kubectl apply -f k8s/grafana/deployment.yaml
kubectl apply -f k8s/grafana/service.yaml
```

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

### 7.2. Estado de los servicios

```bash
kubectl get services -n mlops
```

### 7.3. Verificar métricas de la API

```bash
curl http://localhost:8000/metrics
```

### 7.4. Verificar que Prometheus recibe métricas

Abrir `http://localhost:9090/targets` y confirmar que `mlops-api` aparece en estado `UP`.

### 7.5. Verificar modelo champion

```bash
kubectl exec -n mlops $(kubectl get pod -n mlops -l app=postgres -o jsonpath="{.items[0].metadata.name}") -- \
  psql -U mlops_user -d mlops_diabetes -c \
  "SELECT run_id, model_version, f1_score, promoted_to_champion, training_timestamp FROM monitoring.model_training_runs ORDER BY training_timestamp DESC LIMIT 5;"
```

### 7.6. Verificar logs de inferencia

```bash
kubectl exec -n mlops $(kubectl get pod -n mlops -l app=postgres -o jsonpath="{.items[0].metadata.name}") -- \
  psql -U mlops_user -d mlops_diabetes -c \
  "SELECT request_id, prediction, response_time_ms, timestamp FROM monitoring.inference_logs ORDER BY timestamp DESC LIMIT 5;"
```

---

## 8. Prueba de carga con Locust

1. Abrir `http://localhost:8089`
2. Configurar:
   - Number of users: 10
   - Ramp up: 2
   - Host: http://api:8000
3. Iniciar la prueba
4. Observar en Grafana `http://localhost:3000` cómo cambian las métricas en tiempo real

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

### Aplicar todos los manifiestos de una vez

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

- El dataset `diabetic_data.csv` debe estar disponible dentro del pod del scheduler de Airflow. Ver sección de datos del README principal.
- La API no carga el modelo hasta que exista un modelo con alias `champion` en MLflow. Ejecutar el DAG antes de levantar la API.
- Bajo carga concurrente con Locust, la latencia puede aumentar porque cada predicción escribe síncronamente en PostgreSQL.