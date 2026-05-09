# Proyecto MLOps - Predicción Clínica Diabetes

Este proyecto implementa una arquitectura MLOps para entrenar, registrar, desplegar y monitorear modelos de Machine Learning usando datos clínicos de pacientes con diabetes.

El sistema cubre el flujo completo desde la ingesta incremental de datos hasta la inferencia productiva mediante API, interfaz gráfica, pruebas de carga y despliegue posterior en Kubernetes.

## Objetivo

Construir una solución MLOps containerizada que permita:

- cargar datos por lotes;
- almacenar datos crudos y procesados;
- entrenar modelos de Machine Learning;
- registrar experimentos y modelos en MLflow;
- seleccionar un modelo productivo;
- servir inferencias mediante FastAPI;
- consumir la API desde Streamlit;
- ejecutar pruebas de carga con Locust;
- exponer métricas para Prometheus y Grafana.

## Arquitectura general

La arquitectura se divide en dos fases principales:

### Fase de entrenamiento

Airflow orquesta el flujo de datos y entrenamiento:

1. validación del archivo fuente;
2. carga incremental del dataset;
3. almacenamiento en capa raw;
4. validaciones básicas de calidad;
5. limpieza y transformación;
6. almacenamiento en capa clean;
7. partición train/validation/test;
8. entrenamiento de modelos;
9. registro de experimentos en MLflow;
10. promoción automática del mejor modelo.

### Fase de inferencia

La API de FastAPI consume dinámicamente el modelo productivo registrado en MLflow.

La aplicación Streamlit actúa como interfaz gráfica para enviar datos a la API y visualizar la predicción.

Cada inferencia queda registrada en la base de datos para auditoría, monitoreo y posible reentrenamiento futuro.

## Componentes

| Componente | Responsabilidad |
|---|---|
| Airflow | Orquestación del pipeline MLOps |
| PostgreSQL | Almacenamiento raw, clean, inference logs y backend de MLflow |
| MLflow | Tracking de experimentos y registro de modelos |
| MinIO | Almacenamiento de artefactos de MLflow |
| FastAPI | Servicio de inferencia |
| Streamlit | Interfaz gráfica |
| Locust | Pruebas de carga |
| Prometheus | Recolección de métricas |
| Grafana | Visualización de métricas |

## Estructura del repositorio

```text
.
├── airflow/
│   ├── dags/
│   └── include/
├── api/
│   └── app/
├── streamlit_app/
├── locust/
├── training/
├── database/
│   └── init/
├── docker/
├── configs/
├── docs/
├── notebooks/
├── tests/
├── .env.example
├── docker-compose.yml
└── README.md
```

## Alcance actual del equipo

Este repositorio contiene la base desarrollada en Docker para:

* Airflow;
* PostgreSQL;
* MLflow;
* MinIO;
* FastAPI;
* Streamlit;
* Locust.

La migración final a Kubernetes, junto con Prometheus y Grafana, será integrada posteriormente sobre esta base containerizada.