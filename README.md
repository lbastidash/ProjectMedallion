# ProjectMedallion —  (Bronze, Silver, Gold + Reporte)

## Descripción del proyecto
Este proyecto implementa un pipeline de datos basado en la arquitectura **Medallion** (Bronze, Silver y Gold).
El objetivo es procesar datos crudos provenientes de archivos CSV, limpiarlos, enriquecerlos y generar productos analíticos listos para consumo, junto con un **reporte automático** que resume ejecución, calidad y métricas de negocio.

---

## Estructura del repositorio
```
ProjectMedallion/
├── data/
│   ├── landing/
│   │   ├── credit_events/
│   │   └── region_reference/
│   ├── bronze/
│   │   ├── credit_events/
│   │   └── region_reference/
│   ├── silver/
│   │   ├── credit_events/
│   │   └── region_reference/
│   └── gold/
│       └── marts/
│           ├── loan_cohort_metrics/
│           └── loan_current_state/
│
├── outputs/
│   ├── figures/
│   └── report.md
│
├── src/
│   ├── simulate_ingest.py
│   ├── bronze_batch.py
│   ├── silver_batch.py
│   ├── gold_batch.py
│   └── report.py
│
├── requirements.txt
└── README.md
```
### 1. Simulación de ingesta (Landing → Bronze)

Simula la llegada de datos en micro-batches a la carpeta landing.

python src/simulate_ingest.py \
  --input data/raw/credit_events.csv \
  --out data/landing/credit_events \
  --batch-size 500 \
  --rate 1 \
  --source-name credit_events

### 2. Capa Bronze

Convierte los CSV de landing a Parquet crudos, particionados por fecha de ingesta.
```
python src/bronze_batch.py
```

Salida esperada:
```
data/bronze/credit_events/ingest_date=YYYY-MM-DD/data.parquet
```
### 3. Capa Silver

Aplica reglas generales de calidad y estandarización:
- Eliminación de duplicados
- Eliminación de filas y columnas vacías
- Conversión heurística de tipos de datos
```
python src/silver_batch.py
```

Salida:
```
data/silver/credit_events/data.parquet
data/silver/region_reference/data.parquet
```
### 4. Capa Gold

Construcción de productos analíticos listos para consumo: 
- Métricas agregadas por cohorte
- Vista consolidada del estado actual de los créditos

```
python src/gold_batch.py
```

Salida:
```
data/gold/marts/loan_cohort_metrics/data.parquet
data/gold/marts/loan_current_state/data.parquet
```
### 5. Generación del reporte

Genera un reporte automático en formato Markdown con métricas y visualizaciones.
```
python src/report.py
```

Salida:
```
outputs/report.md
outputs/figures/calidad_datos.png
outputs/figures/saldo_en_riesgo_por_cohorte.png
```
