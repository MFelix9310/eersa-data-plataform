# EERSA Data Platform

Pipeline de datos para Empresa Eléctrica Riobamba S.A. — arquitectura Medallion (Bronze/Silver/Gold) con datos reales de generación diaria 2021.

## Stack

- **Ingesta**: Python + pandas + openpyxl
- **Storage**: DuckDB + Parquet (particionado)
- **Transformaciones**: dbt-core + dbt-duckdb
- **Calidad**: Great Expectations
- **Migración futura**: Microsoft Fabric

## Estructura

```
eersa-data-plataform/
├── src/
│   ├── extractors/       # Ingesta de fuentes (xlsx → Bronze Parquet)
│   ├── transformations/  # Lógica Silver/Gold (pandas)
│   ├── quality/          # Validaciones Great Expectations
│   └── utils/            # Helpers compartidos
├── dbt/                  # Proyecto dbt (Silver → Gold)
├── dags/                 # Orquestación (futuro Airflow/Fabric)
├── data/
│   ├── raw/              # Archivos fuente (.xlsx)
│   ├── bronze/           # Parquet crudo con metadatos
│   ├── silver/           # Datos limpios y normalizados
│   └── gold/             # Métricas y agregados de negocio
├── notebooks/            # Exploración y análisis ad-hoc
├── tests/                # Tests unitarios e integración
├── configs/              # Configuración de fuentes y parámetros
└── docs/                 # Documentación técnica
```

## Setup

```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows
pip install -r requirements.txt
```

## Uso

```bash
make setup          # Crear venv e instalar deps
make ingest-bronze  # Extraer xlsx → Parquet Bronze
make transform-silver
make transform-gold
make test
make clean          # Limpiar data procesada
```

## Fuentes

- 12 archivos Excel mensuales de generación EERSA 2021
- Hoja "RESUMEN" con datos diarios por planta (ALAO, RIO BLANCO, C.NIZAG, S.N.I., etc.)
- Métricas: KW, E.Bruta kWh, C.Int kWh, E.Neta kWh por planta
