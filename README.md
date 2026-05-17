# US Flight 2023 Data Pipeline

An end-to-end data engineering pipeline processing ~6 million US domestic flight records from 2023. Implements a medallion architecture (Bronze → Silver → Gold) on PostgreSQL, orchestrated with Airflow and transformed with dbt, with results visualized in Metabase.

## Pipeline Architecture

![Pipeline Architecture](image/pipeline_architecture.png)

## Tech Stack

- **Python** - ingestion scripting and pipeline logic
- **pandas** - chunk-based CSV reading and preprocessing
- **PostgreSQL** - lightweight data warehouse
- **dbt** - SQL transformations, data modeling, and data tests
- **Apache Airflow** - pipeline orchestration
- **Metabase** - dashboarding and data visualization
- **Docker Compose** - local containerized environment
- **SQLAlchemy / psycopg2** - PostgreSQL connection from Python

## Data Pipeline

1. Airflow checks that the raw CSV file exists.
2. Python ingests `data/raw/US_flights_2023.csv` into `bronze.raw_flights` in PostgreSQL.
3. dbt builds the `silver` layer by cleaning, typing, and deduplicating the raw data.
4. dbt builds the `gold` layer as analytics-ready fact and dimension tables.
5. dbt tests validate key fields and relationships.
6. Metabase connects to PostgreSQL for dashboarding and analysis.

## Data Warehouse Layers

- **Bronze**: raw ingested flight data with light column renaming and metadata
- **Silver**: cleaned and typed staging model, `stg_flights`
- **Gold**: star-schema style analytics models
  - `fact_flights`
  - `dim_airline`
  - `dim_airport`
  - `dim_date`
  - `dim_aircraft`

## Project Structure

```text
US_Flight_2023/
|-- data/raw/                 # Raw flight CSV data
|-- dags/                     # Airflow DAG definition
|-- dbt_project/              # dbt models, tests, and configuration
|-- ingestion/                # Python ingestion script
|-- image/                    # Diagrams and dashboard screenshots
|-- docker-compose.yml        # PostgreSQL, Airflow, and Metabase services
|-- Dockerfile.airflow        # Custom Airflow image
|-- init-db.sql               # PostgreSQL database initialization
|-- requirements.txt          # Python dependencies
`-- README.md
```

## Metabase Dashboard

![Metabase Dashboard](image/metabase_dashboard.png)

## How to Run

Start the local services:

```bash
docker compose up -d
```

Open the tools:

- Airflow: `http://localhost:8080`
- Metabase: `http://localhost:3000`
- PostgreSQL: `localhost:5433`

Run the Airflow DAG:

```text
us_flight_pipeline
```

The DAG runs CSV ingestion, dbt transformations, and dbt tests.

## Notes

Apache Airflow is intentionally used here to simulate a production-grade orchestration layer and demonstrate familiarity with industry-standard tooling.



