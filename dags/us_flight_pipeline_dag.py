from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = Path("/opt/airflow")
CSV_PATH = AIRFLOW_HOME / "data/raw/US_flights_2023.csv"
DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt_project"


def check_source_csv_exists():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Source CSV not found: {CSV_PATH}")


default_args = {
    "owner": "minh",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="us_flight_pipeline",
    description="Ingest US flight CSV data into PostgreSQL and transform it with dbt.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["us-flight", "postgres", "dbt"],
) as dag:

    check_source_csv = PythonOperator(
        task_id="check_source_csv_exists",
        python_callable=check_source_csv_exists,
    )

    ingest_bronze = BashOperator(
        task_id="ingest_csv_to_bronze",
        bash_command="python /opt/airflow/ingestion/ingest_csv_to_bronze.py",
        cwd="/opt/airflow",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir /opt/airflow/dbt_project",
        cwd=str(DBT_PROJECT_DIR),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir /opt/airflow/dbt_project",
        cwd=str(DBT_PROJECT_DIR),
    )

    check_source_csv >> ingest_bronze >> dbt_run >> dbt_test
