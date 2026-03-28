from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt/game_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

default_args = {
    "owner": "angelina340",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="game_market_analytics",
    description="End-to-end pipeline for RAWG and Steam data into S3, Snowflake, and dbt marts.",
    default_args=default_args,
    start_date=datetime(2026, 3, 28),
    schedule="0 9 * * *",
    catchup=False,
    tags=["portfolio", "data-engineering", "snowflake", "dbt"],
) as dag:
    ingest_and_load_raw = BashOperator(
        task_id="ingest_and_load_raw",
        bash_command="python scripts/load_to_snowflake.py",
        cwd=PROJECT_ROOT,
        append_env=True,
        env={
            "PYTHONPATH": PROJECT_ROOT,
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
        cwd=PROJECT_ROOT,
        append_env=True,
        env={
            "PYTHONPATH": PROJECT_ROOT,
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
        cwd=PROJECT_ROOT,
        append_env=True,
        env={
            "PYTHONPATH": PROJECT_ROOT,
        },
    )

    ingest_and_load_raw >> dbt_run >> dbt_test
