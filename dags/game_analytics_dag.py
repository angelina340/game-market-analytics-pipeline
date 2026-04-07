from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context


PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.config import get_settings, get_snowflake_settings
from src.pipeline import build_run_context, run_source_ingestion
from src.snowflake_loader import SnowflakeRawLoader

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


@task(task_id="build_run_context")
def build_run_context_task() -> dict[str, str]:
    context = get_current_context()
    dag_run_conf = (context["dag_run"].conf if context.get("dag_run") else {}) or {}
    logical_date = context["logical_date"].in_timezone("UTC")
    run_context = build_run_context(
        extracted_at_utc=dag_run_conf.get("extracted_at_utc") or logical_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        extract_date=dag_run_conf.get("extract_date") or logical_date.strftime("%Y-%m-%d"),
        file_stamp=dag_run_conf.get("file_stamp") or logical_date.strftime("%Y%m%dT%H%M%SZ"),
    )
    return {
        "extracted_at_utc": run_context.extracted_at_utc,
        "extract_date": run_context.extract_date,
        "file_stamp": run_context.file_stamp,
        "force_upload": bool(dag_run_conf.get("force_upload", False)),
        "force_reload": bool(dag_run_conf.get("force_reload", False)),
    }


@task(retries=2, retry_delay=timedelta(minutes=2))
def ingest_source(source: str, run_context_dict: dict[str, str]) -> dict[str, str]:
    settings = get_settings()
    run_context = build_run_context(
        extracted_at_utc=run_context_dict["extracted_at_utc"],
        extract_date=run_context_dict["extract_date"],
        file_stamp=run_context_dict["file_stamp"],
    )
    result = run_source_ingestion(
        settings=settings,
        source=source,
        run_context=run_context,
        force_upload=bool(run_context_dict.get("force_upload", False)),
    )
    return {
        "source": result.source,
        "local_path": result.local_path,
        "s3_key": result.s3_key,
        "s3_uri": result.s3_uri,
        "upload_status": result.upload_status,
        "extracted_at_utc": result.extracted_at_utc,
        "extract_date": result.extract_date,
        "force_reload": bool(run_context_dict.get("force_reload", False)),
    }


@task(retries=2, retry_delay=timedelta(minutes=2))
def load_source_to_snowflake(extraction_result: dict[str, str]) -> dict[str, str | int]:
    app_settings = get_settings()
    snowflake_settings = get_snowflake_settings()
    loader = SnowflakeRawLoader(snowflake_settings, app_settings)
    load_result = loader.load_single_source_file(
        source_name=extraction_result["source"],
        s3_key=extraction_result["s3_key"],
        force_reload=bool(extraction_result.get("force_reload", False)),
    )
    return {
        "source": extraction_result["source"],
        "s3_key": extraction_result["s3_key"],
        "rows_loaded": load_result.rawg_rows_loaded or load_result.steam_rows_loaded,
    }


@task_group(group_id="rawg_pipeline")
def rawg_pipeline(run_context_dict: dict[str, str]):
    rawg_extract = ingest_source.override(task_id="extract_rawg")(
        source="rawg",
        run_context_dict=run_context_dict,
    )
    rawg_load = load_source_to_snowflake.override(task_id="load_rawg_to_snowflake")(
        extraction_result=rawg_extract,
    )
    rawg_extract >> rawg_load
    return rawg_load


@task_group(group_id="steam_pipeline")
def steam_pipeline(run_context_dict: dict[str, str]):
    steam_extract = ingest_source.override(task_id="extract_steam")(
        source="steam",
        run_context_dict=run_context_dict,
    )
    steam_load = load_source_to_snowflake.override(task_id="load_steam_to_snowflake")(
        extraction_result=steam_extract,
    )
    steam_extract >> steam_load
    return steam_load


with DAG(
    dag_id="game_market_analytics",
    description="End-to-end pipeline for RAWG and Steam data into S3, Snowflake, and dbt marts.",
    default_args=default_args,
    start_date=datetime(2026, 3, 28),
    schedule="0 9 * * *",
    catchup=False,
    tags=["portfolio", "data-engineering", "snowflake", "dbt"],
    params={
        "extract_date": Param(None, type=["null", "string"]),
        "extracted_at_utc": Param(None, type=["null", "string"]),
        "file_stamp": Param(None, type=["null", "string"]),
        "force_upload": Param(False, type="boolean"),
        "force_reload": Param(False, type="boolean"),
    },
) as dag:
    run_context = build_run_context_task()
    rawg_load = rawg_pipeline(run_context)
    steam_load = steam_pipeline(run_context)

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

    [rawg_load, steam_load] >> dbt_run >> dbt_test
