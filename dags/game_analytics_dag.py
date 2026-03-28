from __future__ import annotations

"""
Starter Airflow DAG for the portfolio project.

This file is a scaffold only. It documents the intended orchestration flow:
1. Extract RAWG data
2. Extract Steam data
3. Upload raw data to S3
4. Load raw data into Snowflake
5. Run dbt models
"""

from datetime import datetime


DAG_ID = "game_market_analytics"
START_DATE = datetime(2026, 3, 28)


def dag_summary() -> dict[str, str]:
    return {
        "dag_id": DAG_ID,
        "description": "Scaffold for orchestrating RAWG + Steam ingestion into S3, Snowflake, and dbt.",
        "start_date": START_DATE.isoformat(),
    }
