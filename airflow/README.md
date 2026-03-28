# Local Airflow Setup

This folder contains a local Docker-based Airflow setup for orchestrating the project.

## Services

- `postgres`: metadata database for Airflow
- `airflow-init`: initializes the Airflow metadata database and creates a local admin user
- `airflow-webserver`: serves the Airflow UI on port `8080`
- `airflow-scheduler`: schedules and runs the DAG

## DAG flow

1. Run Python ingestion and load raw files to Snowflake
2. Run `dbt run`
3. Run `dbt test`

## Start Airflow

From the repo root:

```powershell
docker compose -f airflow/docker-compose.yml up --build
```

Then open `http://localhost:8080`

Default local login:

- username: `admin`
- password: `admin`

## Notes

- The containers read credentials from the root `.env`
- The dbt profile is mounted from `airflow/dbt_profiles/profiles.yml`
- If Snowflake MFA is still required for each run, scheduled execution will not be fully unattended until authentication is changed to an automation-friendly method
- This setup uses Postgres for Airflow metadata because `LocalExecutor` is not a good fit with SQLite
