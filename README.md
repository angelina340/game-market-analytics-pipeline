# Game Market Analytics Pipeline

End-to-end batch data engineering project that ingests video game data from RAWG and Steam, stores raw JSON in AWS S3, loads it into Snowflake, transforms it with dbt, and orchestrates the full workflow with Airflow.

## Project Overview

This project answers a simple analytics question:

`Which games are highly rated, actively featured, and broadly available across platforms, and how does that data move through a modern analytics stack?`

It is designed as a portfolio project that demonstrates:

- API ingestion with Python
- cloud raw storage in S3
- warehouse loading into Snowflake
- SQL-based transformation with dbt
- orchestration with Airflow
- automated testing of analytics models

## Architecture

```text
RAWG API ---------\
                   > Python ingestion -> S3 raw zone -> Snowflake raw tables -> dbt staging -> dbt marts
Steam API --------/                                      |
                                                         -> Airflow orchestrates ingestion, load, run, and test
```

## Tech Stack

- Python
- AWS S3
- Snowflake
- dbt
- Apache Airflow
- Docker Compose
- PostgreSQL for Airflow metadata

## End-to-End Workflow

1. Python calls the RAWG API and Steam API.
2. Each response is wrapped in a raw ingestion envelope with extraction metadata.
3. Raw JSON files are written locally for debugging and uploaded to S3.
4. Snowflake loads the latest S3 raw files into `VARIANT` raw tables.
5. dbt builds staging models that flatten and standardize the JSON payloads.
6. dbt builds marts for analytics-ready game-level reporting.
7. Airflow orchestrates the pipeline: raw ingestion, Snowflake load, `dbt run`, and `dbt test`.

## Data Layers

### Raw

Raw API payloads are stored in S3 and loaded into Snowflake as JSON `VARIANT` columns.

Snowflake raw tables:

- `RAWG_GAMES_RAW`
- `STEAM_GAMES_RAW`

### Staging

The staging layer normalizes the raw JSON into relational models:

- `stg_rawg_games`
  One row per RAWG game with core metadata such as rating, reviews, metacritic, release date, and extract timestamp.
- `stg_rawg_game_platforms`
  One row per RAWG game-platform combination.
- `stg_rawg_game_genres`
  One row per RAWG game-genre combination.
- `stg_steam_featured_games`
  One row per Steam featured/store item with pricing, discount, availability, and category attributes.

These models are built from the latest raw snapshot to avoid duplicate game rows across historical ingestions.

### Marts

The mart layer provides analytics-ready outputs:

- `mart_games`
  Curated game-level model combining RAWG metadata with matching Steam featured data when names align.
- `mart_platform_coverage`
  Aggregated platform-level coverage across RAWG games.
- `mart_top_rated_games`
  Top-rated games ranked by RAWG ratings and enriched with Steam pricing and discount fields when available.

## Airflow DAG

The Airflow DAG is defined in [game_analytics_dag.py](E:/Codex/DE_project/dags/game_analytics_dag.py) and runs:

1. `ingest_and_load_raw`
   Runs Python ingestion and loads the resulting S3 files into Snowflake.
2. `dbt_run`
   Builds dbt staging and mart models in Snowflake.
3. `dbt_test`
   Runs data quality checks on sources and marts.

Local Airflow support files:

- [docker-compose.yml](E:/Codex/DE_project/airflow/docker-compose.yml)
- [Dockerfile](E:/Codex/DE_project/airflow/Dockerfile)
- [profiles.yml](E:/Codex/DE_project/airflow/dbt_profiles/profiles.yml)
- [airflow README](E:/Codex/DE_project/airflow/README.md)

## Snowflake Authentication

The project supports two Snowflake auth modes:

- local MFA-based testing
- key-pair authentication for automation and Airflow

For unattended orchestration, the project uses Snowflake key-pair auth with:

- `SNOWFLAKE_PRIVATE_KEY_PATH`
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`

Helper files:

- [generate_snowflake_keypair.ps1](E:/Codex/DE_project/scripts/generate_snowflake_keypair.ps1)
- [003_set_rsa_public_key.sql](E:/Codex/DE_project/snowflake/ddl/003_set_rsa_public_key.sql)

## Important Project Files

Core pipeline:

- [run_pipeline.py](E:/Codex/DE_project/scripts/run_pipeline.py)
- [load_to_snowflake.py](E:/Codex/DE_project/scripts/load_to_snowflake.py)
- [pipeline.py](E:/Codex/DE_project/src/pipeline.py)
- [snowflake_loader.py](E:/Codex/DE_project/src/snowflake_loader.py)

Warehouse SQL:

- [001_create_schemas.sql](E:/Codex/DE_project/snowflake/ddl/001_create_schemas.sql)
- [002_create_raw_tables.sql](E:/Codex/DE_project/snowflake/ddl/002_create_raw_tables.sql)

dbt:

- [sources.yml](E:/Codex/DE_project/dbt/game_analytics/models/sources.yml)
- [stg_rawg_games.sql](E:/Codex/DE_project/dbt/game_analytics/models/staging/stg_rawg_games.sql)
- [stg_steam_featured_games.sql](E:/Codex/DE_project/dbt/game_analytics/models/staging/stg_steam_featured_games.sql)
- [mart_games.sql](E:/Codex/DE_project/dbt/game_analytics/models/marts/mart_games.sql)
- [marts_schema.yml](E:/Codex/DE_project/dbt/game_analytics/models/marts/marts_schema.yml)

## Local Setup

### 1. Python setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Environment variables

Create a local `.env` file using [`.env.example`](E:/Codex/DE_project/.env.example).

Main values:

```env
RAWG_API_KEY=your_rawg_key
STEAM_API_KEY=your_steam_key
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=us-east-2
AWS_BUCKET_NAME=your_bucket_name
AWS_RAW_PREFIX=raw
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=GAME_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_PRIVATE_KEY_PATH=.keys/snowflake_rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_private_key_passphrase
```

### 3. Run ingestion only

```powershell
python scripts\run_pipeline.py
```

### 4. Run ingestion plus Snowflake raw load

```powershell
python scripts\load_to_snowflake.py
```

### 5. Run Airflow locally

```powershell
docker compose -f airflow/docker-compose.yml up --build
```

Then open [http://localhost:8080](http://localhost:8080)

Default local login:

- username: `admin`
- password: `admin`

## Example S3 Layout

```text
raw/source=rawg/extract_date=2026-03-28/rawg_games_20260328T171715Z.json
raw/source=steam/extract_date=2026-03-28/steam_apps_20260328T171715Z.json
```

## Resume Summary

- Built an end-to-end ELT pipeline using Python, AWS S3, Snowflake, dbt, and Airflow
- Automated ingestion of external game-market APIs into a cloud data lake and warehouse
- Designed raw, staging, and mart layers for platform coverage, pricing, and rating analytics
- Implemented automated orchestration and data tests for reproducible batch pipelines

## Notes

- Secrets stay local in `.env` and are excluded from git.
- The repository is structured to be portfolio-friendly and reproducible.
- If you are new to GitHub, see [github_guide.md](E:/Codex/DE_project/docs/github_guide.md).
