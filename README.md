# Game Market Analytics Pipeline

This is a portfolio-ready data engineering project that ingests video game data from the RAWG API and Steam API, lands raw data in AWS S3, loads it into Snowflake, transforms it with dbt, and orchestrates the workflow with Airflow.

## Resume-friendly project story

Built an end-to-end batch data pipeline for game market analytics using Python, AWS S3, Snowflake, Airflow, and dbt. The pipeline ingests external API data, stores raw JSON in a cloud data lake, models analytics-ready tables in Snowflake, and supports reporting on game popularity, ratings, release trends, and platform coverage.

## Project idea

The core idea is:

`Which games are trending, highly rated, and broadly available across platforms, and how does the catalog change over time?`

This gives you a strong business-style analytics use case and lets you show common data engineering patterns:

- API ingestion
- raw to staged to mart layers
- cloud object storage
- warehouse loading
- transformation and testing
- workflow orchestration

## Architecture

```text
RAWG API ----\
              > Python ingestion --> S3 raw zone --> Snowflake raw tables --> dbt staging/marts --> analytics
Steam API ---/                                 \
                                                -> Airflow orchestration and scheduling
```

## Suggested analytics outputs

- Top rated games by platform and genre
- New releases by month
- Steam catalog growth over time
- Games with strong ratings and high review counts
- Cross-platform coverage analysis

## Repo structure

```text
docs/
  github_guide.md
  project_brief.md
dags/
  game_analytics_dag.py
dbt/
  game_analytics/
    dbt_project.yml
    models/
      staging/
      marts/
scripts/
  run_pipeline.py
snowflake/
  ddl/
    001_create_schemas.sql
src/
  clients/
    rawg_client.py
    steam_client.py
  config.py
  pipeline.py
  s3_writer.py
```

## Current pipeline status

Already implemented:

- RAWG extraction with API key auth
- Steam extraction with the public Steam Web API app list endpoint
- Local raw JSON snapshots
- Upload of raw JSON files to S3

Next pieces to build:

- Airflow DAG orchestration
- tests and documentation for GitHub

## dbt models

The project now includes dbt source, staging, and mart models for Snowflake:

- RAWG staging models for games, platforms, and genres
- Steam staging model for featured store items
- marts for unified game analytics, platform coverage, and top-rated games

Key files:

- [sources.yml](E:/Codex/DE_project/dbt/game_analytics/models/sources.yml)
- [stg_rawg_games.sql](E:/Codex/DE_project/dbt/game_analytics/models/staging/stg_rawg_games.sql)
- [stg_steam_featured_games.sql](E:/Codex/DE_project/dbt/game_analytics/models/staging/stg_steam_featured_games.sql)
- [mart_games.sql](E:/Codex/DE_project/dbt/game_analytics/models/marts/mart_games.sql)
- [profiles.yml.example](E:/Codex/DE_project/dbt/game_analytics/profiles.yml.example)

To run dbt later:

1. Install dbt for Snowflake in your virtual environment.
2. Copy `dbt/game_analytics/profiles.yml.example` to your dbt profiles location.
3. Run `dbt debug`, `dbt run`, and `dbt test` from `dbt/game_analytics`.

With the current config, staging models will land in a Snowflake schema like `RAW_STAGING` and marts in `RAW_MART`.

## Airflow orchestration

The project now includes a real Airflow orchestration layer:

- [game_analytics_dag.py](E:/Codex/DE_project/dags/game_analytics_dag.py)
- [airflow/docker-compose.yml](E:/Codex/DE_project/airflow/docker-compose.yml)
- [airflow/Dockerfile](E:/Codex/DE_project/airflow/Dockerfile)
- [airflow/dbt_profiles/profiles.yml](E:/Codex/DE_project/airflow/dbt_profiles/profiles.yml)
- [airflow/README.md](E:/Codex/DE_project/airflow/README.md)

The DAG runs:

1. Python ingestion plus Snowflake raw load
2. `dbt run`
3. `dbt test`

To start local Airflow:

```powershell
docker compose -f airflow/docker-compose.yml up --build
```

Then open `http://localhost:8080` and trigger the `game_market_analytics` DAG.

## Snowflake automation auth

For local testing, the project supports Snowflake MFA. For Airflow and unattended runs, key-pair authentication is the better option.

The repo now supports Snowflake key-pair auth through:

- `SNOWFLAKE_PRIVATE_KEY_PATH`
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`

Helper files:

- [generate_snowflake_keypair.ps1](E:/Codex/DE_project/scripts/generate_snowflake_keypair.ps1)
- [003_set_rsa_public_key.sql](E:/Codex/DE_project/snowflake/ddl/003_set_rsa_public_key.sql)

After generating a key pair and assigning the public key to your Snowflake user, Airflow can connect without a fresh MFA code on every run.

## Environment variables

Create a local `.env` file with:

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
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=GAME_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=your_role
```

The app also accepts `AWS_DEFAULT_REGION`, `S3_BUCKET_NAME`, and `S3_RAW_PREFIX`.

## Snowflake raw load

The project now includes a Snowflake raw-load layer that:

- creates a Snowflake JSON file format
- creates an external S3 stage for the raw bucket prefix
- creates raw `VARIANT` tables for RAWG and Steam
- copies the latest S3 raw files into Snowflake

Run it with:

```powershell
python scripts\load_to_snowflake.py
```

Supporting files:

- [src/snowflake_loader.py](E:/Codex/DE_project/src/snowflake_loader.py)
- [scripts/load_to_snowflake.py](E:/Codex/DE_project/scripts/load_to_snowflake.py)
- [001_create_schemas.sql](E:/Codex/DE_project/snowflake/ddl/001_create_schemas.sql)
- [002_create_raw_tables.sql](E:/Codex/DE_project/snowflake/ddl/002_create_raw_tables.sql)

## Local setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts\run_pipeline.py
```

## Example S3 layout

```text
raw/source=rawg/extract_date=2026-03-28/rawg_games_20260328T143000Z.json
raw/source=steam/extract_date=2026-03-28/steam_apps_20260328T143000Z.json
```

## What to put on your resume

- Built an end-to-end ELT pipeline using Python, AWS S3, Snowflake, Airflow, and dbt
- Automated ingestion of external gaming APIs into a cloud data lake and warehouse
- Designed layered data models for analytics on releases, ratings, and platform coverage
- Implemented reproducible transformations and orchestration for scheduled batch processing

## GitHub

If you have never used GitHub before, start with [github_guide.md](E:/Codex/DE_project/docs/github_guide.md).

## Notes

- Keep secrets only in `.env`, never commit them.
- Rotate any AWS keys that were exposed outside your local machine.
- This repo is designed to become a public portfolio project once secrets are removed and setup steps are documented.
