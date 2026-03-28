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
- Steam extraction with the official `IStoreService/GetAppList` endpoint
- Local raw JSON snapshots
- Upload of raw JSON files to S3

Next pieces to build:

- Snowflake raw table loading
- Airflow DAG orchestration
- dbt staging and mart models
- tests and documentation for GitHub

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
