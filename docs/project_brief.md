# Project Brief

## Title

Game Market Analytics Pipeline

## Goal

Build a modern data engineering project that shows how raw API data can be ingested, stored, modeled, and orchestrated using industry-standard tools.

## Business question

How can we combine public game metadata from RAWG and Steam to analyze trends in releases, ratings, and platform availability?

## Why this is a good resume project

- Uses a realistic multi-tool stack: S3, Snowflake, Airflow, dbt
- Shows both ingestion engineering and analytics engineering
- Produces a story recruiters and hiring managers understand quickly
- Can be shared publicly on GitHub without needing private company data

## Source systems

- RAWG API for game metadata, ratings, genres, and release information
- Steam API for app catalog and store-side game inventory

## Proposed data layers

### Raw layer

Landing JSON payloads in S3 with immutable date-partitioned storage.

### Staging layer

Flatten and standardize raw API data into warehouse-friendly tables.

### Mart layer

Curated tables for dashboarding and analysis, for example:

- `mart_games`
- `mart_platform_coverage`
- `mart_release_trends`
- `mart_top_rated_games`

## Suggested KPIs

- Count of games by release month
- Average rating by genre
- Top games by rating and review count
- Number of supported platforms per game
- Steam app growth over time

## Target stack

- Python for ingestion
- AWS S3 for raw zone
- Snowflake for warehousing
- Airflow for orchestration
- dbt for transformation and tests

## Deliverables for GitHub

- Clean README
- Architecture diagram
- Reproducible setup steps
- Sample SQL or dbt models
- Airflow DAG
- Screenshots of Snowflake tables or dashboard results
