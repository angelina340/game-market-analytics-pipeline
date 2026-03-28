# GitHub Guide

## Goal

Publish this project to GitHub so recruiters and interviewers can see your work.

## Before you upload

- Make sure `.env` is not committed
- Make sure no AWS or API secrets appear in any file
- Rotate any AWS credentials that were shared outside your local machine

## First GitHub workflow

1. Create a GitHub account if you do not already have one.
2. Click `New repository` on GitHub.
3. Name it something clear like `game-market-analytics-pipeline`.
4. Keep it public if you want to use it in your resume.
5. Do not upload your `.env` file.

## Basic git commands

Run these in `E:\Codex\DE_project`:

```powershell
git init
git add .
git commit -m "Initial commit: game market analytics pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/game-market-analytics-pipeline.git
git push -u origin main
```

## What your GitHub repo should show

- Clear project title
- Short business problem statement
- Architecture using S3, Snowflake, Airflow, and dbt
- Setup instructions
- Project screenshots later

## Good commit examples

- `add rawg and steam ingestion pipeline`
- `add snowflake schema and staging ddl`
- `add dbt staging models for raw game data`
- `add airflow dag for daily orchestration`

## What recruiters will care about

- Can they understand the project in under one minute
- Does the stack match real data engineering tools
- Is the repo organized and professional
- Can they see evidence of orchestration, warehousing, and transformation
