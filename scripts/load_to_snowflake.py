from __future__ import annotations

import argparse
from pprint import pprint
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings, get_snowflake_settings
from src.pipeline import build_run_context, run_pipeline, run_source_ingestion
from src.snowflake_loader import SnowflakeRawLoader


def run_full_pipeline(
    extracted_at_utc: str | None = None,
    extract_date: str | None = None,
    file_stamp: str | None = None,
    force_upload: bool = False,
    force_reload: bool = False,
) -> None:
    app_settings = get_settings()
    snowflake_settings = get_snowflake_settings()
    run_context = build_run_context(
        extracted_at_utc=extracted_at_utc,
        extract_date=extract_date,
        file_stamp=file_stamp,
    )

    pipeline_result = run_pipeline(
        app_settings,
        run_context=run_context,
        force_upload=force_upload,
    )
    loader = SnowflakeRawLoader(snowflake_settings, app_settings)
    load_result = loader.load_s3_raw_files(
        rawg_s3_key=pipeline_result["rawg_s3_key"],
        steam_s3_key=pipeline_result["steam_s3_key"],
        force_reload=force_reload,
    )

    pprint(
        {
            "pipeline_result": pipeline_result,
            "snowflake_result": {
                "rawg_rows_loaded": load_result.rawg_rows_loaded,
                "steam_rows_loaded": load_result.steam_rows_loaded,
                "rawg_stage_path": load_result.rawg_stage_path,
                "steam_stage_path": load_result.steam_stage_path,
            },
        }
    )


def load_single_source(
    source: str,
    extracted_at_utc: str,
    extract_date: str,
    file_stamp: str,
    force_upload: bool = False,
    force_reload: bool = False,
) -> None:
    app_settings = get_settings()
    snowflake_settings = get_snowflake_settings()
    run_context = build_run_context(
        extracted_at_utc=extracted_at_utc,
        extract_date=extract_date,
        file_stamp=file_stamp,
    )

    source_result = run_source_ingestion(
        settings=app_settings,
        source=source,
        run_context=run_context,
        force_upload=force_upload,
    )
    loader = SnowflakeRawLoader(snowflake_settings, app_settings)
    load_result = loader.load_single_source_file(
        source_name=source,
        s3_key=source_result.s3_key,
        force_reload=force_reload,
    )

    pprint(
        {
            "source_result": source_result,
            "load_result": load_result,
        }
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run game market raw ingestion and Snowflake loading.")
    parser.add_argument("--extracted-at-utc", dest="extracted_at_utc")
    parser.add_argument("--extract-date", dest="extract_date")
    parser.add_argument("--file-stamp", dest="file_stamp")
    parser.add_argument("--force-upload", action="store_true")
    parser.add_argument("--force-reload", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run_full_pipeline(
        extracted_at_utc=args.extracted_at_utc,
        extract_date=args.extract_date,
        file_stamp=args.file_stamp,
        force_upload=args.force_upload,
        force_reload=args.force_reload,
    )


if __name__ == "__main__":
    main()
