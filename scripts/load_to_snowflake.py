from __future__ import annotations

from pprint import pprint
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings, get_snowflake_settings
from src.pipeline import run_pipeline
from src.snowflake_loader import SnowflakeRawLoader


def main() -> None:
    app_settings = get_settings()
    snowflake_settings = get_snowflake_settings()

    pipeline_result = run_pipeline(app_settings)
    loader = SnowflakeRawLoader(snowflake_settings, app_settings)
    load_result = loader.load_s3_raw_files(
        rawg_s3_key=pipeline_result["rawg_s3_key"],
        steam_s3_key=pipeline_result["steam_s3_key"],
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


if __name__ == "__main__":
    main()
