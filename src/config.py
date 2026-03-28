from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    rawg_api_key: str
    steam_api_key: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str
    s3_bucket_name: str
    s3_raw_prefix: str = "raw"
    local_data_dir: str = "data"
    rawg_page_size: int = 20
    steam_limit: int = 200


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _require_env_any(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    joined = ", ".join(names)
    raise ValueError(f"Missing required environment variable. Expected one of: {joined}")


def get_settings() -> Settings:
    return Settings(
        rawg_api_key=_require_env("RAWG_API_KEY"),
        steam_api_key=_require_env("STEAM_API_KEY"),
        aws_access_key_id=_require_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require_env("AWS_SECRET_ACCESS_KEY"),
        aws_default_region=_require_env_any("AWS_DEFAULT_REGION", "AWS_REGION"),
        s3_bucket_name=_require_env_any("S3_BUCKET_NAME", "AWS_BUCKET_NAME"),
        s3_raw_prefix=(
            os.getenv("S3_RAW_PREFIX", "").strip()
            or os.getenv("AWS_RAW_PREFIX", "").strip()
            or "raw"
        ),
    )
