from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.clients.rawg_client import RawgClient
from src.clients.steam_client import SteamClient
from src.config import Settings
from src.s3_writer import S3RawWriter, write_local_json


def _build_s3_key(prefix: str, source: str, extract_date: str, filename: str) -> str:
    return f"{prefix}/source={source}/extract_date={extract_date}/{filename}"


def _build_envelope(source: str, extracted_at: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": source,
        "extracted_at_utc": extracted_at,
        "payload": payload,
    }


def run_pipeline(settings: Settings) -> dict[str, str]:
    timestamp = datetime.now(timezone.utc)
    extracted_at = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    extract_date = timestamp.strftime("%Y-%m-%d")
    file_stamp = timestamp.strftime("%Y%m%dT%H%M%SZ")

    rawg_client = RawgClient(settings.rawg_api_key)
    steam_client = SteamClient(settings.steam_api_key)
    s3_writer = S3RawWriter(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
        bucket_name=settings.s3_bucket_name,
    )

    rawg_payload = rawg_client.fetch_games(page_size=settings.rawg_page_size)
    steam_payload = steam_client.fetch_apps(limit=settings.steam_limit)

    rawg_envelope = _build_envelope("rawg", extracted_at, rawg_payload)
    steam_envelope = _build_envelope("steam", extracted_at, steam_payload)

    rawg_filename = f"rawg_games_{file_stamp}.json"
    steam_filename = f"steam_apps_{file_stamp}.json"

    rawg_local_path = Path(settings.local_data_dir) / "rawg" / extract_date / rawg_filename
    steam_local_path = Path(settings.local_data_dir) / "steam" / extract_date / steam_filename

    write_local_json(rawg_envelope, rawg_local_path)
    write_local_json(steam_envelope, steam_local_path)

    rawg_s3_key = _build_s3_key(settings.s3_raw_prefix, "rawg", extract_date, rawg_filename)
    steam_s3_key = _build_s3_key(settings.s3_raw_prefix, "steam", extract_date, steam_filename)

    s3_writer.upload_json(rawg_envelope, rawg_s3_key)
    s3_writer.upload_json(steam_envelope, steam_s3_key)

    return {
        "rawg_local_path": str(rawg_local_path),
        "steam_local_path": str(steam_local_path),
        "rawg_s3_key": rawg_s3_key,
        "steam_s3_key": steam_s3_key,
        "settings": str(asdict(settings)),
    }
