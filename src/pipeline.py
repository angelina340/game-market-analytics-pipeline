from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.clients.rawg_client import RawgClient
from src.clients.steam_client import SteamClient
from src.config import Settings
from src.s3_writer import S3RawWriter, write_local_json


@dataclass(frozen=True)
class RunContext:
    extracted_at_utc: str
    extract_date: str
    file_stamp: str


@dataclass(frozen=True)
class SourceIngestionResult:
    source: str
    local_path: str
    s3_key: str
    s3_uri: str
    upload_status: str
    extracted_at_utc: str
    extract_date: str


def _build_s3_key(prefix: str, source: str, extract_date: str, filename: str) -> str:
    return f"{prefix}/source={source}/extract_date={extract_date}/{filename}"


def _build_envelope(source: str, extracted_at: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": source,
        "extracted_at_utc": extracted_at,
        "payload": payload,
    }


def build_run_context(
    extracted_at_utc: str | None = None,
    extract_date: str | None = None,
    file_stamp: str | None = None,
) -> RunContext:
    if extracted_at_utc and extract_date and file_stamp:
        return RunContext(
            extracted_at_utc=extracted_at_utc,
            extract_date=extract_date,
            file_stamp=file_stamp,
        )

    timestamp = datetime.now(timezone.utc)
    return RunContext(
        extracted_at_utc=extracted_at_utc or timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        extract_date=extract_date or timestamp.strftime("%Y-%m-%d"),
        file_stamp=file_stamp or timestamp.strftime("%Y%m%dT%H%M%SZ"),
    )


def _build_source_filename(source: str, file_stamp: str) -> str:
    if source == "rawg":
        return f"rawg_games_{file_stamp}.json"
    if source == "steam":
        return f"steam_apps_{file_stamp}.json"
    raise ValueError(f"Unsupported source: {source}")


def _fetch_source_payload(settings: Settings, source: str) -> dict[str, Any]:
    if source == "rawg":
        return RawgClient(settings.rawg_api_key).fetch_games(page_size=settings.rawg_page_size)
    if source == "steam":
        return SteamClient(settings.steam_api_key).fetch_apps(limit=settings.steam_limit)
    raise ValueError(f"Unsupported source: {source}")


def _build_s3_writer(settings: Settings) -> S3RawWriter:
    return S3RawWriter(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
        bucket_name=settings.s3_bucket_name,
    )


def run_source_ingestion(
    settings: Settings,
    source: str,
    run_context: RunContext | None = None,
    force_upload: bool = False,
) -> SourceIngestionResult:
    run_context = run_context or build_run_context()

    payload = _fetch_source_payload(settings, source)
    envelope = _build_envelope(source, run_context.extracted_at_utc, payload)
    filename = _build_source_filename(source, run_context.file_stamp)
    local_path = Path(settings.local_data_dir) / source / run_context.extract_date / filename

    write_local_json(envelope, local_path)

    s3_key = _build_s3_key(settings.s3_raw_prefix, source, run_context.extract_date, filename)
    s3_writer = _build_s3_writer(settings)

    upload_status = "uploaded"
    if s3_writer.object_exists(s3_key) and not force_upload:
        upload_status = "skipped_existing"
    else:
        s3_writer.upload_json(envelope, s3_key)

    return SourceIngestionResult(
        source=source,
        local_path=str(local_path),
        s3_key=s3_key,
        s3_uri=f"s3://{settings.s3_bucket_name}/{s3_key}",
        upload_status=upload_status,
        extracted_at_utc=run_context.extracted_at_utc,
        extract_date=run_context.extract_date,
    )


def run_pipeline(
    settings: Settings,
    run_context: RunContext | None = None,
    force_upload: bool = False,
) -> dict[str, str]:
    run_context = run_context or build_run_context()

    rawg_result = run_source_ingestion(
        settings=settings,
        source="rawg",
        run_context=run_context,
        force_upload=force_upload,
    )
    steam_result = run_source_ingestion(
        settings=settings,
        source="steam",
        run_context=run_context,
        force_upload=force_upload,
    )

    return {
        "rawg_local_path": rawg_result.local_path,
        "steam_local_path": steam_result.local_path,
        "rawg_s3_key": rawg_result.s3_key,
        "steam_s3_key": steam_result.s3_key,
        "rawg_upload_status": rawg_result.upload_status,
        "steam_upload_status": steam_result.upload_status,
        "aws_region": settings.aws_default_region,
        "s3_bucket_name": settings.s3_bucket_name,
        "extract_date": run_context.extract_date,
        "extracted_at_utc": run_context.extracted_at_utc,
    }
