from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import snowflake.connector

from src.config import Settings, SnowflakeSettings


@dataclass(frozen=True)
class SnowflakeLoadResult:
    rawg_stage_path: str
    steam_stage_path: str
    rawg_rows_loaded: int
    steam_rows_loaded: int


class SnowflakeRawLoader:
    def __init__(self, settings: SnowflakeSettings, app_settings: Settings) -> None:
        self.settings = settings
        self.app_settings = app_settings

    def _connect(self):
        connect_kwargs = {
            "account": self.settings.account,
            "user": self.settings.user,
            "warehouse": self.settings.warehouse,
            "database": self.settings.database,
            "schema": self.settings.schema,
            "role": self.settings.role,
        }

        if self.settings.private_key_path:
            connect_kwargs["authenticator"] = "SNOWFLAKE_JWT"
            connect_kwargs["private_key_file"] = str(Path(self.settings.private_key_path).resolve())
            if self.settings.private_key_passphrase:
                connect_kwargs["private_key_file_pwd"] = self.settings.private_key_passphrase
        else:
            connect_kwargs["password"] = self.settings.password
            connect_kwargs["authenticator"] = self.settings.authenticator
            connect_kwargs["client_request_mfa_token"] = True
            if self.settings.mfa_passcode:
                connect_kwargs["passcode"] = self.settings.mfa_passcode

        return snowflake.connector.connect(
            **connect_kwargs,
        )

    def load_s3_raw_files(self, rawg_s3_key: str, steam_s3_key: str) -> SnowflakeLoadResult:
        rawg_stage_path = f"s3://{self.app_settings.s3_bucket_name}/{rawg_s3_key}"
        steam_stage_path = f"s3://{self.app_settings.s3_bucket_name}/{steam_s3_key}"

        with self._connect() as connection:
            with connection.cursor() as cursor:
                self._initialize_objects(cursor)
                rawg_rows_loaded = self._copy_json_file(
                    cursor=cursor,
                    table_name="RAWG_GAMES_RAW",
                    stage_name="GAME_ANALYTICS_RAW_STAGE",
                    s3_key=rawg_s3_key,
                    source_name="rawg",
                )
                steam_rows_loaded = self._copy_json_file(
                    cursor=cursor,
                    table_name="STEAM_GAMES_RAW",
                    stage_name="GAME_ANALYTICS_RAW_STAGE",
                    s3_key=steam_s3_key,
                    source_name="steam",
                )

        return SnowflakeLoadResult(
            rawg_stage_path=rawg_stage_path,
            steam_stage_path=steam_stage_path,
            rawg_rows_loaded=rawg_rows_loaded,
            steam_rows_loaded=steam_rows_loaded,
        )

    def _initialize_objects(self, cursor) -> None:
        statements = [
            f"create database if not exists {self.settings.database}",
            f"create schema if not exists {self.settings.database}.{self.settings.schema}",
            f"use database {self.settings.database}",
            f"use schema {self.settings.schema}",
            f"use warehouse {self.settings.warehouse}",
            """
            create or replace file format JSON_RAW_FORMAT
            type = json
            strip_outer_array = false
            """,
            f"""
            create or replace stage GAME_ANALYTICS_RAW_STAGE
            url = 's3://{self.app_settings.s3_bucket_name}/{self.app_settings.s3_raw_prefix}/'
            credentials = (
                aws_key_id = '{self.app_settings.aws_access_key_id}'
                aws_secret_key = '{self.app_settings.aws_secret_access_key}'
            )
            file_format = JSON_RAW_FORMAT
            """,
            """
            create table if not exists RAWG_GAMES_RAW (
                source varchar,
                s3_key varchar,
                extracted_at timestamp_ntz,
                payload variant,
                loaded_at timestamp_ntz default current_timestamp()
            )
            """,
            """
            create table if not exists STEAM_GAMES_RAW (
                source varchar,
                s3_key varchar,
                extracted_at timestamp_ntz,
                payload variant,
                loaded_at timestamp_ntz default current_timestamp()
            )
            """,
        ]

        for statement in statements:
            cursor.execute(statement)

    def _copy_json_file(
        self,
        cursor,
        table_name: str,
        stage_name: str,
        s3_key: str,
        source_name: str,
    ) -> int:
        relative_key = s3_key.removeprefix(f"{self.app_settings.s3_raw_prefix}/")
        copy_statement = f"""
        copy into {table_name} (source, s3_key, extracted_at, payload)
        from (
            select
                '{source_name}' as source,
                '{s3_key}' as s3_key,
                to_timestamp_ntz($1:extracted_at_utc::string) as extracted_at,
                $1 as payload
            from @{stage_name}/{relative_key}
        )
        file_format = (format_name = JSON_RAW_FORMAT)
        on_error = 'ABORT_STATEMENT'
        """
        cursor.execute(copy_statement)
        rows = cursor.fetchall()

        rows_loaded = 0
        for row in rows:
            if len(row) >= 4 and isinstance(row[3], int):
                rows_loaded += row[3]
        return rows_loaded
