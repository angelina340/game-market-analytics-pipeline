from __future__ import annotations

import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


class S3RawWriter:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str,
        bucket_name: str,
    ) -> None:
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def upload_json(self, payload: dict, s3_key: str) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=body,
            ContentType="application/json",
        )

    def object_exists(self, s3_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise


def write_local_json(payload: dict, filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(json.dumps(payload, indent=2), encoding="utf-8")
