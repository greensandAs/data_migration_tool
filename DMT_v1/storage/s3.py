# AWS S3 storage backend using boto3.
# Co-authored with CoCo
"""storage.s3 — AWS S3 storage backend."""
from __future__ import annotations

import os
from pathlib import Path

from storage import StorageBackend


class S3Storage(StorageBackend):
    """Store files on AWS S3."""

    def __init__(self, bucket: str = None, prefix: str = "",
                 region: str = None, **kwargs):
        import boto3
        self._bucket = bucket or os.getenv("DMT_S3_BUCKET", "")
        self._prefix = prefix.strip("/")
        session_kwargs = {}
        if region:
            session_kwargs["region_name"] = region
        self._client = boto3.client("s3", **session_kwargs)

    @property
    def storage_type(self) -> str:
        return "s3"

    def _key(self, remote_key: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{remote_key}"
        return remote_key

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        key = self._key(remote_key)
        self._client.upload_file(str(local_path), self._bucket, key)
        return f"s3://{self._bucket}/{key}"

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        key = self._key(remote_key)
        dst = Path(local_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(self._bucket, key, str(dst))
        return dst

    def list_files(self, prefix: str) -> list[str]:
        full_prefix = self._key(prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                files.append(obj["Key"])
        return sorted(files)

    def delete(self, remote_key: str):
        key = self._key(remote_key)
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def exists(self, remote_key: str) -> bool:
        key = self._key(remote_key)
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False

    def get_stage_uri(self, remote_key: str) -> str:
        """Return s3:// URI for use with Snowflake external stage COPY INTO."""
        key = self._key(remote_key)
        return f"s3://{self._bucket}/{key}"

    def move(self, source_key: str, dest_key: str) -> str:
        """Move a file from source to dest within the same bucket (copy + delete)."""
        src = self._key(source_key) if not source_key.startswith(self._prefix) else source_key
        dst = self._key(dest_key) if not dest_key.startswith(self._prefix) else dest_key
        self._client.copy_object(
            Bucket=self._bucket,
            CopySource={"Bucket": self._bucket, "Key": src},
            Key=dst)
        self._client.delete_object(Bucket=self._bucket, Key=src)
        return f"s3://{self._bucket}/{dst}"

    def move_to_processed(self, file_keys: list[str], sub: str, date_str: str) -> list[str]:
        """Move files from active folder to processed/<sub>/<date>/ folder.

        Args:
            file_keys: list of full S3 keys to move
            sub: 'full' or 'incremental'
            date_str: date string for subfolder (e.g., '20260622')

        Returns:
            List of new processed paths.
        """
        moved = []
        for key in file_keys:
            filename = key.rsplit("/", 1)[-1]
            # Replace /full/ or /incremental/ with /processed/<sub>/<date>/
            if f"/{sub}/" in key:
                base = key.split(f"/{sub}/")[0]
                new_key = f"{base}/processed/{sub}/{date_str}/{filename}"
            else:
                new_key = f"{key.rsplit('/', 1)[0]}/processed/{sub}/{date_str}/{filename}"
            self._client.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": key},
                Key=new_key)
            self._client.delete_object(Bucket=self._bucket, Key=key)
            moved.append(f"s3://{self._bucket}/{new_key}")
        return moved
