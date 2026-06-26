# Azure Blob Storage backend using azure-storage-blob.
# Co-authored with CoCo
"""storage.azure_blob — Azure Blob Storage backend."""
from __future__ import annotations

import os
from pathlib import Path

from storage import StorageBackend


class AzureBlobStorage(StorageBackend):
    """Store files on Azure Blob Storage."""

    def __init__(self, container: str = None, prefix: str = "",
                 connection_string: str = None, **kwargs):
        from azure.storage.blob import BlobServiceClient
        conn_str = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
        self._container_name = container or os.getenv("DMT_AZURE_CONTAINER", "")
        self._prefix = prefix.strip("/")
        self._service = BlobServiceClient.from_connection_string(conn_str)
        self._container = self._service.get_container_client(self._container_name)

    @property
    def storage_type(self) -> str:
        return "azure"

    def _blob_name(self, remote_key: str) -> str:
        if self._prefix:
            return f"{self._prefix}/{remote_key}"
        return remote_key

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        blob_name = self._blob_name(remote_key)
        blob_client = self._container.get_blob_client(blob_name)
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        return f"azure://{self._container_name}/{blob_name}"

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        blob_name = self._blob_name(remote_key)
        dst = Path(local_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        blob_client = self._container.get_blob_client(blob_name)
        with open(dst, "wb") as f:
            stream = blob_client.download_blob()
            stream.readinto(f)
        return dst

    def list_files(self, prefix: str) -> list[str]:
        full_prefix = self._blob_name(prefix)
        blobs = self._container.list_blobs(name_starts_with=full_prefix)
        return sorted(b.name for b in blobs)

    def delete(self, remote_key: str):
        blob_name = self._blob_name(remote_key)
        blob_client = self._container.get_blob_client(blob_name)
        blob_client.delete_blob()

    def exists(self, remote_key: str) -> bool:
        blob_name = self._blob_name(remote_key)
        blob_client = self._container.get_blob_client(blob_name)
        try:
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    def get_stage_uri(self, remote_key: str) -> str:
        """Return azure:// URI for use with Snowflake external stage COPY INTO."""
        blob_name = self._blob_name(remote_key)
        return f"azure://{self._container_name}/{blob_name}"
