# Storage abstraction layer with pluggable backends for extract file management.
# Co-authored with CoCo
"""storage — Pluggable storage backends for DMT extracted files.

Backends:
  - local: filesystem (./export or any path)
  - s3: AWS S3 via boto3
  - azure: Azure Blob Storage via azure-storage-blob
  - internal_stage: Snowflake PUT to an internal named stage

Each backend implements the StorageBackend ABC so the orchestrator and loader
can work with files regardless of where they physically reside.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class StorageBackend(ABC):
    """Abstract base for file storage operations."""

    @abstractmethod
    def upload(self, local_path: str | Path, remote_key: str) -> str:
        """Upload a local file to the backend. Returns the remote path/URI."""
        ...

    @abstractmethod
    def download(self, remote_key: str, local_path: str | Path) -> Path:
        """Download a remote file to a local path. Returns the local Path."""
        ...

    @abstractmethod
    def list_files(self, prefix: str) -> list[str]:
        """List files under a prefix/directory."""
        ...

    @abstractmethod
    def delete(self, remote_key: str):
        """Delete a single file from the backend."""
        ...

    @abstractmethod
    def exists(self, remote_key: str) -> bool:
        """Check if a file exists at the given key."""
        ...

    @abstractmethod
    def get_stage_uri(self, remote_key: str) -> str:
        """Return the URI/path usable in a Snowflake COPY INTO statement."""
        ...

    @property
    @abstractmethod
    def storage_type(self) -> str:
        """Return the type identifier: local | s3 | azure | internal_stage."""
        ...


def get_backend(storage_type: str, **kwargs) -> StorageBackend:
    """Factory: instantiate the correct backend by type string."""
    if storage_type == "local":
        from storage.local import LocalStorage
        return LocalStorage(**kwargs)
    elif storage_type == "s3":
        from storage.s3 import S3Storage
        return S3Storage(**kwargs)
    elif storage_type == "azure":
        from storage.azure_blob import AzureBlobStorage
        return AzureBlobStorage(**kwargs)
    elif storage_type == "internal_stage":
        from storage.internal_stage import InternalStageStorage
        return InternalStageStorage(**kwargs)
    else:
        raise ValueError(f"Unknown storage_type: {storage_type!r}")
