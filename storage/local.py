# Local filesystem storage backend.
# Co-authored with CoCo
"""storage.local — Local filesystem storage backend."""
from __future__ import annotations

import shutil
from pathlib import Path

from storage import StorageBackend


class LocalStorage(StorageBackend):
    """Store files on the local filesystem (default: ./export)."""

    def __init__(self, base_dir: str = "./export", **kwargs):
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)

    @property
    def storage_type(self) -> str:
        return "local"

    def _resolve(self, remote_key: str) -> Path:
        return self._base / remote_key

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        """'Upload' = copy/move to the base directory."""
        src = Path(local_path)
        dst = self._resolve(remote_key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src != dst:
            shutil.copy2(src, dst)
        return str(dst)

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        """'Download' = copy from base directory to target."""
        src = self._resolve(remote_key)
        dst = Path(local_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src != dst:
            shutil.copy2(src, dst)
        return dst

    def list_files(self, prefix: str) -> list[str]:
        """List files under a subdirectory prefix."""
        target = self._resolve(prefix)
        if not target.exists():
            return []
        if target.is_file():
            return [str(target.relative_to(self._base))]
        return sorted(
            str(p.relative_to(self._base))
            for p in target.rglob("*") if p.is_file()
        )

    def delete(self, remote_key: str):
        p = self._resolve(remote_key)
        if p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p, ignore_errors=True)

    def exists(self, remote_key: str) -> bool:
        return self._resolve(remote_key).exists()

    def get_stage_uri(self, remote_key: str) -> str:
        """Local files need to be PUT to a stage before COPY INTO.
        Returns the absolute local path (caller must PUT it)."""
        return str(self._resolve(remote_key).resolve())

    def cleanup_table(self, source_table: str):
        """Remove all files for a table (post-load housekeeping)."""
        target = self._base / source_table
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
