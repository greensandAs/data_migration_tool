# Snowflake internal stage storage backend using PUT/GET commands.
# Co-authored with CoCo
"""storage.internal_stage — Snowflake internal stage (PUT/GET) backend."""
from __future__ import annotations

from pathlib import Path

from storage import StorageBackend


class InternalStageStorage(StorageBackend):
    """Store files on a Snowflake internal named stage via PUT."""

    def __init__(self, stage_name: str = "HISTLOAD_DB.META.DMT_STAGE",
                 sf_cursor=None, **kwargs):
        self._stage = stage_name
        self._cursor = sf_cursor

    @property
    def storage_type(self) -> str:
        return "internal_stage"

    def _stage_path(self, remote_key: str) -> str:
        return f"@{self._stage}/{remote_key}"

    def set_cursor(self, cursor):
        """Inject cursor at runtime (connections are managed by orchestrator)."""
        self._cursor = cursor

    def upload(self, local_path: str | Path, remote_key: str) -> str:
        """PUT local file to the internal stage."""
        stage_dir = self._stage_path("/".join(remote_key.split("/")[:-1]))
        self._cursor.execute(
            f"PUT 'file://{Path(local_path).resolve()}' '{stage_dir}/' "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        return self._stage_path(remote_key)

    def upload_parallel(self, sf_cfg: dict, local_paths: list[Path],
                        remote_prefix: str, max_workers: int = 4):
        """PUT multiple files in parallel using separate connections."""
        import snowflake.connector
        from concurrent.futures import ThreadPoolExecutor

        def _put_one(local_path):
            conn = snowflake.connector.connect(**sf_cfg)
            try:
                cur = conn.cursor()
                stage_dir = self._stage_path(remote_prefix)
                cur.execute(
                    f"PUT 'file://{local_path.resolve()}' '{stage_dir}/' "
                    "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                cur.close()
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            list(ex.map(_put_one, local_paths))

    def download(self, remote_key: str, local_path: str | Path) -> Path:
        """GET file from stage to local path."""
        dst = Path(local_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        self._cursor.execute(
            f"GET '{self._stage_path(remote_key)}' 'file://{dst.parent}'"
        )
        return dst

    def list_files(self, prefix: str) -> list[str]:
        self._cursor.execute(f"LIST '{self._stage_path(prefix)}'")
        return [row[0] for row in self._cursor.fetchall()]

    def delete(self, remote_key: str):
        self._cursor.execute(f"REMOVE '{self._stage_path(remote_key)}'")

    def exists(self, remote_key: str) -> bool:
        files = self.list_files(remote_key)
        return len(files) > 0

    def get_stage_uri(self, remote_key: str) -> str:
        """Return @stage/path for use in COPY INTO."""
        return self._stage_path(remote_key)

    def clear_prefix(self, prefix: str):
        """Remove all files under a stage prefix."""
        self._cursor.execute(f"REMOVE '{self._stage_path(prefix)}/'")
