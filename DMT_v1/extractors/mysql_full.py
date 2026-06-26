# MySQL full-load extraction via MySQL Shell (mysqlsh).
# Co-authored with CoCo
"""extractors.mysql_full — Full-load engine using MySQL Shell (mysqlsh).

Uses `util.dumpTables` for fast, parallel, zstd-compressed TSV dumps.
Only *.tsv.zst data files are passed to the loader.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from urllib.parse import quote_plus

from extractors import BaseExtractor, ExtractionResult


class MySQLFullExtractor(BaseExtractor):

    @property
    def source_type(self) -> str:
        return "mysql"

    def extract_full(self, config: dict, src_cfg: dict,
                     output_dir: str | Path) -> ExtractionResult:
        """Run mysqlsh dumpTables for one table."""
        # output_dir is already: ./export/<source_type>/<connection>/<table>
        out_dir = Path(output_dir) / "full"
        if out_dir.exists():
            for p in sorted(out_dir.glob("**/*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            out_dir.rmdir()
        out_dir.parent.mkdir(parents=True, exist_ok=True)

        threads = int(config.get("PARTITION_NUM") or 8)
        out_uri = str(out_dir).replace("\\", "/")

        js_cmd = (
            f'util.dumpTables("{config["SOURCE_DB"]}", '
            f'["{config["SOURCE_TABLE"]}"], '
            f'"{out_uri}", '
            f'{{threads: {threads}, compression: "zstd", showProgress: true, '
            f'tzUtc: false}})'
        )

        pwd = src_cfg.get("password") or ""
        uri = (f'mysql://{quote_plus(src_cfg["user"])}'
               f'@{src_cfg["host"]}:{src_cfg["port"]}')
        cmd = ["mysqlsh", f"--uri={uri}", "--passwords-from-stdin",
               "--js", "--execute", js_cmd]

        print(f"   mysqlsh full dump -> {out_dir}")
        result = subprocess.run(
            cmd, input=(pwd + "\n") if pwd else None,
            capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"mysqlsh failed:\n{result.stderr}")

        data_files = sorted(out_dir.glob("*.tsv.zst"))
        if not data_files:
            raise RuntimeError(
                f"mysqlsh produced no .tsv.zst data files in {out_dir}")
        print(f"   full dump complete: {len(data_files)} file(s)")

        return ExtractionResult(
            files=data_files,
            row_count=0,  # actual count determined post-COPY
            file_format="tsv_zstd",
            engine="mysqlsh",
        )

    def extract_incremental(self, config: dict, src_cfg: dict,
                            output_dir: str | Path,
                            source_conn=None) -> ExtractionResult:
        """Full extractor does not support incremental — delegate to
        MySQLIncrementalExtractor instead."""
        raise NotImplementedError(
            "Use MySQLIncrementalExtractor for incremental loads")
