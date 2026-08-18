# Shared BCP invocation helpers for the MSSQL extractors.
"""extractors.mssql_common — Safe BCP invocation.

Two hazards this module exists to avoid:

  1. Password in argv — anything passed as `-P secret` is visible to every user
     on the host via `ps aux`, because `ps` reads the argument vector straight
     from the kernel. Using shell=False does NOT help. bcp prompts for a
     password when -U is supplied without -P, so we feed it on stdin instead
     and keep it out of argv entirely.

  2. Shell interpolation — building a command string and running it with
     shell=True lets a password (or table name, or delimiter) containing
     $, `, ", ; or | be reinterpreted by the shell. We pass an argument list
     with shell=False so no value is ever parsed by a shell.

This mirrors how extractors/mysql_full.py already handles mysqlsh
(--passwords-from-stdin).
"""
from __future__ import annotations

import subprocess
from pathlib import Path

BCP_TIMEOUT = 7200          # 2 hours
MSSQL_DEFAULT_PORT = 1433

# bcp treats 0 as success and 4 as "completed with warnings" (e.g. nulls in
# non-nullable columns during a copy) — both yield a usable output file.
BCP_OK_RETURNCODES = (0, 4)


def server_spec(host: str, port: int | None = None) -> str:
    """Build the value for bcp's -S flag.

    Appends ",port" only when it is safe to do so. A host that already carries
    a named instance ("HOST\\SQLEXPRESS") or an explicit port ("HOST,1433") is
    passed through untouched, as is the default port.
    """
    host = (host or "").strip()
    if not port or int(port) == MSSQL_DEFAULT_PORT:
        return host
    if "\\" in host or "," in host:
        return host
    return f"{host},{int(port)}"


def build_bcp_args(*, source_spec: str, mode: str, filepath: str | Path,
                   server: str, user: str, delimiter: str,
                   database: str | None = None,
                   codepage: str = "65001") -> list[str]:
    """Build the bcp argument list — deliberately WITHOUT -P.

    Args:
        source_spec: either "db.schema.table" (mode="out") or a SELECT
            statement (mode="queryout").
        mode: "out" for a whole table, "queryout" for a query.
        database: required for queryout, since the query itself is unqualified.
    """
    args = ["bcp", source_spec, mode, str(filepath), "-S", server, "-U", user]
    if database:
        args += ["-d", database]
    args += ["-c", "-t", delimiter, "-C", codepage]
    return args


def run_bcp(args: list[str], password: str,
            timeout: int = BCP_TIMEOUT) -> subprocess.CompletedProcess:
    """Run bcp with the password supplied on stdin.

    The password is absent from argv (not visible in the process list) and
    shell=False means no argument is shell-interpreted.
    """
    return subprocess.run(
        args,
        input=(password or "") + "\n",
        capture_output=True,
        text=True,
        timeout=timeout,
        shell=False,
    )


def format_cmd(args: list[str]) -> str:
    """Render an argv list for logging. Safe — no credential is present."""
    return " ".join(args)


def bcp_error(proc: subprocess.CompletedProcess) -> str:
    """Best-effort error text from a failed bcp run."""
    return (proc.stderr or "").strip() or (proc.stdout or "").strip()


def count_lines(filepath: Path) -> int:
    """Count rows in a bcp output file, streaming to stay memory-bounded."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            return sum(1 for _ in f)
    except OSError:
        return 0
