"""upload.py — Azure Blob (azcopy) upload and file management."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path


def upload_to_blob(local_files: list[Path], tgt_table: str) -> dict:
    """Upload file(s) to Azure Blob under the target table folder.

    Returns: {returncode, uploaded_count, log}
    """
    sas_token = os.getenv("AZ_SAS_TOKEN", "")
    cloud_path = os.getenv("CLOUD_PATH", "")

    if not sas_token or not cloud_path:
        return {"returncode": 1, "uploaded_count": 0, "log": "AZ_SAS_TOKEN or CLOUD_PATH not set"}

    uploaded = 0
    logs = []

    for local_file in local_files:
        dest = f"{cloud_path}{tgt_table}/{local_file.name}?{sas_token}"
        cmd = f'azcopy cp "{local_file}" "{dest}"'

        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
        if proc.returncode == 0:
            uploaded += 1
            logs.append(f"Uploaded: {local_file.name}")
        else:
            err_detail = proc.stderr.strip() or proc.stdout.strip()
            logs.append(f"FAILED: {local_file.name} — {err_detail[:500]}")
            return {"returncode": 1, "uploaded_count": uploaded, "log": "\n".join(logs)}

    return {"returncode": 0, "uploaded_count": uploaded, "log": "\n".join(logs)}


def move_to_processed(tgt_table: str) -> dict:
    """Move files from table folder to processed/ folder in Azure Blob.

    Returns: {returncode, log}
    """
    sas_token = os.getenv("AZ_SAS_TOKEN", "")
    cloud_path = os.getenv("CLOUD_PATH", "")

    if not sas_token or not cloud_path:
        return {"returncode": 1, "log": "AZ_SAS_TOKEN or CLOUD_PATH not set"}

    src_blob = f"{cloud_path}{tgt_table}/?{sas_token}"
    dst_blob = f"{cloud_path}processed/{tgt_table}/?{sas_token}"

    try:
        # Copy to processed/
        cp_cmd = f'azcopy cp "{src_blob}" "{dst_blob}" --recursive'
        proc = subprocess.run(cp_cmd, shell=True, capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            return {"returncode": 1, "log": f"Copy failed: {proc.stderr[:200]}"}

        # Remove originals
        rm_cmd = f'azcopy rm "{src_blob}" --recursive'
        subprocess.run(rm_cmd, shell=True, capture_output=True, text=True, timeout=120)

        return {"returncode": 0, "log": f"Moved {tgt_table}/ → processed/{tgt_table}/"}
    except Exception as e:
        return {"returncode": 1, "log": f"Move failed: {e}"}


def cleanup_local(files: list[Path]):
    """Remove local export files after upload."""
    for f in files:
        try:
            f.unlink(missing_ok=True)
        except Exception:
            pass
