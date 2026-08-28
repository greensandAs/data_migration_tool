import boto3
import os
import sys
from pathlib import Path

# --- Configuration ---
AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY"
AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_KEY"
AWS_REGION = "us-east-1"
BUCKET_NAME = "your-bucket-name"
S3_PREFIX = ""  # e.g. "uploads/data/" (leave empty to upload to bucket root)

LOCAL_PATH = r"C:\path\to\your\folder"  # file or folder path
# ---------------------

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

def upload_file(s3_client, local_file, bucket, s3_key):
    try:
        s3_client.upload_file(local_file, bucket, s3_key)
        print(f"  Uploaded: {s3_key}")
    except Exception as e:
        print(f"  FAILED: {s3_key} -> {e}")

def upload_folder(s3_client, local_folder, bucket, prefix):
    local_folder = Path(local_folder)
    file_count = 0

    for file_path in local_folder.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_folder)
            s3_key = f"{prefix}{relative_path.as_posix()}" if prefix else relative_path.as_posix()
            upload_file(s3_client, str(file_path), bucket, s3_key)
            file_count += 1

    return file_count

def main():
    path = Path(LOCAL_PATH)

    if not path.exists():
        print(f"Error: Path does not exist: {LOCAL_PATH}")
        sys.exit(1)

    s3_client = get_s3_client()
    print(f"Target: s3://{BUCKET_NAME}/{S3_PREFIX}")
    print(f"Source: {LOCAL_PATH}")
    print("-" * 50)

    if path.is_file():
        s3_key = f"{S3_PREFIX}{path.name}" if S3_PREFIX else path.name
        upload_file(s3_client, str(path), BUCKET_NAME, s3_key)
        print(f"\nDone. 1 file uploaded.")
    elif path.is_dir():
        count = upload_folder(s3_client, path, BUCKET_NAME, S3_PREFIX)
        print(f"\nDone. {count} files uploaded.")
    else:
        print(f"Error: {LOCAL_PATH} is not a valid file or folder.")
        sys.exit(1)

if __name__ == "__main__":
    main()
