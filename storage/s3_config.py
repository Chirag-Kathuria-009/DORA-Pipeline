"""MinIO / AWS S3 connection and bucket bootstrap.

Provides a reusable boto3 S3 client (get_s3_client) used by iceberg_tables.py,
streaming_job.py, and any other module that reads or writes to object storage.
setup_storage() is the one-time bootstrap that creates the bucket and folder
structure; it is idempotent and safe to re-run.

Usage:
    python storage/s3_config.py
    python storage/s3_config.py --endpoint http://localhost:9000 --bucket dora-lakehouse
"""

import argparse
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# ── Defaults (overridden by env vars or CLI args) ─────────────────────────────
_DEFAULT_ENDPOINT = "http://localhost:9000"
_DEFAULT_BUCKET   = "dora-lakehouse"

# Folder prefixes to materialise as zero-byte marker objects.
# raw/       — landing zone for Kafka-sourced JSON before Iceberg write
# iceberg/   — PyIceberg HadoopCatalog data and metadata files
FOLDER_PREFIXES: list[str] = [
    "raw/incidents/",
    "iceberg/incidents/",
    "iceberg/vendors/",
    "iceberg/audit_log/",
]


def get_s3_client(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> "boto3.client":
    """Return a boto3 S3 client configured for MinIO (or real AWS S3).

    All parameters fall back to the corresponding environment variable, then
    to local-dev defaults so the function works out-of-the-box after
    `cp .env.example .env`.

    Switching to production AWS only requires unsetting MINIO_ENDPOINT (boto3
    then uses the default AWS endpoint) and supplying real credentials.

    Args:
        endpoint:   S3-compatible endpoint URL. Reads MINIO_ENDPOINT from env.
                    Pass None to use the env var / default.
        access_key: Access key ID. Reads MINIO_ACCESS_KEY from env.
        secret_key: Secret access key. Reads MINIO_SECRET_KEY from env.

    Returns:
        A configured boto3 S3 client instance.
    """
    resolved_endpoint  = endpoint   or os.environ.get("MINIO_ENDPOINT",   _DEFAULT_ENDPOINT)
    resolved_access    = access_key or os.environ.get("MINIO_ACCESS_KEY",  "minioadmin")
    resolved_secret    = secret_key or os.environ.get("MINIO_SECRET_KEY",  "minioadmin")

    return boto3.client(
        "s3",
        endpoint_url=resolved_endpoint,
        aws_access_key_id=resolved_access,
        aws_secret_access_key=resolved_secret,
        # path-style addressing required for MinIO; harmless against real AWS
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _bucket_exists(client, bucket: str) -> bool:
    """Return True if the bucket already exists and is accessible.

    Args:
        client: An active boto3 S3 client.
        bucket: Bucket name to check.
    """
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            return False
        raise


def _ensure_folder(client, bucket: str, prefix: str) -> bool:
    """Create a zero-byte folder-marker object if the prefix does not exist.

    S3 and MinIO have no real directories; a zero-byte object whose key ends
    with '/' acts as a folder placeholder that appears in the console UI.

    Args:
        client: An active boto3 S3 client.
        bucket: Target bucket name.
        prefix: Folder key to create, e.g. "raw/incidents/".

    Returns:
        True if the folder was created, False if it already existed.
    """
    try:
        client.head_object(Bucket=bucket, Key=prefix)
        return False  # already exists
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            client.put_object(Bucket=bucket, Key=prefix, Body=b"")
            return True
        raise


def setup_storage(
    endpoint: str | None = None,
    bucket: str = _DEFAULT_BUCKET,
) -> None:
    """Create the MinIO bucket and all required folder prefixes.

    Connects to MinIO, creates the bucket if absent, then ensures every
    folder prefix in FOLDER_PREFIXES exists as a zero-byte marker object.
    Prints a one-line status for each action taken.

    Args:
        endpoint: MinIO endpoint URL. Defaults to MINIO_ENDPOINT env var.
        bucket:   Bucket name to create. Defaults to 'dora-lakehouse'.
    """
    client = get_s3_client(endpoint=endpoint)
    resolved_endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", _DEFAULT_ENDPOINT)

    print(f"MinIO endpoint : {resolved_endpoint}")
    print(f"Bucket         : {bucket}\n")

    # ── Bucket ────────────────────────────────────────────────────────────────
    if _bucket_exists(client, bucket):
        print(f"  [SKIP]    bucket '{bucket}' already exists")
    else:
        client.create_bucket(Bucket=bucket)
        print(f"  [CREATED] bucket '{bucket}'")

    # ── Folder prefixes ───────────────────────────────────────────────────────
    for prefix in FOLDER_PREFIXES:
        created = _ensure_folder(client, bucket, prefix)
        tag = "CREATED" if created else "SKIP   "
        print(f"  [{tag}] {bucket}/{prefix}")

    print("\nStorage setup complete.")


def main() -> None:
    """Parse CLI arguments and call setup_storage().

    Credentials are always read from MINIO_ACCESS_KEY / MINIO_SECRET_KEY
    environment variables; they are intentionally not exposed as CLI flags
    to avoid leaking secrets into shell history.
    """
    parser = argparse.ArgumentParser(
        description="Bootstrap MinIO bucket and folder layout for the DORA pipeline."
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="MinIO endpoint URL (default: MINIO_ENDPOINT env var or http://localhost:9000)",
    )
    parser.add_argument(
        "--bucket",
        default=_DEFAULT_BUCKET,
        help=f"Bucket name (default: {_DEFAULT_BUCKET})",
    )
    args = parser.parse_args()
    setup_storage(endpoint=args.endpoint, bucket=args.bucket)


if __name__ == "__main__":
    main()
