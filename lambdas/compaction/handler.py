"""Metadata compaction Lambda handler.

Triggered by EventBridge Scheduler (daily). Reads all per-image metadata
JSON files from raw/ prefix, merges them into a single Parquet file under
compacted/ prefix, then deletes the processed raw files.

This keeps the number of files scanned by Athena bounded regardless of
how many images are uploaded.

Layout:
    raw/{user_id}/{upload_id}/{filename}.metadata.json   (written by metadata_extraction)
    compacted/metadata.parquet                           (written by this job)

The compaction is idempotent: it reads ALL raw files, merges with the
existing Parquet (if any), writes a new Parquet, and deletes the raw files
only after a successful write.
"""

import io
import json
import logging
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

METADATA_BUCKET = os.environ["METADATA_BUCKET"]
RAW_PREFIX = "raw/"
COMPACTED_KEY = "compacted/metadata.parquet"

s3_client = boto3.client("s3")

# Schema must match the Glue table definition exactly
SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.string()),
        pa.field("s3_key", pa.string()),
        pa.field("upload_id", pa.string()),
        pa.field("original_filename", pa.string()),
        pa.field("file_size", pa.int64()),
        pa.field("uploaded_at", pa.string()),
        pa.field("camera_make", pa.string()),
        pa.field("camera_model", pa.string()),
        pa.field("datetime_original", pa.string()),
        pa.field("gps_latitude", pa.float64()),
        pa.field("gps_longitude", pa.float64()),
        pa.field("gps_altitude", pa.float64()),
    ]
)


def handler(event, context):
    """Compact raw metadata JSON files into Parquet."""
    # 1. List all raw JSON files
    raw_keys = _list_raw_keys()
    if not raw_keys:
        logger.info("No raw metadata files to compact")
        return {"statusCode": 200, "compacted": 0}

    logger.info("Found %d raw metadata files to compact", len(raw_keys))

    # 2. Read existing Parquet (if any)
    existing_records = _read_existing_parquet()
    logger.info("Existing compacted records: %d", len(existing_records))

    # 3. Read all raw JSON files
    new_records = _read_raw_files(raw_keys)
    logger.info("New records from raw files: %d", len(new_records))

    # 4. Merge: existing + new, deduplicate by s3_key (new wins)
    merged = _merge_records(existing_records, new_records)
    logger.info("Merged total records: %d", len(merged))

    # 5. Write Parquet
    _write_parquet(merged)

    # 6. Delete processed raw files
    _delete_raw_files(raw_keys)

    return {"statusCode": 200, "compacted": len(new_records), "total": len(merged)}


def _list_raw_keys() -> list[str]:
    """List all object keys under raw/ prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=METADATA_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".metadata.json"):
                keys.append(obj["Key"])
    return keys


def _read_existing_parquet() -> list[dict]:
    """Read existing compacted Parquet file, return list of dicts."""
    try:
        response = s3_client.get_object(Bucket=METADATA_BUCKET, Key=COMPACTED_KEY)
        data = response["Body"].read()
        table = pq.read_table(io.BytesIO(data))
        return table.to_pylist()
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception:
        logger.warning("Failed to read existing Parquet, starting fresh", exc_info=True)
        return []


def _read_raw_files(keys: list[str]) -> list[dict]:
    """Read and parse all raw JSON metadata files."""
    records = []
    for key in keys:
        try:
            response = s3_client.get_object(Bucket=METADATA_BUCKET, Key=key)
            content = response["Body"].read().decode("utf-8").strip()
            if content:
                record = json.loads(content)
                records.append(record)
        except Exception:
            logger.warning("Failed to read raw file %s, skipping", key, exc_info=True)
    return records


def _merge_records(existing: list[dict], new: list[dict]) -> list[dict]:
    """Merge existing and new records, deduplicating by s3_key (new wins)."""
    by_key = {}
    for rec in existing:
        s3_key = rec.get("s3_key")
        if s3_key:
            by_key[s3_key] = rec
    for rec in new:
        s3_key = rec.get("s3_key")
        if s3_key:
            by_key[s3_key] = rec
    return list(by_key.values())


def _write_parquet(records: list[dict]) -> None:
    """Write records as a Parquet file to S3."""
    # Normalize records to match schema (fill missing fields with None)
    field_names = [f.name for f in SCHEMA]
    normalized = []
    for rec in records:
        row = {}
        for name in field_names:
            val = rec.get(name)
            # Ensure file_size is int
            if name == "file_size" and val is not None:
                val = int(val)
            row[name] = val
        normalized.append(row)

    # Build columnar data
    columns = {name: [row[name] for row in normalized] for name in field_names}
    table = pa.table(columns, schema=SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3_client.put_object(
        Bucket=METADATA_BUCKET,
        Key=COMPACTED_KEY,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info("Wrote %d records to s3://%s/%s", len(records), METADATA_BUCKET, COMPACTED_KEY)


def _delete_raw_files(keys: list[str]) -> None:
    """Delete raw JSON files in batches of 1000."""
    # S3 delete_objects supports up to 1000 keys per call
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        delete_request = {"Objects": [{"Key": k} for k in batch]}
        response = s3_client.delete_objects(Bucket=METADATA_BUCKET, Delete=delete_request)
        errors = response.get("Errors", [])
        if errors:
            logger.warning("Failed to delete %d raw files: %s", len(errors), errors)
        else:
            logger.info("Deleted %d raw files", len(batch))
