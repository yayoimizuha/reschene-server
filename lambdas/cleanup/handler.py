"""Cleanup Lambda handler.

Triggered by S3 ObjectRemoved events. Deletes the corresponding
per-image metadata JSON file and the thumbnail.

Metadata files live at:
    raw/{user_id}/{upload_id}/{filename}.metadata.json

If the record has already been compacted into Parquet, it will be
excluded at the next compaction run (the source image no longer
exists, so the compaction job can filter it out — or the record
simply remains in the historical Parquet as a tombstone-free design).
"""

import logging
import os
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

METADATA_BUCKET = os.environ["METADATA_BUCKET"]
THUMBNAIL_BUCKET = os.environ["THUMBNAIL_BUCKET"]

s3_client = boto3.client("s3")


def handler(event, context):
    """Process S3 ObjectRemoved events."""
    for record in event.get("Records", []):
        s3_key = unquote_plus(record["s3"]["object"]["key"])
        logger.info("Processing deletion: %s", s3_key)

        # Parse key: {user_id}/{upload_id}/{filename}
        parts = s3_key.split("/", 2)
        if len(parts) < 3:
            logger.error("Unexpected S3 key format: %s", s3_key)
            return

        user_id = parts[0]
        upload_id = parts[1]
        original_filename = parts[2]

        # Delete per-image metadata JSON
        _delete_metadata(user_id, upload_id, original_filename)

        # Delete thumbnail (ignore if not exists)
        _delete_thumbnail(s3_key)

    return {"statusCode": 200}


def _delete_metadata(user_id: str, upload_id: str, filename: str) -> None:
    """Delete the individual metadata JSON file for the removed image."""
    metadata_key = f"raw/{user_id}/{upload_id}/{filename}.metadata.json"
    try:
        s3_client.delete_object(Bucket=METADATA_BUCKET, Key=metadata_key)
        logger.info("Metadata deleted: s3://%s/%s", METADATA_BUCKET, metadata_key)
    except Exception:
        logger.warning("Failed to delete metadata for %s (may not exist)", metadata_key, exc_info=True)


def _delete_thumbnail(s3_key: str) -> None:
    """Delete the thumbnail for the given S3 key."""
    try:
        s3_client.delete_object(Bucket=THUMBNAIL_BUCKET, Key=s3_key)
        logger.info("Thumbnail deleted: s3://%s/%s", THUMBNAIL_BUCKET, s3_key)
    except Exception:
        logger.warning("Failed to delete thumbnail for %s (may not exist)", s3_key, exc_info=True)
