"""Cleanup Lambda handler.

Triggered by S3 ObjectRemoved events. Removes the corresponding metadata
line from the user's JSONL file and deletes the thumbnail.
"""

import json
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

        # Remove from JSONL
        _remove_from_jsonl(user_id, s3_key)

        # Delete thumbnail (ignore if not exists)
        _delete_thumbnail(s3_key)

    return {"statusCode": 200}


def _remove_from_jsonl(user_id: str, s3_key: str) -> None:
    """Remove the metadata line matching s3_key from the user's JSONL file."""
    jsonl_key = f"{user_id}/metadata.jsonl"

    try:
        response = s3_client.get_object(Bucket=METADATA_BUCKET, Key=jsonl_key)
        content = response["Body"].read().decode("utf-8")
    except s3_client.exceptions.NoSuchKey:
        logger.info("JSONL file not found (already deleted): %s", jsonl_key)
        return

    lines = content.strip().split("\n") if content.strip() else []
    filtered_lines = []
    found = False

    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            if record.get("s3_key") == s3_key:
                found = True
                continue
            filtered_lines.append(line)
        except json.JSONDecodeError:
            # Keep malformed lines to avoid data loss
            filtered_lines.append(line)

    if not found:
        logger.info("No matching record found for s3_key=%s (idempotent)", s3_key)
        return

    updated_content = "\n".join(filtered_lines)
    if updated_content:
        updated_content += "\n"

    s3_client.put_object(
        Bucket=METADATA_BUCKET,
        Key=jsonl_key,
        Body=updated_content.encode("utf-8"),
        ContentType="application/jsonlines",
    )
    logger.info("Removed record for %s from %s", s3_key, jsonl_key)


def _delete_thumbnail(s3_key: str) -> None:
    """Delete the thumbnail for the given S3 key."""
    try:
        s3_client.delete_object(Bucket=THUMBNAIL_BUCKET, Key=s3_key)
        logger.info("Thumbnail deleted: s3://%s/%s", THUMBNAIL_BUCKET, s3_key)
    except Exception:
        logger.warning("Failed to delete thumbnail for %s (may not exist)", s3_key, exc_info=True)
