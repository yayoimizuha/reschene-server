"""Presigned URL Lambda handler.

Generates presigned PUT URLs for S3 image uploads.
"""

import json
import os
import uuid

import boto3
from botocore.config import Config

IMAGE_BUCKET = os.environ["IMAGE_BUCKET"]
PRESIGNED_EXPIRY = int(os.environ.get("PRESIGNED_EXPIRY", "900"))

s3_client = boto3.client("s3", config=Config(signature_version="s3v4"))


def handler(event, context):
    """Handle POST /upload/presigned-url."""
    # Extract user_id from JWT claims (set by API Gateway Cognito authorizer)
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        return _response(401, {"error": "Unauthorized: missing user identity"})

    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        return _response(400, {"error": "Invalid JSON body"})

    files = body.get("files", [])
    if not files:
        return _response(400, {"error": "No files specified"})

    # Generate a single upload_id (UUID v6-like, using uuid7 for time-ordered)
    # Python 3.12 doesn't have uuid6, use uuid4 with timestamp prefix approach
    upload_id = _generate_upload_id()

    urls = []
    for file_info in files:
        filename = file_info.get("filename", "")
        if not filename:
            continue

        s3_key = f"{user_id}/{upload_id}/{filename}"
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": IMAGE_BUCKET,
                "Key": s3_key,
                "ContentType": "image/jpeg",
            },
            ExpiresIn=PRESIGNED_EXPIRY,
        )
        urls.append(
            {
                "filename": filename,
                "presigned_url": presigned_url,
                "s3_key": s3_key,
            }
        )

    return _response(200, {"upload_id": upload_id, "urls": urls})


def _generate_upload_id() -> str:
    """Generate a time-ordered UUID for upload grouping."""
    # uuid7 is available in Python 3.12 — but not guaranteed in Lambda runtime
    # Fall back to uuid4 if uuid7 is not available
    try:
        return str(uuid.uuid7())
    except AttributeError:
        return str(uuid.uuid4())


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
