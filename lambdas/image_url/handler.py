"""Image URL Lambda handler.

Handles GET /images/{s3_key_encoded}/url.
Generates a presigned GET URL for image viewing.
"""

import json
import os
from urllib.parse import unquote

import boto3
from botocore.config import Config

IMAGE_BUCKET = os.environ["IMAGE_BUCKET"]
PRESIGNED_EXPIRY = int(os.environ.get("PRESIGNED_EXPIRY", "86400"))  # 24 hours

s3_client = boto3.client("s3", config=Config(signature_version="s3v4"))


def handler(event, context):
    """Handle GET /images/url?s3_key=..."""
    # Verify authentication
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        return _response(401, {"error": "Unauthorized"})

    # Extract s3_key from query string parameter
    query_params = event.get("queryStringParameters") or {}
    s3_key = query_params.get("s3_key", "")
    if not s3_key:
        return _response(400, {"error": "Missing s3_key query parameter"})

    # Generate presigned GET URL (no ownership check — by design)
    presigned_url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": IMAGE_BUCKET,
            "Key": s3_key,
        },
        ExpiresIn=PRESIGNED_EXPIRY,
    )

    return _response(
        200,
        {
            "s3_key": s3_key,
            "presigned_url": presigned_url,
            "expires_in": PRESIGNED_EXPIRY,
        },
    )


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
