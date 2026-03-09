"""Metadata extraction Lambda handler.

Triggered by S3 ObjectCreated events. Extracts EXIF metadata,
generates thumbnails, writes per-image metadata JSON to S3,
and optionally invokes the 3D reconstruction judge Lambda.

Metadata is written as individual JSON files under:
    raw/{user_id}/{upload_id}/{filename}.metadata.json

A separate compaction job periodically merges raw files into
Parquet for efficient Athena queries.
"""

import io
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

IMAGE_BUCKET = os.environ["IMAGE_BUCKET"]
METADATA_BUCKET = os.environ["METADATA_BUCKET"]
THUMBNAIL_BUCKET = os.environ["THUMBNAIL_BUCKET"]
RECONSTRUCTION_JUDGE_FUNCTION_ARN = os.environ.get("RECONSTRUCTION_JUDGE_FUNCTION_ARN", "")

s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")


def handler(event, context):
    """Process S3 ObjectCreated events."""
    for record in event.get("Records", []):
        s3_key = unquote_plus(record["s3"]["object"]["key"])
        bucket = record["s3"]["bucket"]["name"]
        file_size = record["s3"]["object"].get("size", 0)

        logger.info("Processing image: s3://%s/%s", bucket, s3_key)

        # Parse key: {user_id}/{upload_id}/{filename}
        parts = s3_key.split("/", 2)
        if len(parts) < 3:
            logger.error("Unexpected S3 key format: %s", s3_key)
            return

        user_id = parts[0]
        upload_id = parts[1]
        original_filename = parts[2]

        # Download image
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        image_bytes = response["Body"].read()

        # Extract EXIF
        exif_data = _extract_exif(image_bytes)

        # Generate thumbnail
        _generate_thumbnail(image_bytes, s3_key)

        # Build metadata record (user_id included for cross-user Athena queries)
        metadata = {
            "user_id": user_id,
            "s3_key": s3_key,
            "upload_id": upload_id,
            "original_filename": original_filename,
            "file_size": file_size,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "camera_make": exif_data.get("camera_make"),
            "camera_model": exif_data.get("camera_model"),
            "datetime_original": exif_data.get("datetime_original"),
            "gps_latitude": exif_data.get("gps_latitude"),
            "gps_longitude": exif_data.get("gps_longitude"),
            "gps_altitude": exif_data.get("gps_altitude"),
        }

        # Write metadata as individual file (race-condition-free)
        _write_metadata(user_id, upload_id, original_filename, metadata)

        # Invoke 3D reconstruction judge (async) if GPS data present
        if (
            RECONSTRUCTION_JUDGE_FUNCTION_ARN
            and metadata["gps_latitude"] is not None
            and metadata["gps_longitude"] is not None
        ):
            _invoke_reconstruction_judge(s3_key, user_id, metadata["gps_latitude"], metadata["gps_longitude"])

    return {"statusCode": 200}


def _extract_exif(image_bytes: bytes) -> dict:
    """Extract EXIF data from JPEG image bytes.

    Returns dict with nullable fields. On failure, returns all nulls.
    """
    try:
        from PIL import Image
        from PIL.ExifTags import GPSTAGS, TAGS

        img = Image.open(io.BytesIO(image_bytes))
        exif_raw = img._getexif()
        if exif_raw is None:
            return {}

        exif = {}
        for tag_id, value in exif_raw.items():
            tag_name = TAGS.get(tag_id, tag_id)
            exif[tag_name] = value

        result = {
            "camera_make": exif.get("Make"),
            "camera_model": exif.get("Model"),
            "datetime_original": None,
            "gps_latitude": None,
            "gps_longitude": None,
            "gps_altitude": None,
        }

        # Parse datetime
        dt_orig = exif.get("DateTimeOriginal")
        if dt_orig:
            try:
                dt = datetime.strptime(dt_orig, "%Y:%m:%d %H:%M:%S")
                result["datetime_original"] = dt.isoformat()
            except ValueError:
                pass

        # Parse GPS
        gps_info = exif.get("GPSInfo")
        if gps_info:
            gps = {}
            for key, val in gps_info.items():
                gps_tag = GPSTAGS.get(key, key)
                gps[gps_tag] = val

            lat = _convert_gps_coord(gps.get("GPSLatitude"), gps.get("GPSLatitudeRef"))
            lon = _convert_gps_coord(gps.get("GPSLongitude"), gps.get("GPSLongitudeRef"))
            alt = gps.get("GPSAltitude")

            result["gps_latitude"] = lat
            result["gps_longitude"] = lon
            if alt is not None:
                try:
                    result["gps_altitude"] = float(alt)
                except (TypeError, ValueError):
                    pass

        return result

    except Exception:
        logger.warning("EXIF extraction failed", exc_info=True)
        return {}


def _convert_gps_coord(coord, ref) -> float | None:
    """Convert GPS coordinate from EXIF format to decimal degrees."""
    if coord is None or ref is None:
        return None
    try:
        degrees = float(coord[0])
        minutes = float(coord[1])
        seconds = float(coord[2])
        decimal = degrees + minutes / 60.0 + seconds / 3600.0
        if ref in ("S", "W"):
            decimal = -decimal
        return decimal
    except (TypeError, IndexError, ValueError):
        return None


def _generate_thumbnail(image_bytes: bytes, s3_key: str) -> None:
    """Generate a thumbnail (short side 512px, JPEG quality 85) and upload to thumbnail bucket."""
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes))

        # Calculate target size: short side = 512px, maintain aspect ratio
        width, height = img.size
        if width <= 512 and height <= 512:
            # Image is already small enough
            target_size = (width, height)
        elif width < height:
            target_size = (512, int(height * 512 / width))
        else:
            target_size = (int(width * 512 / height), 512)

        img.thumbnail(target_size, Image.LANCZOS)

        # Convert to RGB if necessary (e.g., RGBA or P mode)
        if img.mode not in ("RGB",):
            img = img.convert("RGB")

        # Save to buffer
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        buf.seek(0)

        s3_client.put_object(
            Bucket=THUMBNAIL_BUCKET,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType="image/jpeg",
        )
        logger.info("Thumbnail uploaded: s3://%s/%s", THUMBNAIL_BUCKET, s3_key)

    except Exception:
        logger.warning("Thumbnail generation failed for %s", s3_key, exc_info=True)


def _write_metadata(user_id: str, upload_id: str, filename: str, metadata: dict) -> None:
    """Write a metadata record as an individual JSON file.

    Key format: raw/{user_id}/{upload_id}/{filename}.metadata.json

    Each Lambda writes to a unique key, so there is zero contention.
    A separate compaction job merges these into Parquet periodically.
    """
    key = f"raw/{user_id}/{upload_id}/{filename}.metadata.json"
    body = json.dumps(metadata, ensure_ascii=False) + "\n"
    s3_client.put_object(
        Bucket=METADATA_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Metadata written to s3://%s/%s", METADATA_BUCKET, key)


def _invoke_reconstruction_judge(s3_key: str, user_id: str, lat: float, lon: float) -> None:
    """Invoke the 3D reconstruction judge Lambda asynchronously."""
    payload = {
        "s3_key": s3_key,
        "user_id": user_id,
        "gps_latitude": lat,
        "gps_longitude": lon,
    }
    try:
        lambda_client.invoke(
            FunctionName=RECONSTRUCTION_JUDGE_FUNCTION_ARN,
            InvocationType="Event",  # Async invocation
            Payload=json.dumps(payload).encode("utf-8"),
        )
        logger.info("Reconstruction judge invoked asynchronously for %s", s3_key)
    except Exception:
        logger.error("Failed to invoke reconstruction judge", exc_info=True)
