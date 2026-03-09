"""Mock 3D reconstruction container.

This is a placeholder that simulates the 3D reconstruction pipeline.
It fetches images from S3, updates status.json, and writes dummy output.
The actual algorithm implementation will replace this file.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

IMAGE_BUCKET = os.environ.get("IMAGE_BUCKET", "")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")
CENTER_LATITUDE = float(os.environ.get("CENTER_LATITUDE", "0"))
CENTER_LONGITUDE = float(os.environ.get("CENTER_LONGITUDE", "0"))
RADIUS_KM = float(os.environ.get("RADIUS_KM", "1.0"))
OUTPUT_S3_PREFIX = os.environ.get("OUTPUT_S3_PREFIX", "")
REGION_KEY = os.environ.get("REGION_KEY", "")

s3_client = boto3.client("s3")


def update_status(status: str, phase: str | None = None, progress_pct: int = 0, error_message: str | None = None):
    """Update the status.json file in S3."""
    now = datetime.now(timezone.utc).isoformat()
    status_data = {
        "region_key": REGION_KEY,
        "status": status,
        "phase": phase,
        "progress_pct": progress_pct,
        "center_latitude": CENTER_LATITUDE,
        "center_longitude": CENTER_LONGITUDE,
        "started_at": now,
        "updated_at": now,
        "completed_at": now if status in ("COMPLETED", "FAILED") else None,
        "output_s3_prefix": OUTPUT_S3_PREFIX if status == "COMPLETED" else None,
        "error_message": error_message,
    }
    s3_client.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=f"{REGION_KEY}/status.json",
        Body=json.dumps(status_data).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Status updated: %s (phase=%s, progress=%d%%)", status, phase, progress_pct)


def main():
    logger.info("Mock 3D reconstruction starting")
    logger.info(
        "Region: %s, Center: (%s, %s), Radius: %s km",
        REGION_KEY,
        CENTER_LATITUDE,
        CENTER_LONGITUDE,
        RADIUS_KM,
    )

    try:
        # Phase 1: Update status to RUNNING
        update_status("RUNNING", phase="initialization", progress_pct=0)

        # Phase 2: Simulate image fetching
        logger.info("Simulating image fetch...")
        time.sleep(2)
        update_status("RUNNING", phase="image_fetch", progress_pct=25)

        # Phase 3: Simulate processing
        logger.info("Simulating 3D reconstruction processing...")
        time.sleep(3)
        update_status("RUNNING", phase="reconstruction", progress_pct=50)

        # Phase 4: Simulate output generation
        logger.info("Generating dummy output...")
        time.sleep(2)
        update_status("RUNNING", phase="output_generation", progress_pct=75)

        # Write dummy output file
        dummy_output = json.dumps(
            {
                "type": "mock_reconstruction",
                "region_key": REGION_KEY,
                "center": {"latitude": CENTER_LATITUDE, "longitude": CENTER_LONGITUDE},
                "message": "This is a mock 3D reconstruction output. Replace with actual algorithm.",
            }
        )
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=f"{REGION_KEY}/model.json",
            Body=dummy_output.encode("utf-8"),
            ContentType="application/json",
        )

        # Phase 5: Complete
        update_status("COMPLETED", phase="done", progress_pct=100)
        logger.info("Mock 3D reconstruction completed successfully")

        # Clean up lock file
        try:
            s3_client.delete_object(Bucket=OUTPUT_BUCKET, Key=f"{REGION_KEY}/lock.json")
            logger.info("Lock file deleted")
        except Exception:
            logger.warning("Failed to delete lock file", exc_info=True)

    except Exception as e:
        logger.error("Mock 3D reconstruction failed: %s", e, exc_info=True)
        update_status("FAILED", error_message=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
