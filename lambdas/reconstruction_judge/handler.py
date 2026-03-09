"""3D Reconstruction Judge Lambda handler.

Invoked asynchronously from metadata extraction Lambda.
Checks if sufficient GPS-tagged images exist in a region and
triggers an ECS task for 3D reconstruction.
"""

import hashlib
import json
import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RECONSTRUCTION_RADIUS_KM = float(os.environ.get("RECONSTRUCTION_RADIUS_KM", "1.0"))
RECONSTRUCTION_THRESHOLD = int(os.environ.get("RECONSTRUCTION_THRESHOLD", "50"))
ECS_CLUSTER_ARN = os.environ["ECS_CLUSTER_ARN"]
ECS_TASK_DEFINITION_ARN = os.environ["ECS_TASK_DEFINITION_ARN"]
ECS_SUBNET_IDS = os.environ["ECS_SUBNET_IDS"]  # comma-separated
ECS_SECURITY_GROUP_IDS = os.environ["ECS_SECURITY_GROUP_IDS"]  # comma-separated
ECS_CAPACITY_PROVIDER = os.environ["ECS_CAPACITY_PROVIDER"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
IMAGE_BUCKET = os.environ["IMAGE_BUCKET"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_TABLE = os.environ["GLUE_TABLE"]

athena_client = boto3.client("athena")
ecs_client = boto3.client("ecs")
s3_client = boto3.client("s3")

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
ATHENA_POLL_INTERVAL = 0.5
ATHENA_TIMEOUT = 30


def handler(event, context):
    """Judge whether to start 3D reconstruction."""
    lat = event.get("gps_latitude")
    lon = event.get("gps_longitude")
    s3_key = event.get("s3_key")
    user_id = event.get("user_id")

    if lat is None or lon is None:
        logger.info("No GPS data, skipping: %s", s3_key)
        return

    if not user_id:
        logger.error("No user_id in event, skipping: %s", s3_key)
        return

    logger.info("Judging reconstruction for lat=%s, lon=%s (triggered by %s)", lat, lon, s3_key)

    # Generate region_key (Geohash)
    precision = _radius_to_geohash_precision(RECONSTRUCTION_RADIUS_KM)
    region_key = _encode_geohash(lat, lon, precision)
    logger.info("Region key (geohash): %s (precision=%d)", region_key, precision)

    # Count images in region using Athena (cross-user: counts ALL users' images)
    radius_m = RECONSTRUCTION_RADIUS_KM * 1000
    image_count = _count_images_in_region(lat, lon, radius_m)
    logger.info("Image count in region: %d (threshold: %d)", image_count, RECONSTRUCTION_THRESHOLD)

    if image_count < RECONSTRUCTION_THRESHOLD:
        logger.info("Below threshold, skipping reconstruction")
        return

    # Try to acquire lock
    if not _acquire_lock(region_key, lat, lon):
        logger.info("Lock already held for region %s, skipping", region_key)
        return

    # Start ECS task
    try:
        task_arn = _start_ecs_task(region_key, lat, lon)
        logger.info("ECS task started: %s", task_arn)

        # Update lock with task ARN
        _update_lock_with_task_arn(region_key, lat, lon, task_arn)

        # Create initial status.json
        _create_status_file(region_key, task_arn, lat, lon)

    except Exception:
        # Release lock on failure
        _release_lock(region_key)
        raise

    return {"region_key": region_key, "task_arn": task_arn}


def _count_images_in_region(lat: float, lon: float, radius_m: float) -> int:
    """Count GPS-tagged images within radius using Athena (cross-user)."""
    table = f'"{GLUE_DATABASE}"."{GLUE_TABLE}"'
    sql = f"""
        SELECT COUNT(*) AS image_count
        FROM {table}
        WHERE gps_latitude IS NOT NULL
          AND gps_longitude IS NOT NULL
          AND ST_DISTANCE(
                to_spherical_geography(ST_POINT(gps_longitude, gps_latitude)),
                to_spherical_geography(ST_POINT({lon}, {lat}))
              ) <= {radius_m}
    """

    response = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_id = response["QueryExecutionId"]

    # Poll for completion
    elapsed = 0.0
    while elapsed < ATHENA_TIMEOUT:
        result = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(ATHENA_POLL_INTERVAL)
        elapsed += ATHENA_POLL_INTERVAL
    else:
        raise TimeoutError("Athena query timed out")

    # Get result
    result = athena_client.get_query_results(QueryExecutionId=query_id)
    rows = result["ResultSet"]["Rows"]
    if len(rows) < 2:
        return 0
    return int(rows[1]["Data"][0]["VarCharValue"])


def _acquire_lock(region_key: str, lat: float, lon: float) -> bool:
    """Acquire a lock using S3 conditional PUT (If-None-Match: *)."""
    lock_key = f"{region_key}/lock.json"
    lock_body = json.dumps(
        {
            "center_latitude": lat,
            "center_longitude": lon,
            "started_at": _now_iso(),
        }
    )

    try:
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=lock_key,
            Body=lock_body.encode("utf-8"),
            ContentType="application/json",
            IfNoneMatch="*",
        )
        return True
    except s3_client.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "PreconditionFailed":
            # Lock exists — check if the existing task is still running
            return _try_reclaim_lock(region_key, lat, lon)
        raise


def _try_reclaim_lock(region_key: str, lat: float, lon: float) -> bool:
    """Check if the locked task is still running; if stopped, reclaim the lock."""
    lock_key = f"{region_key}/lock.json"
    try:
        response = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=lock_key)
        lock_data = json.loads(response["Body"].read().decode("utf-8"))
    except Exception:
        return False

    task_arn = lock_data.get("task_arn")
    if not task_arn:
        # Lock without task_arn means it was never started — delete and retry
        _release_lock(region_key)
        return _acquire_lock(region_key, lat, lon)

    # Check ECS task status
    try:
        result = ecs_client.describe_tasks(cluster=ECS_CLUSTER_ARN, tasks=[task_arn])
        tasks = result.get("tasks", [])
        if not tasks or tasks[0]["lastStatus"] == "STOPPED":
            _release_lock(region_key)
            return _acquire_lock(region_key, lat, lon)
    except Exception:
        logger.warning("Failed to check task status for %s", task_arn, exc_info=True)

    return False


def _release_lock(region_key: str) -> None:
    """Delete the lock file."""
    try:
        s3_client.delete_object(Bucket=OUTPUT_BUCKET, Key=f"{region_key}/lock.json")
    except Exception:
        logger.warning("Failed to release lock for %s", region_key, exc_info=True)


def _update_lock_with_task_arn(region_key: str, lat: float, lon: float, task_arn: str) -> None:
    """Update lock file with the started task ARN."""
    lock_key = f"{region_key}/lock.json"
    lock_body = json.dumps(
        {
            "task_arn": task_arn,
            "center_latitude": lat,
            "center_longitude": lon,
            "started_at": _now_iso(),
        }
    )
    s3_client.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=lock_key,
        Body=lock_body.encode("utf-8"),
        ContentType="application/json",
    )


def _start_ecs_task(region_key: str, lat: float, lon: float) -> str:
    """Start an ECS task for 3D reconstruction."""
    subnets = [s.strip() for s in ECS_SUBNET_IDS.split(",")]
    security_groups = [s.strip() for s in ECS_SECURITY_GROUP_IDS.split(",")]
    output_prefix = f"s3://{OUTPUT_BUCKET}/{region_key}/"

    response = ecs_client.run_task(
        cluster=ECS_CLUSTER_ARN,
        taskDefinition=ECS_TASK_DEFINITION_ARN,
        capacityProviderStrategy=[
            {
                "capacityProvider": ECS_CAPACITY_PROVIDER,
                "weight": 1,
                "base": 1,
            }
        ],
        count=1,
        overrides={
            "containerOverrides": [
                {
                    "name": "reconstruction",
                    "environment": [
                        {"name": "CENTER_LATITUDE", "value": str(lat)},
                        {"name": "CENTER_LONGITUDE", "value": str(lon)},
                        {"name": "RADIUS_KM", "value": str(RECONSTRUCTION_RADIUS_KM)},
                        {"name": "OUTPUT_S3_PREFIX", "value": output_prefix},
                        {"name": "IMAGE_BUCKET", "value": IMAGE_BUCKET},
                        {"name": "OUTPUT_BUCKET", "value": OUTPUT_BUCKET},
                        {"name": "REGION_KEY", "value": region_key},
                    ],
                }
            ]
        },
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "DISABLED",
            }
        },
    )

    tasks = response.get("tasks", [])
    if not tasks:
        failures = response.get("failures", [])
        raise RuntimeError(f"Failed to start ECS task: {failures}")

    return tasks[0]["taskArn"]


def _create_status_file(region_key: str, task_arn: str, lat: float, lon: float) -> None:
    """Create the initial status.json for the reconstruction job."""
    status = {
        "region_key": region_key,
        "task_arn": task_arn,
        "status": "PENDING",
        "phase": None,
        "progress_pct": 0,
        "center_latitude": lat,
        "center_longitude": lon,
        "started_at": _now_iso(),
        "updated_at": _now_iso(),
        "completed_at": None,
        "output_s3_prefix": None,
        "error_message": None,
    }
    s3_client.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=f"{region_key}/status.json",
        Body=json.dumps(status).encode("utf-8"),
        ContentType="application/json",
    )


def _encode_geohash(lat: float, lon: float, precision: int) -> str:
    """Encode latitude/longitude to a Geohash string."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    geohash = []
    bit = 0
    ch = 0
    even = True

    while len(geohash) < precision:
        if even:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                ch |= 1 << (4 - bit)
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                ch |= 1 << (4 - bit)
                lat_range[0] = mid
            else:
                lat_range[1] = mid

        even = not even
        bit += 1

        if bit == 5:
            geohash.append(GEOHASH_BASE32[ch])
            bit = 0
            ch = 0

    return "".join(geohash)


def _radius_to_geohash_precision(radius_km: float) -> int:
    """Map radius to appropriate Geohash precision."""
    # Approximate: precision 6 ~ +-0.61km, precision 5 ~ +-2.4km, precision 4 ~ +-20km
    if radius_km <= 1.0:
        return 6
    elif radius_km <= 5.0:
        return 5
    elif radius_km <= 20.0:
        return 4
    else:
        return 3


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
