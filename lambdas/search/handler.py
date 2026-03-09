"""Search Lambda handler.

Handles POST /search requests. Builds Athena SQL based on search type
and returns results.
"""

import json
import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
GLUE_TABLE = os.environ["GLUE_TABLE"]
ATHENA_RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]

athena_client = boto3.client("athena")

# Max wait time for Athena query (seconds)
QUERY_TIMEOUT = 30
POLL_INTERVAL = 0.5


def handler(event, context):
    """Handle POST /search."""
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    caller_user_id = claims.get("sub")
    if not caller_user_id:
        return _response(401, {"error": "Unauthorized"})

    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        return _response(400, {"error": "Invalid JSON body"})

    search_type = body.get("type")
    if not search_type:
        return _response(400, {"error": "Missing 'type' field"})

    try:
        sql = _build_query(search_type, body, caller_user_id)
    except ValueError as e:
        return _response(400, {"error": str(e)})

    try:
        results = _execute_athena_query(sql)
    except TimeoutError:
        return _response(504, {"error": "Query timed out"})
    except Exception as e:
        logger.error("Athena query failed", exc_info=True)
        return _response(500, {"error": f"Query execution failed: {str(e)}"})

    return _response(200, {"results": results})


def _build_query(search_type: str, body: dict, caller_user_id: str) -> str:
    """Build Athena SQL based on search type."""
    table = f'"{GLUE_DATABASE}"."{GLUE_TABLE}"'

    if search_type == "geo_radius":
        lat = body.get("latitude")
        lon = body.get("longitude")
        radius_km = body.get("radius_km", 5)
        if lat is None or lon is None:
            raise ValueError("geo_radius requires 'latitude' and 'longitude'")
        radius_m = radius_km * 1000
        return f"""
            SELECT
                s3_key, user_id, uploaded_at,
                gps_latitude, gps_longitude,
                ST_DISTANCE(
                    to_spherical_geography(ST_POINT(gps_longitude, gps_latitude)),
                    to_spherical_geography(ST_POINT({lon}, {lat}))
                ) / 1000.0 AS distance_km
            FROM {table}
            WHERE gps_latitude IS NOT NULL
              AND gps_longitude IS NOT NULL
              AND ST_DISTANCE(
                    to_spherical_geography(ST_POINT(gps_longitude, gps_latitude)),
                    to_spherical_geography(ST_POINT({lon}, {lat}))
                  ) <= {radius_m}
            ORDER BY distance_km ASC
        """

    elif search_type == "user_images":
        user_id = body.get("user_id", caller_user_id)
        return f"""
            SELECT s3_key, original_filename, uploaded_at, gps_latitude, gps_longitude
            FROM {table}
            WHERE user_id = '{user_id}'
            ORDER BY uploaded_at DESC
        """

    elif search_type == "batch":
        upload_id = body.get("upload_id")
        if not upload_id:
            raise ValueError("batch requires 'upload_id'")
        return f"""
            SELECT s3_key, original_filename, uploaded_at
            FROM {table}
            WHERE user_id = '{caller_user_id}'
              AND upload_id = '{upload_id}'
            ORDER BY original_filename
        """

    else:
        raise ValueError(f"Unknown search type: {search_type}")


def _execute_athena_query(sql: str) -> list[dict]:
    """Execute an Athena query and return results as a list of dicts."""
    response = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_execution_id = response["QueryExecutionId"]

    # Poll for completion
    elapsed = 0.0
    while elapsed < QUERY_TIMEOUT:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Query {state}: {reason}")

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    else:
        raise TimeoutError("Athena query timed out")

    # Fetch results
    paginator = athena_client.get_paginator("get_query_results")
    rows = []
    columns = []

    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        result_set = page["ResultSet"]
        if not columns:
            columns = [col["Name"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]]

        for i, row in enumerate(result_set["Rows"]):
            # Skip header row
            if not rows and i == 0:
                continue
            values = [datum.get("VarCharValue") for datum in row["Data"]]
            rows.append(dict(zip(columns, values)))

    return rows


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
