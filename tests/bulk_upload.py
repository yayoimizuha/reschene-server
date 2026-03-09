"""Bulk upload JPEG images to Reschene via Presigned URL API.

Usage:
    uv run python tests/bulk_upload.py

Requires COGNITO_TOKEN env var or fetches one automatically.
"""

import glob
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests

API_ENDPOINT = "https://gylct49hy6.execute-api.us-east-1.amazonaws.com"
USER_POOL_ID = "us-east-1_CEAxv1pIt"
CLIENT_ID = "7155g9onuvp3tu50hs2n1dpufl"
USERNAME = "testuser@reschene.example.com"
PASSWORD = "TestPass123!"

JPEG_DIR = os.path.join(os.path.dirname(__file__), "..", "グラウンド前_jpeg")
MAX_WORKERS = 20


def get_token() -> str:
    """Get Cognito ID token."""
    token = os.environ.get("COGNITO_TOKEN")
    if token:
        return token

    client = boto3.client("cognito-idp", region_name="us-east-1")
    response = client.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        ClientId=CLIENT_ID,
        AuthParameters={"USERNAME": USERNAME, "PASSWORD": PASSWORD},
    )
    return response["AuthenticationResult"]["IdToken"]


def get_presigned_urls(token: str, filenames: list[str]) -> dict:
    """Get presigned URLs for a batch of files."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"files": [{"filename": fn} for fn in filenames]}

    resp = requests.post(f"{API_ENDPOINT}/upload/presigned-url", headers=headers, json=payload)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to get presigned URLs: {resp.status_code} {resp.text}")

    data = resp.json()
    url_map = {}
    for item in data.get("urls", []):
        url_map[item["filename"]] = item
    return data.get("upload_id", "N/A"), url_map


def upload_one(filepath: str, presigned_url: str) -> tuple[str, int | None, str | None]:
    """Upload a single image. Returns (filename, size, error)."""
    filename = os.path.basename(filepath)
    try:
        with open(filepath, "rb") as f:
            file_data = f.read()
        put_resp = requests.put(presigned_url, data=file_data, headers={"Content-Type": "image/jpeg"})
        if put_resp.status_code not in (200, 204):
            return filename, None, f"HTTP {put_resp.status_code}"
        return filename, len(file_data), None
    except Exception as e:
        return filename, None, str(e)


def main():
    jpeg_files = sorted(glob.glob(os.path.join(JPEG_DIR, "*.jpg")))
    if not jpeg_files:
        print("No JPEG files found in", JPEG_DIR)
        sys.exit(1)

    print(f"Found {len(jpeg_files)} JPEG files")

    token = get_token()
    print("Token acquired")

    filenames = [os.path.basename(fp) for fp in jpeg_files]
    print(f"Requesting presigned URLs for {len(filenames)} files...")
    upload_id, url_map = get_presigned_urls(token, filenames)
    print(f"Got {len(url_map)} presigned URLs  (upload_id: {upload_id})")

    # Build work items
    work = []
    for filepath in jpeg_files:
        fn = os.path.basename(filepath)
        info = url_map.get(fn)
        if info:
            work.append((filepath, info["presigned_url"]))

    success_count = 0
    fail_count = 0
    total_bytes = 0
    start_time = time.time()
    done = 0

    print(f"Uploading with {MAX_WORKERS} parallel workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(upload_one, fp, url): fp for fp, url in work}
        for future in as_completed(futures):
            done += 1
            filename, size, error = future.result()
            elapsed = time.time() - start_time
            if error:
                fail_count += 1
                print(f"  [{done}/{len(work)}] FAIL: {filename} - {error}  [{elapsed:.1f}s]")
            else:
                success_count += 1
                total_bytes += size or 0
                # Print progress every 10 files or on the last one
                if done % 10 == 0 or done == len(work):
                    print(f"  [{done}/{len(work)}] {success_count} ok / {fail_count} fail  [{elapsed:.1f}s]")

    total_time = time.time() - start_time
    print(f"\nDone in {total_time:.1f}s: {success_count} success, {fail_count} failed, {total_bytes:,} bytes total")
    print(f"Upload ID: {upload_id}")


if __name__ == "__main__":
    main()
