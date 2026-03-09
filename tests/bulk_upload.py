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
import uuid

import boto3
import requests

API_ENDPOINT = "https://gylct49hy6.execute-api.us-east-1.amazonaws.com"
USER_POOL_ID = "us-east-1_CEAxv1pIt"
CLIENT_ID = "7155g9onuvp3tu50hs2n1dpufl"
USERNAME = "testuser@reschene.example.com"
PASSWORD = "TestPass123!"

JPEG_DIR = os.path.join(os.path.dirname(__file__), "..", "グラウンド前_jpeg")


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
    # Build map: filename -> {presigned_url, s3_key}
    url_map = {}
    for item in data.get("urls", []):
        url_map[item["filename"]] = item
    return url_map


def upload_image(presigned_url: str, filepath: str) -> int:
    """Upload a single image using presigned URL."""
    with open(filepath, "rb") as f:
        file_data = f.read()

    put_resp = requests.put(presigned_url, data=file_data, headers={"Content-Type": "image/jpeg"})
    if put_resp.status_code not in (200, 204):
        raise RuntimeError(f"Upload failed: {put_resp.status_code} {put_resp.text}")

    return len(file_data)


def main():
    jpeg_files = sorted(glob.glob(os.path.join(JPEG_DIR, "*.jpg")))
    if not jpeg_files:
        print("No JPEG files found in", JPEG_DIR)
        sys.exit(1)

    print(f"Found {len(jpeg_files)} JPEG files")

    token = get_token()
    print("Token acquired")

    # Get presigned URLs for all files in one API call
    filenames = [os.path.basename(fp) for fp in jpeg_files]
    print(f"Requesting presigned URLs for {len(filenames)} files...")
    url_map = get_presigned_urls(token, filenames)
    print(f"Got {len(url_map)} presigned URLs")

    success_count = 0
    fail_count = 0
    start_time = time.time()

    for i, filepath in enumerate(jpeg_files):
        filename = os.path.basename(filepath)
        try:
            info = url_map.get(filename)
            if not info:
                raise RuntimeError(f"No presigned URL for {filename}")
            size = upload_image(info["presigned_url"], filepath)
            success_count += 1
            elapsed = time.time() - start_time
            print(f"  [{i + 1}/{len(jpeg_files)}] OK: {filename} ({size:,} bytes) [{elapsed:.1f}s]")
        except Exception as e:
            fail_count += 1
            print(f"  [{i + 1}/{len(jpeg_files)}] FAIL: {filename} - {e}")

        # Small delay to avoid throttling
        if (i + 1) % 10 == 0:
            time.sleep(0.5)

    total_time = time.time() - start_time
    print(f"\nDone in {total_time:.1f}s: {success_count} success, {fail_count} failed")
    print(
        f"Upload ID from API: {url_map.get(filenames[0], {}).get('s3_key', 'N/A').split('/')[1] if url_map else 'N/A'}"
    )


if __name__ == "__main__":
    main()
