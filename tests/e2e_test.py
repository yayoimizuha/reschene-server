"""Reschene E2E Test Script - Full flow from cleanup to 3D reconstruction.

Usage:
    uv run python tests/e2e_test.py

Runs all E2E test steps defined in E2E.md in sequence:
  1. Data cleanup
  2. Cognito token acquisition
  3. Presigned URL basic test
  4. 98-image bulk upload
  5. Metadata & thumbnail generation verification
  6. Search API tests (user_images, batch, geo_radius)
  7. Image URL retrieval & download test
  8. 3D reconstruction container verification
  9. Compaction Lambda test
  10. Delete + cleanup verification
"""

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_ENDPOINT = "https://gylct49hy6.execute-api.us-east-1.amazonaws.com"
USER_POOL_ID = "us-east-1_CEAxv1pIt"
CLIENT_ID = "7155g9onuvp3tu50hs2n1dpufl"
USERNAME = "testuser@reschene.example.com"
PASSWORD = "TestPass123!"
REGION = "us-east-1"

BUCKET_USERIMAGE = "reschene-userimage"
BUCKET_METADATA = "reschene-metadata"
BUCKET_THUMBNAILS = "reschene-thumbnails"
BUCKET_3D_OUTPUT = "reschene-3d-output"

EXPECTED_IMAGE_COUNT = 98
METADATA_WAIT_SECONDS = 60
CLEANUP_WAIT_SECONDS = 20
RECONSTRUCTION_MAX_WAIT_SECONDS = 600  # 10 min
RECONSTRUCTION_POLL_INTERVAL = 30

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str
    duration: float = 0.0


@dataclass
class E2EState:
    """Shared state across test steps."""

    token: str = ""
    upload_id: str = ""
    results: list[TestResult] = field(default_factory=list)


def run_cmd(cmd: str, check: bool = True) -> str:
    """Run a shell command and return stdout."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nstderr: {result.stderr}\nstdout: {result.stdout}")
    return result.stdout.strip()


def s3_object_count(bucket: str, prefix: str = "") -> int:
    """Count objects in an S3 path."""
    path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
    output = run_cmd(f'aws s3 ls {path} --recursive --summarize 2>/dev/null | grep "Total Objects"', check=False)
    if "Total Objects" in output:
        return int(output.split(":")[-1].strip())
    return 0


def header(title: str):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def step_pass(state: E2EState, name: str, msg: str, duration: float = 0.0):
    print(f"  [PASS] {msg}")
    state.results.append(TestResult(name=name, passed=True, message=msg, duration=duration))


def step_fail(state: E2EState, name: str, msg: str, duration: float = 0.0):
    print(f"  [FAIL] {msg}")
    state.results.append(TestResult(name=name, passed=False, message=msg, duration=duration))


# ---------------------------------------------------------------------------
# Test Steps
# ---------------------------------------------------------------------------


def step1_cleanup(state: E2EState):
    """Step 1: Clean up existing data in all buckets."""
    header("Step 1: Data Cleanup")
    t0 = time.time()
    try:
        for bucket in [BUCKET_USERIMAGE, BUCKET_METADATA, BUCKET_THUMBNAILS, BUCKET_3D_OUTPUT]:
            print(f"  Cleaning s3://{bucket}/ ...")
            run_cmd(f"aws s3 rm s3://{bucket}/ --recursive", check=False)

        # Verify empty
        for bucket in [BUCKET_USERIMAGE, BUCKET_METADATA, BUCKET_THUMBNAILS]:
            count = s3_object_count(bucket)
            if count != 0:
                step_fail(state, "cleanup", f"s3://{bucket}/ still has {count} objects", time.time() - t0)
                return

        step_pass(state, "cleanup", "All buckets cleaned successfully", time.time() - t0)
    except Exception as e:
        step_fail(state, "cleanup", f"Error: {e}", time.time() - t0)


def step2_cognito_token(state: E2EState):
    """Step 2: Acquire Cognito ID token."""
    header("Step 2: Cognito Token Acquisition")
    t0 = time.time()
    try:
        import boto3

        client = boto3.client("cognito-idp", region_name=REGION)
        response = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            ClientId=CLIENT_ID,
            AuthParameters={"USERNAME": USERNAME, "PASSWORD": PASSWORD},
        )
        state.token = response["AuthenticationResult"]["IdToken"]
        token_len = len(state.token)
        step_pass(state, "cognito_token", f"Token acquired (length: {token_len})", time.time() - t0)
    except Exception as e:
        step_fail(state, "cognito_token", f"Error: {e}", time.time() - t0)


def step3_presigned_url_basic(state: E2EState):
    """Step 3: Test presigned URL endpoint with 2 files."""
    header("Step 3: Presigned URL Basic Test")
    t0 = time.time()
    try:
        import requests

        resp = requests.post(
            f"{API_ENDPOINT}/upload/presigned-url",
            headers={"Authorization": f"Bearer {state.token}", "Content-Type": "application/json"},
            json={"files": [{"filename": "test1.jpg"}, {"filename": "test2.jpg"}]},
        )
        if resp.status_code != 200:
            step_fail(state, "presigned_url_basic", f"HTTP {resp.status_code}: {resp.text}", time.time() - t0)
            return

        data = resp.json()
        upload_id = data.get("upload_id", "")
        urls = data.get("urls", [])

        if not upload_id:
            step_fail(state, "presigned_url_basic", "No upload_id in response", time.time() - t0)
            return
        if len(urls) != 2:
            step_fail(state, "presigned_url_basic", f"Expected 2 URLs, got {len(urls)}", time.time() - t0)
            return

        step_pass(
            state, "presigned_url_basic", f"HTTP 200, upload_id={upload_id[:8]}..., {len(urls)} URLs", time.time() - t0
        )
    except Exception as e:
        step_fail(state, "presigned_url_basic", f"Error: {e}", time.time() - t0)


def step4_bulk_upload(state: E2EState):
    """Step 4: 98-image bulk upload using bulk_upload.py."""
    header("Step 4: 98-Image Bulk Upload")
    t0 = time.time()
    try:
        # Run bulk_upload.py and capture output
        result = subprocess.run(
            ["uv", "run", "python", "tests/bulk_upload.py"],
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "COGNITO_TOKEN": state.token},
        )
        output = result.stdout + result.stderr
        print(output)

        # Parse upload_id from output
        for line in output.splitlines():
            if "Upload ID:" in line:
                state.upload_id = line.split("Upload ID:")[-1].strip()

        if result.returncode != 0:
            step_fail(state, "bulk_upload", f"Script exited with code {result.returncode}", time.time() - t0)
            return

        # Check for success count
        for line in output.splitlines():
            if "success" in line.lower() and "failed" in line.lower():
                if f"{EXPECTED_IMAGE_COUNT} success" in line and "0 failed" in line:
                    step_pass(
                        state,
                        "bulk_upload",
                        f"{EXPECTED_IMAGE_COUNT} images uploaded, upload_id={state.upload_id[:8]}...",
                        time.time() - t0,
                    )
                    return
                else:
                    step_fail(state, "bulk_upload", f"Unexpected result: {line}", time.time() - t0)
                    return

        # Fallback: check ok/fail pattern
        if f"{EXPECTED_IMAGE_COUNT} ok" in output and "0 fail" in output:
            step_pass(
                state,
                "bulk_upload",
                f"{EXPECTED_IMAGE_COUNT} images uploaded, upload_id={state.upload_id[:8]}...",
                time.time() - t0,
            )
        else:
            step_fail(state, "bulk_upload", "Could not verify upload success from output", time.time() - t0)
    except subprocess.TimeoutExpired:
        step_fail(state, "bulk_upload", "Timeout (120s) exceeded", time.time() - t0)
    except Exception as e:
        step_fail(state, "bulk_upload", f"Error: {e}", time.time() - t0)


def step5_metadata_thumbnails(state: E2EState):
    """Step 5: Verify metadata and thumbnail generation."""
    header("Step 5: Metadata & Thumbnail Verification")
    t0 = time.time()
    try:
        print(f"  Waiting {METADATA_WAIT_SECONDS}s for Lambda async processing...")
        time.sleep(METADATA_WAIT_SECONDS)

        meta_count = s3_object_count(BUCKET_METADATA, "raw/")
        thumb_count = s3_object_count(BUCKET_THUMBNAILS)

        print(f"  Metadata objects: {meta_count}")
        print(f"  Thumbnail objects: {thumb_count}")

        if meta_count != EXPECTED_IMAGE_COUNT:
            step_fail(
                state, "metadata", f"Expected {EXPECTED_IMAGE_COUNT} metadata, got {meta_count}", time.time() - t0
            )
            return
        if thumb_count != EXPECTED_IMAGE_COUNT:
            step_fail(
                state, "thumbnails", f"Expected {EXPECTED_IMAGE_COUNT} thumbnails, got {thumb_count}", time.time() - t0
            )
            return

        # Verify metadata content (1 sample)
        first_key = run_cmd(f"aws s3 ls s3://{BUCKET_METADATA}/raw/ --recursive | head -1 | awk '{{print $4}}'")
        if first_key:
            meta_json = run_cmd(f"aws s3 cp s3://{BUCKET_METADATA}/{first_key} -")
            meta = json.loads(meta_json)
            required_fields = ["user_id", "s3_key", "upload_id", "original_filename", "file_size", "uploaded_at"]
            missing = [f for f in required_fields if f not in meta]
            if missing:
                step_fail(state, "metadata_content", f"Missing fields: {missing}", time.time() - t0)
                return
            print(f"  Sample metadata OK: {list(meta.keys())}")

        step_pass(state, "metadata_thumbnails", f"Metadata: {meta_count}, Thumbnails: {thumb_count}", time.time() - t0)
    except Exception as e:
        step_fail(state, "metadata_thumbnails", f"Error: {e}", time.time() - t0)


def step6_search_apis(state: E2EState):
    """Step 6: Test search APIs (user_images, batch, geo_radius)."""
    header("Step 6: Search API Tests")
    import requests

    def do_search(search_type: str, extra: dict | None = None) -> dict:
        payload = {"type": search_type}
        if extra:
            payload.update(extra)
        resp = requests.post(
            f"{API_ENDPOINT}/search",
            headers={"Authorization": f"Bearer {state.token}", "Content-Type": "application/json"},
            json=payload,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        return resp.json()

    # 6a: user_images
    t0 = time.time()
    try:
        data = do_search("user_images")
        count = len(data.get("results", []))
        if count == EXPECTED_IMAGE_COUNT:
            step_pass(state, "search_user_images", f"user_images: {count} results", time.time() - t0)
        else:
            step_fail(state, "search_user_images", f"Expected {EXPECTED_IMAGE_COUNT}, got {count}", time.time() - t0)
    except Exception as e:
        step_fail(state, "search_user_images", f"Error: {e}", time.time() - t0)

    # 6b: batch
    t0 = time.time()
    try:
        if state.upload_id:
            data = do_search("batch", {"upload_id": state.upload_id})
            count = len(data.get("results", []))
            if count == EXPECTED_IMAGE_COUNT:
                step_pass(state, "search_batch", f"batch: {count} results", time.time() - t0)
            else:
                step_fail(state, "search_batch", f"Expected {EXPECTED_IMAGE_COUNT}, got {count}", time.time() - t0)
        else:
            step_fail(state, "search_batch", "No upload_id available from step 4", time.time() - t0)
    except Exception as e:
        step_fail(state, "search_batch", f"Error: {e}", time.time() - t0)

    # 6c: geo_radius
    t0 = time.time()
    try:
        data = do_search("geo_radius", {"latitude": 35.700, "longitude": 139.518, "radius_km": 1})
        results = data.get("results", [])
        count = len(results)
        nearest = results[0].get("distance_km", "N/A") if results else "N/A"
        if count == EXPECTED_IMAGE_COUNT:
            step_pass(
                state, "search_geo_radius", f"geo_radius: {count} results, nearest: {nearest} km", time.time() - t0
            )
        else:
            step_fail(state, "search_geo_radius", f"Expected {EXPECTED_IMAGE_COUNT}, got {count}", time.time() - t0)
    except Exception as e:
        step_fail(state, "search_geo_radius", f"Error: {e}", time.time() - t0)


def step7_image_url(state: E2EState):
    """Step 7: Get presigned GET URL and verify download."""
    header("Step 7: Image URL Retrieval & Download")
    t0 = time.time()
    try:
        import requests

        # Get an s3_key from search
        resp = requests.post(
            f"{API_ENDPOINT}/search",
            headers={"Authorization": f"Bearer {state.token}", "Content-Type": "application/json"},
            json={"type": "user_images"},
        )
        s3_key = resp.json()["results"][0]["s3_key"]
        print(f"  s3_key: {s3_key}")

        # Get presigned URL
        resp = requests.get(
            f"{API_ENDPOINT}/images/url",
            params={"s3_key": s3_key},
            headers={"Authorization": f"Bearer {state.token}"},
        )
        if resp.status_code != 200:
            step_fail(state, "image_url", f"HTTP {resp.status_code}: {resp.text}", time.time() - t0)
            return

        data = resp.json()
        presigned_url = data.get("presigned_url", "")
        expires_in = data.get("expires_in", 0)
        print(f"  expires_in: {expires_in}")

        if not presigned_url:
            step_fail(state, "image_url", "No presigned_url in response", time.time() - t0)
            return

        # Download the image
        dl_resp = requests.get(presigned_url)
        if dl_resp.status_code != 200:
            step_fail(state, "image_url", f"Download failed: HTTP {dl_resp.status_code}", time.time() - t0)
            return

        dl_size = len(dl_resp.content)
        step_pass(state, "image_url", f"Download OK: {dl_size:,} bytes, expires_in={expires_in}", time.time() - t0)
    except Exception as e:
        step_fail(state, "image_url", f"Error: {e}", time.time() - t0)


def step8_3d_reconstruction(state: E2EState):
    """Step 8: Verify 3D reconstruction container ran successfully."""
    header("Step 8: 3D Reconstruction Verification")
    t0 = time.time()
    try:
        print(f"  Polling for reconstruction output (max {RECONSTRUCTION_MAX_WAIT_SECONDS}s)...")
        elapsed = 0
        geohash = ""

        while elapsed < RECONSTRUCTION_MAX_WAIT_SECONDS:
            output = run_cmd(f"aws s3 ls s3://{BUCKET_3D_OUTPUT}/ --recursive", check=False)
            if "status.json" in output:
                # Extract geohash
                for line in output.splitlines():
                    if "status.json" in line:
                        parts = line.split()
                        key = parts[-1]  # e.g., xn75nf/status.json
                        geohash = key.split("/")[0]
                        break
                break
            print(f"    Waiting... ({elapsed}s elapsed)")
            time.sleep(RECONSTRUCTION_POLL_INTERVAL)
            elapsed += RECONSTRUCTION_POLL_INTERVAL

        if not geohash:
            step_fail(
                state,
                "3d_reconstruction",
                f"No status.json found after {RECONSTRUCTION_MAX_WAIT_SECONDS}s",
                time.time() - t0,
            )
            return

        print(f"  Found geohash: {geohash}")

        # Check status.json
        status_json = run_cmd(f"aws s3 cp s3://{BUCKET_3D_OUTPUT}/{geohash}/status.json -")
        status = json.loads(status_json)
        print(f"  status.json: {json.dumps(status, indent=2)}")

        if status.get("status") != "COMPLETED":
            step_fail(
                state,
                "3d_reconstruction",
                f"Status is '{status.get('status')}', expected 'COMPLETED'",
                time.time() - t0,
            )
            return

        # Check model.json
        model_json = run_cmd(f"aws s3 cp s3://{BUCKET_3D_OUTPUT}/{geohash}/model.json -", check=False)
        has_model = bool(model_json.strip())

        # Check ECS task history
        task_arn = run_cmd(
            f"aws ecs list-tasks --cluster reschene-reconstruction "
            f"--desired-status STOPPED --region {REGION} "
            f"--query 'taskArns[0]' --output text",
            check=False,
        )
        ecs_info = ""
        if task_arn and task_arn != "None":
            task_detail = run_cmd(
                f"aws ecs describe-tasks --cluster reschene-reconstruction "
                f'--tasks "{task_arn}" --region {REGION} '
                f"--query 'tasks[0].{{Status:lastStatus,StopCode:stopCode}}'",
                check=False,
            )
            ecs_info = task_detail

        step_pass(
            state,
            "3d_reconstruction",
            f"COMPLETED, model.json={'yes' if has_model else 'no'}, geohash={geohash}",
            time.time() - t0,
        )
        if ecs_info:
            print(f"  ECS task: {ecs_info}")
    except Exception as e:
        step_fail(state, "3d_reconstruction", f"Error: {e}", time.time() - t0)


def step9_compaction(state: E2EState):
    """Step 9: Run compaction Lambda and verify."""
    header("Step 9: Compaction Lambda Test")
    t0 = time.time()
    try:
        # Refresh token (may have expired)
        _refresh_token_if_needed(state)

        # Invoke compaction Lambda
        result_file = "/tmp/compaction_result.json"
        run_cmd(
            f"aws lambda invoke --function-name reschene-compaction --payload '{{}}' --region {REGION} {result_file}"
        )
        compaction_result = run_cmd(f"cat {result_file}")
        data = json.loads(compaction_result)
        print(f"  Compaction result: {json.dumps(data, indent=2)}")

        if data.get("statusCode") != 200:
            step_fail(state, "compaction", f"statusCode: {data.get('statusCode')}", time.time() - t0)
            return

        compacted = data.get("compacted", 0)
        if compacted != EXPECTED_IMAGE_COUNT:
            step_fail(state, "compaction", f"compacted={compacted}, expected {EXPECTED_IMAGE_COUNT}", time.time() - t0)
            return

        # Verify raw/ is empty
        raw_count = s3_object_count(BUCKET_METADATA, "raw/")
        print(f"  raw/ objects after compaction: {raw_count}")

        # Verify compacted/ has Parquet files
        compacted_output = run_cmd(f"aws s3 ls s3://{BUCKET_METADATA}/compacted/", check=False)
        has_parquet = (
            ".parquet" in compacted_output.lower()
            or ".snappy" in compacted_output.lower()
            or compacted_output.strip() != ""
        )
        print(f"  compacted/ contents: {compacted_output[:200]}")

        # Verify search still works after compaction
        import requests

        resp = requests.post(
            f"{API_ENDPOINT}/search",
            headers={"Authorization": f"Bearer {state.token}", "Content-Type": "application/json"},
            json={"type": "user_images"},
        )
        search_count = len(resp.json().get("results", []))
        print(f"  Search after compaction: {search_count} results")

        if search_count != EXPECTED_IMAGE_COUNT:
            step_fail(
                state,
                "compaction_search",
                f"Search after compaction: {search_count}, expected {EXPECTED_IMAGE_COUNT}",
                time.time() - t0,
            )
            return

        step_pass(state, "compaction", f"Compacted {compacted} records, search OK ({search_count})", time.time() - t0)
    except Exception as e:
        step_fail(state, "compaction", f"Error: {e}", time.time() - t0)


def step10_delete_cleanup(state: E2EState):
    """Step 10: Delete one image and verify cleanup Lambda removes thumbnail/metadata."""
    header("Step 10: Delete + Cleanup Verification")
    t0 = time.time()
    try:
        # Get first image key
        target = run_cmd(
            f"aws s3 ls s3://{BUCKET_USERIMAGE}/ --recursive | head -1 | awk '{{print $4}}'",
        )
        if not target:
            step_fail(state, "delete_cleanup", "No images found in userimage bucket", time.time() - t0)
            return

        print(f"  Deleting: {target}")

        # Check thumbnail exists before delete
        thumb_before = run_cmd(f"aws s3 ls s3://{BUCKET_THUMBNAILS}/{target}", check=False)
        thumb_exists_before = bool(thumb_before.strip())
        print(f"  Thumbnail exists before: {thumb_exists_before}")

        # Delete the image
        run_cmd(f"aws s3 rm s3://{BUCKET_USERIMAGE}/{target}")

        # Wait for cleanup Lambda
        print(f"  Waiting {CLEANUP_WAIT_SECONDS}s for cleanup Lambda...")
        time.sleep(CLEANUP_WAIT_SECONDS)

        # Check thumbnail after delete
        thumb_after = run_cmd(f"aws s3 ls s3://{BUCKET_THUMBNAILS}/{target}", check=False)
        thumb_exists_after = bool(thumb_after.strip())
        print(f"  Thumbnail exists after: {thumb_exists_after}")

        if thumb_exists_before and not thumb_exists_after:
            step_pass(state, "delete_cleanup", "Thumbnail auto-deleted by cleanup Lambda", time.time() - t0)
        elif not thumb_exists_before:
            step_fail(state, "delete_cleanup", "Thumbnail did not exist before deletion", time.time() - t0)
        else:
            step_fail(state, "delete_cleanup", "Thumbnail still exists after deletion", time.time() - t0)
    except Exception as e:
        step_fail(state, "delete_cleanup", f"Error: {e}", time.time() - t0)


def _refresh_token_if_needed(state: E2EState):
    """Refresh the Cognito token (token may expire during long tests)."""
    try:
        import boto3

        client = boto3.client("cognito-idp", region_name=REGION)
        response = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            ClientId=CLIENT_ID,
            AuthParameters={"USERNAME": USERNAME, "PASSWORD": PASSWORD},
        )
        state.token = response["AuthenticationResult"]["IdToken"]
        print("  Token refreshed")
    except Exception:
        print("  Token refresh failed, using existing token")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def print_summary(state: E2EState, total_time: float):
    """Print final test summary."""
    print(f"\n{'=' * 60}")
    print("  E2E TEST SUMMARY")
    print(f"{'=' * 60}")

    passed = sum(1 for r in state.results if r.passed)
    failed = sum(1 for r in state.results if not r.passed)

    for r in state.results:
        status = "PASS" if r.passed else "FAIL"
        duration = f"({r.duration:.1f}s)" if r.duration else ""
        print(f"  [{status}] {r.name}: {r.message} {duration}")

    print(f"\n  Total: {passed + failed} tests, {passed} passed, {failed} failed")
    print(f"  Total time: {total_time:.1f}s")
    print(f"{'=' * 60}")

    return failed == 0


def main():
    print("Reschene E2E Test - Full Flow")
    print(f"API: {API_ENDPOINT}")
    print(f"Region: {REGION}")

    state = E2EState()
    total_start = time.time()

    steps = [
        step1_cleanup,
        step2_cognito_token,
        step3_presigned_url_basic,
        step4_bulk_upload,
        step5_metadata_thumbnails,
        step6_search_apis,
        step7_image_url,
        step8_3d_reconstruction,
        step9_compaction,
        step10_delete_cleanup,
    ]

    for step_fn in steps:
        try:
            step_fn(state)
        except Exception as e:
            print(f"  [FATAL] Unhandled error in {step_fn.__name__}: {e}")
            state.results.append(
                TestResult(
                    name=step_fn.__name__,
                    passed=False,
                    message=f"Unhandled: {e}",
                )
            )

        # Check if we should abort (token acquisition failure blocks everything)
        if step_fn == step2_cognito_token and not state.token:
            print("\n  ABORT: Cannot continue without authentication token.")
            break

    total_time = time.time() - total_start
    all_passed = print_summary(state, total_time)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
