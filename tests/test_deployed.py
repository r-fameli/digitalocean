"""Test a deployed Batch Inference Engine instance.

Usage:
    python tests/test_deployed.py https://batch-inference-engine-snwih.ondigitalocean.app

Or set TEST_BASE_URL env var:
    TEST_BASE_URL=https://my-app.ondigitalocean.app python tests/test_deployed.py
"""

import os
import sys
import time
import requests

BASE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("TEST_BASE_URL", "http://localhost:8080")
print(f"Testing: {BASE}\n")


def wait_for_completion(batch_id, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{BASE}/status/{batch_id}", timeout=10)
        status = resp.json()
        if status.get("status") in ("completed", "failed"):
            return status
        time.sleep(0.5)
    raise TimeoutError(f"Batch {batch_id} did not complete within {timeout}s")


def run_test(name, fn):
    try:
        fn()
        print(f"  PASS  {name}")
        return True
    except Exception as e:
        print(f"  FAIL  {name}: {e}")
        return False


def check_health():
    resp = requests.get(f"{BASE}/", timeout=10)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def check_single_prompt():
    resp = requests.post(f"{BASE}/", json={"prompts": ["What is 2+2?"]}, timeout=10)
    assert resp.status_code == 200
    data = resp.json()
    batch_id = data["batch_id"]
    assert data["status"] == "accepted"
    assert data["count"] == 1

    wait_for_completion(batch_id)

    results = requests.get(f"{BASE}/results/{batch_id}", timeout=10).json()
    assert len(results) == 1
    assert results[0]["prompt"] == "What is 2+2?"


def check_multiple_prompts():
    prompts = [f"Prompt {i}" for i in range(5)]
    resp = requests.post(f"{BASE}/", json={"prompts": prompts}, timeout=10)
    data = resp.json()
    assert data["count"] == 5

    wait_for_completion(data["batch_id"])
    results = requests.get(f"{BASE}/results/{data['batch_id']}", timeout=10).json()
    assert len(results) == 5
    assert results[0]["prompt"] == "Prompt 0"
    assert results[4]["prompt"] == "Prompt 4"


def check_parallel_execution():
    prompts = [f"P{i}" for i in range(10)]
    t0 = time.time()
    resp = requests.post(f"{BASE}/", json={"prompts": prompts}, timeout=10)
    batch_id = resp.json()["batch_id"]

    wait_for_completion(batch_id)
    elapsed = time.time() - t0

    results = requests.get(f"{BASE}/results/{batch_id}", timeout=10).json()
    assert len(results) == 10
    assert results[0]["prompt"] == "P0"
    assert results[9]["prompt"] == "P9"
    print(f"          (10 prompts in {elapsed:.1f}s)")


def check_status_404():
    resp = requests.get(f"{BASE}/status/nonexistent", timeout=10)
    assert resp.status_code == 200
    assert "error" in resp.json()


def check_results_404():
    resp = requests.get(f"{BASE}/results/nonexistent", timeout=10)
    assert resp.status_code == 200
    assert "error" in resp.json()


def check_results_while_processing():
    resp = requests.post(f"{BASE}/", json={"prompts": ["slow"]}, timeout=10)
    batch_id = resp.json()["batch_id"]
    resp2 = requests.get(f"{BASE}/results/{batch_id}", timeout=10)
    assert "error" in resp2.json()


def check_empty_batch():
    resp = requests.post(f"{BASE}/", json={"prompts": []}, timeout=10)
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0

    wait_for_completion(data["batch_id"])
    results = requests.get(f"{BASE}/results/{data['batch_id']}", timeout=10).json()
    assert results == []


def check_concurrent_batches():
    batch_ids = []
    for i in range(3):
        resp = requests.post(f"{BASE}/", json={"prompts": [f"b{i}-p{j}" for j in range(3)]}, timeout=10)
        batch_ids.append(resp.json()["batch_id"])

    for bid in batch_ids:
        wait_for_completion(bid)
        results = requests.get(f"{BASE}/results/{bid}", timeout=10).json()
        assert len(results) == 3


def check_429_retry():
    """Submit 30 prompts — some will hit 429s but all should complete."""
    prompts = [f"retry-{i}" for i in range(30)]
    resp = requests.post(f"{BASE}/", json={"prompts": prompts}, timeout=10)
    batch_id = resp.json()["batch_id"]

    status = wait_for_completion(batch_id, timeout=45)
    assert status["status"] == "completed"
    assert status["completed"] == 30

    results = requests.get(f"{BASE}/results/{batch_id}", timeout=10).json()
    assert len(results) == 30

    failed = [r for r in results if "FAILED" in str(r.get("response", ""))]
    succeeded = len(results) - len(failed)
    print(f"          ({succeeded} succeeded, {len(failed)} failed after max retries)")


def check_status_shows_progress():
    prompts = [f"prog-{i}" for i in range(10)]
    resp = requests.post(f"{BASE}/", json={"prompts": prompts}, timeout=10)
    batch_id = resp.json()["batch_id"]

    time.sleep(1)
    status = requests.get(f"{BASE}/status/{batch_id}", timeout=10).json()
    assert "completed" in status
    assert "total" in status
    assert status["total"] == 10
    assert "batch_id" in status


if __name__ == "__main__":
    tests = [
        ("health", check_health),
        ("single prompt", check_single_prompt),
        ("multiple prompts", check_multiple_prompts),
        ("parallel execution", check_parallel_execution),
        ("status 404", check_status_404),
        ("results 404", check_results_404),
        ("results while processing", check_results_while_processing),
        ("empty batch", check_empty_batch),
        ("concurrent batches", check_concurrent_batches),
        ("429 retry (30 prompts)", check_429_retry),
        ("status progress", check_status_shows_progress),
    ]

    passed = 0
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed}/{len(tests)} passed, {len(tests) - passed} failed")
    sys.exit(0 if passed == len(tests) else 1)
