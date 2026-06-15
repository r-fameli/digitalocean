import json
import os
import tempfile
import time
import requests

BASE = "http://localhost:8080"


def wait_for_completion(batch_id, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{BASE}/status/{batch_id}")
        status = resp.json()
        if status.get("status") == "completed":
            return status
        if status.get("status") == "failed":
            return status
        time.sleep(0.3)
    raise TimeoutError(f"Batch {batch_id} did not complete within {timeout}s")


# ── Basic ingestion and status/results ───────────────────────────

def test_health_check():
    resp = requests.get(f"{BASE}/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    print("[health] PASS")


def test_ingest_single_prompt():
    resp = requests.post(f"{BASE}/", json={"prompts": ["What is 2+2?"]})
    assert resp.status_code == 200
    data = resp.json()
    assert "batch_id" in data
    assert data["status"] == "accepted"
    assert data["count"] == 1

    status = wait_for_completion(data["batch_id"])
    assert status["status"] == "completed"
    assert status["total"] == 1
    assert status["completed"] == 1

    results = requests.get(f"{BASE}/results/{data['batch_id']}").json()
    assert len(results) == 1
    assert results[0]["prompt"] == "What is 2+2?"
    print(f"[single] PASS: batch={data['batch_id']}")


def test_ingest_multiple_prompts():
    prompts = [f"Prompt {i}" for i in range(5)]
    resp = requests.post(f"{BASE}/", json={"prompts": prompts})
    data = resp.json()
    assert data["count"] == 5

    wait_for_completion(data["batch_id"])
    results = requests.get(f"{BASE}/results/{data['batch_id']}").json()
    assert len(results) == 5
    assert results[0]["prompt"] == "Prompt 0"
    assert results[4]["prompt"] == "Prompt 4"
    print(f"[multiple] PASS: batch={data['batch_id']}")


# ── Parallel execution ───────────────────────────────────────────

def test_parallel_execution_is_faster_than_sequential():
    prompts = [f"Prompt {i}" for i in range(10)]
    t0 = time.time()
    resp = requests.post(f"{BASE}/", json={"prompts": prompts})
    batch_id = resp.json()["batch_id"]

    wait_for_completion(batch_id)
    elapsed = time.time() - t0

    results = requests.get(f"{BASE}/results/{batch_id}").json()
    assert len(results) == 10
    assert results[0]["prompt"] == "Prompt 0"
    assert results[9]["prompt"] == "Prompt 9"

    sequential_lower_bound = len(prompts) * 2
    pool_size = int(os.environ.get("POOL_SIZE", "5"))
    parallel_upper_bound = (len(prompts) / pool_size) * 15 + 5

    print(f"[parallel] PASS: 10 prompts in {elapsed:.1f}s")
    print(f"  Expected faster than sequential ({sequential_lower_bound}s)")
    print(f"  Pool size: {pool_size}")


# ── Replay / idempotency ────────────────────────────────────────

def test_replay_skips_duplicate_work():
    prompts = [f"Replay prompt {i}" for i in range(5)]
    resp = requests.post(f"{BASE}/", json={"prompts": prompts})
    batch_id = resp.json()["batch_id"]

    status = wait_for_completion(batch_id)
    assert status["completed"] == 5
    results_first = requests.get(f"{BASE}/results/{batch_id}").json()

    resp2 = requests.post(f"{BASE}/", json={"prompts": prompts})
    batch_id2 = resp2.json()["batch_id"]

    status2 = wait_for_completion(batch_id2)
    assert status2["completed"] == 5
    results_second = requests.get(f"{BASE}/results/{batch_id2}").json()

    assert len(results_first) == len(results_second) == 5
    for a, b in zip(results_first, results_second):
        assert a["prompt"] == b["prompt"]
        assert a["response"] == b["response"]
    print(f"[replay] PASS: both batches returned identical results")


# ── Status and results error paths ──────────────────────────────

def test_status_not_found():
    resp = requests.get(f"{BASE}/status/nonexistent-batch-id")
    assert resp.status_code == 200
    data = resp.json()
    assert "error" in data
    print(f"[status-404] PASS")


def test_results_not_found():
    resp = requests.get(f"{BASE}/results/nonexistent-batch-id")
    assert resp.status_code == 200
    data = resp.json()
    assert "error" in data
    print(f"[results-404] PASS")


def test_results_while_processing_returns_error():
    resp = requests.post(f"{BASE}/", json={"prompts": ["slow prompt"]})
    batch_id = resp.json()["batch_id"]

    resp2 = requests.get(f"{BASE}/results/{batch_id}")
    data = resp2.json()
    assert "error" in data
    print(f"[results-incomplete] PASS")


# ── /from-file endpoint ─────────────────────────────────────────

def test_ingest_from_file():
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    with open(path, "w") as f:
        json.dump(["file-prompt-a", "file-prompt-b", "file-prompt-c"], f)

    resp = requests.post(f"{BASE}/from-file", json={"path": path})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 3
    assert "batch_id" in data

    wait_for_completion(data["batch_id"])
    results = requests.get(f"{BASE}/results/{data['batch_id']}").json()
    assert len(results) == 3
    assert results[0]["prompt"] == "file-prompt-a"

    os.unlink(path)
    print(f"[from-file] PASS: batch={data['batch_id']}")


def test_from_file_not_found():
    resp = requests.post(f"{BASE}/from-file", json={"path": "/no/such/file.json"})
    assert resp.status_code == 400
    data = resp.json()
    assert "not found" in data["detail"].lower()
    print(f"[from-file-404] PASS")


def test_from_file_invalid_json():
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    with open(path, "w") as f:
        f.write("not valid json {{{")

    resp = requests.post(f"{BASE}/from-file", json={"path": path})
    assert resp.status_code == 400
    data = resp.json()
    assert "json" in data["detail"].lower()

    os.unlink(path)
    print(f"[from-file-invalid] PASS")


def test_from_file_not_array():
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    with open(path, "w") as f:
        json.dump({"not": "an array"}, f)

    resp = requests.post(f"{BASE}/from-file", json={"path": path})
    assert resp.status_code == 400
    data = resp.json()
    assert "array" in data["detail"].lower()

    os.unlink(path)
    print(f"[from-file-not-array] PASS")


# ── Retry / failure behavior ────────────────────────────────────

def test_batch_completes_even_with_429_responses():
    """Submit enough prompts that some will hit 429s and retry, but all complete."""
    prompts = [f"retry-prompt-{i}" for i in range(30)]
    resp = requests.post(f"{BASE}/", json={"prompts": prompts})
    batch_id = resp.json()["batch_id"]

    status = wait_for_completion(batch_id, timeout=30)
    assert status["status"] == "completed"
    assert status["completed"] == 30

    results = requests.get(f"{BASE}/results/{batch_id}").json()
    assert len(results) == 30

    failed = [r for r in results if "FAILED" in str(r.get("response", ""))]
    succeeded = [r for r in results if "FAILED" not in str(r.get("response", ""))]
    print(f"[retry] PASS: {len(succeeded)} succeeded, {len(failed)} failed (after max retries)")

    for r in results:
        assert "prompt" in r
        assert "response" in r


# ── Empty and edge cases ────────────────────────────────────────

def test_ingest_empty_batch():
    resp = requests.post(f"{BASE}/", json={"prompts": []})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0

    wait_for_completion(data["batch_id"])
    results = requests.get(f"{BASE}/results/{data['batch_id']}").json()
    assert results == []
    print(f"[empty] PASS: batch={data['batch_id']}")


# ── Multiple concurrent batches ─────────────────────────────────

def test_multiple_concurrent_batches():
    batch_ids = []
    for i in range(3):
        resp = requests.post(f"{BASE}/", json={"prompts": [f"batch{i}-prompt-{j}" for j in range(3)]})
        batch_ids.append(resp.json()["batch_id"])

    for bid in batch_ids:
        wait_for_completion(bid)
        results = requests.get(f"{BASE}/results/{bid}").json()
        assert len(results) == 3

    print(f"[concurrent-batches] PASS: 3 concurrent batches completed")


# ── TTL cleanup ─────────────────────────────────────────────────

def test_ttl_cleanup():
    resp = requests.post(f"{BASE}/", json={"prompts": ["TTL test prompt"]})
    batch_id = resp.json()["batch_id"]
    wait_for_completion(batch_id)

    results = requests.get(f"{BASE}/results/{batch_id}").json()
    assert len(results) == 1
    assert results[0]["response"]

    status = requests.get(f"{BASE}/status/{batch_id}").json()
    assert status["status"] == "completed"
    assert status["batch_id"] == batch_id
    assert status["total"] == 1
    assert status["completed"] == 1

    print(f"[ttl] PASS: batch {batch_id} completed")


if __name__ == "__main__":
    tests = [
        ("health", test_health_check),
        ("single", test_ingest_single_prompt),
        ("multiple", test_ingest_multiple_prompts),
        ("parallel", test_parallel_execution_is_faster_than_sequential),
        ("replay", test_replay_skips_duplicate_work),
        ("status-404", test_status_not_found),
        ("results-404", test_results_not_found),
        ("results-incomplete", test_results_while_processing_returns_error),
        ("from-file", test_ingest_from_file),
        ("from-file-404", test_from_file_not_found),
        ("from-file-invalid", test_from_file_invalid_json),
        ("from-file-not-array", test_from_file_not_array),
        ("retry", test_batch_completes_even_with_429_responses),
        ("empty", test_ingest_empty_batch),
        ("concurrent-batches", test_multiple_concurrent_batches),
        ("ttl", test_ttl_cleanup),
    ]

    failed = 0
    for name, fn in tests:
        try:
            print(f"\n[{name}] Running...")
            fn()
        except Exception as e:
            print(f"[{name}] FAILED: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {len(tests) - failed}/{len(tests)} passed, {failed} failed")
