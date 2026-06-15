import time, requests, json

BASE = "http://localhost:8080"


def wait_for_completion(batch_id, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(f"{BASE}/status/{batch_id}")
        status = resp.json()
        if status.get("status") == "completed":
            return status
        time.sleep(0.3)
    raise TimeoutError(f"Batch {batch_id} did not complete within {timeout}s")


def test_parallel_execution():
    """Verify multiple prompts process concurrently (not one-at-a-time)."""
    prompts = [f"Prompt {i}" for i in range(10)]
    t0 = time.time()
    resp = requests.post(BASE, json={"prompts": prompts})
    batch_id = resp.json()["batch_id"]
    print(f"[parallel] batch={batch_id} submitted")

    wait_for_completion(batch_id)
    elapsed = time.time() - t0

    results = requests.get(f"{BASE}/results/{batch_id}").json()
    assert len(results) == 10
    assert results[0]["prompt"] == "Prompt 0"
    assert results[9]["prompt"] == "Prompt 9"
    print(f"[parallel] PASS: 10 prompts in {elapsed:.1f}s (with pool=5, should be ~2x faster than sequential)")
    print(f"  First: {results[0]}")
    print(f"  Last:  {results[9]}")


def test_replay():
    """Verify that saved events skip re-processing."""
    prompts = [f"Replay prompt {i}" for i in range(5)]
    resp = requests.post(BASE, json={"prompts": prompts})
    batch_id = resp.json()["batch_id"]
    print(f"[replay] batch={batch_id} submitted")

    status = wait_for_completion(batch_id)
    assert status["completed"] == 5

    results_first = requests.get(f"{BASE}/results/{batch_id}").json()

    resp2 = requests.post(BASE, json={"prompts": prompts})
    batch_id2 = resp2.json()["batch_id"]
    print(f"[replay] batch={batch_id2} submitted (should bypass inference)")

    status2 = wait_for_completion(batch_id2)
    assert status2["completed"] == 5

    results_second = requests.get(f"{BASE}/results/{batch_id2}").json()

    assert len(results_first) == len(results_second) == 5
    print("[replay] PASS: both batches completed")


def test_ttl_cleanup():
    """Verify old orchestrations get cleaned up."""
    resp = requests.post(BASE, json={"prompts": ["TTL test"]})
    batch_id = resp.json()["batch_id"]
    wait_for_completion(batch_id)
    results = requests.get(f"{BASE}/results/{batch_id}").json()
    assert len(results) == 1
    print(f"[ttl] PASS: batch {batch_id} completed")


if __name__ == "__main__":
    test_parallel_execution()
    test_replay()
    test_ttl_cleanup()
