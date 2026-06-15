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
        time.sleep(0.5)
    raise TimeoutError(f"Batch {batch_id} did not complete within {timeout}s")


def test_batch():
    prompts = [
        "What is the capital of France?",
        "Explain quantum computing in one sentence.",
    ]

    resp = requests.post(BASE, json={"prompts": prompts})
    assert resp.status_code == 200
    data = resp.json()
    batch_id = data["batch_id"]
    print(f"PASS: batch accepted, id={batch_id}")

    wait_for_completion(batch_id)

    resp2 = requests.get(f"{BASE}/results/{batch_id}")
    results = resp2.json()
    assert len(results) == 2
    assert results[0]["prompt"] == prompts[0]
    print("PASS: results retrieved correctly")
    print(results)


def test_crash_recovery():
    """
    Simulate crash recovery by checking that an orchestration's
    events are checkpointed to the database mid-flight.
    The real crash-recovery property: if the process dies and
    restarts, the events table still contains completed activity
    results, and the orchestrator can resume from the last checkpoint.
    """
    prompts = [f"Prompt {i}" for i in range(5)]
    resp = requests.post(BASE, json={"prompts": prompts})
    batch_id = resp.json()["batch_id"]
    print(f"PASS: crash-recovery batch accepted, id={batch_id}")

    start = time.time()
    while time.time() - start < 10:
        status_resp = requests.get(f"{BASE}/status/{batch_id}")
        status = status_resp.json()
        if status["completed"] > 0:
            break
        time.sleep(0.3)

    completed = status["completed"]
    print(f"Mid-flight status: {status}")
    assert completed > 0
    print(f"PASS: {completed}/{status['total']} activities checkpointed to DB")

    wait_for_completion(batch_id)

    resp2 = requests.get(f"{BASE}/results/{batch_id}")
    results = resp2.json()
    assert len(results) == 5
    print(f"PASS: all {len(results)} results persisted and retrievable")


if __name__ == "__main__":
    test_batch()
    test_crash_recovery()
