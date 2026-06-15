import time
import requests

BASE = "http://localhost:8080"


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

    time.sleep(0.5)

    resp2 = requests.get(f"{BASE}/results/{batch_id}")
    results = resp2.json()
    assert len(results) == 2
    assert results[0]["prompt"] == prompts[0]
    print("PASS: results retrieved correctly")
    print(results)


if __name__ == "__main__":
    test_batch()
