import json
import os
import tempfile
import time
import threading
from unittest.mock import patch, MagicMock
from durable import DurableStore, _execute_activity, run_orchestrator


def _tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _cleanup(path):
    try:
        os.unlink(path)
    except OSError:
        pass


# ── DurableStore: basic lifecycle ──────────────────────────────────

def test_create_and_complete_orchestration():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("test-1", ["hello", "world"])

    status = store.get_status("test-1")
    assert status["batch_id"] == "test-1"
    assert status["total"] == 2
    assert status["completed"] == 0
    assert status["status"] == "running"
    assert "created_at" in status
    assert "updated_at" in status

    store.save_event("test-1", 0, "hello", {"prompt": "hello", "response": "hi!"})
    store.save_event("test-1", 1, "world", {"prompt": "world", "response": "hey!"})
    store.complete_orchestration("test-1", [
        {"prompt": "hello", "response": "hi!"},
        {"prompt": "world", "response": "hey!"},
    ])

    status = store.get_status("test-1")
    assert status["completed"] == 2
    assert status["status"] == "completed"

    result = store.get_result("test-1")
    assert len(result) == 2
    assert result[0]["prompt"] == "hello"
    assert result[1]["response"] == "hey!"

    _cleanup(db)


def test_fail_orchestration():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("fail-1", ["a"])
    store.fail_orchestration("fail-1", "something went wrong")

    status = store.get_status("fail-1")
    assert status["status"] == "failed"

    result = store.get_result("fail-1")
    assert result is None

    _cleanup(db)


def test_missing_orchestration():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    assert store.get_status("nonexistent") is None
    assert store.get_result("nonexistent") is None
    assert store.get_event("nonexistent", 0) is None
    _cleanup(db)


# ── DurableStore: events ──────────────────────────────────────────

def test_get_event_found():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("ev-1", ["x"])
    store.save_event("ev-1", 0, "x", {"prompt": "x", "response": "ok"})

    ev = store.get_event("ev-1", 0)
    assert ev is not None
    assert ev["prompt"] == "x"
    assert ev["response"] == "ok"
    _cleanup(db)


def test_get_event_missing_prompt_index():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("ev-2", ["a", "b"])
    store.save_event("ev-2", 0, "a", {"response": "A"})

    assert store.get_event("ev-2", 0) is not None
    assert store.get_event("ev-2", 1) is None
    assert store.get_event("ev-2", 99) is None
    _cleanup(db)


def test_get_event_nonexistent_orchestration():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    assert store.get_event("no-such-id", 0) is None
    _cleanup(db)


def test_save_event_idempotent():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("idem-1", ["p"])

    store.save_event("idem-1", 0, "p", {"response": "first"})
    store.save_event("idem-1", 0, "p", {"response": "second"})

    ev = store.get_event("idem-1", 0)
    assert ev["response"] == "first"

    status = store.get_status("idem-1")
    assert status["completed"] == 1
    _cleanup(db)


def test_save_event_updates_timestamp():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("ts-1", ["x"])

    s1 = store.get_status("ts-1")
    time.sleep(0.01)
    store.save_event("ts-1", 0, "x", {"response": "ok"})
    s2 = store.get_status("ts-1")

    assert s2["updated_at"] > s1["updated_at"]
    _cleanup(db)


# ── DurableStore: get_result edge cases ──────────────────────────

def test_get_result_while_running():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("run-1", ["x"])
    store.save_event("run-1", 0, "x", {"response": "ok"})

    assert store.get_result("run-1") is None
    _cleanup(db)


def test_get_result_after_failure():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("fail-2", ["x"])
    store.fail_orchestration("fail-2", "boom")

    assert store.get_result("fail-2") is None
    _cleanup(db)


# ── DurableStore: status details ─────────────────────────────────

def test_status_empty_prompts():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("empty-1", [])

    status = store.get_status("empty-1")
    assert status["total"] == 0
    assert status["completed"] == 0
    assert status["status"] == "running"
    _cleanup(db)


def test_status_large_batch():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    prompts = [f"prompt-{i}" for i in range(100)]
    store.create_orchestration("big-1", prompts)

    status = store.get_status("big-1")
    assert status["total"] == 100
    assert status["completed"] == 0
    _cleanup(db)


# ── DurableStore: thread safety ──────────────────────────────────

def test_concurrent_event_writes():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    N = 30

    store.create_orchestration("conc-1", [f"p{i}" for i in range(N)])

    def write_event(i):
        store.save_event("conc-1", i, f"p{i}", {"response": f"r{i}"})

    threads = [threading.Thread(target=write_event, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    status = store.get_status("conc-1")
    assert status["completed"] == N

    for i in range(N):
        ev = store.get_event("conc-1", i)
        assert ev is not None
        assert ev["response"] == f"r{i}"

    _cleanup(db)


# ── DurableStore: cleanup ────────────────────────────────────────

def test_ttl_cleanup_removes_old():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("old-1", ["x"])

    assert store.get_status("old-1") is not None
    store.cleanup_old(ttl_days=0)
    assert store.get_status("old-1") is None
    _cleanup(db)


def test_ttl_cleanup_preserves_recent():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("recent-1", ["x"])

    store.cleanup_old(ttl_days=365)

    assert store.get_status("recent-1") is not None
    _cleanup(db)


def test_ttl_cleanup_also_removes_events():
    db = _tmp_db()
    store = DurableStore(db_path=db)
    store.create_orchestration("ev-old", ["x"])
    store.save_event("ev-old", 0, "x", {"response": "ok"})

    store.cleanup_old(ttl_days=0)

    assert store.get_event("ev-old", 0) is None
    assert store.get_status("ev-old") is None
    _cleanup(db)


# ── _execute_activity: happy path ────────────────────────────────

def test_execute_activity_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"prompt": "hello", "response": "world"}

    with patch("durable.requests.post", return_value=mock_resp):
        result = _execute_activity("hello", "http://mock/infer")

    assert result["prompt"] == "hello"
    assert result["response"] == "world"


# ── _execute_activity: 429 retry ─────────────────────────────────

def test_execute_activity_retry_on_429_then_succeed():
    mock_429 = MagicMock()
    mock_429.status_code = 429

    mock_ok = MagicMock()
    mock_ok.status_code = 200
    mock_ok.json.return_value = {"prompt": "p", "response": "ok"}

    with patch("durable.requests.post", side_effect=[mock_429, mock_ok]) as mock_post:
        with patch("durable.time.sleep") as mock_sleep:
            result = _execute_activity("p", "http://mock/infer")

    assert result["response"] == "ok"
    assert mock_post.call_count == 2
    assert mock_sleep.call_count == 1


def test_execute_activity_max_retries_exhausted():
    mock_429 = MagicMock()
    mock_429.status_code = 429

    with patch("durable.requests.post", return_value=mock_429) as mock_post:
        with patch("durable.time.sleep") as mock_sleep:
            result = _execute_activity("p", "http://mock/infer")

    assert result["response"] == "FAILED after max retries"
    assert mock_post.call_count == 5
    assert mock_sleep.call_count == 5


def test_execute_activity_retry_on_connection_error():
    import requests as req_lib

    mock_ok = MagicMock()
    mock_ok.status_code = 200
    mock_ok.json.return_value = {"prompt": "p", "response": "ok"}

    with patch("durable.requests.post", side_effect=[req_lib.ConnectionError("fail"), mock_ok]) as mock_post:
        with patch("durable.time.sleep") as mock_sleep:
            result = _execute_activity("p", "http://mock/infer")

    assert result["response"] == "ok"
    assert mock_post.call_count == 2
    assert mock_sleep.call_count == 1


def test_execute_activity_exponential_backoff():
    mock_429 = MagicMock()
    mock_429.status_code = 429

    mock_ok = MagicMock()
    mock_ok.status_code = 200
    mock_ok.json.return_value = {"prompt": "p", "response": "ok"}

    with patch("durable.requests.post", side_effect=[mock_429, mock_429, mock_429, mock_ok]) as mock_post:
        with patch("durable.time.sleep") as mock_sleep:
            result = _execute_activity("p", "http://mock/infer")

    assert result["response"] == "ok"
    assert mock_post.call_count == 4
    assert mock_sleep.call_count == 3

    calls = mock_sleep.call_args_list
    assert calls[0][0][0] == 1
    assert calls[1][0][0] == 2
    assert calls[2][0][0] == 4


# ── _execute_activity: custom retry count ────────────────────────

def test_execute_activity_custom_max_retries():
    mock_429 = MagicMock()
    mock_429.status_code = 429

    with patch("durable.requests.post", return_value=mock_429) as mock_post:
        with patch("durable.time.sleep"):
            result = _execute_activity("p", "http://mock/infer", max_retries=3)

    assert result["response"] == "FAILED after max retries"
    assert mock_post.call_count == 3


# ── run_orchestrator: happy path ─────────────────────────────────

def test_run_orchestrator_happy_path():
    db = _tmp_db()
    store_setup = DurableStore(db_path=db)
    store_setup.create_orchestration("orch-1", ["a", "b", "c"])

    prompts = ["a", "b", "c"]
    results = [
        {"prompt": "a", "response": "A"},
        {"prompt": "b", "response": "B"},
        {"prompt": "c", "response": "C"},
    ]

    def fake_execute(prompt, url, max_retries=5):
        return next(r for r in results if r["prompt"] == prompt)

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute):
            run_orchestrator("orch-1", prompts, "http://mock/infer")

    store = DurableStore(db_path=db)
    status = store.get_status("orch-1")
    assert status["status"] == "completed"
    assert status["completed"] == 3

    output = store.get_result("orch-1")
    assert len(output) == 3
    assert output[0]["prompt"] == "a"
    assert output[1]["response"] == "B"
    assert output[2]["prompt"] == "c"

    _cleanup(db)


def test_run_orchestrator_preserves_input_order():
    db = _tmp_db()
    prompts = [f"p{i}" for i in range(20)]
    store_setup = DurableStore(db_path=db)
    store_setup.create_orchestration("order-1", prompts)

    def fake_execute(prompt, url, max_retries=5):
        return {"prompt": prompt, "response": f"r-{prompt}"}

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute):
            run_orchestrator("order-1", prompts, "http://mock/infer")

    store = DurableStore(db_path=db)
    output = store.get_result("order-1")
    for i, item in enumerate(output):
        assert item["prompt"] == f"p{i}"
        assert item["response"] == f"r-p{i}"

    _cleanup(db)


def test_run_orchestrator_with_failures():
    db = _tmp_db()
    prompts = ["ok-1", "fail-1", "ok-2"]
    store_setup = DurableStore(db_path=db)
    store_setup.create_orchestration("failbatch-1", prompts)

    def fake_execute(prompt, url, max_retries=5):
        if "fail" in prompt:
            return {"prompt": prompt, "response": "FAILED after max retries"}
        return {"prompt": prompt, "response": f"OK-{prompt}"}

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute):
            run_orchestrator("failbatch-1", prompts, "http://mock/infer")

    store = DurableStore(db_path=db)
    output = store.get_result("failbatch-1")
    assert len(output) == 3

    ok_responses = [r for r in output if "FAILED" not in r["response"]]
    failed_responses = [r for r in output if "FAILED" in r["response"]]
    assert len(ok_responses) == 2
    assert len(failed_responses) == 1
    assert failed_responses[0]["prompt"] == "fail-1"

    _cleanup(db)


def test_run_orchestrator_single_prompt():
    db = _tmp_db()
    store_setup = DurableStore(db_path=db)
    store_setup.create_orchestration("single-1", ["only"])

    def fake_execute(prompt, url, max_retries=5):
        return {"prompt": prompt, "response": "done"}

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute):
            run_orchestrator("single-1", ["only"], "http://mock/infer")

    store = DurableStore(db_path=db)
    output = store.get_result("single-1")
    assert len(output) == 1
    assert output[0]["prompt"] == "only"
    _cleanup(db)


def test_run_orchestrator_empty_prompts():
    db = _tmp_db()
    store_setup = DurableStore(db_path=db)
    store_setup.create_orchestration("empty-2", [])

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity") as mock_exec:
            run_orchestrator("empty-2", [], "http://mock/infer")

    mock_exec.assert_not_called()

    store = DurableStore(db_path=db)
    status = store.get_status("empty-2")
    assert status["status"] == "completed"
    assert status["total"] == 0

    output = store.get_result("empty-2")
    assert output == []

    _cleanup(db)


# ── run_orchestrator: replay (event skip) ───────────────────────

def test_run_orchestrator_replay_skips_inference():
    db = _tmp_db()
    prompts = ["x", "y", "z"]

    store1 = DurableStore(db_path=db)
    store1.create_orchestration("replay-2", prompts)
    store1.save_event("replay-2", 0, "x", {"prompt": "x", "response": "cached-x"})
    store1.save_event("replay-2", 1, "y", {"prompt": "y", "response": "cached-y"})
    store1.save_event("replay-2", 2, "z", {"prompt": "z", "response": "cached-z"})

    def fake_execute(prompt, url, max_retries=5):
        return {"prompt": prompt, "response": "SHOULD-NOT-BE-CALLED"}

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute) as mock_exec:
            run_orchestrator("replay-2", prompts, "http://mock/infer")

    mock_exec.assert_not_called()

    store = DurableStore(db_path=db)
    output = store.get_result("replay-2")
    assert output[0]["response"] == "cached-x"
    assert output[1]["response"] == "cached-y"
    assert output[2]["response"] == "cached-z"

    _cleanup(db)


def test_run_orchestrator_partial_replay():
    db = _tmp_db()
    prompts = ["done", "pending", "also-pending"]

    store1 = DurableStore(db_path=db)
    store1.create_orchestration("partial-1", prompts)
    store1.save_event("partial-1", 0, "done", {"prompt": "done", "response": "cached"})

    def fake_execute(prompt, url, max_retries=5):
        return {"prompt": prompt, "response": f"fresh-{prompt}"}

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute) as mock_exec:
            run_orchestrator("partial-1", prompts, "http://mock/infer")

    assert mock_exec.call_count == 2

    store = DurableStore(db_path=db)
    output = store.get_result("partial-1")
    assert output[0]["response"] == "cached"
    assert output[1]["response"] == "fresh-pending"
    assert output[2]["response"] == "fresh-also-pending"

    _cleanup(db)


# ── run_orchestrator: concurrency ────────────────────────────────

def test_run_orchestrator_uses_thread_pool():
    db = _tmp_db()
    prompts = [f"p{i}" for i in range(10)]
    store_setup = DurableStore(db_path=db)
    store_setup.create_orchestration("conc-2", prompts)

    thread_ids = set()
    lock = threading.Lock()

    def fake_execute(prompt, url, max_retries=5):
        with lock:
            thread_ids.add(threading.current_thread().ident)
        time.sleep(0.01)
        return {"prompt": prompt, "response": f"r-{prompt}"}

    with patch("durable.DurableStore", return_value=DurableStore(db_path=db)):
        with patch("durable._execute_activity", side_effect=fake_execute):
            with patch("durable.POOL_SIZE", 5):
                run_orchestrator("conc-2", prompts, "http://mock/infer")

    assert len(thread_ids) > 1

    store = DurableStore(db_path=db)
    output = store.get_result("conc-2")
    assert len(output) == 10

    _cleanup(db)
