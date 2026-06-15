import json
import os
import tempfile
from durable import DurableStore, _execute_activity


def test_create_and_complete_orchestration():
    db = tempfile.mktemp(suffix=".db")
    store = DurableStore(db_path=db)
    store.create_orchestration("test-1", ["hello", "world"])

    status = store.get_status("test-1")
    assert status["batch_id"] == "test-1"
    assert status["total"] == 2
    assert status["completed"] == 0
    assert status["status"] == "running"

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

    os.unlink(db)


def test_event_skip_on_replay():
    db = tempfile.mktemp(suffix=".db")
    store = DurableStore(db_path=db)
    store.create_orchestration("replay-1", ["a", "b"])

    store.save_event("replay-1", 0, "a", {"response": "A"})

    saved = store.get_event("replay-1", 0)
    assert saved is not None
    assert saved["response"] == "A"

    missing = store.get_event("replay-1", 1)
    assert missing is None

    os.unlink(db)


def test_missing_orchestration():
    db = tempfile.mktemp(suffix=".db")
    store = DurableStore(db_path=db)
    assert store.get_status("nonexistent") is None
    assert store.get_result("nonexistent") is None
    os.unlink(db)


def test_ttl_cleanup():
    db = tempfile.mktemp(suffix=".db")
    store = DurableStore(db_path=db)
    store.create_orchestration("ttl-old", ["x"])

    assert store.get_status("ttl-old") is not None

    store.cleanup_old(ttl_days=0)

    status = store.get_status("ttl-old")
    assert status is None
    os.unlink(db)


def test_fail_orchestration():
    db = tempfile.mktemp(suffix=".db")
    store = DurableStore(db_path=db)
    store.create_orchestration("fail-1", ["a"])
    store.fail_orchestration("fail-1", "something went wrong")

    status = store.get_status("fail-1")
    assert status["status"] == "failed"

    result = store.get_result("fail-1")
    assert result is None

    os.unlink(db)
