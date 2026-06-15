import json
import os
import sqlite3
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

DB_PATH = os.environ.get("DURABLE_DB_PATH", "orchestrator.db")
POOL_SIZE = int(os.environ.get("POOL_SIZE", "5"))
TTL_DAYS = int(os.environ.get("TTL_DAYS", "0"))


def _now():
    return datetime.now(timezone.utc).isoformat()


class DurableStore:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    @property
    def _conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orchestrations (
                id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                input TEXT,
                output TEXT,
                created_at TEXT,
                updated_at TEXT
            );
        """)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(orchestration_events)").fetchall()] if list(
            conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orchestration_events'")) else []
        if "sequence" in cols:
            conn.execute("DROP TABLE orchestration_events")
            cols = []
        if not cols:
            conn.execute("""
                CREATE TABLE orchestration_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    orchestration_id TEXT,
                    prompt_index INTEGER,
                    activity_input TEXT,
                    activity_output TEXT,
                    created_at TEXT,
                    UNIQUE(orchestration_id, prompt_index)
                );
            """)
        conn.commit()
        conn.close()

    def cleanup_old(self, ttl_days):
        cutoff = (datetime.now(timezone.utc) -
                  timedelta(days=ttl_days)).isoformat()
        conn = sqlite3.connect(self.db_path)
        old = conn.execute(
            "SELECT id FROM orchestrations WHERE updated_at < ?", (cutoff,)).fetchall()
        for row in old:
            conn.execute(
                "DELETE FROM orchestration_events WHERE orchestration_id=?", (row[0],))
            conn.execute("DELETE FROM orchestrations WHERE id=?", (row[0],))
        conn.commit()
        conn.close()

    def create_orchestration(self, orchestration_id, prompts):
        self._conn.execute(
            "INSERT INTO orchestrations (id, status, input, created_at, updated_at) VALUES (?, 'running', ?, ?, ?)",
            (orchestration_id, json.dumps(prompts), _now(), _now()),
        )
        self._conn.commit()

    def complete_orchestration(self, orchestration_id, output):
        self._conn.execute(
            "UPDATE orchestrations SET status='completed', output=?, updated_at=? WHERE id=?",
            (json.dumps(output), _now(), orchestration_id),
        )
        self._conn.commit()

    def fail_orchestration(self, orchestration_id, error):
        self._conn.execute(
            "UPDATE orchestrations SET status='failed', output=?, updated_at=? WHERE id=?",
            (json.dumps({"error": str(error)}), _now(), orchestration_id),
        )
        self._conn.commit()

    def get_event(self, orchestration_id, prompt_index):
        row = self._conn.execute(
            "SELECT activity_output FROM orchestration_events WHERE orchestration_id=? AND prompt_index=?",
            (orchestration_id, prompt_index),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["activity_output"])

    def save_event(self, orchestration_id, prompt_index, activity_input, activity_output):
        self._conn.execute(
            "INSERT OR IGNORE INTO orchestration_events (orchestration_id, prompt_index, activity_input, activity_output, created_at) VALUES (?, ?, ?, ?, ?)",
            (orchestration_id, prompt_index, activity_input,
             json.dumps(activity_output), _now()),
        )
        self._conn.commit()
        self._conn.execute(
            "UPDATE orchestrations SET updated_at=? WHERE id=?", (_now(
            ), orchestration_id)
        )
        self._conn.commit()

    def get_status(self, orchestration_id):
        row = self._conn.execute(
            "SELECT id, status, input, output, created_at, updated_at FROM orchestrations WHERE id=?",
            (orchestration_id,),
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        total = len(json.loads(d["input"])) if d["input"] else 0
        completed = self._conn.execute(
            "SELECT COUNT(*) FROM orchestration_events WHERE orchestration_id=?",
            (orchestration_id,),
        ).fetchone()[0]
        return {
            "batch_id": d["id"],
            "status": d["status"],
            "total": total,
            "completed": completed,
            "created_at": d["created_at"],
            "updated_at": d["updated_at"],
        }

    def get_result(self, orchestration_id):
        row = self._conn.execute(
            "SELECT status, output FROM orchestrations WHERE id=?", (
                orchestration_id,)
        ).fetchone()
        if row is None:
            return None
        if row["status"] == "completed" and row["output"]:
            return json.loads(row["output"])
        return None


def _execute_activity(prompt, inference_url, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.post(inference_url, json={
                                 "prompt": prompt}, timeout=5)
            if resp.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            return resp.json()
        except requests.RequestException:
            wait = 2 ** attempt
            time.sleep(wait)
    return {"prompt": prompt, "response": "FAILED after max retries"}


def run_orchestrator(orchestration_id, prompts, inference_url):
    store = DurableStore()
    pool_size = POOL_SIZE
    ttl_days = TTL_DAYS
    total = len(prompts)

    if ttl_days > 0:
        store.cleanup_old(ttl_days)

    print(f"[{orchestration_id}] Processing {total} prompts (pool={pool_size})...")

    activity_results = {}

    def worker(prompt_index, prompt):
        saved = store.get_event(orchestration_id, prompt_index)
        if saved is not None:
            return (prompt_index, saved)
        result = _execute_activity(prompt, inference_url)
        store.save_event(orchestration_id, prompt_index, prompt, result)
        return (prompt_index, result)

    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        futures = {executor.submit(worker, i, p)                   : i for i, p in enumerate(prompts)}
        for future in as_completed(futures):
            idx, result = future.result()
            activity_results[idx] = result

    output = [activity_results[i] for i in range(total)]
    store.complete_orchestration(orchestration_id, output)
    print(f"[{orchestration_id}] Done.")
