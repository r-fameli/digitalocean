import json
import os
import sqlite3
import threading
import time
import requests
from datetime import datetime, timezone

DB_PATH = os.environ.get("DURABLE_DB_PATH", "orchestrator.db")


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
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS orchestrations (
                id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                input TEXT,
                output TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS orchestration_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                orchestration_id TEXT,
                sequence INTEGER,
                activity_name TEXT,
                activity_input TEXT,
                activity_output TEXT,
                created_at TEXT,
                UNIQUE(orchestration_id, sequence)
            );
        """)
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

    def save_event(self, orchestration_id, sequence, activity_name, activity_input, activity_output):
        self._conn.execute(
            "INSERT OR IGNORE INTO orchestration_events (orchestration_id, sequence, activity_name, activity_input, activity_output, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (orchestration_id, sequence, activity_name, json.dumps(activity_input), json.dumps(activity_output), _now()),
        )
        self._conn.commit()
        self._conn.execute(
            "UPDATE orchestrations SET updated_at=? WHERE id=?", (_now(), orchestration_id)
        )
        self._conn.commit()

    def get_events(self, orchestration_id):
        rows = self._conn.execute(
            "SELECT sequence, activity_name, activity_input, activity_output FROM orchestration_events WHERE orchestration_id=? ORDER BY sequence",
            (orchestration_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_status(self, orchestration_id):
        row = self._conn.execute(
            "SELECT id, status, input, output, created_at, updated_at FROM orchestrations WHERE id=?",
            (orchestration_id,),
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        total = len(json.loads(d["input"])) if d["input"] else 0
        completed = len(self.get_events(orchestration_id))
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
            "SELECT status, output FROM orchestrations WHERE id=?", (orchestration_id,)
        ).fetchone()
        if row is None:
            return None
        if row["status"] == "completed" and row["output"]:
            return json.loads(row["output"])
        return None


class RetryPolicy:
    def __init__(self, max_retries=5, initial_delay=1, backoff_multiplier=2):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_multiplier = backoff_multiplier


class OrchestrationContext:
    def __init__(self, orchestration_id, store, inference_url):
        self.orchestration_id = orchestration_id
        self.store = store
        self.inference_url = inference_url
        self._events = store.get_events(orchestration_id)
        self._event_index = 0
        self._new_events = False

    def call_activity(self, prompt, retry_policy=None):
        if retry_policy is None:
            retry_policy = RetryPolicy()

        if self._event_index < len(self._events):
            event = self._events[self._event_index]
            if event["activity_input"] == prompt:
                self._event_index += 1
                return json.loads(event["activity_output"])

        result = self._run_activity(prompt, retry_policy)

        self.store.save_event(self.orchestration_id, self._event_index, "infer", prompt, result)
        self._event_index += 1
        self._new_events = True
        return result

    def _run_activity(self, prompt, retry_policy):
        for attempt in range(retry_policy.max_retries):
            try:
                resp = requests.post(self.inference_url, json={"prompt": prompt}, timeout=5)
                if resp.status_code == 429:
                    wait = retry_policy.initial_delay * (retry_policy.backoff_multiplier ** attempt)
                    print(f"      429 on '{prompt[:30]}...' - retry in {wait}s (attempt {attempt+1}/{retry_policy.max_retries})")
                    time.sleep(wait)
                    continue
                return resp.json()
            except requests.RequestException as e:
                wait = retry_policy.initial_delay * (retry_policy.backoff_multiplier ** attempt)
                print(f"      Error on '{prompt[:30]}...' - {e}, retry in {wait}s")
                time.sleep(wait)
        return {"prompt": prompt, "response": "FAILED after max retries"}

    def has_new_events(self):
        return self._new_events


def run_orchestrator(orchestration_id, prompts, inference_url):
    store = DurableStore()
    ctx = OrchestrationContext(orchestration_id, store, inference_url)
    total = len(prompts)
    print(f"[{orchestration_id}] Orchestrator started: {total} prompts")

    try:
        output = []
        for i, prompt in enumerate(prompts):
            result = ctx.call_activity(prompt)
            output.append(result)

        store.complete_orchestration(orchestration_id, output)
        print(f"[{orchestration_id}] Orchestrator completed")
        return output
    except Exception as e:
        store.fail_orchestration(orchestration_id, e)
        print(f"[{orchestration_id}] Orchestrator failed: {e}")
        raise
