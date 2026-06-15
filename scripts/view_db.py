#!/usr/bin/env python3
import sqlite3, json, os, sys

DB = os.environ.get("DURABLE_DB_PATH", "orchestrator.db")
conn = sqlite3.connect(DB)

ids = [r[0] for r in conn.execute("SELECT id FROM orchestrations ORDER BY updated_at DESC")]
if not ids:
    print("No orchestrations found.")
    sys.exit(0)

print("Orchestrations:")
for i, oid in enumerate(ids):
    row = conn.execute("SELECT status, input, updated_at FROM orchestrations WHERE id=?", (oid,)).fetchone()
    prompts = json.loads(row[1])
    print(f"  [{i}] {oid}  {row[0]:12s}  {len(prompts)} prompts  {row[2]}")

choice = input("\nEnter number to inspect (or Enter to quit): ").strip()
if choice == "" or not choice.isdigit():
    sys.exit(0)

oid = ids[int(choice)]
row = conn.execute("SELECT * FROM orchestrations WHERE id=?", (oid,)).fetchone()

print(f"\n=== {oid} ===")
print(f"Status: {row[1]}")
print(f"Input:  {json.dumps(json.loads(row[2]), indent=2)[:200]}")
if row[3]:
    print(f"Output: {json.dumps(json.loads(row[3]), indent=2)[:200]}")
print(f"Events: {conn.execute('SELECT COUNT(*) FROM orchestration_events WHERE orchestration_id=?', (oid,)).fetchone()[0]}")
print()

for ev in conn.execute(
    "SELECT prompt_index, activity_input, activity_output FROM orchestration_events WHERE orchestration_id=? ORDER BY prompt_index",
    (oid,),
):
    out = json.loads(ev[2])
    print(f"  [{ev[0]}] {ev[1]} -> {out.get('response', str(out)[:40])}")
