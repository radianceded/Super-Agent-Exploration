"""Offline acceptance of routing, direct subscriptions, recall and restart."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agent_runtime.backends import DemoBackend
from agent_runtime.io import deliver_file
from agent_runtime.runtime import Runtime
from agent_runtime.store import Store, stable_id


def event(source, identity, thread, text):
    return {"source": source, "source_id": identity, "thread": thread, "actor": "synthetic-user",
            "kind": "message", "text": text, "occurred_at": "2026-11-01T00:00:00Z", "reply_to": None}


def main():
    with tempfile.TemporaryDirectory(prefix="super-agent-demo-") as temp:
        root = Path(temp)
        backend = DemoBackend()
        policy = {"acceptance_sha256": {"report.txt": hashlib.sha256(b"verified demo artifact").hexdigest()}}
        runtime = Runtime(root, backend, policy)
        store = Store(root)
        try:
            store.ingest(event("chat", "1", "topic-1", "Create a report"))
            runtime.run(once=True)
            task_id = store.tasks()[0]["id"]
            session = store.session(task_id)
            store.ingest(event("github", "1", "issue-1", "attach:" + task_id))
            runtime.run(once=True)
            calls = backend.router_calls
            store.close()
            store = Store(root)
            assert store.ingest(event("github", "2", "issue-1", "Revise report"))["path"] == "direct"
            runtime.run(once=True)
            assert backend.router_calls == calls
            assert store.session(task_id) == session
            assert len(store.tasks()) == 1 and store.tasks()[0]["state"] == "succeeded"
            assert len(store.recall("task", task_id)["refs"]) >= 5
            delivered = deliver_file(store, root / "delivered.jsonl")
            assert delivered == 3 and deliver_file(store, root / "delivered.jsonl") == 0
            print("PASS: cross-source continuity, direct routing, persisted session identity, graph recall, verified artifacts, durable delivery")
            print("Backend: deterministic synthetic fixture; no LLM or external messaging service was called.")
        finally:
            store.close()


if __name__ == "__main__":
    main()
