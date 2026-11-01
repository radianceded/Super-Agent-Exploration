"""Opt-in real model/session/acceptance test; uses existing CLI login and quota."""
import hashlib
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agent_runtime.backends import CodexBackend
from agent_runtime.runtime import Runtime
from agent_runtime.store import Store
from scripts.demo import event


def main():
    root = Path(__file__).resolve().parents[1] / ".demo" / ("codex-" + uuid.uuid4().hex)
    store = Store(root)
    runtime = Runtime(root, CodexBackend(timeout=180), {
        "acceptance_sha256": {"report.txt": hashlib.sha256(b"verified demo artifact").hexdigest()}})
    print(f"Private smoke state: {root}", flush=True)
    try:
        store.ingest(event("chat", "1", "topic", "Create report.txt with exactly the UTF-8 text verified demo artifact and no newline. Read it back to verify. Declare report.txt as the artifact."))
        runtime.run(once=True)
        tasks = store.tasks()
        assert len(tasks) == 1 and tasks[0]["state"] == "succeeded", store.outbox()
        task_id = tasks[0]["id"]
        sid = store.session(task_id)
        router_sid = store.session("router")
        store.close()
        store = Store(root)
        assert store.ingest(event("chat", "2", "topic", "Confirm report.txt is unchanged. Declare it again. Keep its exact bytes."))["path"] == "direct"
        runtime.run(once=True)
        assert store.session(task_id) == sid and store.tasks()[0]["state"] == "succeeded", store.outbox()
        store.ingest(event("mail", "1", "ack", "Thank you. This is only an acknowledgement, no task or action is requested."))
        runtime.run(once=True)
        assert store.session("router") == router_sid and len(store.tasks()) == 1, store.tasks()
        print("PASS: real Router and Worker, both resumed exact sessions after restart; direct follow-up; owner hash acceptance.")
    finally:
        store.close()


if __name__ == "__main__":
    main()
