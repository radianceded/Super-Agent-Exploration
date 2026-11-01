from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from agent_runtime.backends import DemoBackend
from agent_runtime.context import materialize
from agent_runtime.io import deliver_file, ingest_file
from agent_runtime.runtime import Runtime, accept_artifacts
from agent_runtime.store import Store, stable_id


def event(identity="1", *, source="chat", thread="topic", text="Create a report", reply=None):
    return {"source": source, "source_id": identity, "thread": thread, "actor": "synthetic-user",
            "kind": "message", "text": text, "occurred_at": "2026-11-01T00:00:00+00:00", "reply_to": reply}


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "runtime"
        self.store = Store(self.root)
        self.backend = DemoBackend()
        self.policy = {"acceptance_sha256": {"report.txt": hashlib.sha256(b"verified demo artifact").hexdigest()}}
        self.runtime = Runtime(self.root, self.backend, self.policy)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def task(self):
        self.store.ingest(event())
        self.runtime.run(once=True)
        return self.store.tasks()[0]["id"]

    def test_source_identity_idempotency_and_conflicting_replay(self):
        self.assertEqual(self.store.ingest(event())["path"], "pending")
        self.assertEqual(self.store.ingest(event())["path"], "duplicate")
        with self.assertRaises(ValueError):
            self.store.ingest(event(text="Changed old event"))
        self.assertNotEqual(self.store.ingest(event(source="mail"))["id"], stable_id("chat", "1"))

    def test_program_filter_skips_model_but_retains_event(self):
        result = self.store.ingest(event(), allowed_sources=["mail"])
        self.assertEqual(result["path"], "ignored")
        self.runtime.run(once=True)
        self.assertEqual(self.backend.router_calls, 0)
        self.assertEqual(self.store.event(result["id"])["body"]["text"], "Create a report")

    def test_direct_followup_and_sessions_survive_restart(self):
        task_id = self.task()
        worker_sid = self.store.session(task_id)
        router_sid = self.store.session("router")
        self.store.close()
        self.store = Store(self.root)
        result = self.store.ingest(event("2", text="Revise the report", reply="1"))
        self.assertEqual(result["path"], "direct")
        self.runtime.run(once=True)
        self.assertEqual(self.backend.router_calls, 1)
        self.assertEqual(self.store.session(task_id), worker_sid)
        self.assertEqual(self.store.session("router"), router_sid)
        self.assertEqual(self.store.tasks()[0]["revision"], 2)
        self.assertEqual(self.store.tasks()[0]["state"], "succeeded")

    def test_cross_source_attachment_establishes_fast_path(self):
        task_id = self.task()
        self.store.ingest(event(source="github", thread="issue-10", text="attach:" + task_id))
        self.runtime.run(once=True)
        router_calls = self.backend.router_calls
        result = self.store.ingest(event("2", source="github", thread="issue-10", text="Follow-up"))
        self.assertEqual(result["subscribers"], [task_id])
        self.runtime.run(once=True)
        self.assertEqual(len(self.store.tasks()), 1)
        self.assertEqual(self.backend.router_calls, router_calls)
        # Input subscriptions do not silently change the result audience.
        self.assertTrue(all(x["destination"] == stable_id("chat", "topic") for x in self.store.outbox()))

    def test_multicast_subscriptions_are_many_to_many(self):
        one = self.task()
        self.store.ingest(event("2", thread="other"))
        self.runtime.run(once=True)
        two = self.store.tasks()[1]["id"]
        self.store.subscribe(two, stable_id("chat", "topic"))
        result = self.store.ingest(event("3"))
        self.assertEqual(set(result["subscribers"]), {one, two})

    def test_route_batch_is_atomic_and_cannot_repeat(self):
        result = self.store.ingest(event())
        decision = {"event_id": result["id"], "action": "new", "task_id": None, "title": "T", "note": ""}
        with self.assertRaises(ValueError):
            self.store.route([decision, {**decision, "event_id": "missing"}])
        self.assertEqual(self.store.tasks(), [])
        self.store.route([decision])
        with self.assertRaises(ValueError):
            self.store.route([decision])
        self.assertEqual(len(self.store.tasks()), 1)

    def test_spill_preserves_full_text_and_recall_links(self):
        raw = "  多行\n😀\t" * 1000
        result = self.store.ingest(event(text=raw))
        context = materialize(self.store, [{"kind": "event", "id": result["id"]}], self.root / "recall", budget=512)
        self.assertLessEqual(len(json.dumps(context, ensure_ascii=False)), 512)
        item = json.loads((self.root / "recall" / f"event-{result['id']}.json").read_text(encoding="utf-8"))
        self.assertEqual(item["value"]["body"]["text"], raw)
        self.assertEqual(item["refs"][0]["kind"], "thread")
        self.assertTrue(Path(item["refs"][0]["file"]).exists())

    def test_chain_event_thread_task_other_thread(self):
        task_id = self.task()
        self.store.ingest(event(source="github", thread="issue", text="attach:" + task_id))
        self.runtime.run(once=True)
        e = self.store.recall("event", stable_id("chat", "1"))
        thread = self.store.recall(**{"kind": e["refs"][0]["kind"], "identity": e["refs"][0]["id"]})
        task = self.store.recall("task", thread["refs"][0]["id"])
        self.assertIn({"kind": "thread", "id": stable_id("github", "issue")}, task["refs"])

    def test_handoff_returns_event_to_router_and_unsubscribes(self):
        task_id = self.task()
        incoming = self.store.ingest(event("2", text="Different work"))
        task = self.store.claim()
        self.store.finish(task, {"state": "needs_input", "summary": "Unrelated event", "handoff": [incoming["id"]]})
        self.assertEqual(self.store.pending()[0]["id"], incoming["id"])
        self.assertEqual(self.store.ingest(event("3"))["path"], "pending")

    def test_handed_off_event_can_be_explicitly_reattached(self):
        task_id = self.task()
        incoming = self.store.ingest(event("2"))
        task = self.store.claim()
        self.store.finish(task, {"state": "needs_input", "summary": "Reclassify", "handoff": [incoming["id"]]})
        self.store.route([{"event_id": incoming["id"], "action": "attach", "task_id": task_id, "title": "T", "note": "Explicit reassignment"}])
        self.assertEqual(len(self.store.inputs(task_id)), 2)
        self.assertEqual(self.store.tasks()[0]["state"], "queued")

    def test_context_failure_finishes_task_instead_of_leaking_running_state(self):
        self.store.ingest(event())
        self.runtime.route()
        task = self.store.claim()
        with patch("agent_runtime.runtime.materialize", side_effect=OSError("workspace failure")):
            self.runtime.work(task)
        self.assertEqual(self.store.tasks()[0]["state"], "failed")

    def test_followup_during_running_job_does_not_block_ingest(self):
        self.store.ingest(event())
        self.runtime.route()
        task = self.store.claim()
        entered, release = threading.Event(), threading.Event()
        original = self.backend.run
        def blocking(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test timed out")
            return original(*args, **kwargs)
        with patch.object(self.backend, "run", side_effect=blocking):
            thread = threading.Thread(target=self.runtime.work, args=(task,))
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(self.store.ingest(event("2"))["path"], "direct")
            finally:
                release.set()
                thread.join(5)
        self.assertEqual(self.store.tasks()[0]["state"], "queued")
        self.assertTrue(self.store.outbox()[-1]["body"]["superseded"])

    def test_expired_job_is_interrupted_and_stale_result_rejected(self):
        self.store.ingest(event())
        self.runtime.route()
        task = self.store.claim()
        self.store.db.execute("UPDATE tasks SET deadline=?", (time.time() - 1,))
        self.assertEqual(self.store.recover(), [task["id"]])
        self.assertIsNone(self.store.claim())
        with self.assertRaises(ValueError):
            self.store.finish(task, {"state": "succeeded", "summary": "stale"})
        self.store.ingest(event("2"))
        self.assertEqual(self.store.tasks()[0]["state"], "interrupted")

    def test_worker_report_alone_is_not_success(self):
        (self.root / "artifact.txt").write_bytes(b"actual")
        reply = {"status": "complete", "summary": "claimed done", "artifacts": ["artifact.txt"], "handoff": []}
        self.assertEqual(accept_artifacts(self.root, reply, {})["state"], "needs_review")
        self.assertEqual(accept_artifacts(self.root, reply, {"artifact.txt": "wrong"})["state"], "needs_review")
        with self.assertRaises(ValueError):
            accept_artifacts(self.root, {**reply, "artifacts": ["../escape"]}, {})

    def test_delivery_recovery_deduplicates_persisted_output(self):
        self.task()
        output = self.root / "output.jsonl"
        with patch.object(self.store, "ack", side_effect=RuntimeError("crash after fsync")):
            with self.assertRaises(RuntimeError):
                deliver_file(self.store, output)
        before = output.read_bytes()
        self.assertEqual(deliver_file(self.store, output), 1)
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual(self.store.outbox(), [])

    def test_incomplete_input_tail_is_not_consumed(self):
        path = self.root / "input.jsonl"
        body = json.dumps(event())
        path.write_text(body, encoding="utf-8")
        self.assertEqual(ingest_file(self.store, path, {}), 0)
        path.write_text(body + "\n", encoding="utf-8")
        self.assertEqual(ingest_file(self.store, path, {}), 1)
        self.assertEqual(ingest_file(self.store, path, {}), 0)


if __name__ == "__main__":
    unittest.main()
