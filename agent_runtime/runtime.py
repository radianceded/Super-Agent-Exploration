"""Nonblocking coordinator with durable sessions and independent acceptance."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import time
import uuid

from .context import materialize
from .io import atomic_json, deliver_file, ingest_file, lock
from .store import Store


def accept_artifacts(workspace, reply, expected):
    """Hash expectations come from owner policy, never from worker output."""
    if not isinstance(reply, dict) or set(reply) != {"status", "summary", "artifacts", "handoff"}:
        raise ValueError("Invalid worker report")
    if reply["status"] not in {"complete", "needs_input"} or not isinstance(reply["summary"], str):
        raise ValueError("Invalid report status or summary")
    if not isinstance(reply["artifacts"], list) or any(not isinstance(x, str) for x in reply["artifacts"]):
        raise ValueError("Invalid artifact paths")
    root = Path(workspace).resolve()
    manifest = []
    for name in reply["artifacts"]:
        path = root / name
        if Path(name).is_absolute() or not path.resolve().is_relative_to(root) or path.is_symlink() or not path.is_file():
            raise ValueError("Artifact is missing or escapes the task workspace")
        if Path(name).parts[0] in {"context", "ctx", "runs"}:
            raise ValueError("Runtime context and logs are not deliverables")
        manifest.append({"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size})
    verified = bool(expected) and {x["path"]: x["sha256"] for x in manifest} == expected
    state = "needs_input" if reply["status"] == "needs_input" else "succeeded" if verified else "needs_review"
    return {"state": state, "summary": reply["summary"], "manifest": manifest,
            "acceptance": "owner hashes matched" if verified else "no matching owner acceptance; report requires review",
            "handoff": reply["handoff"]}


class Runtime:
    def __init__(self, root, backend, policy=None):
        self.root = Path(root).resolve()
        self.backend = backend
        # Snapshot owner policy before workers run; external event fields cannot
        # change sources, permissions, acceptance hashes or output destinations.
        self.policy = json.loads(json.dumps(policy or {}))

    def route(self):
        store = Store(self.root)
        try:
            events = store.pending(16)
            if not events:
                return
            roots = [{"kind": "event", "id": e["id"]} for e in events]
            roots += [{"kind": "task", "id": t["id"]} for t in store.tasks()]
            workspace = self.root / "router"
            context = materialize(store, roots, workspace / "ctx" / str(events[-1]["seq"]))
            request = {"event_ids": [e["id"] for e in events], "events": events, "context": context}
            reply, sid = self.backend.run("router", request, workspace, store.session("router"))
            if not isinstance(reply, dict) or set(reply) != {"decisions"} or not isinstance(reply["decisions"], list):
                raise ValueError("Invalid router response")
            ids = [d.get("event_id") for d in reply["decisions"] if isinstance(d, dict)]
            if len(ids) != len(events) or set(ids) != set(request["event_ids"]):
                raise ValueError("Router must classify the exact batch once each")
            store.route(reply["decisions"], sid)
        finally:
            store.close()

    def work(self, task):
        store = Store(self.root)
        try:
            sid = None
            try:
                workspace = self.root / "w" / task["id"].replace("-", "")
                context = materialize(store, [{"kind": "task", "id": task["id"]}], workspace / "ctx" / str(task["run_revision"]))
                request = {"task_id": task["id"], "revision": task["run_revision"], "context": context}
                reply, sid = self.backend.run("worker", request, workspace, store.session(task["id"]))
                result = accept_artifacts(workspace, reply, self.policy.get("acceptance_sha256", {}))
            except Exception as exc:
                result = {"state": "failed", "summary": f"{type(exc).__name__}: {exc}", "handoff": []}
            store.finish(task, result, sid)
        finally:
            store.close()

    def run(self, *, once=False, workers=2, inbox=None, output=None):
        if not 1 <= workers <= 4:
            raise ValueError("workers must be between 1 and 4")
        with lock(self.root / "runtime.lock"):
            store = Store(self.root)
            try:
                routing, active, retry_at = None, [], 0.0
                with ThreadPoolExecutor(max_workers=workers + 1) as pool:
                    while True:
                        stopping = (self.root / "STOP").exists()
                        if inbox and not stopping:
                            ingest_file(store, inbox, self.policy)
                        store.recover()
                        if routing and routing.done():
                            try:
                                routing.result()
                            except Exception as exc:
                                store.alert("router_error", f"{type(exc).__name__}: {exc}")
                                retry_at = time.monotonic() + 30
                            routing = None
                        pending = []
                        for future in active:
                            if future.done():
                                try:
                                    future.result()
                                except Exception as exc:
                                    store.alert("worker_error", f"{type(exc).__name__}: {exc}")
                            else:
                                pending.append(future)
                        active = pending
                        if not stopping:
                            if not routing and store.pending(1) and time.monotonic() >= retry_at:
                                routing = pool.submit(self.route)
                            while len(active) < workers:
                                task = store.claim(timeout=getattr(self.backend, "timeout", 600) + 60)
                                if task is None:
                                    break
                                active.append(pool.submit(self.work, task))
                        if output:
                            deliver_file(store, output)
                        atomic_json(self.root / "heartbeat.json", {"at": time.time(), "active": len(active), "router_active": routing is not None})
                        idle = not routing and not active
                        if idle and (stopping or (once and (not store.pending(1) or retry_at > time.monotonic()))):
                            break
                        time.sleep(.1)
            finally:
                store.close()
