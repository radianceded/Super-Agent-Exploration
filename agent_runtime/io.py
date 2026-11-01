"""Local JSONL transport and process utilities; no external service side effects."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import subprocess


@contextmanager
def lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("Another owner holds this runtime/transport lock") from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def kill_tree(process):
    if os.name == "nt":
        result = subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                                capture_output=True, timeout=15, creationflags=subprocess.CREATE_NO_WINDOW)
        if result.returncode and process.poll() is None:
            process.kill()
    else:
        # Jobs may start their own process groups. Snapshot descendants before
        # killing the parent, otherwise they can be reparented and missed.
        rows = subprocess.check_output(["ps", "-eo", "pid=,ppid="], text=True)
        parents = [tuple(map(int, line.split())) for line in rows.splitlines()]
        descendants, frontier = [], {process.pid}
        while frontier:
            children = {pid for pid, ppid in parents if ppid in frontier} - set(descendants)
            descendants.extend(children)
            frontier = children
        for pid in reversed(descendants):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=15)


def process_options():
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW}
    return {"start_new_session": True}


def ingest_file(store, path, policy):
    # Re-reading the spool is safe because source IDs are durable. A trailing
    # incomplete line is left for the producer to finish on the next pass.
    count = 0
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.endswith("\n"):
                break
            event = json.loads(line)
            result = store.ingest(event, allowed_sources=policy.get("allowed_sources"),
                                  ignored_actors=policy.get("ignored_actors", []))
            count += result["path"] != "duplicate"
    return count


def deliver_file(store, path):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with lock(str(path) + ".lock"):
        seen = set()
        if path.exists():
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    if not line.endswith("\n"):
                        raise ValueError("Output has an incomplete tail; inspect before recovery")
                    seen.add(json.loads(line)["delivery_id"])
        acked = []
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            for row in store.outbox():
                identity = f"{store.instance}:{row['id']}"
                if identity not in seen:
                    handle.write(json.dumps({"delivery_id": identity, **row}, ensure_ascii=False) + "\n")
                    seen.add(identity)
                acked.append(row["id"])
            handle.flush()
            os.fsync(handle.fileno())
        store.ack(acked)
        return len(acked)
