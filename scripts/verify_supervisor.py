"""Fault injection: stop only our spawned child and observe external restart."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agent_runtime.io import kill_tree, process_options


def wait_for(path, predicate, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if predicate(data):
                return data
        except (OSError, ValueError):
            pass
        time.sleep(.1)
    raise RuntimeError(f"Timed out waiting for {path.name}")


def main():
    root = Path(__file__).resolve().parents[1] / ".demo" / ("supervisor-" + uuid.uuid4().hex)
    root.mkdir(parents=True)
    process = subprocess.Popen([sys.executable, "-m", "agent_runtime", "--state", str(root), "supervise", "--backend", "demo"], **process_options())
    try:
        first = wait_for(root / "supervisor.json", lambda d: d["restarts"] == 0)
        wait_for(root / "heartbeat.json", lambda d: d["at"] > 0)
        # This PID was just returned by our own supervisor, not discovered by a
        # process-name search. The test never targets an existing user service.
        os.kill(first["child_pid"], signal.SIGTERM)
        second = wait_for(root / "supervisor.json", lambda d: d["restarts"] >= 1 and d["child_pid"] != first["child_pid"])
        assert second["child_pid"] != first["child_pid"]
        print("PASS: external supervisor detected child exit and launched a different child PID.")
    finally:
        (root / "STOP").write_text("test complete", encoding="utf-8")
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            kill_tree(process)
    assert process.returncode == 0


if __name__ == "__main__":
    main()
