"""An external control loop; the runtime cannot supervise its own death."""
import os
from pathlib import Path
import subprocess
import sys
import time
import json

from .io import atomic_json, kill_tree, lock, process_options
from .store import Store


def supervise(root, runtime_args, *, stale_after=20):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    with lock(root / "supervisor.lock"):
        restart_count = 0
        while not (root / "STOP").exists():
            started = time.time()
            command = [sys.executable, "-m", "agent_runtime", "--state", str(root), "run", *runtime_args]
            with (root / "runtime.log").open("ab") as log:
                process = subprocess.Popen(command, stdout=log, stderr=log, **process_options())
                reason = "child exited"
                try:
                    while process.poll() is None:
                        if (root / "STOP").exists():
                            reason = "owner stop"
                            break
                        try:
                            heartbeat = json.loads((root / "heartbeat.json").read_text(encoding="utf-8"))
                            last = max(started, heartbeat["at"])
                        except (OSError, ValueError, KeyError):
                            last = started
                        if time.time() - last > stale_after:
                            reason = "runtime heartbeat stalled"
                            break
                        atomic_json(root / "supervisor.json", {"at": time.time(), "child_pid": process.pid, "restarts": restart_count})
                        time.sleep(.25)
                finally:
                    if process.poll() is None:
                        try:
                            process.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            kill_tree(process)
            if (root / "STOP").exists():
                break
            restart_count += 1
            store = Store(root)
            try:
                store.alert("supervisor_restart", f"{reason}; restart {restart_count}. In-flight tasks keep their lease and are not blindly rerun.")
            finally:
                store.close()
            # Infrastructure retry only. This does not judge semantic progress.
            time.sleep(min(restart_count, 5))
