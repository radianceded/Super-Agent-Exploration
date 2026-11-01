import argparse
import json
from pathlib import Path
import sys

from .backends import CodexBackend, DemoBackend
from .io import deliver_file, ingest_file
from .runtime import Runtime
from .store import Store


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Durable topic-based Router/Worker runtime")
    parser.add_argument("--state", type=Path, default=Path(".state"))
    subs = parser.add_subparsers(dest="cmd", required=True)
    for name in ("run", "supervise"):
        p = subs.add_parser(name)
        p.add_argument("--backend", choices=("demo", "codex"), required=True)
        p.add_argument("--inbox", type=Path)
        p.add_argument("--output", type=Path)
        p.add_argument("--policy", type=Path)
        if name == "run":
            p.add_argument("--once", action="store_true")
    p = subs.add_parser("ingest")
    p.add_argument("file", type=Path)
    p = subs.add_parser("recall")
    p.add_argument("kind", choices=("event", "thread", "task"))
    p.add_argument("id")
    p = subs.add_parser("deliver")
    p.add_argument("file", type=Path)
    p = subs.add_parser("subscribe")
    p.add_argument("task")
    p.add_argument("thread")
    p.add_argument("--remove", action="store_true")
    for name in ("status", "stop"):
        subs.add_parser(name)
    args = parser.parse_args()
    args.state = args.state.resolve()
    try:
        if args.cmd == "supervise":
            from .supervisor import supervise
            forwarded = ["--backend", args.backend]
            for key in ("inbox", "output", "policy"):
                if getattr(args, key):
                    forwarded.extend(["--" + key, str(getattr(args, key).resolve())])
            supervise(args.state, forwarded)
            return 0
        if args.cmd == "run":
            policy = json.loads(args.policy.read_text(encoding="utf-8")) if args.policy else {}
            backend = CodexBackend() if args.backend == "codex" else DemoBackend()
            Runtime(args.state, backend, policy).run(once=args.once, inbox=args.inbox, output=args.output)
            return 0
        store = Store(args.state)
        try:
            if args.cmd == "ingest":
                result = {"new_events": ingest_file(store, args.file, {})}
            elif args.cmd == "recall":
                result = store.recall(args.kind, args.id)
            elif args.cmd == "deliver":
                result = {"delivered": deliver_file(store, args.file)}
            elif args.cmd == "subscribe":
                store.subscribe(args.task, args.thread, not args.remove)
                result = {"updated": True}
            elif args.cmd == "stop":
                (args.state / "STOP").write_text("owner requested stop", encoding="utf-8")
                result = {"stop_requested": True}
            else:
                result = {"instance": store.instance, "tasks": store.tasks(), "pending": len(store.pending()), "outbox": store.outbox()}
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        finally:
            store.close()
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
