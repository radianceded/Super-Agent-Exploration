"""Transactional event log, topic subscriptions, task revisions and outbox."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time
import uuid


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def stable_id(*parts):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, encode(parts)))


def validate_event(event):
    required = {"source", "source_id", "thread", "actor", "kind", "text", "occurred_at", "reply_to"}
    if not isinstance(event, dict) or set(event) != required:
        raise ValueError("Invalid event fields")
    for key in required - {"reply_to"}:
        if not isinstance(event[key], str):
            raise ValueError(f"{key} must be text")
    if not all(event[k].strip() for k in ("source", "source_id", "thread", "kind")):
        raise ValueError("Source identity, topic and kind cannot be empty")
    if event["reply_to"] is not None and not isinstance(event["reply_to"], str):
        raise ValueError("reply_to must be a source message ID or null")
    timestamp = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("occurred_at requires a timezone")
    encode(event)


class Store:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / "state.sqlite3", isolation_level=None, timeout=15)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS alerts(code TEXT PRIMARY KEY,detail TEXT);
        CREATE TABLE IF NOT EXISTS threads(id TEXT PRIMARY KEY,source TEXT,external_id TEXT,UNIQUE(source,external_id));
        CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,thread_id TEXT REFERENCES threads(id),body TEXT,state TEXT);
        CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY,title TEXT,state TEXT,origin TEXT REFERENCES threads(id),revision INTEGER,run_revision INTEGER DEFAULT 0,token TEXT,deadline REAL,summary TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS subscriptions(task_id TEXT REFERENCES tasks(id),thread_id TEXT REFERENCES threads(id),PRIMARY KEY(task_id,thread_id));
        CREATE TABLE IF NOT EXISTS inputs(task_id TEXT REFERENCES tasks(id),event_id TEXT REFERENCES events(id),active INTEGER DEFAULT 1,PRIMARY KEY(task_id,event_id));
        CREATE TABLE IF NOT EXISTS sessions(owner TEXT PRIMARY KEY,provider_id TEXT);
        CREATE TABLE IF NOT EXISTS decisions(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT,body TEXT);
        CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,destination TEXT,body TEXT,acked INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,task_id TEXT,revision INTEGER,state TEXT,result TEXT);
        CREATE INDEX IF NOT EXISTS event_state ON events(state,seq);
        CREATE INDEX IF NOT EXISTS topic_events ON events(thread_id,seq);
        ''')
        self.db.execute("INSERT OR IGNORE INTO meta VALUES ('instance',?)", (str(uuid.uuid4()),))
        self.instance = self.db.execute("SELECT value FROM meta WHERE key='instance'").fetchone()[0]

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _emit(self, destination, body):
        self.db.execute("INSERT INTO outbox(destination,body) VALUES (?,?)", (destination, encode(body)))

    def _attach(self, task_id, event_id):
        row = self.db.execute("SELECT state FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown task")
        changed = self.db.execute("INSERT INTO inputs(task_id,event_id) VALUES (?,?) ON CONFLICT(task_id,event_id) DO UPDATE SET active=1 WHERE inputs.active=0", (task_id, event_id)).rowcount
        if not changed:
            return
        self.db.execute("UPDATE tasks SET revision=revision+1,state=CASE WHEN state IN ('running','interrupted') THEN state ELSE 'queued' END WHERE id=?", (task_id,))

    def ingest(self, event, *, allowed_sources=None, ignored_actors=()):
        validate_event(event)
        event_id = stable_id(event["source"], event["source_id"])
        thread_id = stable_id(event["source"], event["thread"])
        with self.transaction():
            prior = self.db.execute("SELECT body FROM events WHERE id=?", (event_id,)).fetchone()
            if prior:
                if json.loads(prior[0]) != event:
                    raise ValueError("Source ID reused with different content")
                return {"id": event_id, "path": "duplicate"}
            self.db.execute("INSERT OR IGNORE INTO threads VALUES (?,?,?)", (thread_id, event["source"], event["thread"]))
            admitted = (allowed_sources is None or event["source"] in allowed_sources) and event["actor"] not in ignored_actors
            subscribers = [r[0] for r in self.db.execute("SELECT task_id FROM subscriptions WHERE thread_id=?", (thread_id,))] if admitted else []
            path = "ignored" if not admitted else "direct" if subscribers else "pending"
            self.db.execute("INSERT INTO events(id,thread_id,body,state) VALUES (?,?,?,?)", (event_id, thread_id, encode(event), path))
            for task_id in subscribers:
                self._attach(task_id, event_id)
            return {"id": event_id, "path": path, "subscribers": subscribers}

    def event(self, event_id):
        row = self.db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown event")
        result = dict(row)
        result["body"] = json.loads(result["body"])
        return result

    def pending(self, limit=16):
        return [self.event(r[0]) for r in self.db.execute("SELECT id FROM events WHERE state='pending' ORDER BY seq LIMIT ?", (limit,))]

    def tasks(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM tasks ORDER BY rowid")]

    def subscribe(self, task_id, thread_id, enabled=True):
        with self.transaction():
            if not self.db.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone() or not self.db.execute("SELECT 1 FROM threads WHERE id=?", (thread_id,)).fetchone():
                raise ValueError("Unknown task or thread")
            if enabled:
                self.db.execute("INSERT OR IGNORE INTO subscriptions VALUES (?,?)", (task_id, thread_id))
            else:
                self.db.execute("DELETE FROM subscriptions WHERE task_id=? AND thread_id=?", (task_id, thread_id))

    def route(self, decisions, session_id=None):
        if not isinstance(decisions, list):
            raise ValueError("decisions must be a list")
        created, seen = [], set()
        with self.transaction():
            for d in decisions:
                if not isinstance(d, dict) or set(d) != {"event_id", "action", "task_id", "title", "note"}:
                    raise ValueError("Invalid decision fields")
                if any(not isinstance(d[k], str) for k in ("event_id", "action", "title", "note")):
                    raise ValueError("Decision values must be text")
                if d["event_id"] in seen or d["action"] not in {"new", "attach", "ignore", "needs_input"}:
                    raise ValueError("Duplicate or invalid decision")
                seen.add(d["event_id"])
                event = self.event(d["event_id"])
                if event["state"] != "pending":
                    raise ValueError("Event is no longer pending")
                if d["action"] == "attach":
                    if not isinstance(d["task_id"], str) or not self.db.execute("SELECT 1 FROM tasks WHERE id=?", (d["task_id"],)).fetchone():
                        raise ValueError("Attach requires an existing task")
                elif d["task_id"] is not None:
                    raise ValueError("Only attach may specify a task")
            for d in decisions:
                event = self.event(d["event_id"])
                task_id = d["task_id"]
                if d["action"] == "new":
                    task_id = str(uuid.uuid4())
                    self.db.execute("INSERT INTO tasks(id,title,state,origin,revision) VALUES (?,?,'queued',?,0)", (task_id, d["title"], event["thread_id"]))
                    created.append(task_id)
                if d["action"] in {"new", "attach"}:
                    self._attach(task_id, event["id"])
                    self.db.execute("INSERT OR IGNORE INTO subscriptions VALUES (?,?)", (task_id, event["thread_id"]))
                elif d["action"] == "needs_input":
                    self._emit(event["thread_id"], {"type": "needs_input", "event_id": event["id"], "text": d["note"]})
                self.db.execute("UPDATE events SET state=? WHERE id=?", (d["action"], event["id"]))
                self.db.execute("INSERT INTO decisions(event_id,body) VALUES (?,?)", (event["id"], encode({**d, "resolved_task_id": task_id})))
            if session_id:
                self._session("router", session_id)
        return created

    def session(self, owner):
        row = self.db.execute("SELECT provider_id FROM sessions WHERE owner=?", (owner,)).fetchone()
        return row[0] if row else None

    def _session(self, owner, provider_id):
        self.db.execute("INSERT INTO sessions VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET provider_id=excluded.provider_id", (owner, provider_id))

    def claim(self, timeout=900):
        with self.transaction():
            task = self.db.execute("SELECT * FROM tasks WHERE state='queued' ORDER BY rowid LIMIT 1").fetchone()
            if task is None:
                return None
            token = str(uuid.uuid4())
            self.db.execute("UPDATE tasks SET state='running',token=?,deadline=?,run_revision=revision WHERE id=?", (token, time.time() + timeout, task["id"]))
            self.db.execute("INSERT INTO attempts VALUES (?,?,?,'running',NULL)", (token, task["id"], task["revision"]))
            return dict(self.db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone())

    def inputs(self, task_id):
        return [self.event(r[0]) for r in self.db.execute("SELECT e.id FROM inputs i JOIN events e ON e.id=i.event_id WHERE i.task_id=? AND i.active=1 ORDER BY e.seq", (task_id,))]

    def finish(self, task, result, session_id=None):
        if not isinstance(result, dict) or result.get("state") not in {"succeeded", "needs_review", "needs_input", "failed"} or not isinstance(result.get("summary"), str):
            raise ValueError("Invalid worker result")
        handoff = result.get("handoff", [])
        if not isinstance(handoff, list) or any(not isinstance(x, str) for x in handoff) or len(set(handoff)) != len(handoff):
            raise ValueError("Invalid handoff")
        with self.transaction():
            row = self.db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()
            if row is None or row["state"] != "running" or row["token"] != task["token"]:
                raise ValueError("Stale run token")
            owned = {e["id"] for e in self.inputs(task["id"])}
            if not set(handoff) <= owned:
                raise ValueError("Cannot hand off another task's events")
            for event_id in handoff:
                self.db.execute("UPDATE inputs SET active=0 WHERE task_id=? AND event_id=?", (task["id"], event_id))
                self.db.execute("UPDATE events SET state='pending' WHERE id=?", (event_id,))
                self.db.execute("DELETE FROM subscriptions WHERE task_id=? AND thread_id=?", (task["id"], self.event(event_id)["thread_id"]))
            state = "queued" if row["revision"] > row["run_revision"] else result["state"]
            self.db.execute("UPDATE tasks SET state=?,summary=?,token=NULL,deadline=NULL WHERE id=?", (state, result["summary"], task["id"]))
            self.db.execute("UPDATE attempts SET state=?,result=? WHERE id=?", (result["state"], encode(result), task["token"]))
            if session_id:
                self._session(task["id"], session_id)
            self._emit(row["origin"], {"type": "task_result", "task_id": task["id"], "revision": row["run_revision"], "superseded": state == "queued", **result})

    def recover(self):
        with self.transaction():
            rows = self.db.execute("SELECT * FROM tasks WHERE state='running' AND deadline<?", (time.time(),)).fetchall()
            for row in rows:
                self.db.execute("UPDATE tasks SET state='interrupted',token=NULL,deadline=NULL WHERE id=?", (row["id"],))
                self.db.execute("UPDATE attempts SET state='interrupted' WHERE id=?", (row["token"],))
                self._emit(row["origin"], {"type": "interrupted", "task_id": row["id"], "text": "Inspect old process and side effects before retry; not automatically requeued."})
            return [r["id"] for r in rows]

    def recall(self, kind, identity):
        if kind == "event":
            value = self.event(identity)
            refs = [{"kind": "thread", "id": value["thread_id"]}]
            reply = value["body"]["reply_to"]
            if reply:
                reply_id = stable_id(value["body"]["source"], reply)
                if self.db.execute("SELECT 1 FROM events WHERE id=?", (reply_id,)).fetchone():
                    refs.append({"kind": "event", "id": reply_id})
        elif kind == "thread":
            row = self.db.execute("SELECT * FROM threads WHERE id=?", (identity,)).fetchone()
            if row is None:
                raise ValueError("Unknown thread")
            value = dict(row)
            refs = [{"kind": "task", "id": r[0]} for r in self.db.execute("SELECT task_id FROM subscriptions WHERE thread_id=?", (identity,))]
        elif kind == "task":
            row = self.db.execute("SELECT * FROM tasks WHERE id=?", (identity,)).fetchone()
            if row is None:
                raise ValueError("Unknown task")
            value = {k: row[k] for k in ("id", "title", "state", "revision", "summary")}
            refs = [{"kind": "thread", "id": r[0]} for r in self.db.execute("SELECT thread_id FROM subscriptions WHERE task_id=?", (identity,))]
            refs += [{"kind": "event", "id": e["id"]} for e in self.inputs(identity)]
        else:
            raise ValueError("Unknown object kind")
        return {"kind": kind, "id": identity, "value": value, "refs": refs}

    def outbox(self):
        return [{"id": r["id"], "destination": r["destination"], "body": json.loads(r["body"])} for r in self.db.execute("SELECT * FROM outbox WHERE acked=0 ORDER BY id")]

    def ack(self, ids):
        with self.transaction():
            self.db.executemany("UPDATE outbox SET acked=1 WHERE id=?", [(i,) for i in ids])

    def alert(self, code, detail):
        with self.transaction():
            prior = self.db.execute("SELECT detail FROM alerts WHERE code=?", (code,)).fetchone()
            if prior is None or prior[0] != detail:
                self._emit("local", {"type": "alert", "code": code, "text": detail})
            self.db.execute("INSERT INTO alerts VALUES (?,?) ON CONFLICT(code) DO UPDATE SET detail=excluded.detail", (code, detail))
