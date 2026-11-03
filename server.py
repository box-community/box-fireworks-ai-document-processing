#!/usr/bin/env python3
"""
Local control plane for the Box -> Fireworks demo UI.

    python3 server.py            # then open http://localhost:8765

Standard library only, so there is nothing to install beyond what the pipeline
itself needs. Runs happen on a background thread and report progress over
server-sent events, so the page can be refreshed mid-run without losing it.

Credentials live in .env and are read by the pipeline. They are never sent to
the browser and never written into a saved run.
"""

from __future__ import annotations

import json
import mimetypes
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pipeline as P

HERE = Path(__file__).resolve().parent
RUNS = HERE / "runs"
PORT = 8765

STATIC_OK = {".html", ".js", ".css", ".svg", ".png", ".ico", ".woff2"}

# The taxonomy is defined once, in pipeline.py. The server only passes it on.
DEFAULT_ALLOWED_LABELS = P.DEFAULT_ALLOWED_LABELS


def ensure_runs_dir():
    """Create the generated-artifact directory only when the server runs."""
    RUNS.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- #
# run registry
# --------------------------------------------------------------------------- #

class Run:
    def __init__(self, run_id, cfg, kind="live"):
        self.id = run_id
        self.cfg = cfg
        self.kind = kind
        self.events = []
        self.status = "running"
        self.started = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.stop = threading.Event()
        self.cond = threading.Condition()

    def emit(self, ev):
        ev.setdefault("ts", round(time.time(), 3))
        with self.cond:
            self.events.append(ev)
            self.cond.notify_all()

    def finish(self, status):
        self.status = status
        self.emit({"t": "end", "status": status})
        # Keep runs that got far enough to be worth replaying; a run that failed
        # on the first call is noise in the replay list.
        if self.kind == "live" and (status == "completed" or len(self.events) > 12):
            self.save()

    def save(self):
        ensure_runs_dir()
        safe_cfg = {k: v for k, v in self.cfg.items() if k != "source"}
        payload = {"id": self.id, "kind": self.kind, "startedAt": self.started,
                   "status": self.status, "cfg": safe_cfg,
                   "folder": self.folder_name(), "events": self.events}
        (RUNS / f"{self.id}.json").write_text(json.dumps(payload, ensure_ascii=False))

    def folder_name(self):
        for e in self.events:
            if e.get("t") == "folder":
                return e.get("name", "")
        return ""

    def summary(self):
        return {"id": self.id, "kind": self.kind, "startedAt": self.started,
                "status": self.status, "folder": self.folder_name(),
                "events": len(self.events)}


RUN_LOCK = threading.Lock()
ACTIVE: dict[str, Run] = {}


def new_id(prefix="run"):
    return f"{prefix}-{datetime.now().strftime('%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"


def start_live_run(cfg):
    ensure_runs_dir()
    run_id = new_id()
    cfg = {**cfg, "runId": run_id, "outDir": str(RUNS)}
    run = Run(run_id, cfg)
    with RUN_LOCK:
        ACTIVE[run_id] = run

    def worker():
        status = P.run_pipeline(cfg, run.emit, run.stop.is_set)
        run.finish(status)

    threading.Thread(target=worker, daemon=True, name=f"pipeline-{run_id}").start()
    return run


def start_replay(source_id, speed=1.0):
    ensure_runs_dir()
    path = RUNS / f"{source_id}.json"
    if not path.exists():
        raise FileNotFoundError(source_id)
    saved = json.loads(path.read_text())
    run_id = new_id("replay")
    run = Run(run_id, {"replayOf": source_id, "speed": speed}, kind="replay")
    with RUN_LOCK:
        ACTIVE[run_id] = run

    def worker():
        events = saved["events"]
        prev = None
        for ev in events:
            if run.stop.is_set():
                run.finish("cancelled")
                return
            ts = ev.get("ts")
            if ts is not None and prev is not None:
                gap = max(0.0, min(ts - prev, 2.0)) / max(speed, 0.01)
                time.sleep(gap)
            prev = ts if ts is not None else prev
            ev = {k: v for k, v in ev.items() if k != "ts"}
            if ev.get("t") == "end":
                continue
            run.emit(ev)
        run.finish(saved.get("status", "completed"))

    threading.Thread(target=worker, daemon=True, name=f"replay-{run_id}").start()
    return run


def list_saved():
    ensure_runs_dir()
    out = []
    for p in sorted(RUNS.glob("*.json"), reverse=True):
        try:
            d = json.loads(p.read_text())
        except Exception:
            continue
        n = len(d.get("events", []))
        if d.get("status") != "completed" and n <= 12:
            continue          # a run that failed immediately is not worth replaying
        out.append({"id": d.get("id", p.stem), "startedAt": d.get("startedAt", ""),
                    "status": d.get("status", ""), "folder": d.get("folder", ""),
                    "events": n})
    return out[:40]


# --------------------------------------------------------------------------- #
# http
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "CorpusDemo/1.0"

    def log_message(self, fmt, *args):
        if "/api/events/" not in self.path:
            print(f"  {self.command} {self.path}")

    # -- helpers --

    def send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n))
        except Exception:
            return {}

    # -- routes --

    def do_GET(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)

        if path == "/api/config":
            st = P.env_status()
            default = st.pop("defaultSource", "")
            st["hasDefaultSource"] = bool(default)
            st["savedRuns"] = list_saved()
            st["allowedLabels"] = DEFAULT_ALLOWED_LABELS
            st["labelDefinitions"] = dict(P.LABEL_DEFINITIONS)
            st["labelNoun"] = P.LABEL_NOUN
            return self.send_json(st)

        if path == "/api/runs":
            with RUN_LOCK:
                active = [r.summary() for r in ACTIVE.values() if r.status == "running"]
            return self.send_json({"active": active, "saved": list_saved()})

        m = re.fullmatch(r"/api/events/([\w.-]+)", path)
        if m:
            return self.stream_events(m.group(1), int((qs.get("from") or [0])[0]))

        m = re.fullmatch(r"/api/run/([\w.-]+)", path)
        if m:
            run = ACTIVE.get(m.group(1))
            if run:
                return self.send_json({**run.summary(), "events": run.events})
            p = RUNS / f"{m.group(1)}.json"
            if p.exists():
                return self.send_json(json.loads(p.read_text()))
            return self.send_json({"error": "no such run"}, 404)

        return self.serve_static(path)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        body = self.read_json()

        if path == "/api/run":
            cfg = {
                "source": (body.get("source") or "").strip(),
                "subpath": (body.get("subpath") or "").strip(),
                "minChars": int(body.get("minChars") or 200),
                "maxChars": int(body.get("maxChars") or 10000),
                "allowedLabels": body.get("allowedLabels") or DEFAULT_ALLOWED_LABELS,
                "baseModel": body.get("baseModel") or "qwen3p5-9b",
                "epochs": int(body.get("epochs") or 1),
                "train": bool(body.get("train")),
            }
            # Optional taxonomy/dataset overrides for API clients using another
            # use case; omitted keys fall back to the defaults in pipeline.py.
            for key in ("labelDefinitions", "labelNoun", "labelNounPlural",
                        "datasetPrefix"):
                if body.get(key):
                    cfg[key] = body[key]
            try:
                run = start_live_run(cfg)
            except Exception as e:  # noqa: BLE001
                return self.send_json({"error": str(e)}, 400)
            return self.send_json({"runId": run.id})

        if path == "/api/replay":
            source_id = (body.get("runId") or "").strip()
            if not source_id:
                return self.send_json({"error": "no saved runs available"}, 400)
            try:
                run = start_replay(source_id, float(body.get("speed") or 1.0))
            except FileNotFoundError:
                return self.send_json({"error": "no such saved run"}, 404)
            return self.send_json({"runId": run.id})

        m = re.fullmatch(r"/api/stop/([\w.-]+)", path)
        if m:
            run = ACTIVE.get(m.group(1))
            if run:
                run.stop.set()
                return self.send_json({"ok": True})
            return self.send_json({"error": "no such run"}, 404)

        return self.send_json({"error": "not found"}, 404)

    # -- sse --

    def stream_events(self, run_id, start_at=0):
        run = ACTIVE.get(run_id)
        if run is None:
            p = RUNS / f"{run_id}.json"
            if not p.exists():
                return self.send_json({"error": "no such run"}, 404)
            saved = json.loads(p.read_text())
            self.begin_sse()
            for i, ev in enumerate(saved["events"][start_at:], start_at):
                if not self.write_event(i, ev):
                    return
            self.write_event(len(saved["events"]), {"t": "end", "status": saved["status"]})
            return

        self.begin_sse()
        i = start_at
        last_ping = time.time()
        while True:
            with run.cond:
                while i >= len(run.events) and run.status == "running":
                    if not run.cond.wait(timeout=10):
                        break
                pending = run.events[i:]
            for ev in pending:
                if not self.write_event(i, ev):
                    return
                i += 1
            if not pending:
                if run.status != "running" and i >= len(run.events):
                    return
                if time.time() - last_ping > 9:
                    last_ping = time.time()
                    try:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        return

    def begin_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

    def write_event(self, idx, ev):
        try:
            self.wfile.write(f"id: {idx}\ndata: {json.dumps(ev)}\n\n".encode())
            self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, ValueError):
            return False

    # -- static --

    def serve_static(self, path):
        rel = "app.html" if path in ("/", "") else path.lstrip("/")
        target = (HERE / rel).resolve()
        if HERE not in target.parents and target != HERE:
            return self.send_json({"error": "forbidden"}, 403)
        if not target.is_file() or target.suffix.lower() not in STATIC_OK:
            return self.send_json({"error": "not found"}, 404)
        data = target.read_bytes()
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)


def main():
    ensure_runs_dir()
    P.load_env(str(HERE / ".env"))
    st = P.env_status()
    print(f"\n  Corpus demo  →  http://localhost:{PORT}\n")
    print(f"  Box credentials       {'ready' if st['boxReady'] else 'MISSING (see .env.example)'}")
    print(f"  Fireworks credentials {'ready' if st['fireworksReady'] else 'MISSING (see .env.example)'}")
    print(f"  Saved runs            {len(list_saved())}\n")
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    srv.daemon_threads = True
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  stopped")


if __name__ == "__main__":
    main()
