#!/usr/bin/env python3
"""Local console for the Math Gambling runner.

Serves the GUI from a local HTTP server bound to 127.0.0.1 only, and runs
the upstream runner as a child process. Standard library only.

See README.md for the design notes.
"""

import atexit
import ctypes
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ------------------------------------------------------------------ paths

ROOT = Path(__file__).resolve().parent.parent
RUNNER_DIR = ROOT / "math-gambling"
RUNNER = RUNNER_DIR / "tools" / "runner.py"
OUT_DIR = RUNNER_DIR / "math-gambling-run"
DB_FILE = OUT_DIR / "checkpoint.sqlite3"
CONFIG_FILE = ROOT / "config.json"
STATE_FILE = ROOT / "state.json"
RESULTS_FILE = OUT_DIR / "results.jsonl"
LOG_FILE = ROOT / "logs" / "console.log"
STATIC = Path(__file__).resolve().parent / "static"

CPU = os.cpu_count() or 1
MAX_WORKERS = max(1, min(32, CPU))
DEFAULT_WORKERS = max(1, min(4, CPU // 2))
MAX_MINUTES = 1440          # upstream hard limit
OUTBOX_LIMIT = 4096         # upstream cap on unsubmitted tasks
SUBMIT_MAX_ROUNDS = 6       # max drain rounds per Submit action

RETRY_DELAYS = (15, 30, 60)     # seconds between automatic retries
RETRY_MAX = 4                   # consecutive automatic retries before giving up
HEARTBEAT = 30.0                # how often the in-progress runtime is persisted
SCAN_THROTTLE = 5.0             # min seconds between results.jsonl state writes

# Fixed arguments that the UI does not expose. This console targets k=114
# only, so the upstream cross-target slice is always disabled.
FIXED_ARGS = ["--targets-share", "0", "--bank-every", "256"]

IS_WINDOWS = sys.platform == "win32"

PROGRESS_RE = re.compile(
    r"^([\d,]+) tasks \(([\d,]+) cross-target\) \| ([\d,]+) inputs \| ([\d,]+) curves \| "
    r"([\d,]+) exact square tests \| ([\d,]+) q positions \| ([\d.]+)s \| epoch (\d+) \| "
    r"([\d,]+) banks pending"
)
BANK_READY_RE = re.compile(r"^Bank ready: (.+?) \(([\d,]+) tasks?;")
TARGET_BANK_READY_RE = re.compile(r"^Target bank ready: (.+?) \(([\d,]+) tasks?;")
SUBMITTED_RE = re.compile(r"^(?:Submitted|Existing submission retained): (\S+)")
COVERAGE_RE = re.compile(r"^Coverage: ([\d,]+) published tasks")
FINISHED_RE = re.compile(r"^Finished ([\d,]+) completed tasks")


def log_to_file(text):
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(f"[{stamp}] {text}\n")
    except OSError:
        pass


def load_config():
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_config(data):
    try:
        CONFIG_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass


# ------------------------------------------------------------ persistent state
# state.json survives restarts and holds the accumulated runtime, the
# results.jsonl scan cursor and the auto-submit preference.

_STATE_LOCK = threading.RLock()
_STATE = {}


def load_state():
    try:
        value = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def save_state():
    try:
        STATE_FILE.write_text(json.dumps(_STATE, indent=2), encoding="utf-8")
    except OSError:
        pass


def total_cpu_seconds():
    """Accumulated core-seconds. Falls back to the old wall-clock key."""
    with _STATE_LOCK:
        if "cpu_seconds" in _STATE:
            return float(_STATE.get("cpu_seconds") or 0.0)
        return float(_STATE.get("total_seconds") or 0.0)


def add_elapsed(seconds, workers=1):
    """Record a finished segment, weighted by the number of cores it used."""
    if not seconds or seconds <= 0:
        return
    cores = max(1, int(workers or 1))
    with _STATE_LOCK:
        base = float(_STATE.get("cpu_seconds", _STATE.get("total_seconds") or 0.0) or 0.0)
        _STATE["cpu_seconds"] = round(base + seconds * cores, 3)
        _STATE.pop("total_seconds", None)
        _STATE["sessions"] = int(_STATE.get("sessions") or 0) + 1
        save_state()


def set_in_progress(seg, seconds, workers=1):
    cores = max(1, int(workers or 1))
    with _STATE_LOCK:
        _STATE["in_progress"] = {"seg": seg, "seconds": round(seconds * cores, 3)}
        save_state()


def clear_in_progress():
    with _STATE_LOCK:
        if _STATE.get("in_progress") is not None:
            _STATE["in_progress"] = None
            save_state()


def recover_interrupted_segment():
    """A previous process died mid-run, so its final stretch was never banked.

    Fold the last heartbeat into the total instead of losing it.
    """
    with _STATE_LOCK:
        pending = _STATE.get("in_progress")
        if not isinstance(pending, dict):
            _STATE["in_progress"] = None
            return 0.0
        seconds = float(pending.get("seconds") or 0.0)
        _STATE["in_progress"] = None
        if seconds > 0:
            _STATE["total_seconds"] = round(float(_STATE.get("total_seconds") or 0.0) + seconds, 3)
        save_state()
        return seconds


def auto_submit_enabled():
    with _STATE_LOCK:
        return bool(_STATE.get("auto_submit"))


def set_auto_submit(flag):
    with _STATE_LOCK:
        _STATE["auto_submit"] = bool(flag)
        save_state()


# results.jsonl counter key -> name used by the UI
_SCAN_FIELDS = {
    "generators": "inputs",
    "curves": "curves",
    "exact_tests": "exact_tests",
    "quotient_points": "q_positions",
}


def scan_totals():
    """Incrementally sum the per-task counters in results.jsonl.

    The file is append-only, so the byte offset already consumed is remembered and
    only the new tail is parsed. A shrinking file means it was replaced: rescan.
    The persisted (offset, sums) pair is always written together, so a crash can
    only lose the tail, never double count it.
    """
    with _STATE_LOCK:
        scan = dict(_STATE.get("scan") or {})
        offset = int(scan.get("offset") or 0)
        sums = {dst: int(scan.get(dst) or 0) for dst in _SCAN_FIELDS.values()}
        saved_at = float(scan.get("saved_at") or 0.0)

    try:
        size = RESULTS_FILE.stat().st_size
    except OSError:
        return sums

    if size < offset:                      # truncated or replaced
        offset = 0
        sums = {dst: 0 for dst in _SCAN_FIELDS.values()}

    if size <= offset:
        return sums

    try:
        with RESULTS_FILE.open("rb") as fh:
            fh.seek(offset)
            chunk = fh.read(size - offset)
    except OSError:
        return sums

    end = chunk.rfind(b"\n")               # never consume a partial line
    if end < 0:
        return sums

    consumed = end + 1
    for line in chunk[:consumed].splitlines():
        if not line.strip():
            continue
        try:
            counters = json.loads(line).get("counters") or {}
        except (ValueError, AttributeError):
            continue
        for src, dst in _SCAN_FIELDS.items():
            sums[dst] += int(counters.get(src) or 0)
    offset += consumed

    now = time.time()
    with _STATE_LOCK:
        _STATE["scan"] = dict(sums, offset=offset, saved_at=saved_at)
        if now - saved_at >= SCAN_THROTTLE:
            _STATE["scan"]["saved_at"] = now
            save_state()
    return sums


_STATE.update(load_state())


def gh_identity():
    """Read the login and the display name from the local gh credentials."""
    def query(field):
        try:
            proc = subprocess.run(
                ["gh", "api", "user", "--jq", field],
                capture_output=True, text=True, timeout=20,
            )
            if proc.returncode == 0:
                return proc.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
        return ""

    return {"login": query(".login"), "name": query(".name")}


INSTANCE_FILE = Path(__file__).resolve().parent / ".instance.json"
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000


def pid_alive(pid):
    """Check whether a pid is alive. Never use os.kill(pid, 0) on Windows: it kills."""
    if not IS_WINDOWS:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    try:
        k32 = ctypes.windll.kernel32
        handle = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        k32.CloseHandle(handle)
        return True
    except (AttributeError, OSError):
        return False


def existing_instance():
    """Return the URL of a console that is already running, if any."""
    try:
        info = json.loads(INSTANCE_FILE.read_text(encoding="utf-8"))
        pid, port = int(info["pid"]), int(info["port"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if pid == os.getpid() or not pid_alive(pid):
        return None
    url = f"http://127.0.0.1:{port}/"
    try:
        import urllib.request
        urllib.request.urlopen(url + "api/state", timeout=2).read()
    except OSError:
        return None
    return url


def send_ctrl_c(_pid=None):
    """Broadcast CTRL_C_EVENT to this console group, like pressing Ctrl+C once.

    On Windows CTRL_C_EVENT cannot be delivered to another process group (the
    system silently ignores it), so it has to be broadcast. This process ignores
    SIGINT, so only the runner child receives it and shuts down gracefully.
    """
    if IS_WINDOWS:
        try:
            return bool(ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, 0))
        except (AttributeError, OSError):
            return False
    try:
        os.kill(_pid, signal.SIGINT)
        return True
    except (OSError, TypeError):
        return False


def checkpoint_identity():
    """Read the locked identity from the checkpoint, or None if there is none."""
    if not DB_FILE.exists():
        return None
    try:
        con = sqlite3.connect(DB_FILE.as_uri() + "?mode=ro", uri=True, timeout=1.5)
    except sqlite3.Error:
        return None
    try:
        row = con.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
        return json.loads(row[0]) if row else None
    except (sqlite3.Error, ValueError, TypeError):
        return None
    finally:
        con.close()


def db_stats():
    """Read-only query of the checkpoint for counters. None if unavailable."""
    if not DB_FILE.exists():
        return None
    uri = DB_FILE.as_uri() + "?mode=ro"
    try:
        con = sqlite3.connect(uri, uri=True, timeout=1.5)
    except sqlite3.Error:
        return None
    try:
        def scalar(sql):
            try:
                return con.execute(sql).fetchone()[0]
            except sqlite3.Error:
                return None

        unbanked = scalar(
            "SELECT count(*) FROM tasks LEFT JOIN banks ON tasks.bank=banks.id "
            "WHERE tasks.result IS NOT NULL AND (banks.submitted IS NULL "
            "OR banks.submitted LIKE 'uncertain;%')"
        )
        unbanked_target = scalar(
            "SELECT count(*) FROM target_tasks LEFT JOIN banks ON target_tasks.bank=banks.id "
            "WHERE target_tasks.result IS NOT NULL AND (banks.submitted IS NULL "
            "OR banks.submitted LIKE 'uncertain;%')"
        )
        discoveries = []
        try:
            for did, xyz, bank in con.execute(
                    "SELECT id, xyz, bank FROM discoveries ORDER BY id").fetchall():
                try:
                    triple = json.loads(xyz)
                except (ValueError, TypeError):
                    continue
                if isinstance(triple, (list, tuple)) and len(triple) == 3:
                    # The three integers are far beyond 2^53, so they must be sent to the
                    # front end as strings; a JS Number would silently lose precision.
                    discoveries.append({
                        "id": did,
                        "xyz": [str(v) for v in triple],
                        "bank": bank,
                    })
        except sqlite3.Error:
            pass

        return {
            "unbanked": (unbanked or 0) + (unbanked_target or 0),
            "banks_total": scalar("SELECT count(*) FROM banks") or 0,
            "pending_banks": scalar("SELECT count(*) FROM banks WHERE submitted IS NULL") or 0,
            "uncertain_banks": scalar(
                "SELECT count(*) FROM banks WHERE submitted LIKE 'uncertain;%'"
            ) or 0,
            "submitted_banks": scalar(
                "SELECT count(*) FROM banks WHERE submitted LIKE 'https://github.com/%'"
            ) or 0,
            "total_tasks": scalar("SELECT count(*) FROM tasks WHERE result IS NOT NULL") or 0,
            "discoveries": discoveries,
        }
    finally:
        con.close()


# ---------------------------------------------------------------- console


class Console:
    def __init__(self):
        self.lock = threading.RLock()
        self.proc = None
        self.mode = None                 # run / submit
        self.status = "idle"             # idle|running|stopping|submitting|paused_cap|found|error
        self.started_at = None
        self.finished_at = None
        self.workers = DEFAULT_WORKERS
        self.minutes = MAX_MINUTES
        self.name = load_config().get("name", "")
        self.github = ""
        self.display_name = ""
        self.progress = {}
        self.counter = {"banks_written": 0, "submitted": 0, "tasks_done": 0}
        self.log = deque(maxlen=600)
        self.last_error = ""
        self.cap_hit = False
        self.user_stop = False
        self.relay = 0
        self.submit_rounds = 0
        self.tasks_at_start = 0
        self.exit_code = None
        self.version = ""
        self.seg_id = ""
        self.retry_token = 0
        self.retry_attempts = 0
        self.auto_pending = False
        self.error_kind = ""
        self.scan_cache = (0.0, {})
        threading.Thread(target=self._init_identity, daemon=True).start()
        threading.Thread(target=self._heartbeat, daemon=True).start()

    # -- init

    def _init_identity(self):
        ident = gh_identity()
        saved = checkpoint_identity() or {}
        with self.lock:
            self.github = ident["login"]
            self.display_name = ident["name"] or ident["login"]
            # The checkpoint is authoritative; otherwise use the GitHub display name.
            if saved.get("name"):
                self.name = saved["name"]
            elif not self.name:
                self.name = self.display_name

    # -- runtime accounting

    def _elapsed(self):
        with self.lock:
            if self.started_at is None:
                return 0.0
            end = self.finished_at if self.finished_at else time.monotonic()
            return max(0.0, end - self.started_at)

    def _heartbeat(self):
        """Persist the in-progress runtime so a hard kill loses at most HEARTBEAT."""
        while True:
            time.sleep(HEARTBEAT)
            try:
                with self.lock:
                    alive = self.proc is not None and self.proc.poll() is None
                    seg = self.seg_id
                    cores = self.workers
                if alive and seg:
                    set_in_progress(seg, self._elapsed(), cores)
            except Exception:                      # never let the heartbeat die
                pass

    def _schedule_retry(self):
        with self.lock:
            self.retry_attempts += 1
            attempt = self.retry_attempts
            self.retry_token += 1
            token = self.retry_token
            self.status = "retrying"
            delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
        self._emit(f">> network error; retrying in {delay}s (attempt {attempt}/{RETRY_MAX})")

        def worker():
            time.sleep(delay)
            with self.lock:
                if self.retry_token != token or self.user_stop:
                    return
                self.status = "idle"
            self.start_run(self.workers, self.minutes, self.name, relay=True)

        threading.Thread(target=worker, daemon=True).start()

    # -- internals

    def _emit(self, line):
        with self.lock:
            self.log.append(line)
        log_to_file("[runner] " + line)

    def _parse(self, line):
        with self.lock:
            m = PROGRESS_RE.match(line)
            if m:
                self.progress = {
                    "tasks": int(m.group(1).replace(",", "")),
                    "inputs": int(m.group(3).replace(",", "")),
                    "curves": int(m.group(4).replace(",", "")),
                    "exact_tests": int(m.group(5).replace(",", "")),
                    "q_positions": int(m.group(6).replace(",", "")),
                    "elapsed": float(m.group(7)),
                    "epoch": int(m.group(8)),
                    "banks_pending": int(m.group(9).replace(",", "")),
                }
                self.counter["tasks_done"] = self.progress["tasks"]
                return
            if BANK_READY_RE.match(line) or TARGET_BANK_READY_RE.match(line):
                self.counter["banks_written"] += 1
                return
            m = SUBMITTED_RE.match(line)
            if m:
                self.counter["submitted"] += 1
                return
            m = COVERAGE_RE.match(line)
            if m:
                self.progress["coverage"] = int(m.group(1).replace(",", ""))
                return
            if "EXACT SOLUTION PRESERVED" in line or "EXACT IDENTITY RECOVERED" in line:
                self.status = "found"
                return
            if "Outbox capacity reached" in line:
                self.cap_hit = True
                return
            if line.startswith("Scheduling log:"):
                self.seg_id = line.rsplit("\\", 1)[-1].rsplit("/", 1)[-1].replace(".jsonl", "")
                return
            m = re.match(r"^Math Gambling ([\d.]+) \|", line)
            if m:
                self.version = m.group(1)

    def _spawn(self, cmd, mode):
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        # Deliberately no CREATE_NEW_PROCESS_GROUP: on Windows CTRL_C_EVENT cannot
        # reach another process group. The child stays in this console group and
        # stop() broadcasts with GenerateConsoleCtrlEvent instead.
        base = db_stats() or {}
        with self.lock:
            self.mode = mode
            self.exit_code = None
            self.user_stop = False
            self.cap_hit = False
            self.error_kind = ""
            self.seg_id = f"seg-{int(time.time() * 1000)}"
            self.tasks_at_start = base.get("total_tasks", 0)
            self.started_at = time.monotonic()
            self.finished_at = None
            self.last_error = ""
            self.progress = {}
            self.counter = {"banks_written": 0, "submitted": 0, "tasks_done": 0}
            self.status = "running" if mode == "run" else "submitting"
            if mode == "run":
                self.auto_pending = False
        proc = subprocess.Popen(
            cmd, cwd=str(RUNNER_DIR), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1, env=env,
        )
        with self.lock:
            self.proc = proc
        threading.Thread(target=self._watch, args=(proc, mode), daemon=True).start()

    def _watch(self, proc, mode):
        for line in proc.stdout:
            line = line.rstrip("\r\n")
            if not line:
                continue
            self._emit(line)
            self._parse(line)
        rc = proc.wait()
        self._on_exit(rc, mode)

    # -- start / stop

    def start_run(self, workers, minutes, name, relay=False):
        with self.lock:
            if self.proc is not None and self.proc.poll() is None:
                return False, "already running"
        if not RUNNER.exists():
            return False, f"tools/runner.py not found; unzip the upstream release into {RUNNER_DIR}"
        locked = DB_FILE.exists()
        name = (name or self.name or self.display_name or "").strip()
        if not locked and not name:
            return False, "a credit name is required"
        workers = max(1, min(MAX_WORKERS, int(workers)))
        minutes = max(1, min(MAX_MINUTES, int(minutes)))
        with self.lock:
            self.workers = workers
            self.minutes = minutes
            if not relay:
                self.relay = 0
                self.retry_attempts = 0
                self.retry_token += 1      # a manual start cancels any pending retry
            if not locked:
                self.name = name
        if not locked:
            cfg = load_config()
            cfg["name"] = name
            save_config(cfg)

        cmd = [
            sys.executable, str(RUNNER),
            "--minutes", str(minutes),
            "--workers", str(workers),
            "--output", str(OUT_DIR),
        ] + FIXED_ARGS
        # Once a checkpoint exists the credit name is frozen: let the upstream
        # reuse the saved attribution instead of passing --name/--github again.
        if not locked:
            cmd += ["--name", name]
            if self.github:
                cmd += ["--github", self.github]
        self._emit("$ " + " ".join(cmd[1:]))
        try:
            self._spawn(cmd, "run")
        except OSError as exc:
            with self.lock:
                self.status = "error"
                self.last_error = str(exc)
            return False, str(exc)
        return True, ""

    def start_submit(self):
        with self.lock:
            if self.proc is not None and self.proc.poll() is None:
                return False, "already running"
        if not RUNNER.exists():
            return False, f"tools/runner.py not found; unzip the upstream release into {RUNNER_DIR}"
        if not DB_FILE.exists():
            return False, "there is nothing local to submit yet"
        if not self.github:
            return False, "the local gh credentials are not available; cannot submit"
        # --max-tasks 1 makes the upstream take its "drain the queue" branch.
        # The saved attribution is reused, so no --name/--github is passed.
        cmd = [
            sys.executable, str(RUNNER),
            "--submit",
            "--minutes", "60",
            "--workers", "1",
            "--max-tasks", "1",
            "--output", str(OUT_DIR),
        ] + FIXED_ARGS
        self._emit("$ " + " ".join(cmd[1:]))
        try:
            self._spawn(cmd, "submit")
        except OSError as exc:
            with self.lock:
                self.status = "error"
                self.last_error = str(exc)
            return False, str(exc)
        return True, ""

    def stop(self):
        with self.lock:
            if self.status == "retrying":
                self.retry_token += 1          # cancel the pending automatic retry
                self.user_stop = True
                self.status = "idle"
                cancelled = True
                proc = None
            else:
                cancelled = False
                proc = self.proc
                if proc is None or proc.poll() is not None:
                    return False, "nothing is running"
                self.user_stop = True
                self.auto_pending = False
                if self.status in ("running", "submitting"):
                    self.status = "stopping"
        if cancelled:
            self._emit(">> automatic retry cancelled")
            return True, ""
        if not send_ctrl_c(proc.pid):
            return False, "could not send the stop signal"
        self._emit(">> stop signal sent, waiting for the current task to finish…")
        return True, ""

    # -- exit handling

    def _on_exit(self, rc, mode):
        with self.lock:
            self.exit_code = rc
            self.proc = None
            self.finished_at = time.monotonic()
            found = self.status == "found"
            cap = self.cap_hit
            stopped_by_user = self.user_stop
            elapsed = (self.finished_at - self.started_at) if self.started_at else 0
            minutes = self.minutes
            auto_next = self.auto_pending
            recent = "\n".join(list(self.log)[-40:])
            cores_used = 1 if mode == "submit" else self.workers

        self._emit(f">> process exited with code {rc}")
        if elapsed > 0:
            add_elapsed(elapsed, cores_used)
            clear_in_progress()

        if mode == "submit":
            pending = (db_stats() or {}).get("pending_banks", 0)
            with self.lock:
                self.submit_rounds += 1
                rounds = self.submit_rounds
            if rc == 0 and pending > 0 and rounds < SUBMIT_MAX_ROUNDS and not stopped_by_user:
                self._emit(f">> {pending} bank(s) still pending, draining again (round {rounds + 1})…")
                self.start_submit()
                return
            with self.lock:
                self.submit_rounds = 0
                self.auto_pending = False
                if rc != 0:
                    self.status = "error"
                    self.error_kind = "generic"
                    self.last_error = "Submitting failed; see the log below."
                else:
                    self.status = "idle"
                    self.error_kind = ""
            if auto_next and rc == 0 and not stopped_by_user:
                self._emit(">> queue drained; continuing with the next compute round…")
                self.start_run(self.workers, self.minutes, self.name, relay=True)
            return

        network = rc != 0 and any(token in recent for token in (
            "Coverage download unavailable",
            "Coverage refresh failed",
            "Coverage unavailable",
        ))
        cap_auto = bool(cap and not stopped_by_user and auto_submit_enabled())

        with self.lock:
            self.submit_rounds = 0
            relay = False
            segment = 0
            if found:
                self.status = "found"
                self.error_kind = ""
            elif rc == 0 and stopped_by_user:
                self.status = "idle"
                self.error_kind = ""
            elif cap_auto:
                self.status = "submitting"
                self.error_kind = ""
                self.auto_pending = True
            elif cap:
                self.status = "paused_cap"
                self.error_kind = "cap"
                self.last_error = ("Local unsubmitted tasks reached the 4096 cap. "
                                   "Submit to continue.")
            elif network:
                self.status = "error"
                self.error_kind = "network"
                self.last_error = ("Network error while fetching the upstream coverage "
                                   "index. Finished work is saved.")
            elif rc != 0:
                self.status = "error"
                self.error_kind = "generic"
                self.last_error = "The run failed; see the log below."
            else:
                self.status = "idle"
                self.error_kind = ""
                self.retry_attempts = 0
            if (rc == 0 and not stopped_by_user and not found and not cap
                    and minutes and elapsed >= minutes * 60 * 0.95):
                relay = True
                self.relay += 1
                segment = self.relay

        if cap_auto:
            self._emit(">> outbox cap reached and auto-submit is on; submitting, then continuing…")
            self.start_submit()
            return
        if network and not stopped_by_user and self.retry_attempts < RETRY_MAX:
            self._schedule_retry()
            return
        if relay:
            self._emit(f">> time cap reached, continuing automatically (segment {segment})…")
            self.start_run(self.workers, self.minutes, self.name, relay=True)

    # -- snapshot

    def snapshot(self):
        with self.lock:
            proc = self.proc
            running = proc is not None and proc.poll() is None
            now = time.monotonic()
            if running and self.started_at:
                elapsed = now - self.started_at
            elif self.started_at and self.finished_at:
                elapsed = self.finished_at - self.started_at
            elif self.progress.get("elapsed"):
                elapsed = self.progress["elapsed"]
            else:
                elapsed = 0
            snap = {
                "status": self.status,
                "mode": self.mode,
                "running": running,
                "elapsed": round(elapsed, 1),
                "workers": self.workers,
                "minutes": self.minutes,
                "name": self.name,
                "github": self.github,
                "display_name": self.display_name,
                "progress": dict(self.progress),
                "counter": dict(self.counter),
                "last_error": self.last_error,
                "exit_code": self.exit_code,
                "version": self.version,
                "relay": self.relay,
            }
            tasks_at_start = self.tasks_at_start
        stats = db_stats()
        # After a stop the progress line stops updating; correct it from the checkpoint.
        if not running and stats:
            snap["counter"]["tasks_done"] = max(0, stats["total_tasks"] - tasks_at_start)
        counters = scan_totals()
        cpu_total = total_cpu_seconds()
        with self.lock:
            error_kind = self.error_kind
            auto_pending = self.auto_pending
            attempts = self.retry_attempts
        snap.update({
            "runtime": {
                "cpu_seconds": round(cpu_total, 1),
                "total_hours": round(cpu_total / 3600.0, 2),
            },
            "totals": {
                "tasks": (stats or {}).get("total_tasks", 0),
                "banks": (stats or {}).get("banks_total", 0),
                "inputs": counters.get("inputs", 0),
                "curves": counters.get("curves", 0),
                "exact_tests": counters.get("exact_tests", 0),
            },
            "auto_submit": auto_submit_enabled(),
            "auto_pending": auto_pending,
            "error_kind": error_kind,
            "retry": {"attempts": attempts, "max": RETRY_MAX},
            "limits": {
                "max_workers": MAX_WORKERS,
                "default_workers": DEFAULT_WORKERS,
                "max_minutes": MAX_MINUTES,
                "outbox_limit": OUTBOX_LIMIT,
                "cpu": CPU,
            },
            "paths": {
                "runner": RUNNER.exists(),
                "runner_dir": str(RUNNER_DIR),
                "output_dir": str(OUT_DIR),
                "db": DB_FILE.exists(),
            },
            "db": stats,
            "log": list(self.log)[-120:],
        })
        return snap


CONSOLE = Console()
TOKEN = os.urandom(16).hex()


# ---------------------------------------------------------------- HTTP


class Handler(BaseHTTPRequestHandler):
    server_version = "MathGamblingConsole/1.0"

    def log_message(self, *args):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False)
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}

    def _authorized(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        if host not in ("127.0.0.1", "localhost"):
            return False
        return self.headers.get("X-Token") == TOKEN

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                page = (STATIC / "index.html").read_text(encoding="utf-8")
            except OSError:
                self._send(500, "missing app/static/index.html", "text/plain; charset=utf-8")
                return
            self._send(200, page.replace("__TOKEN__", TOKEN), "text/html; charset=utf-8")
            return
        if self.path == "/api/state":
            self._send(200, CONSOLE.snapshot())
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self._authorized():
            self._send(403, {"ok": False, "error": "forbidden"})
            return
        body = self._json_body()
        if self.path == "/api/start":
            ok, err = CONSOLE.start_run(
                body.get("workers", DEFAULT_WORKERS),
                body.get("minutes", MAX_MINUTES),
                body.get("name", ""),
            )
        elif self.path == "/api/stop":
            ok, err = CONSOLE.stop()
        elif self.path == "/api/submit":
            ok, err = CONSOLE.start_submit()
        elif self.path == "/api/settings":
            if "auto_submit" in body:
                set_auto_submit(bool(body["auto_submit"]))
            ok, err = True, ""
        elif self.path == "/api/quit":
            self._send(200, {"ok": True, "error": ""})
            threading.Thread(target=quit_console, daemon=True).start()
            return
        else:
            self._send(404, {"ok": False, "error": "not found"})
            return
        self._send(200, {"ok": ok, "error": err})


def shutdown_runner(wait=20):
    """Let the runner finish gracefully, then exit the console."""
    with CONSOLE.lock:
        proc = CONSOLE.proc
    if proc is None or proc.poll() is not None:
        return
    send_ctrl_c(proc.pid)
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.2)


def quit_console():
    log_to_file("quit requested from the UI")
    shutdown_runner()
    try:
        INSTANCE_FILE.unlink()
    except OSError:
        pass
    os._exit(0)


def main():
    log_to_file("=" * 60)
    log_to_file(f"console starting | python {sys.version.split()[0]} | cpu={CPU}")
    if not RUNNER.exists():
        log_to_file(f"warning: {RUNNER} not found")
    recovered = recover_interrupted_segment()
    if recovered > 0:
        log_to_file(f"recovered {recovered:.0f}s of runtime from an interrupted session")

    if IS_WINDOWS:
        # Stopping broadcasts CTRL_C_EVENT to the whole console group, so the
        # console itself must ignore it or it would exit along with the runner.
        signal.signal(signal.SIGINT, lambda *_: None)

    running = existing_instance()
    if running:
        log_to_file(f"console already running, reusing {running}")
        if not os.environ.get("MG_NO_BROWSER"):
            webbrowser.open(running)
        return

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    url = f"http://127.0.0.1:{port}/"
    try:
        INSTANCE_FILE.write_text(json.dumps({"pid": os.getpid(), "port": port}), encoding="utf-8")
    except OSError:
        pass
    log_to_file(f"listening on {url}")
    try:
        print(f"Math Gambling console running at {url}", flush=True)
    except (AttributeError, OSError):
        pass

    if not os.environ.get("MG_NO_BROWSER"):
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    atexit.register(shutdown_runner)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        shutdown_runner()
        httpd.server_close()


if __name__ == "__main__":
    main()
