#!/usr/bin/env python3
from __future__ import annotations
import os, sys, time, shlex, subprocess, signal, threading, pathlib

APP_DIR = pathlib.Path(os.getenv("APP_DIR", "/app"))
GIT_ENABLE   = (os.getenv("GIT_ENABLE", "1").lower() in ("1","true","yes"))
GIT_MODE     = os.getenv("GIT_MODE", "watch").lower()   # watch|update|off
GIT_REMOTE   = os.getenv("GIT_REMOTE", "origin")
GIT_BRANCH   = os.getenv("GIT_BRANCH", "main")
GIT_INTERVAL = int(os.getenv("GIT_INTERVAL", "30"))
GIT_RESET    = (os.getenv("GIT_RESET", "1").lower() in ("1","true","yes"))
GIT_RESTART  = (os.getenv("GIT_RESTART", "1").lower() in ("1","true","yes"))
GIT_VERBOSE  = (os.getenv("GIT_VERBOSE", "1").lower() in ("1","true","yes"))
PIP_ON_CHANGE= (os.getenv("PIP_ON_CHANGE", "1").lower() in ("1","true","yes"))
PIP_FILE     = os.getenv("PIP_FILE", "requirements.txt")

APP_CMD      = os.getenv("APP_CMD", "python -u run.py")
EXIT_ON_CRASH= (os.getenv("APP_EXIT_ON_CRASH", "0").lower() in ("1","true","yes"))

_restart_evt = threading.Event()
_lock = threading.Lock()

def log(msg: str): print(msg, flush=True)

def run(cmd: str, cwd: pathlib.Path | None = None, check=True) -> subprocess.CompletedProcess:
    if GIT_VERBOSE: log(f"[exec] {cmd}")
    return subprocess.run(shlex.split(cmd), cwd=cwd, text=True, capture_output=not GIT_VERBOSE, check=check)

def have_git() -> bool:
    try: run("git --version"); return True
    except Exception: return False

def in_repo() -> bool: return (APP_DIR / ".git").exists()

def get_rev(ref: str) -> str | None:
    try:
        cp = run(f"git rev-parse {ref}", cwd=APP_DIR)
        return (cp.stdout or "").strip()
    except Exception:
        return None

def try_fix_lock():
    try:
        run(f"git remote prune {GIT_REMOTE}", cwd=APP_DIR, check=False)
        run("git gc --prune=now", cwd=APP_DIR, check=False)
    except Exception:
        pass

def fetch_reset_once() -> tuple[bool, str, str]:
    """(changed, old, new)"""
    if not (GIT_ENABLE and have_git() and in_repo()):
        if not have_git(): log("[git] git недоступен — автообновление выключено")
        elif not in_repo(): log("[git] .git не найден — автообновление выключено")
        return (False, "", "")

    old_rev = get_rev("HEAD") or ""
    try:
        run(f"git fetch {GIT_REMOTE} {GIT_BRANCH}", cwd=APP_DIR)
    except subprocess.CalledProcessError as e:
        if "cannot lock ref" in (e.stderr or "") or "unable to update local ref" in (e.stderr or ""):
            log("[git] warning: lock ref issue -> prune/gc")
            try_fix_lock()
            run(f"git fetch {GIT_REMOTE} {GIT_BRANCH}", cwd=APP_DIR)
        else:
            log(f"[git] fetch error: {e}")
            return (False, old_rev, old_rev)

    new_remote = f"{GIT_REMOTE}/{GIT_BRANCH}"
    new_rev = get_rev(new_remote) or old_rev

    if old_rev != new_rev:
        log(f"[git] update: {old_rev} -> {new_rev}")
        if GIT_RESET:
            run(f"git reset --hard {new_remote}", cwd=APP_DIR)
        else:
            run(f"git merge --ff-only {new_remote}", cwd=APP_DIR)

        if PIP_ON_CHANGE and (APP_DIR / PIP_FILE).exists():
            log("[deps] installing requirements...")
            run(f"pip install -r {PIP_FILE}", cwd=APP_DIR, check=False)

        return (True, old_rev, new_rev)

    if GIT_VERBOSE: log("[git] up-to-date")
    return (False, old_rev, new_rev)

class AppProc:
    def __init__(self, cmd: str):
        self.cmd = cmd
        self.p: subprocess.Popen | None = None

    def start(self):
        with _lock:
            log(f"[app] starting: {self.cmd}")
            self.p = subprocess.Popen(shlex.split(self.cmd), cwd=APP_DIR)

    def stop(self, timeout=10):
        with _lock:
            if not self.p: return
            try:
                self.p.terminate()
                self.p.wait(timeout=timeout)
            except Exception:
                try: self.p.kill()
                except Exception: pass

    def is_running(self) -> bool:
        with _lock:
            return bool(self.p and self.p.poll() is None)

def watch_git_loop():
    log(f"[git] watch loop started (every {GIT_INTERVAL}s, branch {GIT_BRANCH})")
    while True:
        changed, *_ = fetch_reset_once()
        if changed and GIT_RESTART:
            log("[git] changes detected -> scheduling app restart")
            _restart_evt.set()
        time.sleep(GIT_INTERVAL)

def main():
    app = AppProc(APP_CMD)

    if GIT_MODE == "off" or not GIT_ENABLE:
        log("[git] disabled -> run app")
        app.start()
        while True:
            if not app.is_running():
                rc = app.p.returncode if app.p else 1
                log(f"[app] exited rc={rc}")
                if EXIT_ON_CRASH: sys.exit(rc or 0)
                log("[app] restarting...")
                app.start()
            time.sleep(1)

    # initial update if needed
    fetch_reset_once()
    app.start()

    if GIT_MODE == "update":
        # one-time update, then stay up & auto-restart on crash
        log("[git] one-shot update mode")
    else:
        t = threading.Thread(target=watch_git_loop, daemon=True)
        t.start()

    # main supervisor loop
    while True:
        if _restart_evt.is_set():
            _restart_evt.clear()
            log("[supervisor] restarting app (git update)")
            app.stop(timeout=8)
            app.start()
        elif not app.is_running():
            rc = app.p.returncode if app.p else 1
            log(f"[supervisor] app exited rc={rc}")
            if EXIT_ON_CRASH:
                log("[supervisor] exit on crash enabled -> stopping container")
                sys.exit(rc or 0)
            log("[supervisor] restarting app...")
            app.start()
        time.sleep(1)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try: main()
    except KeyboardInterrupt: pass
