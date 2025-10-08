#!/usr/bin/env python3
# docker/git_entrypoint.py
from __future__ import annotations

import os
import sys
import time
import shlex
import hashlib
import signal
import subprocess as sp
from pathlib import Path
from typing import Optional

# --------------------------- env helpers ---------------------------

def env_str(key: str, default: str = "") -> str:
    return os.environ.get(key, default)

def env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)).strip())
    except Exception:
        return default

def env_bool(key: str, default: bool = False) -> bool:
    v = os.environ.get(key)
    if v is None:
        return default
    v = str(v).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

# Core app/git settings
APP_CMD         = env_str("APP_CMD", "")
APP_DIR         = Path(env_str("APP_DIR", "/app")).resolve()
GIT_DIR         = Path(env_str("GIT_DIR", "/git/.git")).resolve()
GIT_WORK_TREE   = Path(env_str("GIT_WORK_TREE", str(APP_DIR))).resolve()
GIT_MODE        = env_str("GIT_MODE", "watch").lower()      # watch|update|off
GIT_ENABLE      = env_bool("GIT_ENABLE", True)
GIT_REMOTE_NAME = env_str("GIT_REMOTE", "origin")
GIT_BRANCH      = env_str("GIT_BRANCH", "main")
GIT_URL         = env_str("GIT_URL", "")                    # опционально, если нужно явно задать URL
GIT_INTERVAL    = env_int("GIT_INTERVAL", 30)
GIT_RESET       = env_bool("GIT_RESET", True)               # hard reset на origin/BRANCH
GIT_RESTART     = env_bool("GIT_RESTART", False)            # рестартовать приложение при обнове
GIT_VERBOSE     = env_bool("GIT_VERBOSE", True)

# Python deps auto-install
PIP_ON_CHANGE   = env_bool("PIP_ON_CHANGE", True)
PIP_FILE        = env_str("PIP_FILE", "requirements.txt")

# --------------------------- logging ---------------------------

def log(*a):
    print("[entry]", *a, flush=True)

def vlog(*a):
    if GIT_VERBOSE:
        log(*a)

# --------------------------- git helpers ---------------------------

def git_env() -> dict:
    e = dict(os.environ)
    e["GIT_DIR"] = str(GIT_DIR)
    e["GIT_WORK_TREE"] = str(GIT_WORK_TREE)
    # чтобы git не ругался на "dubious ownership"
    e.setdefault("HOME", "/tmp")
    return e

def run_git(args: list[str], check=True, capture=False, allow_fail=False) -> sp.CompletedProcess:
    """Запуск git с GIT_DIR/GIT_WORK_TREE."""
    cmd = ["git"] + args
    vlog("git:", " ".join(shlex.quote(c) for c in cmd))
    try:
        return sp.run(
            cmd,
            env=git_env(),
            cwd=str(GIT_WORK_TREE),
            check=check,
            text=True,
            capture_output=capture
        )
    except sp.CalledProcessError as e:
        if allow_fail:
            return e
        raise

def run_git_app_tree(args: list[str], capture=True) -> Optional[str]:
    """
    Запуск git в рабочем дереве БЕЗ GIT_DIR/GIT_WORK_TREE —
    полезно, чтобы прочитать URL из /app/.git, если он есть.
    """
    env = dict(os.environ)
    env.pop("GIT_DIR", None)
    env.pop("GIT_WORK_TREE", None)
    try:
        cp = sp.run(["git"] + args, cwd=str(GIT_WORK_TREE), text=True, capture_output=True, check=True, env=env)
        return cp.stdout.strip()
    except Exception:
        return None

def ensure_dirs():
    GIT_DIR.parent.mkdir(parents=True, exist_ok=True)
    APP_DIR.mkdir(parents=True, exist_ok=True)
    GIT_WORK_TREE.mkdir(parents=True, exist_ok=True)

def ensure_repo_initialized():
    """Инициализация репозитория в GIT_DIR + настройка origin/ветки."""
    ensure_dirs()

    # Проверяем, инициализирован ли репозиторий
    inited = False
    try:
        cp = run_git(["rev-parse", "--git-dir"], capture=True, check=True)
        inited = (cp.stdout.strip() != "")
    except Exception:
        inited = False

    if not inited:
        log(f"Initializing repository: GIT_DIR={GIT_DIR}, WORK_TREE={GIT_WORK_TREE}")
        run_git(["init"])
        # Привязываем worktree (на случай, если git сам не увидел)
        run_git(["config", "core.worktree", str(GIT_WORK_TREE)], check=False, allow_fail=True)

        # Попробуем унаследовать URL origin из /app/.git, если он есть
        remote_url = GIT_URL
        if not remote_url:
            inherited = run_git_app_tree(["config", "--get", f"remote.{GIT_REMOTE_NAME}.url"])
            if inherited:
                remote_url = inherited
                vlog(f"Inherited remote URL from /app/.git: {remote_url}")

        if remote_url:
            run_git(["remote", "add", GIT_REMOTE_NAME, remote_url], check=False, allow_fail=True)
            # ВАЖНО: корректный refspec только для remote-tracking веток
            run_git([
                "config",
                f"remote.{GIT_REMOTE_NAME}.fetch",
                f"+refs/heads/*:refs/remotes/{GIT_REMOTE_NAME}/*"
            ], check=False, allow_fail=True)
            log(f"Remote set: {GIT_REMOTE_NAME} -> {remote_url}")
        else:
            log("WARNING: remote URL is not set (env GIT_URL empty and /app/.git missing). "
                "Fetch/pull will be skipped until you set it.")

    # Разрешаем безопасно работать с worktree
    try:
        sp.run(["git", "config", "--global", "--add", "safe.directory", str(GIT_WORK_TREE)],
               text=True, check=False)
    except Exception:
        pass

def remote_url_exists() -> bool:
    try:
        out = run_git(["config", "--get", f"remote.{GIT_REMOTE_NAME}.url"], capture=True, check=True)
        return bool(out.stdout.strip())
    except Exception:
        return False

def fetch_branch() -> bool:
    """git fetch; True если прошло успешно и есть удалённый URL."""
    if not remote_url_exists():
        vlog("No remote URL configured, skip fetch.")
        return False
    # ВАЖНО: просто обновляем remote-tracking refs. Никаких main:main!
    run_git(["fetch", "--prune", GIT_REMOTE_NAME], check=False)
    return True

def current_head() -> Optional[str]:
    try:
        out = run_git(["rev-parse", "HEAD"], capture=True, check=True)
        return out.stdout.strip()
    except Exception:
        return None

def remote_head() -> Optional[str]:
    try:
        out = run_git(["rev-parse", f"{GIT_REMOTE_NAME}/{GIT_BRANCH}"], capture=True, check=True)
        return out.stdout.strip()
    except Exception:
        return None

def checkout_branch_if_needed():
    """Убедимся, что на нужной ветке (или создадим её)."""
    try:
        out = run_git(["symbolic-ref", "--short", "HEAD"], capture=True, check=True)
        cur = out.stdout.strip()
    except Exception:
        cur = ""

    if cur != GIT_BRANCH:
        # Попробуем привязать к удалённой ветке, если есть
        if remote_head():
            run_git(["checkout", "-B", GIT_BRANCH, f"{GIT_REMOTE_NAME}/{GIT_BRANCH}"], check=False)
        else:
            run_git(["checkout", "-B", GIT_BRANCH], check=False)

def hard_sync_to_remote() -> bool:
    """
    Синхронизация рабочего дерева с origin/BRANCH.
    Возвращает True, если рабочее дерево изменилось.
    """
    before = current_head()
    if not fetch_branch():
        return False
    checkout_branch_if_needed()

    if GIT_RESET:
        run_git(["reset", "--hard", f"{GIT_REMOTE_NAME}/{GIT_BRANCH}"], check=False)
    else:
        # ff-only merge
        run_git(["merge", "--ff-only", f"{GIT_REMOTE_NAME}/{GIT_BRANCH}"], check=False)

    after = current_head()
    changed = (before != after)
    if changed:
        vlog(f"Repo changed: {before} -> {after}")
    else:
        vlog("No changes in repo.")
    return changed

# --------------------------- pip helpers ---------------------------

def sha256_of(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def pip_install_if_needed(prev_hash: Optional[str]) -> Optional[str]:
    """Если requirements.txt изменился — ставим зависимости. Возвращаем новый хэш."""
    req = GIT_WORK_TREE / PIP_FILE
    if not PIP_ON_CHANGE or not req.exists():
        return prev_hash
    new_hash = sha256_of(req)
    if new_hash and new_hash != prev_hash:
        log(f"requirements changed -> pip install -r {PIP_FILE}")
        try:
            sp.run([sys.executable, "-m", "pip", "install", "-r", str(req)], check=True)
        except sp.CalledProcessError as e:
            log("pip install failed:", e)
        return new_hash
    return prev_hash

# --------------------------- app runner ---------------------------

class AppProc:
    def __init__(self, cmd: str):
        self.cmd = cmd
        self.proc: Optional[sp.Popen] = None

    def start(self):
        if not self.cmd:
            vlog("APP_CMD is empty, nothing to run.")
            return
        log("Starting app:", self.cmd)
        # shell=False для безопасности
        args = shlex.split(self.cmd)
        self.proc = sp.Popen(args, cwd=str(GIT_WORK_TREE), env=os.environ.copy())

    def stop(self):
        if self.proc and self.proc.poll() is None:
            log("Stopping app...")
            try:
                self.proc.send_signal(signal.SIGTERM)
                try:
                    self.proc.wait(timeout=10)
                except sp.TimeoutExpired:
                    self.proc.kill()
            except Exception:
                pass
        self.proc = None

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

# --------------------------- main flow ---------------------------

def one_update_cycle(prev_req_hash: Optional[str]) -> tuple[bool, Optional[str]]:
    """Возвращает (repo_changed, new_req_hash)."""
    changed = hard_sync_to_remote()
    new_hash = pip_install_if_needed(prev_req_hash)
    return changed, new_hash

def main():
    ensure_repo_initialized()

    req_hash = sha256_of(GIT_WORK_TREE / PIP_FILE)
    # Первая синхронизация (best-effort)
    try:
        repo_changed, req_hash = one_update_cycle(req_hash)
    except Exception as e:
        log("Initial sync error:", e)

    # Стартуем приложение
    app = AppProc(APP_CMD)
    app.start()

    if not GIT_ENABLE or GIT_MODE == "off":
        # Просто ждём, чтобы контейнер не выходил
        log("Git watcher disabled (GIT_ENABLE=0 or GIT_MODE=off).")
        if app.running():
            app.proc.wait()
        else:
            while True:
                time.sleep(3600)

    elif GIT_MODE == "update":
        # Один проход обновления — и всё
        log("Git mode=update: single update cycle done.")
        if app.running():
            app.proc.wait()
        else:
            while True:
                time.sleep(3600)

    elif GIT_MODE == "watch":
        log(f"Git mode=watch: interval={GIT_INTERVAL}s, restart_on_update={int(GIT_RESTART)}")
        while True:
            time.sleep(max(3, GIT_INTERVAL))
            try:
                changed, req_hash = one_update_cycle(req_hash)
                if changed and GIT_RESTART:
                    log("Changes detected -> restarting app")
                    app.stop()
                    app.start()
            except Exception as e:
                log("Watch update error:", e)
                # продолжаем цикл
                continue
    else:
        log(f"Unknown GIT_MODE='{GIT_MODE}', doing nothing.")
        if app.running():
            app.proc.wait()
        else:
            while True:
                time.sleep(3600)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
