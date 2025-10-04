#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/app}"
REPO_URL="${REPO_URL:-}"
REPO_BRANCH="${REPO_BRANCH:-main}"
GIT_INTERVAL="${GIT_INTERVAL:-60}"

mkdir -p "$APP_DIR"
cd "$APP_DIR"

install_reqs() {
  if [[ -f requirements.txt ]]; then
    local cur
    cur="$(sha256sum requirements.txt | awk '{print $1}')"
    if [[ ! -f .req.hash ]] || [[ "$cur" != "$(cat .req.hash)" ]]; then
      echo "[deps] installing requirements..."
      pip install --no-cache-dir -r requirements.txt
      echo -n "$cur" > .req.hash
    fi
  fi
}

update_repo_once() {
  [[ -z "$REPO_URL" ]] && return 0
  if [[ ! -d .git ]]; then
    echo "[git] clone $REPO_URL (branch $REPO_BRANCH) into $APP_DIR"
    git clone --depth 1 --branch "$REPO_BRANCH" "$REPO_URL" "$APP_DIR"
    install_reqs
    return 1
  else
    {
      flock 9
      git remote set-url origin "$REPO_URL" || true
      git fetch origin "$REPO_BRANCH" || true
      local LOCAL REMOTE
      LOCAL="$(git rev-parse HEAD || echo 0)"
      REMOTE="$(git rev-parse "origin/$REPO_BRANCH" || echo 1)"
      if [[ "$LOCAL" != "$REMOTE" ]]; then
        echo "[git] update: $LOCAL -> $REMOTE"
        git reset --hard "origin/$REPO_BRANCH" || true
        install_reqs
        return 1
      fi
    } 9>/.gitpull.lock
  fi
  return 0
}

update_repo_once || true

echo "[app] starting: $*"
"$@" &
APP_PID=$!
echo $APP_PID > /tmp/app.pid

(
  while true; do
    sleep "$GIT_INTERVAL"
    if update_repo_once; then
      :
    else
      echo "[git] changes detected -> terminating app for restart"
      kill -TERM "$(cat /tmp/app.pid 2>/dev/null || echo 0)" 2>/dev/null || true
    fi
  done
) &

wait "$APP_PID"
