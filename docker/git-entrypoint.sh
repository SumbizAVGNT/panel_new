#!/usr/bin/env bash
set -euo pipefail

APP_CMD=("$@")
BRANCH="${AUTOGIT_BRANCH:-main}"
REMOTE="${AUTOGIT_REMOTE:-origin}"
POLL="${AUTOGIT_POLL_SEC:-15}"
MODE="${AUTOGIT_MODE:-update}"     # update | watch | off

log(){ echo "[$(date +%H:%M:%S)] [git] $*"; }

run_app() {
  log "start app: ${APP_CMD[*]}"
  exec "${APP_CMD[@]}"
}

# когда автогит выключен — просто запускаем приложение
if [[ "$MODE" == "off" ]]; then
  run_app
fi

# убеждаемся что это git-репо
if [[ ! -d .git ]]; then
  log "no .git here -> running app without watcher"
  run_app
fi

# текущий коммит
current_head() { git rev-parse HEAD 2>/dev/null || echo "none"; }
origin_head()  { git rev-parse "${REMOTE}/${BRANCH}" 2>/dev/null || echo "none"; }

# одноразовый депс-инсталл
install_deps(){
  if [[ -f requirements.txt ]]; then
    log "installing requirements..."
    pip install --no-cache-dir -r requirements.txt >/dev/null 2>&1 || true
  fi
}

# запустим приложение в дочернем процессе и будем его перезапускать
set +e
"${APP_CMD[@]}" &
APP_PID=$!
set -e

PREV="$(current_head)"
log "mode=$MODE prev=$PREV branch=${BRANCH}"

while sleep "$POLL"; do
  if [[ "$MODE" == "update" ]]; then
    # primary: тянем репозиторий и делаем reset
    git fetch --all -q || true
    NEW="$(origin_head)"

    if [[ "$NEW" != "$PREV" && "$NEW" != "none" ]]; then
      log "update: $PREV -> $NEW"
      git reset --hard "$NEW" -q || true
      install_deps
      kill -TERM "$APP_PID" 2>/dev/null || true
      wait "$APP_PID" 2>/dev/null || true
      set +e; "${APP_CMD[@]}" & APP_PID=$!; set -e
      PREV="$NEW"
    fi
  else
    # watch-only: только следим за HEAD (без fetch/reset),
    # изменения подтянет другой контейнер через bind-mount
    NEW="$(current_head)"
    if [[ "$NEW" != "$PREV" ]]; then
      log "detected change: $PREV -> $NEW (watch-only)"
      install_deps
      kill -TERM "$APP_PID" 2>/dev/null || true
      wait "$APP_PID" 2>/dev/null || true
      set +e; "${APP_CMD[@]}" & APP_PID=$!; set -e
      PREV="$NEW"
    fi
  fi
done
