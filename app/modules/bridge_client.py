# modules/bridge_client.py
from __future__ import annotations

import os
import json
import asyncio
import contextlib
import threading
import logging
from logging import Logger
from typing import Any, Dict, Optional, Sequence, Iterable, Union
from typing import TYPE_CHECKING, Dict, Any, Tuple, Iterable, Union, List
import websockets

# -------- конфиг --------
BRIDGE_URL: str = os.getenv("SP_BRIDGE_URL", "ws://127.0.0.1:8765/ws")
BRIDGE_TOKEN: str = os.getenv("SP_TOKEN", "SUPER_SECRET")
BRIDGE_TIMEOUT: float = float(os.getenv("BRIDGE_TIMEOUT", "8.0"))   # сек
BRIDGE_MAX_SIZE: int = int(os.getenv("SP_MAX_SIZE", "131072"))      # 128 KiB (как у сервера по умолчанию)

# -------- логирование --------
def _setup_logger() -> Logger:
    level_name = (os.getenv("SP_LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    log = logging.getLogger("bridge_client")
    if log.handlers:
        # уже инициализирован (например, через gunicorn/flask)
        log.setLevel(level)
        return log

    # Формат: либо JSON, либо краткий текстовый
    json_mode = (os.getenv("SP_LOG_JSON") or "").lower() in ("1", "true", "yes", "y", "on")

    handler = logging.StreamHandler()
    if json_mode:
        # Простой JSON-формат без сторонних зависимостей
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                obj = {
                    "level": record.levelname,
                    "name": record.name,
                    "msg": record.getMessage(),
                    "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
                }
                if record.exc_info:
                    obj["exc_info"] = self.formatException(record.exc_info)
                return json.dumps(obj, ensure_ascii=False)
        handler.setFormatter(JsonFormatter())
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

    log.addHandler(handler)
    log.setLevel(level)
    return log

_log = _setup_logger()
_TRUNC = int(os.getenv("SP_LOG_MAX_PAYLOAD", "800"))  # обрезаем большие строки в логах


def _safe_trunc(s: Any) -> str:
    try:
        ss = str(s)
    except Exception:
        return "<unprintable>"
    if len(ss) > _TRUNC:
        return ss[:_TRUNC] + f"... (+{len(ss)-_TRUNC} chars)"
    return ss


# ====================== ВСПОМОГАТЕЛЬНОЕ ======================

def _run(coro) -> Any:
    """
    Безопасно выполнить async-корутину из синхронного кода.
    Если уже есть запущенный loop — исполним в отдельном треде со своим event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        _log.debug("run: using asyncio.run for coroutine=%s", getattr(coro, "__name__", "<coro>"))
        return asyncio.run(coro)

    box: Dict[str, Any] = {}

    def runner():
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            _log.debug("runner: new event loop started")
            box["result"] = loop.run_until_complete(coro)
        except Exception:
            _log.exception("runner: coroutine crashed")
            box["error"] = RuntimeError("bridge_client runner failed")
        finally:
            with contextlib.suppress(Exception):
                loop.run_until_complete(asyncio.sleep(0))
            loop.close()
            _log.debug("runner: event loop closed")

    t = threading.Thread(target=runner, name="bridge-client-runner", daemon=True)
    t.start()
    t.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _json_loads(s: str) -> Dict[str, Any]:
    try:
        return json.loads(s or "{}")
    except Exception:
        _log.warning("json_loads: bad json: %s", _safe_trunc(s))
        return {"type": "bridge.error", "error": "bad_json", "payload": {"raw": (s[:200] + "...") if s else ""}}


# ====================== НИЗКОУРОВНЕВЫЙ WS ======================

async def _connect():
    """
    Устанавливает одноразовое подключение к бриджу.
    По умолчанию совпадает с настройками сервера (ping, max_size).
    """
    headers = {"Authorization": "Bearer <hidden>"}
    _log.info("ws.connect: url=%s, timeout=%s, max_size=%s", BRIDGE_URL, BRIDGE_TIMEOUT, BRIDGE_MAX_SIZE)
    try:
        ws = await asyncio.wait_for(
            websockets.connect(
                BRIDGE_URL,
                extra_headers={"Authorization": f"Bearer {BRIDGE_TOKEN}"},
                ping_interval=20,
                ping_timeout=20,
                max_size=BRIDGE_MAX_SIZE,
            ),
            timeout=BRIDGE_TIMEOUT,
        )
        _log.info("ws.connect: connected")
        return ws
    except Exception:
        _log.exception("ws.connect: failed")
        raise


async def _graceful_close(ws, code: int = 1000, reason: str = "ok") -> None:
    try:
        await ws.close(code=code, reason=reason)
        if hasattr(ws, "wait_closed"):
            await ws.wait_closed()
        _log.debug("ws.close: code=%s reason=%s", code, reason)
    except Exception:
        _log.exception("ws.close: failed")


async def _recv_with_timeout(ws, timeout: float) -> Dict[str, Any]:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    except asyncio.TimeoutError:
        _log.warning("ws.recv: timeout after %.2fs", timeout)
        raise
    if isinstance(raw, (bytes, bytearray)):
        _log.debug("ws.recv: binary frame len=%d", len(raw))
        return {"type": "bridge.binary", "len": len(raw)}
    obj = _json_loads(raw)
    _log.debug("ws.recv: type=%s", obj.get("type"))
    return obj


async def _send_and_wait(
    message: Dict[str, Any],
    expect_types: Optional[Sequence[str]] = None,
    realm: Optional[str] = None,
    timeout: float = BRIDGE_TIMEOUT,
) -> Dict[str, Any]:
    """
    Отправить message и дождаться кадра одного из типов expect_types.
    Если expect_types=None — вернуть первый полученный кадр (обычно это bridge.ack).
    При указании realm — фильтруем ответы: obj.realm или payload.realm или data.realm должны совпадать.
    """
    ws = await _connect()
    try:
        payload_for_log = dict(message)
        # не логируем токены
        if "headers" in payload_for_log:
            payload_for_log["headers"] = "<hidden>"
        _log.info("ws.send: %s", _safe_trunc(payload_for_log))
        await ws.send(json.dumps(message, ensure_ascii=False))

        if not expect_types:
            obj = await _recv_with_timeout(ws, timeout)
            _log.info("ws.wait: first-frame type=%s", obj.get("type"))
            return obj

        # ждём пока не прилетит один из ожидаемых типов
        while True:
            obj = await _recv_with_timeout(ws, timeout)
            t = obj.get("type")
            if t not in expect_types:
                _log.debug("ws.wait: skip frame type=%s, expect=%s", t, expect_types)
                continue
            if realm:
                r = (
                    obj.get("realm")
                    or (obj.get("payload") or {}).get("realm")
                    or (obj.get("data") or {}).get("realm")
                )
                if r != realm:
                    _log.debug("ws.wait: realm mismatch got=%s want=%s", r, realm)
                    continue
            _log.info("ws.wait: got expected type=%s", t)
            return obj
    finally:
        await _graceful_close(ws)


async def _send_only(message: Dict[str, Any]) -> None:
    """Fire-and-forget отправка кадра (без ожидания ответа)."""
    ws = await _connect()
    try:
        _log.info("ws.send-only: %s", _safe_trunc(message))
        await ws.send(json.dumps(message, ensure_ascii=False))
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.sleep(0.02), timeout=0.05)
    finally:
        await _graceful_close(ws)


# ====================== УТИЛИТЫ ДЛЯ ФРОНТА ======================

def normalize_server_stats(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Принимает кадр server.stats или stats.report.
    Возвращает стабилизированный словарь для UI.
    """
    if obj.get("type") not in ("server.stats", "stats.report"):
        _log.warning("normalize_stats: unexpected frame type=%s", obj.get("type"))
        return {"type": "bridge.error", "error": "not_stats_frame", "payload": {}}

    # Одни плагины кладут данные в data, другие — в payload
    d = obj.get("data") or obj.get("payload") or {}
    realm = d.get("realm") or obj.get("realm")
    plugins = d.get("plugins") or {}

    return {
        "type": obj["type"],
        "realm": realm,
        "players": {
            "online": d.get("players_online"),
            "max": d.get("players_max"),
        },
        "motd": d.get("motd"),
        "tps": {
            "1m": d.get("tps_1m"),
            "5m": d.get("tps_5m"),
            "15m": d.get("tps_15m"),
            "mspt": d.get("mspt"),
        },
        "heap": {
            "used": d.get("heap_used"),
            "max": d.get("heap_max"),
        },
        "nonheap": {
            "used": d.get("nonheap_used"),
            "max": d.get("nonheap_max"),
        },
        "jvm": {
            "uptime_ms": d.get("jvm_uptime_ms"),
            "classes": {
                "loaded": d.get("classes_loaded"),
                "total_loaded": d.get("classes_total_loaded"),
                "unloaded": d.get("classes_unloaded"),
            },
            "threads": {
                "live": d.get("threads_live"),
                "peak": d.get("threads_peak"),
                "daemon": d.get("threads_daemon"),
            },
            "gc": d.get("gc") or {},
            "mem_pools": d.get("mem_pools") or {},
        },
        "os": {
            "name": d.get("os_name"),
            "arch": d.get("os_arch"),
            "cores": d.get("os_cores"),
            "cpu_load": {
                "system": d.get("cpu_system_load"),
                "process": d.get("cpu_process_load"),
            },
        },
        "plugins": plugins,  # dict: {name: version}
    }


# ====================== ПУБЛИЧНЫЙ API ======================

# ---- Health / meta ----

def bridge_ping() -> Dict[str, Any]:
    """
    Пинг через доступный метод сервера: bridge.list -> bridge.list.result.
    (В bridge.py нет отдельной обработки 'bridge.ping'.)
    """
    try:
        return _run(_send_and_wait({"type": "bridge.list"}, expect_types=("bridge.list.result",)))
    except Exception as e:
        _log.exception("bridge_ping failed")
        return {"type": "bridge.error", "error": str(e), "payload": {}}


def bridge_info() -> Dict[str, Any]:
    """Краткая сводка по бриджу (используем bridge.list)."""
    return bridge_ping()


def bridge_list() -> Dict[str, Any]:
    """Список онлайновых плагинов по realm’ам (type=bridge.list.result)."""
    msg = {"type": "bridge.list"}
    try:
        return _run(_send_and_wait(msg, expect_types=("bridge.list.result",)))
    except Exception as e:
        _log.exception("bridge_list failed")
        return {"type": "bridge.error", "error": str(e), "payload": {}}


# ---- Stats ----

def stats_query(realm: str) -> Dict[str, Any]:
    """
    Запрос статуса сервера.
    Бридж/плагин может прислать как {"type":"server.stats", ...}, так и {"type":"stats.report", ...}.
    """
    msg = {"type": "stats.query", "realm": realm, "payload": {"realm": realm}}
    try:
        return _run(_send_and_wait(msg, expect_types=("server.stats", "stats.report"), realm=realm))
    except Exception as e:
        _log.exception("stats_query failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {}}


# ---- Console ----

def console_exec(realm: str, cmd: str) -> Dict[str, Any]:
    """
    Выполнить одну консольную команду.
    Возвращаем первый ответ (обычно это bridge.ack), чтобы фронт не висел.
    На стороне бриджа запрос нормализуется в {"type":"console.exec","command": "..."}
    """
    msg = {"type": "console.exec", "realm": realm, "payload": {"realm": realm, "cmd": cmd}}
    try:
        return _run(_send_and_wait(msg, expect_types=None))
    except Exception as e:
        _log.exception("console_exec failed: realm=%s", realm)
        return {
            "type": "bridge.ack",
            "payload": {"sent": False, "realm": realm, "cmd": cmd, "error": str(e)},
        }


def console_exec_lines(realm: str, lines: Sequence[str]) -> Dict[str, Any]:
    """
    Отправить несколько строк разом (аналог console.execLines).
    Ждём первый ответ (ACK).
    """
    msg = {"type": "console.execLines", "realm": realm, "payload": {"realm": realm}, "lines": list(lines)}
    try:
        return _run(_send_and_wait(msg, expect_types=None))
    except Exception as e:
        _log.exception("console_exec_lines failed: realm=%s", realm)
        return {
            "type": "bridge.ack",
            "payload": {"sent": False, "realm": realm, "lines": list(lines), "error": str(e)},
        }


# ---- Broadcast / Online ----

def broadcast(realm: str, message: str) -> Dict[str, Any]:
    msg = {"type": "broadcast", "realm": realm, "message": message}
    try:
        return _run(_send_and_wait(msg, expect_types=None))
    except Exception as e:
        _log.exception("broadcast failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def player_is_online(realm: str, name_or_uuid: str) -> Dict[str, Any]:
    msg = {"type": "player.is_online", "realm": realm, "name": name_or_uuid}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("player_is_online failed: realm=%s name=%s", realm, name_or_uuid)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "name": name_or_uuid}}


# ---- Maintenance ----

def maintenance_set(realm: str, enabled: bool, kick_message: str = "") -> bool:
    """
    Включить/выключить техработы. Текст дублируем в разных ключах (совместимость).
    """
    msg = (kick_message or "").strip()
    payload = {
        "realm": realm,
        "enabled": bool(enabled),
        "message": msg,
        "kickMessage": msg,
        "reason": msg,
        "text": msg,
    }
    obj = {"type": "maintenance.set", "realm": realm, "payload": payload}
    return bridge_send(obj)


def _normalize_op(op: str) -> str:
    o = (op or "").strip().lower()
    if o in ("add", "+"): return "add"
    if o in ("remove", "del", "rm", "-"): return "remove"
    if o in ("list", "show"): return "list"
    # дефолт: list — безопасней всего
    return "list"


def _is_seq_of_users(x: Union[str, Iterable[str]]) -> bool:
    if isinstance(x, str): return False
    try:
        iter(x)  # type: ignore[arg-type]
        return True
    except Exception:
        return False


if TYPE_CHECKING:
    # только для линтера/IDE; во время исполнения не импортируется
    from .bridge_client import ws_send_and_wait as _ws_send_and_wait_typed  # noqa: F401


def _ws_call(payload: Dict[str, Any], expect: Tuple[str, ...]) -> Dict[str, Any]:
    """
    Унифицированный вызов «отправить и подождать ответа».
    Пытается найти любой доступный хелпер ожидания в текущем модуле, иначе шлёт fire-and-forget.
    """
    # 1) Попробуем найти подходящую функцию прямо в globals текущего модуля:
    for name in ("ws_send_and_wait", "send_and_wait", "bridge_send_and_wait",
                 "_send_and_wait_sync", "_send_and_wait"):
        fn = globals().get(name)
        if callable(fn):
            try:
                return fn(payload, expect=expect)  # type: ignore[misc]
            except TypeError:
                # запасной вызов (вдруг другая сигнатура)
                try:
                    return fn(payload)  # type: ignore[misc]
                except Exception:
                    pass
            except Exception:
                # если конкретная реализация упала — попробуем следующую
                pass

    # 2) Нет «ждущих» — шлём синхронно без ожидания через bridge_send и возвращаем синтетический ACK
    try:
        # bridge_send определён в этом же модуле
        bs = globals().get("bridge_send")
        if callable(bs):
            bs(payload)  # type: ignore[misc]
            return {"type": "bridge.ack", "synthetic": True}
        # на всякий случай попробуем мягко импортнуть
        from .bridge_client import bridge_send as _send_only  # type: ignore[no-redef]
        _send_only(payload)
        return {"type": "bridge.ack", "synthetic": True}
    except Exception as e:
        return {"type": "bridge.error", "error": str(e)}


def maintenance_whitelist(realm: str, op: str, players: Union[str, Iterable[str], None]) -> Dict[str, Any]:
    """
    Отправить изменения whitelist в плагин Maintenance.

    op: "add" | "remove" | "list"
    players: str (ник/uuid) или Iterable[str] или None (для "list")

    Возвращает агрегированный словарь:
      {
        "type": "maintenance.whitelist",
        "ok": True/False,
        "data": {
          "realm": <realm>,
          "action": <add/remove/list>,
          "users": [...],         # для add/remove
          "applied": <int>,
          "size": <int>
        },
        "errors": [ {"user": "...", "error": "..."} ]  # если были ошибки/bridge.error
      }
    """
    action = (op or "").strip().lower()
    if action not in ("add", "remove", "list"):
        raise ValueError("op must be add|remove|list")

    # нормализация входа
    if isinstance(players, (list, tuple, set)):
        users: List[str] = [str(x).strip() for x in players if str(x or "").strip()]
    elif isinstance(players, str):
        users = [players.strip()] if players.strip() else []
    else:
        users = []

    # list-запрос (без user)
    if action == "list":
        payload = {
            "type": "maintenance.whitelist",
            "realm": realm,
            "payload": {"realm": realm, "action": "list"}
        }
        resp = _ws_call(payload, expect=("maintenance.whitelist", "bridge.ack", "bridge.error"))
        ok = isinstance(resp, dict) and resp.get("type") in ("maintenance.whitelist", "bridge.ack")
        return {
            "type": "maintenance.whitelist",
            "ok": bool(ok),
            "data": {"realm": realm, "action": "list", "users": [], "applied": 0, "size": 0},
            "raw": resp if isinstance(resp, dict) else {"type": "unknown"}
        }

    # add/remove — по одному кадру на пользователя
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    applied = 0

    for u in users:
        payload = {
            "type": "maintenance.whitelist",
            "realm": realm,
            "payload": {
                "realm": realm,
                "action": action,   # плагин читает именно "action" и "user"
                "user": u
            }
        }
        resp = _ws_call(payload, expect=("maintenance.whitelist", "bridge.ack", "bridge.error"))

        if isinstance(resp, dict) and resp.get("type") == "bridge.error":
            errors.append({"user": u, "error": resp.get("error") or "bridge error"})
            results.append({"user": u, "ok": False, "source": "error", "raw": resp})
            continue

        if isinstance(resp, dict) and resp.get("type") in ("maintenance.whitelist", "bridge.ack"):
            applied += 1
            results.append({
                "user": u,
                "ok": True,
                "source": resp.get("type"),
                "data": resp.get("data") or resp.get("payload") or {}
            })
        else:
            # fire-and-forget/неизвестный формат — считаем успешным применением
            applied += 1
            results.append({"user": u, "ok": True, "source": "raw", "raw": resp})

    agg: Dict[str, Any] = {
        "type": "maintenance.whitelist",
        "ok": applied == len(users) and not errors,
        "data": {
            "realm": realm,
            "action": action,
            "users": users,
            "applied": applied,
            "size": len(users),
        }
    }
    if errors:
        agg["errors"] = errors
    return agg


def maintenance_whitelist_add(realm: str, user: str) -> Dict[str, Any]:
    return maintenance_whitelist(realm, "add", user)

def maintenance_whitelist_remove(realm: str, user: str) -> Dict[str, Any]:
    return maintenance_whitelist(realm, "remove", user)

def maintenance_whitelist_add_many(realm: str, users: Iterable[str]) -> Dict[str, Any]:
    return maintenance_whitelist(realm, "add", users)

def maintenance_whitelist_remove_many(realm: str, users: Iterable[str]) -> Dict[str, Any]:
    return maintenance_whitelist(realm, "remove", users)


# ---- LuckPerms wrappers ----

def lp_web_open(realm: str) -> Dict[str, Any]:
    msg = {"type": "lp.web.open", "realm": realm, "payload": {"realm": realm}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_web_open failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_web_apply(realm: str, code: str) -> Dict[str, Any]:
    msg = {"type": "lp.web.apply", "realm": realm, "payload": {"realm": realm, "code": code}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_web_apply failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_perm_add(realm: str, user: str, permission: str, value: bool = True) -> Dict[str, Any]:
    msg = {"type": "lp.user.perm.add", "realm": realm,
           "payload": {"realm": realm, "user": user, "permission": permission, "value": bool(value)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_user_perm_add failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_perm_remove(realm: str, user: str, permission: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.perm.remove", "realm": realm,
           "payload": {"realm": realm, "user": user, "permission": permission}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_user_perm_remove failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_group_add(realm: str, user: str, group: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.group.add", "realm": realm,
           "payload": {"realm": realm, "user": user, "group": group}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_user_group_add failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_group_remove(realm: str, user: str, group: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.group.remove", "realm": realm,
           "payload": {"realm": realm, "user": user, "group": group}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_user_group_remove failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_group_perm_add(realm: str, group: str, permission: str, value: bool = True) -> Dict[str, Any]:
    msg = {"type": "lp.group.perm.add", "realm": realm,
           "payload": {"realm": realm, "group": group, "permission": permission, "value": bool(value)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_group_perm_add failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_group_perm_remove(realm: str, group: str, permission: str) -> Dict[str, Any]:
    msg = {"type": "lp.group.perm.remove", "realm": realm,
           "payload": {"realm": realm, "group": group, "permission": permission}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_group_perm_remove failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_info(realm: str, user: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.info", "realm": realm, "payload": {"realm": realm, "user": user}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_user_info failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_group_info(realm: str, group: str) -> Dict[str, Any]:
    msg = {"type": "lp.group.info", "realm": realm, "payload": {"realm": realm, "group": group}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("lp_group_info failed: realm=%s", realm)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


# ---- JetPay / Coins (JP) wrappers ----

def jp_balance_get(realm: str, user: str) -> Dict[str, Any]:
    """
    Получить баланс пользователя.
    Тип ответа не фиксируем — возвращаем первый кадр (ACK/результат).
    """
    msg = {"type": "jp.balance.get", "realm": realm, "payload": {"realm": realm, "user": user}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("jp_balance_get failed: realm=%s user=%s", realm, user)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


def jp_balance_set(realm: str, user: str, amount: int) -> Dict[str, Any]:
    msg = {"type": "jp.balance.set", "realm": realm,
           "payload": {"realm": realm, "user": user, "amount": int(amount)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("jp_balance_set failed: realm=%s user=%s", realm, user)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


def jp_balance_add(realm: str, user: str, delta: int) -> Dict[str, Any]:
    msg = {"type": "jp.balance.add", "realm": realm,
           "payload": {"realm": realm, "user": user, "delta": int(delta)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("jp_balance_add failed: realm=%s user=%s", realm, user)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


def jp_balance_take(realm: str, user: str, amount: int) -> Dict[str, Any]:
    """
    Списать фиксированную сумму с баланса пользователя.
    Если на бридже нет отдельного метода — он может быть алиасом к "add" с отрицательным значением.
    """
    msg = {"type": "jp.balance.take", "realm": realm,
           "payload": {"realm": realm, "user": user, "amount": int(amount)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("jp_balance_take failed: realm=%s user=%s", realm, user)
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user, "amount": int(amount)}}


def jp_transfer(realm: str, src_user: str, dst_user: str, amount: int, *, reason: str = "") -> Dict[str, Any]:
    msg = {"type": "jp.transfer", "realm": realm,
           "payload": {"realm": realm, "from": src_user, "to": dst_user, "amount": int(amount), "reason": reason}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        _log.exception("jp_transfer failed: realm=%s from=%s to=%s", realm, src_user, dst_user)
        return {"type": "bridge.error", "error": str(e),
                "payload": {"realm": realm, "from": src_user, "to": dst_user, "amount": int(amount)}}


# ---- Универсальная отправка ----

def bridge_send(obj: Dict[str, Any]) -> bool:
    """
    Fire-and-forget отправка любого кадра.
    Возвращает True, если отправка прошла без исключений.
    """
    try:
        _run(_send_only(obj))
        return True
    except Exception:
        _log.exception("bridge_send failed")
        return False
