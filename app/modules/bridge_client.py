# modules/bridge_client.py
from __future__ import annotations

import os
import json
import asyncio
import contextlib
import threading
from typing import Any, Dict, Optional, Sequence

import websockets

# -------- конфиг --------
BRIDGE_URL: str = os.getenv("SP_BRIDGE_URL", "wss://websocket.teighto.net/ws")
BRIDGE_TOKEN: str = os.getenv("SP_TOKEN", "SUPER_SECRET")
BRIDGE_TIMEOUT: float = float(os.getenv("BRIDGE_TIMEOUT", "8.0"))   # сек
BRIDGE_MAX_SIZE: int = int(os.getenv("SP_MAX_SIZE", "131072"))      # 128 KiB (как у сервера по умолчанию)


# ====================== ВСПОМОГАТЕЛЬНОЕ ======================

def _run(coro) -> Any:
    """
    Безопасно выполнить async-корутину из синхронного кода.
    Если уже есть запущенный loop — исполним в отдельном треде со своим event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    box: Dict[str, Any] = {}

    def runner():
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            box["result"] = loop.run_until_complete(coro)
        except Exception as e:
            box["error"] = e
        finally:
            with contextlib.suppress(Exception):
                loop.run_until_complete(asyncio.sleep(0))
            loop.close()

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
        return {"type": "bridge.error", "error": "bad_json", "payload": {"raw": (s[:200] + "...") if s else ""}}


# ====================== НИЗКОУРОВНЕВЫЙ WS ======================

async def _connect():
    """
    Устанавливает одноразовое подключение к бриджу.
    По умолчанию совпадает с настройками сервера (ping, max_size).
    """
    headers = {"Authorization": f"Bearer {BRIDGE_TOKEN}"}
    return await asyncio.wait_for(
        websockets.connect(
            BRIDGE_URL,
            extra_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            max_size=BRIDGE_MAX_SIZE,
        ),
        timeout=BRIDGE_TIMEOUT,
    )


async def _graceful_close(ws, code: int = 1000, reason: str = "ok") -> None:
    with contextlib.suppress(Exception):
        await ws.close(code=code, reason=reason)
        if hasattr(ws, "wait_closed"):
            await ws.wait_closed()


async def _recv_with_timeout(ws, timeout: float) -> Dict[str, Any]:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    if isinstance(raw, (bytes, bytearray)):
        # бинарь не разбираем
        return {"type": "bridge.binary", "len": len(raw)}
    return _json_loads(raw)


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
        await ws.send(json.dumps(message, ensure_ascii=False))

        if not expect_types:
            return await _recv_with_timeout(ws, timeout)

        # ждём пока не прилетит один из ожидаемых типов
        while True:
            obj = await _recv_with_timeout(ws, timeout)
            t = obj.get("type")
            if t not in expect_types:
                # игнорируем промежуточные ACK/echo и т.п.
                continue
            if realm:
                r = (
                    obj.get("realm")
                    or (obj.get("payload") or {}).get("realm")
                    or (obj.get("data") or {}).get("realm")
                )
                if r != realm:
                    continue
            return obj
    finally:
        await _graceful_close(ws)


async def _send_only(message: Dict[str, Any]) -> None:
    """Fire-and-forget отправка кадра (без ожидания ответа)."""
    ws = await _connect()
    try:
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
        return {"type": "bridge.error", "error": str(e), "payload": {}}


def bridge_info() -> Dict[str, Any]:
    """
    Краткая сводка: используем тот же 'bridge.list'.
    """
    return bridge_ping()


def bridge_list() -> Dict[str, Any]:
    """Список онлайновых плагинов по realm’ам (type=bridge.list.result)."""
    msg = {"type": "bridge.list"}
    try:
        return _run(_send_and_wait(msg, expect_types=("bridge.list.result",)))
    except Exception as e:
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
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def player_is_online(realm: str, name_or_uuid: str) -> Dict[str, Any]:
    msg = {"type": "player.is_online", "realm": realm, "name": name_or_uuid}
    try:
        # Ждём целевой ответ от плагина; если плагин так не отвечает — вернётся первый кадр (ACK) через expect=None.
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
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


def maintenance_whitelist(realm: str, op: str, player: str) -> bool:
    """
    Управление локальным whitelist в режиме техработ.
    Поддержаны оба формата ключей: (action,user) и (op,player).
    """
    op_l = (op or "").lower()
    if op_l not in ("add", "remove"):
        raise ValueError("op must be 'add' or 'remove'")
    obj = {
        "type": "maintenance.whitelist",
        "realm": realm,
        "payload": {"realm": realm, "op": op_l, "player": player, "action": op_l, "user": player},
    }
    return bridge_send(obj)


# ---- LuckPerms wrappers ----
# Типы согласованы с серверной частью: 'lp.web.open', 'lp.web.apply',
# 'lp.user.perm.add/remove', 'lp.user.group.add/remove',
# 'lp.group.perm.add/remove', 'lp.user.info', 'lp.group.info'.

def lp_web_open(realm: str) -> Dict[str, Any]:
    msg = {"type": "lp.web.open", "realm": realm, "payload": {"realm": realm}}
    try:
        # Ожидаем любой первый ответ (ACK/redirect/url)
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_web_apply(realm: str, code: str) -> Dict[str, Any]:
    msg = {"type": "lp.web.apply", "realm": realm, "payload": {"realm": realm, "code": code}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_perm_add(realm: str, user: str, permission: str, value: bool = True) -> Dict[str, Any]:
    msg = {"type": "lp.user.perm.add", "realm": realm,
           "payload": {"realm": realm, "user": user, "permission": permission, "value": bool(value)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_perm_remove(realm: str, user: str, permission: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.perm.remove", "realm": realm,
           "payload": {"realm": realm, "user": user, "permission": permission}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_group_add(realm: str, user: str, group: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.group.add", "realm": realm,
           "payload": {"realm": realm, "user": user, "group": group}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_group_remove(realm: str, user: str, group: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.group.remove", "realm": realm,
           "payload": {"realm": realm, "user": user, "group": group}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_group_perm_add(realm: str, group: str, permission: str, value: bool = True) -> Dict[str, Any]:
    msg = {"type": "lp.group.perm.add", "realm": realm,
           "payload": {"realm": realm, "group": group, "permission": permission, "value": bool(value)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_group_perm_remove(realm: str, group: str, permission: str) -> Dict[str, Any]:
    msg = {"type": "lp.group.perm.remove", "realm": realm,
           "payload": {"realm": realm, "group": group, "permission": permission}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_user_info(realm: str, user: str) -> Dict[str, Any]:
    msg = {"type": "lp.user.info", "realm": realm, "payload": {"realm": realm, "user": user}}
    try:
        # Если плагин шлёт lp.user.info.result — можно указать expect_types соответствующим образом.
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


def lp_group_info(realm: str, group: str) -> Dict[str, Any]:
    msg = {"type": "lp.group.info", "realm": realm, "payload": {"realm": realm, "group": group}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm}}


# ---- JustPoints wrappers ----
# Типы согласованы с серверной частью: 'jp.balance.get/set/add/take'.

def jp_balance_get(realm: str, user: str) -> Dict[str, Any]:
    msg = {"type": "jp.balance.get", "realm": realm, "payload": {"realm": realm, "user": user}}
    try:
        # можно ждать конкретный тип 'jp.balance', но оставим гибко (ACK/результат)
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


def jp_balance_set(realm: str, user: str, amount: float) -> Dict[str, Any]:
    msg = {"type": "jp.balance.set", "realm": realm, "payload": {"realm": realm, "user": user, "amount": float(amount)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


def jp_balance_add(realm: str, user: str, amount: float) -> Dict[str, Any]:
    msg = {"type": "jp.balance.add", "realm": realm, "payload": {"realm": realm, "user": user, "amount": float(amount)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


def jp_balance_take(realm: str, user: str, amount: float) -> Dict[str, Any]:
    msg = {"type": "jp.balance.take", "realm": realm, "payload": {"realm": realm, "user": user, "amount": float(amount)}}
    try:
        return _run(_send_and_wait(msg, expect_types=None, realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {"realm": realm, "user": user}}


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
        return False
