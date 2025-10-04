from __future__ import annotations

import os
import json
import asyncio
import contextlib
import threading
from typing import Any, Dict, Optional, Sequence

import websockets

# -------- конфиг --------
BRIDGE_URL: str = os.getenv("SP_BRIDGE_URL", "ws://127.0.0.1:8765/ws")
BRIDGE_TOKEN: str = os.getenv("SP_TOKEN", "SUPER_SECRET")
BRIDGE_TIMEOUT: float = float(os.getenv("BRIDGE_TIMEOUT", "6"))  # sec


def _run(coro) -> Any:
    """
    Безопасно выполнить async-корутину из синхронного кода.
    Если уже есть запущенный loop (например, внутри другого async контекста) —
    исполним в отдельном треде со своим event loop.
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
            try:
                loop.run_until_complete(asyncio.sleep(0))
            except Exception:
                pass
            loop.close()

    t = threading.Thread(target=runner, name="bridge-client-runner", daemon=True)
    t.start()
    t.join()
    if "error" in box:
        raise box["error"]
    return box.get("result")


# -------- низкоуровневый WS --------
async def _connect():
    headers = {"Authorization": f"Bearer {BRIDGE_TOKEN}"}
    return await asyncio.wait_for(
        websockets.connect(
            BRIDGE_URL,
            extra_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            max_size=4 * 1024 * 1024,
        ),
        timeout=BRIDGE_TIMEOUT,
    )


async def _graceful_close(ws, code: int = 1000, reason: str = "ok") -> None:
    with contextlib.suppress(Exception):
        await ws.close(code=code, reason=reason)
        if hasattr(ws, "wait_closed"):
            await ws.wait_closed()


async def _send_and_wait(
    message: Dict[str, Any],
    expect_types: Optional[Sequence[str]] = None,
    realm: Optional[str] = None,
    timeout: float = BRIDGE_TIMEOUT,
) -> Dict[str, Any]:
    """
    Отправить message и дождаться кадра одного из типов expect_types.
    Если expect_types=None — вернём самый первый принятый кадр как есть.
    При указании realm дополнительно фильтруем ответы по полю realm (в корне/в payload).
    """
    ws = await _connect()
    try:
        await ws.send(json.dumps(message, ensure_ascii=False))
        if not expect_types:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            return json.loads(raw or "{}")

        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            obj = json.loads(raw or "{}")
            t = obj.get("type")
            if t in expect_types:
                if realm:
                    r = obj.get("realm") or (obj.get("payload") or {}).get("realm")
                    if r != realm:
                        continue
                return obj
    finally:
        await _graceful_close(ws)


async def _send_only(message: Dict[str, Any]) -> None:
    """
    Fire-and-forget отправка кадра (без ожидания ответа).
    """
    ws = await _connect()
    try:
        await ws.send(json.dumps(message, ensure_ascii=False))
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.sleep(0.02), timeout=0.05)
    finally:
        await _graceful_close(ws)


# -------- публичный API --------
def bridge_list() -> Dict[str, Any]:
    msg = {"type": "bridge.list"}
    try:
        return _run(_send_and_wait(msg, expect_types=("bridge.list.result",)))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {}}


def stats_query(realm: str) -> Dict[str, Any]:
    msg = {"type": "stats.query", "realm": realm, "payload": {"realm": realm}}
    try:
        return _run(_send_and_wait(msg, expect_types=("stats.report",), realm=realm))
    except Exception as e:
        return {"type": "bridge.error", "error": str(e), "payload": {}}


def console_exec(realm: str, cmd: str) -> Dict[str, Any]:
    msg = {"type": "console.exec", "realm": realm, "payload": {"realm": realm, "cmd": cmd}}
    try:
        # ждём первый фрейм (echo/ack/result) — фронту достаточно, чтобы не висеть
        return _run(_send_and_wait(msg, expect_types=None))
    except Exception as e:
        return {
            "type": "bridge.ack",
            "payload": {"sent": False, "realm": realm, "cmd": cmd, "error": str(e)},
        }


def bridge_send(obj: Dict[str, Any]) -> bool:
    try:
        _run(_send_only(obj))
        return True
    except Exception:
        return False


# -------- maintenance helpers --------
def maintenance_set(realm: str, enabled: bool, kick_message: str = "") -> bool:
    """
    Включить/выключить техработы. Текст дублим в разных ключах (совместимость).
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
    op_l = (op or "").lower()
    if op_l not in ("add", "remove"):
        raise ValueError("op must be 'add' or 'remove'")
    obj = {
        "type": "maintenance.whitelist",
        "realm": realm,
        "payload": {"realm": realm, "op": op_l, "player": player},
    }
    return bridge_send(obj)
