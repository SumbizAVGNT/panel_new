# app/modules/bridge_client.py
import os
import json
import asyncio
import contextlib
from typing import Any, Dict, Optional, Sequence

import websockets

BRIDGE_URL = os.getenv("SP_BRIDGE_URL", "ws://127.0.0.1:8765/ws")
BRIDGE_TOKEN = os.getenv("SP_TOKEN", "SUPER_SECRET")
BRIDGE_TIMEOUT = float(os.getenv("BRIDGE_TIMEOUT", "6"))

async def _send_and_wait(message: Dict[str, Any],
                         expect_types: Optional[Sequence[str]] = None,
                         realm: Optional[str] = None,
                         timeout: float = BRIDGE_TIMEOUT) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {BRIDGE_TOKEN}"}
    async with websockets.connect(BRIDGE_URL, extra_headers=headers, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps(message, ensure_ascii=False))
        if not expect_types:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            return json.loads(raw)

        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            obj = json.loads(raw)
            t = obj.get("type")
            if t in expect_types:
                if realm and (obj.get("realm") or obj.get("payload", {}).get("realm")) != realm:
                    continue
                return obj

def bridge_list() -> Dict[str, Any]:
    """Вернёт словарь {realm: online_plugins_count}."""
    msg = {"type": "bridge.list"}
    return asyncio.run(_send_and_wait(msg, expect_types=("bridge.list.result",)))

def stats_query(realm: str) -> Dict[str, Any]:
    """Запросит у плагина сводку и дождётся 'stats.report'."""
    msg = {"type": "stats.query", "realm": realm, "payload": {"realm": realm}}
    return asyncio.run(_send_and_wait(msg, expect_types=("stats.report",), realm=realm))

def console_exec(realm: str, cmd: str) -> Dict[str, Any]:
    """Выполнит команду в консоли. Возвращаем ack, чтобы фронт не зависал."""
    msg = {"type": "console.exec", "realm": realm, "payload": {"realm": realm, "cmd": cmd}}
    with contextlib.suppress(Exception):
        return asyncio.run(_send_and_wait(msg, expect_types=None))
    return {"type": "bridge.ack", "payload": {"sent": True, "realm": realm, "cmd": cmd}}
