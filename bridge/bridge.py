# bridge/bridge.py
# pip install websockets
from __future__ import annotations

import asyncio
import json
import argparse
import os
import sys
import contextlib
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from collections import deque
from typing import Dict, Set, Optional

import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# ------------------ state ------------------

# realm -> set(ws)
PLUGINS: Dict[str, Set] = {}
# admin connections
ADMINS: Set = set()

# типы, которые чаще всего шлёт плагин — логируем их заметнее (единый стиль: dot.case)
PLUGIN_TYPICAL_TYPES = {
    # консоль
    "console.out", "console.done",
    # телеметрия
    "server.stats", "stats.report",
    # базовые
    "player.online", "broadcast.ok",
    "error", "pong", "bridge.log",
    # ops/cmdwl
    "ops.stats", "ops.list", "cmdwl.stats", "cmdwl.list", "cmdwl.commands",
    # luckperms
    "lp.user.changed", "lp.group.changed", "lp.web.url", "lp.web.apply.ok",
    # justpoints
    "jp.balance", "jp.ok",
}

# простая защита от флуда (token bucket per-connection)
RATE_LIMIT = int(os.getenv("SP_RATE_LIMIT", "30"))   # сообщений
RATE_WINDOW = float(os.getenv("SP_RATE_WINDOW", "5"))  # секунд
MAX_TEXT_LEN = int(os.getenv("SP_MAX_TEXT_LEN", "4096"))

class RateLimiter:
    def __init__(self, limit: int = RATE_LIMIT, window: float = RATE_WINDOW):
        self.limit, self.window = limit, window
        self.ts = deque()
    def hit(self) -> bool:
        now = asyncio.get_event_loop().time()
        self.ts.append(now)
        while self.ts and (now - self.ts[0]) > self.window:
            self.ts.popleft()
        return len(self.ts) <= self.limit

# === LOG HELPERS ===
def _short_json(obj, limit=2000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        s = str(obj)
    if len(s) <= limit:
        return s
    return s[:limit] + f"...(+{len(s)-limit}b)"

def _pretty_tag(obj) -> str:
    t = obj.get("type") if isinstance(obj, dict) else None
    return t or "?"

def _log_recv(tag: str, realm: str | None, obj: dict, verbose: bool) -> None:
    if tag in ("server.stats", "stats.report"):
        print(f"[bridge] <- {tag} from realm='{realm}' payload={_short_json(obj)}")
        return
    if tag in PLUGIN_TYPICAL_TYPES:
        if verbose:
            print(f"[bridge] <- {tag} from realm='{realm}' payload={_short_json(obj)}")
        else:
            print(f"[bridge] <- {tag} from realm='{realm}'")
        return
    if verbose:
        print(f"[bridge] <- other:{tag} from realm='{realm}' payload={_short_json(obj)}")
    else:
        print(f"[bridge] <- other:{tag} from realm='{realm}'")

# ------------------ helpers ------------------

def _extract_qs(path: str) -> dict[str, list[str]]:
    try:
        return parse_qs(urlparse(path).query)
    except Exception:
        return {}

def _extract_token(ws, path: str) -> str:
    h = ws.request_headers  # case-insensitive
    auth = h.get("Authorization") or h.get("authorization") or ""
    token = ""
    if auth:
        low = auth.lower()
        if low.startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
        else:
            token = auth.strip()
    if not token:
        token = h.get("X-Auth-Token") or h.get("x-auth-token") or ""
    if not token:
        q = _extract_qs(path)
        token = (q.get("token") or [""])[0]
    return token or ""

def _extract_realm(ws, path: str, first_msg: dict | None, default_realm: str) -> str:
    h = ws.request_headers
    realm = h.get("X-Realm") or h.get("x-realm") or ""
    if not realm:
        q = _extract_qs(path)
        realm = (q.get("realm") or [""])[0]
    if not realm and isinstance(first_msg, dict):
        realm = (
            first_msg.get("realm")
            or (first_msg.get("payload") or {}).get("realm")
            or ""
        )
    return realm or default_realm

async def _send_json(ws, obj: dict):
    await ws.send(json.dumps(obj, ensure_ascii=False))

async def broadcast_admin(msg: dict):
    dead = []
    data = json.dumps(msg, ensure_ascii=False)
    for ws in list(ADMINS):
        try:
            await ws.send(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ADMINS.discard(ws)

def realm_has_plugins(realm: str) -> bool:
    return realm in PLUGINS and len(PLUGINS[realm]) > 0

def _single_online_realm() -> str | None:
    live = [r for r, s in PLUGINS.items() if len(s) > 0]
    return live[0] if len(live) == 1 else None

async def route_to_realm(realm: Optional[str], msg: dict):
    if not realm:
        realm = _single_online_realm()
    if not realm or not realm_has_plugins(realm):
        print(f"[bridge] NO PLUGIN for realm='{realm}', drop: {msg.get('type')}")
        await broadcast_admin({
            "type": "bridge.warn",
            "payload": {
                "message": f"No plugin online for realm '{realm}'",
                "request": msg
            }
        })
        return
    t = msg.get("type")
    print(f"[bridge] ROUTE {t} -> realm='{realm}'")
    data = json.dumps(msg, ensure_ascii=False)
    dead = []
    for ws in list(PLUGINS[realm]):
        try:
            await ws.send(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        PLUGINS[realm].discard(ws)

# ------------------ admin side mapping ------------------

_ADMIN_FIRST_TYPES = {
    # существующие
    "bridge.list", "console.exec", "cmd.exec", "cmd.execLines",
    "stats.query", "server.stats", "maintenance.set", "broadcast", "player.is_online",
    # новые: ops/cmdwl
    "ops.set", "cmdwl.set", "cmdwl.commands",
    # luckperms
    "lp.web.open", "lp.web.apply",
    "lp.user.perm.add", "lp.user.perm.remove",
    "lp.user.group.add", "lp.user.group.remove",
    "lp.group.perm.add", "lp.group.perm.remove",
    "lp.user.info", "lp.group.info",
    # justpoints
    "jp.balance.get", "jp.balance.set", "jp.balance.add", "jp.balance.take",
    # health
    "bridge.ping", "bridge.info",
}

def _map_admin_request(obj: dict) -> dict:
    """
    Нормализуем вход от админов в единый формат для плагина.
    Возвращаем НОВЫЙ объект, который пойдёт плагину как есть.
    """
    t = obj.get("type")
    p = obj.get("payload") or {}
    realm = obj.get("realm") or p.get("realm")

    # --- console / exec ---
    if t in ("console.exec", "cmd.exec"):
        cmd = p.get("command") or p.get("cmd") or obj.get("command")
        return {"type": "console.exec", "realm": realm, "id": p.get("id"), "command": cmd}

    if t in ("console.execLines", "cmd.execLines"):
        lines = p.get("lines") or obj.get("lines") or []
        return {"type": "console.execLines", "realm": realm, "id": p.get("id"), "lines": lines}

    # --- stats ---
    if t in ("stats.query", "server.stats"):
        return {"type": "server.stats", "realm": realm}

    # --- maintenance ---
    if t == "maintenance.set":
        enabled = bool(p.get("enabled"))
        message = p.get("message") or p.get("kickMessage") or p.get("reason") or ""
        if enabled:
            cmd = "realm maintenance on"
            if message:
                cmd += " kick " + message
        else:
            cmd = "realm maintenance off"
        return {"type": "console.exec", "realm": realm, "command": cmd}

    # --- maintenance whitelist via console alias ---
    if t == "maintenance.whitelist":
        action = (p.get("action") or p.get("op") or "list").strip()
        user = (p.get("user") or p.get("player") or "").strip()
        cmd = f"realm maintwl {action} {user}".strip()
        return {"type": "console.exec", "realm": realm, "command": cmd}

    # --- broadcast ---
    if t == "broadcast":
        msg = p.get("message") or obj.get("message") or ""
        return {"type": "broadcast", "realm": realm, "message": msg}

    # --- player.is_online ---
    if t == "player.is_online":
        return {
            "type": "player.is_online",
            "realm": realm,
            "uuid": p.get("uuid") or obj.get("uuid"),
            "name": p.get("name") or obj.get("name"),
        }

    # --- OPS/CMDWL прямые события, плагин их уже понимает ---
    if t in ("ops.set", "cmdwl.set", "cmdwl.commands"):
        return {"type": t, "realm": realm, "payload": p}

    # --- LuckPerms: транзит ---
    if t in {
        "lp.web.open", "lp.web.apply",
        "lp.user.perm.add", "lp.user.perm.remove",
        "lp.user.group.add", "lp.user.group.remove",
        "lp.group.perm.add", "lp.group.perm.remove",
        "lp.user.info", "lp.group.info"
    }:
        return {"type": t, "realm": realm, "payload": p}

    # --- JustPoints: транзит ---
    if t in {"jp.balance.get", "jp.balance.set", "jp.balance.add", "jp.balance.take"}:
        return {"type": t, "realm": realm, "payload": p}

    # health (обрабатываются в process_admin, здесь оставим как есть)
    if t in {"bridge.ping", "bridge.info", "bridge.list"}:
        return obj

    # по умолчанию пропускаем как есть
    return obj

# ------------------ core handler ------------------

async def handler(ws, path, token_required: str, default_realm: str, *, verbose: bool):
    # auth
    provided = _extract_token(ws, path)
    if token_required and provided != token_required:
        addr = getattr(ws, "remote_address", None)
        print(
            f"[bridge] unauthorized from {addr}. "
            f"Provided token len={len(provided)} matches={provided == token_required}"
        )
        await ws.close(code=4401, reason="unauthorized")
        return

    role = "plugin"  # по умолчанию — плагин
    realm: Optional[str] = None
    rl = RateLimiter()

    print(f"[bridge] connect from {ws.remote_address}")

    first_msg = None
    try:
        # пробуем прочитать первый фрейм (до 5с), чтобы угадать роль/realm
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                first_msg = json.loads(raw)
            except Exception:
                first_msg = {}
            print("[bridge] first frame:", _short_json(first_msg))
        except Exception:
            first_msg = None

        # Определяем роль (учитываем новые админ-типы)
        if isinstance(first_msg, dict) and first_msg.get("type") in _ADMIN_FIRST_TYPES:
            role = "admin"
        else:
            role = "plugin"

        # realm
        realm = _extract_realm(ws, path, first_msg, default_realm)

        # регистрация
        if role == "plugin":
            PLUGINS.setdefault(realm, set()).add(ws)
            print(f"[bridge] plugin registered realm='{realm}'")
            await _send_json(ws, {
                "type": "hello.ok",
                "realm": realm,
                "server_time": datetime.utcnow().isoformat() + "Z"
            })
            await broadcast_admin({
                "type": "bridge.info",
                "payload": {"message": f"Plugin online realm='{realm}'"}
            })
            # если плагин первым прислал кадр (например, hello) — отдадим ACK/ok ниже в основном цикле
            if isinstance(first_msg, dict) and first_msg:
                _log_recv(first_msg.get("type") or "?", realm, first_msg, verbose)
                await broadcast_admin({**first_msg, "realm": realm})
        else:
            ADMINS.add(ws)
            print("[bridge] admin connected")
            # если первое сообщение админское — обработаем
            if isinstance(first_msg, dict) and first_msg:
                await process_admin(ws, first_msg, verbose=verbose)

        # основной цикл
        async for raw in ws:
            # rate-limit
            if not rl.hit():
                with contextlib.suppress(Exception):
                    await _send_json(ws, {"type": "bridge.error", "payload": {"reason": "rate_limited"}})
                await ws.close(code=4412, reason="rate_limited")
                return

            if isinstance(raw, (bytes, bytearray)):
                await broadcast_admin({"type": "bridge.binary", "realm": realm, "len": len(raw)})
                continue

            if isinstance(raw, str) and len(raw) > MAX_TEXT_LEN:
                with contextlib.suppress(Exception):
                    await _send_json(ws, {"type": "bridge.error", "payload": {"reason": "text_too_long"}})
                continue

            try:
                obj = json.loads(raw)
            except Exception:
                if role == "plugin":
                    await broadcast_admin({"type": "bridge.echo", "realm": realm, "payload": raw})
                else:
                    await _send_json(ws, {"type": "bridge.ack", "payload": {"seenText": raw}})
                continue

            if role == "plugin":
                t = obj.get("type") or "?"
                # базовые сервисные ответы
                if t == "ping":
                    await _send_json(ws, {"type": "pong", "realm": realm})
                    continue
                if t == "hello":
                    await _send_json(ws, {
                        "type": "hello.ok",
                        "realm": realm,
                        "server_time": datetime.utcnow().isoformat() + "Z"
                    })
                    continue

                _log_recv(t, realm, obj, verbose)
                # транслируем админам ВСЁ, добавив realm
                await broadcast_admin({**obj, "realm": realm})
            else:
                await process_admin(ws, obj, verbose=verbose)

    except (ConnectionClosedOK, ConnectionClosedError):
        pass
    finally:
        if role == "plugin" and realm:
            PLUGINS.get(realm, set()).discard(ws)
            print(f"[bridge] plugin disconnected realm='{realm}'")
            await broadcast_admin({
                "type": "bridge.info",
                "payload": {"message": f"Plugin offline realm='{realm}'"}
            })
        elif role == "admin":
            ADMINS.discard(ws)
            print("[bridge] admin disconnected")

async def process_admin(ws, obj: dict, *, verbose: bool):
    # нормализуем запрос
    norm = _map_admin_request(obj)
    t = norm.get("type")
    p = norm.get("payload") or {}
    realm = norm.get("realm") or p.get("realm")

    # быстрый health
    if t == "bridge.ping":
        await _send_json(ws, {"type": "bridge.pong", "server_time": datetime.utcnow().isoformat()+"Z"})
        return

    if t == "bridge.info":
        payload = {
            "realms": {r: len(s) for r, s in PLUGINS.items()},
            "admins": len(ADMINS),
            "server_time": datetime.utcnow().isoformat()+"Z"
        }
        await _send_json(ws, {"type": "bridge.info.result", "payload": payload})
        return

    # быстрые ACK’и админам (кроме явных list/info/ping)
    if t not in {"bridge.list", "bridge.info", "bridge.ping"}:
        with contextlib.suppress(Exception):
            await _send_json(ws, {"type": "bridge.ack", "payload": {"seenType": t}})

    # прямые типы, которые должен обработать плагин:
    direct_to_plugin = {
        "console.exec", "console.execLines",
        "server.stats", "broadcast", "player.is_online",
        "ops.set", "cmdwl.set", "cmdwl.commands",
        "lp.web.open", "lp.web.apply",
        "lp.user.perm.add", "lp.user.perm.remove",
        "lp.user.group.add", "lp.user.group.remove",
        "lp.group.perm.add", "lp.group.perm.remove",
        "lp.user.info", "lp.group.info",
        "jp.balance.get", "jp.balance.set", "jp.balance.add", "jp.balance.take",
    }
    if t in direct_to_plugin:
        # если realm указали, но он оффлайн — сразу скажем отправителю
        if realm and not realm_has_plugins(realm):
            await _send_json(ws, {"type": "bridge.error", "payload": {"reason": f"no_plugin_for_realm:{realm}"}})
            return
        await route_to_realm(realm, norm)
        return

    if t == "bridge.list":
        listing = {r: len(s) for r, s in PLUGINS.items()}
        await _send_json(ws, {"type": "bridge.list.result", "payload": listing})
        return

    # неизвестное — просто подтвердим
    await _send_json(ws, {"type": "bridge.ack", "payload": {"seenType": t, "note": "unknown"}})

# ------------------ REPL (optional) ------------------

async def repl(uri, token, default_realm: str, *, verbose: bool):
    async with websockets.connect(uri, extra_headers={"Authorization": f"Bearer {token}"}) as ws:
        print("REPL ready. Commands:")
        print(" list")
        print(" exec <realm?> <cmd...>")
        print(" execm <realm?> # множественные строки, окончание пустой строкой")
        print(" stats <realm?>")
        print(" maint <realm?> on|off [kick message]")
        print(" bc <realm?> <message>")
        print(" online <realm?> <name|uuid>")
        print(" lpopen <realm?>")
        print(" lpapply <realm?> <code>")
        print(" lpuadd <realm?> <user> <perm> [true|false]")
        print(" lpurem <realm?> <user> <perm>")
        print(" lpugadd <realm?> <user> <group>")
        print(" lpugrem <realm?> <user> <group>")
        print(" lpgadd <realm?> <group> <perm> [true|false]")
        print(" lpgrem <realm?> <group> <perm>")
        print(" jpget <realm?> <user>")
        print(" jpset <realm?> <user> <amount>")
        print(" jpadd <realm?> <user> <amount>")
        print(" jptake <realm?> <user> <amount>")

        async def reader():
            async for m in ws:
                try:
                    obj = json.loads(m)
                    print("[recv]", _short_json(obj) if verbose else _pretty_tag(obj))
                except Exception:
                    print("[recv]", m)

        async def writer():
            loop = asyncio.get_running_loop()
            while True:
                line = await loop.run_in_executor(None, sys.stdin.readline)
                line = (line or "").strip()
                if not line:
                    continue
                if line == "list":
                    await ws.send(json.dumps({"type": "bridge.list"})); continue

                if line.startswith("execm"):
                    parts = line.split(" ", 1)
                    realm = parts[1].strip() if len(parts) > 1 and parts[1] else None
                    print("enter commands, blank line to finish")
                    lines = []
                    while True:
                        l = await loop.run_in_executor(None, sys.stdin.readline)
                        l = l.rstrip("\n")
                        if not l:
                            break
                        lines.append(l)
                    payload = {"type": "console.execLines", **({"realm": realm} if realm else {}), "lines": lines}
                    await ws.send(json.dumps(payload)); continue

                if line.startswith("exec "):
                    _, *rest = line.split(" ")
                    if not rest:
                        print("usage: exec <realm?> <cmd...>"); continue
                    realm = rest[0] if len(rest) > 1 else None
                    cmd = " ".join(rest[1:]) if len(rest) > 1 else " ".join(rest)
                    if not cmd:
                        print("usage: exec <realm?> <cmd...>"); continue
                    await ws.send(json.dumps({"type": "console.exec", **({"realm": realm} if realm else {}), "command": cmd})); continue

                if line.startswith("stats"):
                    _, *rest = line.split(" ", 1)
                    realm = rest[0] if rest else None
                    await ws.send(json.dumps({"type": "server.stats", **({"realm": realm} if realm else {})})); continue

                if line.startswith("maint "):
                    _, *rest = line.split(" ")
                    if not rest:
                        print("usage: maint <realm?> on|off [kick message]"); continue
                    if rest[0] in ("on", "off"):
                        realm = None; state = rest[0]; msg = " ".join(rest[1:]) if len(rest) > 1 else ""
                    else:
                        realm = rest[0]
                        if len(rest) < 2:
                            print("usage: maint <realm> on|off [kick message]"); continue
                        state = rest[1]; msg = " ".join(rest[2:]) if len(rest) > 2 else ""
                    await ws.send(json.dumps({
                        "type": "maintenance.set",
                        **({"realm": realm} if realm else {}),
                        "payload": {"realm": realm, "enabled": state.lower() == "on", "message": msg}
                    })); continue

                if line.startswith("bc "):
                    _, *rest = line.split(" ")
                    if not rest:
                        print("usage: bc <realm?> <message>"); continue
                    realm = None
                    message = " ".join(rest)
                    await ws.send(json.dumps({"type": "broadcast", **({"realm": realm} if realm else {}), "message": message})); continue

                if line.startswith("online "):
                    _, *rest = line.split(" ")
                    if not rest:
                        print("usage: online <realm?> <name|uuid>"); continue
                    realm = None
                    who = rest[-1]
                    await ws.send(json.dumps({"type": "player.is_online", **({"realm": realm} if realm else {}), "name": who})); continue

                # ---- LuckPerms REPL helpers ----
                if line.startswith("lpopen"):
                    _, *rest = line.split(" ", 1)
                    realm = (rest[0].strip() if rest else None) or None
                    await ws.send(json.dumps({"type": "lp.web.open", **({"realm": realm} if realm else {})})); continue

                if line.startswith("lpapply "):
                    _, *rest = line.split(" ", 1)
                    if not rest:
                        print("usage: lpapply <realm?> <code>"); continue
                    parts = rest[0].split(" ")
                    if len(parts) == 1:
                        realm = None; code = parts[0]
                    else:
                        realm = parts[0]; code = parts[1]
                    await ws.send(json.dumps({"type": "lp.web.apply", **({"realm": realm} if realm else {}), "payload": {"code": code}})); continue

                if line.startswith("lpuadd "):
                    # lpuadd <realm?> <user> <perm> [true|false]
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: lpuadd <realm?> <user> <perm> [true|false]"); continue
                    if len(rest) == 2:
                        realm = None; user, perm = rest
                        val = True
                    else:
                        realm = rest[0]; user = rest[1]; perm = rest[2]; val = (rest[3].lower() == "true") if len(rest) > 3 else True
                    await ws.send(json.dumps({"type": "lp.user.perm.add", **({"realm": realm} if realm else {}), "payload": {"user": user, "permission": perm, "value": val}})); continue

                if line.startswith("lpurem "):
                    # lpurem <realm?> <user> <perm>
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: lpurem <realm?> <user> <perm>"); continue
                    if len(rest) == 2:
                        realm = None; user, perm = rest
                    else:
                        realm = rest[0]; user = rest[1]; perm = rest[2]
                    await ws.send(json.dumps({"type": "lp.user.perm.remove", **({"realm": realm} if realm else {}), "payload": {"user": user, "permission": perm}})); continue

                if line.startswith("lpugadd "):
                    # lpugadd <realm?> <user> <group>
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: lpugadd <realm?> <user> <group>"); continue
                    if len(rest) == 2:
                        realm = None; user, group = rest
                    else:
                        realm = rest[0]; user = rest[1]; group = rest[2]
                    await ws.send(json.dumps({"type": "lp.user.group.add", **({"realm": realm} if realm else {}), "payload": {"user": user, "group": group}})); continue

                if line.startswith("lpugrem "):
                    # lpugrem <realm?> <user> <group>
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: lpugrem <realm?> <user> <group>"); continue
                    if len(rest) == 2:
                        realm = None; user, group = rest
                    else:
                        realm = rest[0]; user = rest[1]; group = rest[2]
                    await ws.send(json.dumps({"type": "lp.user.group.remove", **({"realm": realm} if realm else {}), "payload": {"user": user, "group": group}})); continue

                if line.startswith("lpgadd "):
                    # lpgadd <realm?> <group> <perm> [true|false]
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: lpgadd <realm?> <group> <perm> [true|false]"); continue
                    if len(rest) == 2:
                        realm = None; group, perm = rest
                        val = True
                    else:
                        realm = rest[0]; group = rest[1]; perm = rest[2]; val = (rest[3].lower() == "true") if len(rest) > 3 else True
                    await ws.send(json.dumps({"type": "lp.group.perm.add", **({"realm": realm} if realm else {}), "payload": {"group": group, "permission": perm, "value": val}})); continue

                if line.startswith("lpgrem "):
                    # lpgrem <realm?> <group> <perm>
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: lpgrem <realm?> <group> <perm>"); continue
                    if len(rest) == 2:
                        realm = None; group, perm = rest
                    else:
                        realm = rest[0]; group = rest[1]; perm = rest[2]
                    await ws.send(json.dumps({"type": "lp.group.perm.remove", **({"realm": realm} if realm else {}), "payload": {"group": group, "permission": perm}})); continue

                # ---- JustPoints REPL helpers ----
                if line.startswith("jpget "):
                    _, *rest = line.split(" ")
                    if len(rest) == 1:
                        realm = None; user = rest[0]
                    else:
                        realm = rest[0]; user = rest[1]
                    await ws.send(json.dumps({"type": "jp.balance.get", **({"realm": realm} if realm else {}), "payload": {"user": user}})); continue

                if line.startswith("jpset "):
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: jpset <realm?> <user> <amount>"); continue
                    if len(rest) == 2:
                        realm = None; user, amt = rest
                    else:
                        realm = rest[0]; user = rest[1]; amt = rest[2]
                    await ws.send(json.dumps({"type": "jp.balance.set", **({"realm": realm} if realm else {}), "payload": {"user": user, "amount": float(amt)}})); continue

                if line.startswith("jpadd "):
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: jpadd <realm?> <user> <amount>"); continue
                    if len(rest) == 2:
                        realm = None; user, amt = rest
                    else:
                        realm = rest[0]; user = rest[1]; amt = rest[2]
                    await ws.send(json.dumps({"type": "jp.balance.add", **({"realm": realm} if realm else {}), "payload": {"user": user, "amount": float(amt)}})); continue

                if line.startswith("jptake "):
                    _, *rest = line.split(" ")
                    if len(rest) < 2:
                        print("usage: jptake <realm?> <user> <amount>"); continue
                    if len(rest) == 2:
                        realm = None; user, amt = rest
                    else:
                        realm = rest[0]; user = rest[1]; amt = rest[2]
                    await ws.send(json.dumps({"type": "jp.balance.take", **({"realm": realm} if realm else {}), "payload": {"user": user, "amount": float(amt)}})); continue

                print("unknown command")

        await asyncio.gather(reader(), writer())

# ------------------ main ------------------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("SP_BRIDGE_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.getenv("SP_BRIDGE_PORT", "8765")))
    ap.add_argument("--token", default=os.getenv("SP_TOKEN", "SUPER_SECRET"))
    ap.add_argument("--realm", default=os.getenv("SP_REALM", "default"),
                    help="default realm for plugin connections without hello/realm")
    ap.add_argument("--repl", action="store_true", help="run local REPL admin client")
    ap.add_argument("--verbose", action="store_true",
                    default=os.getenv("BRIDGE_VERBOSE", "0") not in ("0", "", "false", "False"),
                    help="print full payloads for all frames")
    # Тонкая настройка сервера
    ap.add_argument("--max-size", type=int, default=int(os.getenv("SP_MAX_SIZE", "131072")))
    ap.add_argument("--max-queue", type=int, default=int(os.getenv("SP_MAX_QUEUE", "32")))
    ap.add_argument("--read-limit", type=int, default=int(os.getenv("SP_READ_LIMIT", str(2**16))))
    ap.add_argument("--write-limit", type=int, default=int(os.getenv("SP_WRITE_LIMIT", str(2**16))))
    args = ap.parse_args()

    async def ws_handler(ws, path):
        if urlparse(path).path != "/ws":
            await ws.close(code=4404, reason="not_found"); return
        return await handler(ws, path, args.token, args.realm, verbose=args.verbose)

    print(f"[bridge] starting ws server on ws://{args.host}:{args.port}/ws "
          f"(token len={len(args.token)}, default realm='{args.realm}')")

    async with websockets.serve(
        ws_handler, args.host, args.port,
        ping_interval=20, ping_timeout=20, max_size=args.max_size,
        max_queue=args.max_queue, read_limit=args.read_limit, write_limit=args.write_limit,
    ):
        if args.repl:
            await repl(f"ws://127.0.0.1:{args.port}/ws", args.token, args.realm, verbose=args.verbose)
        else:
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
