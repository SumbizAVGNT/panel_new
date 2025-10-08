# bridge/bridge.py
# pip install websockets
from __future__ import annotations

import asyncio
import json
import argparse
import os
from urllib.parse import urlparse, parse_qs

import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# realm -> set(ws)
PLUGINS: dict[str, set] = {}
# admin connections
ADMINS: set = set()

# какие типы обычно шлёт плагин
PLUGIN_TYPICAL_TYPES = {
    "console_out", "console_done",
    "server.stats",
    "player.online", "broadcast.ok",
    "error", "pong", "bridge.log"
}

# ------------------ helpers ------------------

def _extract_qs(path: str) -> dict[str, list[str]]:
    try:
        return parse_qs(urlparse(path).query)
    except Exception:
        return {}

def _extract_token(ws, path: str) -> str:
    """Токен из Authorization / X-Auth-Token / query ?token="""
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
    # 1) из заголовка
    h = ws.request_headers
    realm = h.get("X-Realm") or h.get("x-realm") or ""
    # 2) из query
    if not realm:
        q = _extract_qs(path)
        realm = (q.get("realm") or [""])[0]
    # 3) из первого сообщения
    if not realm and isinstance(first_msg, dict):
        realm = (
            first_msg.get("realm") or
            (first_msg.get("payload") or {}).get("realm") or
            ""
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

async def route_to_realm(realm: str | None, msg: dict):
    # если realm не указан, а есть ровно один — возьмём его
    if not realm:
        realm = _single_online_realm()
    if not realm or not realm_has_plugins(realm):
        print(f"[bridge] NO PLUGIN for realm='{realm}', drop: {msg.get('type')}")
        await broadcast_admin({
            "type": "bridge.warn",
            "payload": {"message": f"No plugin online for realm '{realm}'", "request": msg}
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

def _map_admin_request(obj: dict) -> dict:
    """
    Приводим старые типы к новым и нормализуем payload.
    Возвращаем НОВЫЙ объект, который пойдёт плагину как есть.
    """
    t = obj.get("type")
    p = obj.get("payload") or {}
    realm = obj.get("realm") or p.get("realm")

    # --- console / exec ---
    if t in ("console.exec", "cmd.exec"):
        cmd = p.get("cmd") or p.get("command") or obj.get("command")
        return {"type": "cmd.exec", "realm": realm, "command": cmd}

    if t in ("console.execLines", "cmd.execLines"):
        lines = p.get("lines") or obj.get("lines") or []
        return {"type": "cmd.execLines", "realm": realm, "lines": lines}

    # --- stats ---
    if t in ("stats.query", "server.stats"):
        # Плагину достаточно послать type=server.stats — он ответит этим же типом с данными
        return {"type": "server.stats", "realm": realm}

    # --- maintenance ---
    if t == "maintenance.set":
        enabled = bool(p.get("enabled"))
        message = p.get("message") or ""
        # Маппим в консольную команду плагина /realm maintenance ...
        if enabled:
            cmd = "realm maintenance on"
            if message:
                cmd += " kick " + message
        else:
            cmd = "realm maintenance off"
        return {"type": "cmd.exec", "realm": realm, "command": cmd}

    if t == "maintenance.whitelist":
        # нет нативного API в плагине — пусть тоже идёт как консольная команда
        action = p.get("action") or "list"
        user = p.get("user") or ""
        cmd = f"realm maintwl {action} {user}".strip()
        return {"type": "cmd.exec", "realm": realm, "command": cmd}

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

    # по умолчанию пропускаем как есть
    return obj

# ------------------ core handler ------------------

async def handler(ws, path, token_required: str, default_realm: str):
    # auth
    provided = _extract_token(ws, path)
    if token_required and provided != token_required:
        print(f"[bridge] unauthorized from {ws.remote_address}. "
              f"Provided token len={len(provided)} matches={provided == token_required}")
        await ws.close(code=4401, reason="unauthorized")
        return

    role = "plugin"  # по умолчанию считаем, что это плагин (под новый клиент без hello)
    realm = None
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
            print("[bridge] first frame:", first_msg)
        except Exception:
            first_msg = None

        # Определяем роль
        if isinstance(first_msg, dict) and first_msg.get("type") == "hello":
            role = "plugin"
        elif isinstance(first_msg, dict) and first_msg.get("type") in PLUGIN_TYPICAL_TYPES:
            role = "plugin"
        else:
            # если пришёл «админский» тип (list/exec и т.п.) — это админ
            if isinstance(first_msg, dict) and first_msg.get("type") in {
                "bridge.list", "console.exec", "cmd.exec", "cmd.execLines",
                "stats.query", "server.stats", "maintenance.set", "broadcast", "player.is_online"
            }:
                role = "admin"
            else:
                # если вообще ничего не пришло — пусть будет плагин (он может просто молчать)
                role = "plugin"

        # realm
        realm = _extract_realm(ws, path, first_msg, default_realm)

        # регистрация
        if role == "plugin":
            PLUGINS.setdefault(realm, set()).add(ws)
            print(f"[bridge] plugin registered realm='{realm}'")
            await broadcast_admin({"type": "bridge.info",
                                   "payload": {"message": f"Plugin online realm='{realm}'"}})
        else:
            ADMINS.add(ws)
            print("[bridge] admin connected")

        # если первое сообщение админское — обработаем
        if role == "admin" and isinstance(first_msg, dict) and first_msg:
            await process_admin(ws, first_msg)

        # основной цикл
        try:
            async for raw in ws:
                try:
                    obj = json.loads(raw)
                except Exception:
                    print("[bridge] bad json:", raw)
                    continue

                if role == "plugin":
                    t = obj.get("type")
                    print(f"[bridge] <- {t} from {realm}")

                    # транслируем админам важные события/ответы
                    if t in {"console_out", "console_done", "server.stats",
                             "player.online", "broadcast.ok", "error", "pong"}:
                        await broadcast_admin(obj)
                    else:
                        # echo на всякий случай
                        await broadcast_admin({"type": "bridge.echo", "payload": obj})
                else:
                    await process_admin(ws, obj)

        except (ConnectionClosedOK, ConnectionClosedError):
            pass

    finally:
        if role == "plugin" and realm:
            PLUGINS.get(realm, set()).discard(ws)
            print(f"[bridge] plugin disconnected realm='{realm}'")
        elif role == "admin":
            ADMINS.discard(ws)
            print("[bridge] admin disconnected")

async def process_admin(ws, obj: dict):
    # нормализуем запрос
    norm = _map_admin_request(obj)
    t = norm.get("type")
    p = norm.get("payload") or {}
    realm = norm.get("realm") or p.get("realm")

    if t in {"cmd.exec", "cmd.execLines", "server.stats",
             "broadcast", "player.is_online"}:
        await route_to_realm(realm, norm)
        return

    if t == "bridge.list":
        listing = {r: len(s) for r, s in PLUGINS.items()}
        await _send_json(ws, {"type": "bridge.list.result", "payload": listing})
        return

    # по умолчанию — ack
    await _send_json(ws, {"type": "bridge.ack", "payload": {"seenType": t}})

# ------------------ REPL (optional) ------------------

async def repl(uri, token, default_realm: str):
    async with websockets.connect(uri, extra_headers={"Authorization": f"Bearer {token}"}) as ws:
        print("REPL ready. Commands:")
        print("  list")
        print("  exec <realm?> <cmd...>")
        print("  execm <realm?>  # множественные строки, окончание пустой строкой")
        print("  stats <realm?>")
        print("  maint <realm?> on|off [kick message]")
        print("  bc <realm?> <message>")
        print("  online <realm?> <name|uuid>")

        async def reader():
            async for m in ws:
                print("[recv]", m)

        async def writer():
            while True:
                line = input("> ").strip()
                if not line:
                    continue
                if line == "list":
                    await ws.send(json.dumps({"type": "bridge.list"})); continue
                if line.startswith("execm"):
                    # многократные строки команд
                    parts = line.split(" ", 1)
                    realm = None
                    if len(parts) > 1 and parts[1]:
                        realm = parts[1].strip()
                    print("enter commands, blank line to finish")
                    lines = []
                    while True:
                        l = input("")
                        if not l: break
                        lines.append(l)
                    payload = {"type": "cmd.execLines"}
                    if realm: payload["realm"] = realm
                    payload["lines"] = lines
                    await ws.send(json.dumps(payload)); continue
                if line.startswith("exec "):
                    _, *rest = line.split(" ")
                    # если указан realm — он первый
                    if rest:
                        # предполагаем, что если realm без пробелов и онлайн ровно один — можно его опустить
                        realm = rest[0]
                        cmd = " ".join(rest[1:]) if len(rest) > 1 else ""
                        if not cmd:
                            print("usage: exec <realm?> <cmd...>")
                            continue
                        await ws.send(json.dumps({"type": "cmd.exec", "realm": realm, "command": cmd})); continue
                if line.startswith("stats"):
                    _, *rest = line.split(" ", 1)
                    realm = rest[0] if rest else None
                    await ws.send(json.dumps({"type": "server.stats", **({"realm": realm} if realm else {})})); continue
                if line.startswith("maint "):
                    _, *rest = line.split(" ")
                    if len(rest) < 1:
                        print("usage: maint <realm?> on|off [kick message]"); continue
                    # если realm не указан явно — попытаемся без него (бридж сам подставит если ровно один)
                    if rest[0] in ("on", "off"):
                        realm = None
                        state = rest[0]
                        msg = " ".join(rest[1:]) if len(rest) > 1 else ""
                    else:
                        realm = rest[0]
                        if len(rest) < 2:
                            print("usage: maint <realm> on|off [kick message]"); continue
                        state = rest[1]
                        msg = " ".join(rest[2:]) if len(rest) > 2 else ""
                    await ws.send(json.dumps({
                        "type": "maintenance.set",
                        **({"realm": realm} if realm else {}),
                        "payload": {"realm": realm, "enabled": state.lower() == "on", "message": msg}
                    })); continue
                if line.startswith("bc "):
                    _, *rest = line.split(" ")
                    if not rest:
                        print("usage: bc <realm?> <message>"); continue
                    # если realm был — он первый
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
    args = ap.parse_args()

    async def ws_handler(ws, path):
        if urlparse(path).path != "/ws":
            await ws.close(code=4404, reason="not_found"); return
        return await handler(ws, path, args.token, args.realm)

    print(f"[bridge] starting ws server on ws://{args.host}:{args.port}/ws "
          f"(token len={len(args.token)}, default realm='{args.realm}')")
    async with websockets.serve(ws_handler, args.host, args.port, ping_interval=20, ping_timeout=20):
        if args.repl:
            await repl(f"ws://127.0.0.1:{args.port}/ws", args.token, args.realm)
        else:
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
