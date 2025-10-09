# bridge/bridge.py
# pip install websockets
from __future__ import annotations

import asyncio
import json
import argparse
import os
from urllib.parse import urlparse, parse_qs
from datetime import datetime

import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# realm -> set(ws)
PLUGINS: dict[str, set] = {}
# admin connections
ADMINS: set = set()

# типы, которые чаще всего шлёт плагин — логируем их чуть заметнее
PLUGIN_TYPICAL_TYPES = {
    "console_out", "console_done",
    "server.stats", "stats.report",
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

async def route_to_realm(realm: str | None, msg: dict):
    # если realm не указан, а есть ровно один — возьмём его
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
        # Плагин слушает общий поток и может сам отвечать "server.stats" (или "stats.report")
        return {"type": "server.stats", "realm": realm}

    # --- maintenance ---
    if t == "maintenance.set":
        enabled = bool(p.get("enabled"))
        message = p.get("message") or p.get("kickMessage") or p.get("reason") or ""
        # Маппим в консольные команды плагина (универсально)
        if enabled:
            cmd = "realm maintenance on"
            if message:
                cmd += " kick " + message
        else:
            cmd = "realm maintenance off"
        return {"type": "console.exec", "realm": realm, "command": cmd}

    if t == "maintenance.whitelist":
        # принимаем оба варианта ключей
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

    # по умолчанию пропускаем как есть
    return obj

# ------------------ core handler ------------------

async def handler(ws, path, token_required: str, default_realm: str):
    # auth
    provided = _extract_token(ws, path)
    if token_required and provided != token_required:
        print(
            f"[bridge] unauthorized from {ws.remote_address}. "
            f"Provided token len={len(provided)} matches={provided == token_required}"
        )
        await ws.close(code=4401, reason="unauthorized")
        return

    role = "plugin"  # по умолчанию — плагин (тихий клиент без hello)
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
        if isinstance(first_msg, dict) and first_msg.get("type") in {
            "bridge.list", "console.exec", "cmd.exec", "cmd.execLines",
            "stats.query", "server.stats", "maintenance.set", "broadcast", "player.is_online"
        }:
            role = "admin"
        else:
            role = "plugin"

        # realm
        realm = _extract_realm(ws, path, first_msg, default_realm)

        # регистрация
        if role == "plugin":
            PLUGINS.setdefault(realm, set()).add(ws)
            print(f"[bridge] plugin registered realm='{realm}'")
            # Отвечаем плагину, что связь OK
            await _send_json(ws, {
                "type": "hello.ok",
                "realm": realm,
                "server_time": datetime.utcnow().isoformat() + "Z"
            })
            await broadcast_admin({
                "type": "bridge.info",
                "payload": {"message": f"Plugin online realm='{realm}'"}
            })
        else:
            ADMINS.add(ws)
            print("[bridge] admin connected")
            # если первое сообщение админское — обработаем
            if isinstance(first_msg, dict) and first_msg:
                await process_admin(ws, first_msg)

        # основной цикл
        async for raw in ws:
            # допускаем ping/pong текстовые
            if isinstance(raw, (bytes, bytearray)):
                # бинарь транслируем админам как есть
                await broadcast_admin({"type": "bridge.binary", "realm": realm, "len": len(raw)})
                continue

            try:
                obj = json.loads(raw)
            except Exception:
                # не-JSON от плагина — ретранслируем админам как echo
                if role == "plugin":
                    await broadcast_admin({"type": "bridge.echo", "realm": realm, "payload": raw})
                else:
                    await _send_json(ws, {"type": "bridge.ack", "payload": {"seenText": raw}})
                continue

            if role == "plugin":
                t = obj.get("type")
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

                # логируем приоритетные типы, остальное тоже транслируем
                tag = t or "?"
                if t in PLUGIN_TYPICAL_TYPES:
                    print(f"[bridge] <- {tag} from realm='{realm}'")
                else:
                    print(f"[bridge] <- other:{tag} from realm='{realm}'")

                # транслируем админам ВСЁ
                await broadcast_admin({**obj, "realm": realm})
            else:
                await process_admin(ws, obj)

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

async def process_admin(ws, obj: dict):
    # нормализуем запрос
    norm = _map_admin_request(obj)
    t = norm.get("type")
    p = norm.get("payload") or {}
    realm = norm.get("realm") or p.get("realm")

    # быстрые ACK’и админам
    if t not in {"bridge.list"}:
        with contextlib.suppress(Exception):
            await _send_json(ws, {"type": "bridge.ack", "payload": {"seenType": t}})

    if t in {"console.exec", "console.execLines", "server.stats", "broadcast", "player.is_online"}:
        await route_to_realm(realm, norm)
        return

    if t == "bridge.list":
        listing = {r: len(s) for r, s in PLUGINS.items()}
        await _send_json(ws, {"type": "bridge.list.result", "payload": listing})
        return

    # неизвестное — просто подтвердим
    await _send_json(ws, {"type": "bridge.ack", "payload": {"seenType": t, "note": "unknown"}})

# ------------------ REPL (optional) ------------------

async def repl(uri, token, default_realm: str):
    async with websockets.connect(uri, extra_headers={"Authorization": f"Bearer {token}"}) as ws:
        print("REPL ready. Commands:")
        print(" list")
        print(" exec <realm?> <cmd...>")
        print(" execm <realm?> # множественные строки, окончание пустой строкой")
        print(" stats <realm?>")
        print(" maint <realm?> on|off [kick message]")
        print(" bc <realm?> <message>")
        print(" online <realm?> <name|uuid>")

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
                    parts = line.split(" ", 1)
                    realm = parts[1].strip() if len(parts) > 1 and parts[1] else None
                    print("enter commands, blank line to finish")
                    lines = []
                    while True:
                        l = input("")
                        if not l: break
                        lines.append(l)
                    payload = {"type": "console.execLines", **({"realm": realm} if realm else {}), "lines": lines}
                    await ws.send(json.dumps(payload)); continue

                if line.startswith("exec "):
                    _, *rest = line.split(" ")
                    if not rest:
                        print("usage: exec <realm?> <cmd...>"); continue
                    # если указан realm — он первый
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

    async with websockets.serve(ws_handler, args.host, args.port,
                                ping_interval=20, ping_timeout=20, max_size=131072):
        if args.repl:
            await repl(f"ws://127.0.0.1:{args.port}/ws", args.token, args.realm)
        else:
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
