# bridge.py
# pip install websockets
import asyncio
import json
import argparse
import os
from urllib.parse import urlparse, parse_qs

import websockets

PLUGINS: dict[str, set] = {}   # realm -> set(ws)
ADMINS: set = set()            # admin connections


# ------------------ helpers ------------------

def _extract_token(ws, path: str) -> str:
    """Достаём токен из Authorization/X-Auth-Token/query ?token=."""
    h = ws.request_headers  # case-insensitive CIMultiDict
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
        try:
            q = parse_qs(urlparse(path).query)
            token = q.get("token", [""])[0]
        except Exception:
            pass
    return token or ""


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


async def route_to_realm(realm: str, msg: dict):
    if not realm_has_plugins(realm):
        print(f"[bridge] NO PLUGIN for realm='{realm}', drop: {msg.get('type')}")
        await broadcast_admin({"type": "bridge.warn", "payload": {
            "message": f"No plugin online for realm '{realm}'", "request": msg}})
        return
    data = json.dumps(msg, ensure_ascii=False)
    dead = []
    for ws in list(PLUGINS[realm]):
        try:
            await ws.send(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        PLUGINS[realm].discard(ws)


# ------------------ core handler ------------------

async def handler(ws, path, token_required: str):
    # auth
    provided = _extract_token(ws, path)
    if token_required and provided != token_required:
        print(f"[bridge] unauthorized from {ws.remote_address}. "
              f"Provided token len={len(provided)} matches={provided == token_required}")
        await ws.close(code=4401, reason="unauthorized")
        return

    role = "admin"
    realm = None
    print(f"[bridge] connect from {ws.remote_address}")

    try:
        # ждём первое сообщение максимум 10с
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            msg = json.loads(raw)
            print("[bridge] first frame:", msg)
        except Exception:
            msg = {}

        if isinstance(msg, dict) and msg.get("type") == "hello":
            realm = msg.get("realm") or "default"
            role = "plugin"
            PLUGINS.setdefault(realm, set()).add(ws)
            print(f"[bridge] plugin registered realm='{realm}'")
            await broadcast_admin({"type": "bridge.info", "payload": {
                "message": f"Plugin online realm='{realm}'"}})
        else:
            role = "admin"
            ADMINS.add(ws)
            print(f"[bridge] admin connected")
            if msg:
                await process_admin(ws, msg)

        # основной цикл
        async for raw in ws:
            try:
                obj = json.loads(raw)
            except Exception:
                print("[bridge] bad json:", raw)
                continue

            if role == "plugin":
                t = obj.get("type")
                print(f"[bridge] <- {t} from {realm}")
                if t in ("console.result", "stats.report", "maintenance.status", "bridge.log"):
                    # для наглядности распечатаем полезную нагрузку
                    try:
                        print(json.dumps(obj.get("payload", {}), ensure_ascii=False, indent=2))
                    except Exception:
                        pass
                    await broadcast_admin(obj)
                else:
                    await broadcast_admin({"type": "bridge.echo", "payload": obj})
            else:
                await process_admin(ws, obj)

    finally:
        if role == "plugin" and realm:
            PLUGINS.get(realm, set()).discard(ws)
            print(f"[bridge] plugin disconnected realm='{realm}'")
        elif role == "admin":
            ADMINS.discard(ws)
            print(f"[bridge] admin disconnected")


async def process_admin(ws, obj: dict):
    t = obj.get("type")
    p = obj.get("payload") or {}
    realm = obj.get("realm") or p.get("realm")

    if t in ("console.exec", "stats.query", "maintenance.set", "maintenance.whitelist"):
        if not realm:
            await ws.send(json.dumps({"type": "bridge.error", "payload": {"message": "realm required"}}))
            return
        await route_to_realm(realm, obj)
    elif t == "bridge.list":
        listing = {r: len(s) for r, s in PLUGINS.items()}
        await ws.send(json.dumps({"type": "bridge.list.result", "payload": listing}, ensure_ascii=False))
    else:
        await ws.send(json.dumps({"type": "bridge.ack", "payload": {"seenType": t}}))


# ------------------ REPL (по желанию) ------------------

async def repl(uri, token):
    async with websockets.connect(uri, extra_headers={"Authorization": f"Bearer {token}"}) as ws:
        print("REPL ready. Commands: list | exec <realm> <cmd...> | stats <realm> | maint <realm> on|off [msg]")
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
                if line.startswith("exec "):
                    _, realm, *cmd = line.split(" ")
                    await ws.send(json.dumps({"type": "console.exec", "realm": realm,
                                              "payload": {"realm": realm, "cmd": " ".join(cmd)}})); continue
                if line.startswith("stats "):
                    _, realm = line.split(" ", 1)
                    await ws.send(json.dumps({"type": "stats.query", "realm": realm})); continue
                if line.startswith("maint "):
                    _, realm, state, *msg = line.split(" ")
                    await ws.send(json.dumps({"type": "maintenance.set", "realm": realm,
                                              "payload": {"realm": realm, "enabled": state.lower() == "on",
                                                          "message": " ".join(msg)}})); continue
                print("unknown command")
        await asyncio.gather(reader(), writer())


# ------------------ main ------------------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("SP_BRIDGE_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.getenv("SP_BRIDGE_PORT", "8765")))
    ap.add_argument("--token", default=os.getenv("SP_TOKEN", "SUPER_SECRET"))
    ap.add_argument("--repl", action="store_true", help="run local REPL admin client")
    args = ap.parse_args()

    async def ws_handler(ws, path):
        if urlparse(path).path != "/ws":
            await ws.close(code=4404, reason="not_found"); return
        return await handler(ws, path, args.token)

    print(f"[bridge] starting ws server on ws://{args.host}:{args.port}/ws (token len={len(args.token)})")
    async with websockets.serve(ws_handler, args.host, args.port, ping_interval=20, ping_timeout=20):
        if args.repl:
            await repl(f"ws://127.0.0.1:{args.port}/ws", args.token)
        else:
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
