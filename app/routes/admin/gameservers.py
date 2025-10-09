# admin/gameservers.py
from __future__ import annotations

import os
import io
import json
import base64
import asyncio
import logging
import threading
from functools import lru_cache
from queue import Queue, Empty
from typing import Optional

import requests
import websockets
from PIL import Image
from flask import render_template, jsonify, request, send_file, current_app

from ...decorators import login_required
from ...modules.bridge_client import (
    bridge_list, bridge_info, stats_query, console_exec,
    maintenance_set, maintenance_whitelist, normalize_server_stats,
    # LuckPerms
    lp_web_open, lp_web_apply,
    lp_user_perm_add, lp_user_perm_remove,
    lp_user_group_add, lp_user_group_remove,
    lp_group_perm_add, lp_group_perm_remove,
    lp_user_info, lp_group_info,
    # JustPoints
    jp_balance_get, jp_balance_set, jp_balance_add, jp_balance_take,
)
from . import admin_bp  # существующий Blueprint всего админ-раздела

# ===================== HTML =====================

@admin_bp.route("/gameservers")
@login_required
def gameservers_index():
    return render_template("admin/gameservers/index.html")

@admin_bp.route("/gameservers/<realm>")
@login_required
def gameservers_section(realm: str):
    return render_template("admin/gameservers/section.html", realm=realm)

# ===================== API: list / stats / console =====================

@admin_bp.route("/gameservers/api/list")
@login_required
def api_list():
    try:
        data = bridge_list()
        if data.get("type") != "bridge.list.result":
            return jsonify({"ok": False, "error": data.get("error") or "bridge unavailable"}), 502
        return jsonify({"ok": True, "data": data.get("payload", {})})
    except Exception as e:
        current_app.logger.exception("bridge_list failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/bridge-info")
@login_required
def api_bridge_info():
    try:
        data = bridge_info()
        if data.get("type") != "bridge.info.result":
            return jsonify({"ok": False, "error": data.get("error") or "bridge unavailable"}), 502
        return jsonify({"ok": True, "data": data.get("payload", {})})
    except Exception as e:
        current_app.logger.exception("bridge_info failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/stats")
@login_required
def api_stats():
    realm = (request.args.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    try:
        raw = stats_query(realm)
        if raw.get("type") == "bridge.error":
            return jsonify({"ok": False, "error": raw.get("error") or "bridge error"}), 502
        norm = normalize_server_stats(raw)
        if norm.get("type") == "bridge.error":
            return jsonify({"ok": True, "data": raw})
        return jsonify({"ok": True, "data": norm})
    except Exception as e:
        current_app.logger.exception("stats_query failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/console", methods=["POST"])
@login_required
def api_console():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    cmd = (j.get("cmd") or "").strip()
    if not realm or not cmd:
        return jsonify({"ok": False, "error": "realm and cmd required"}), 400
    try:
        data = console_exec(realm, cmd)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("console_exec failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

# ===================== API: maintenance =====================

@admin_bp.route("/gameservers/api/maintenance", methods=["POST"])
@login_required
def api_maintenance_toggle():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    enabled = bool(j.get("enabled"))
    kick_message = (j.get("kickMessage") or j.get("message") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    try:
        ok = maintenance_set(realm, enabled, kick_message)
        if not ok:
            return jsonify({"ok": False, "error": "bridge send failed"}), 502
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.exception("maintenance_set failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/maintenance/whitelist", methods=["POST"])
@login_required
def api_maintenance_whitelist():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    op = (j.get("op") or "").strip().lower()
    player = (j.get("player") or "").strip()
    if not realm or op not in ("add", "remove") or not player:
        return jsonify({"ok": False, "error": "realm, op(add|remove) and player required"}), 400
    try:
        ok = maintenance_whitelist(realm, op, player)
        if not ok:
            return jsonify({"ok": False, "error": "bridge send failed"}), 502
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.exception("maintenance_whitelist failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

# ===================== Player head proxy =====================

REQ_TIMEOUT = (4, 6)
MOJANG_UUID_URL    = "https://api.mojang.com/users/profiles/minecraft/{name}"
MOJANG_PROFILE_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
CRAFATAR_AVATAR    = "https://crafatar.com/avatars/{uuid}?size=32&overlay"
CRAFATAR_SKIN      = "https://crafatar.com/skins/{uuid}"

def _http_get(url: str):
    headers = {"User-Agent": "MoonReinPanel/1.0 (+bridge)"}
    return requests.get(url, headers=headers, timeout=REQ_TIMEOUT)

@lru_cache(maxsize=512)
def _cached_uuid_for_name(name: str) -> Optional[str]:
    r = _http_get(MOJANG_UUID_URL.format(name=name))
    if r.status_code == 204:
        return None
    r.raise_for_status()
    return (r.json().get("id") or "").strip() or None

@lru_cache(maxsize=1024)
def _cached_skin_url_for_uuid(uuid_nodash: str) -> Optional[str]:
    r = _http_get(MOJANG_PROFILE_URL.format(uuid=uuid_nodash))
    if r.status_code == 204:
        return None
    r.raise_for_status()
    data = r.json()
    prop = next((p for p in data.get("properties", []) if p.get("name") == "textures"), None)
    if not prop:
        return None
    decoded = json.loads(base64.b64decode(prop["value"]).decode("utf-8"))
    return decoded.get("textures", {}).get("SKIN", {}).get("url")

def _compose_head_png_from_skin_url(url: str) -> io.BytesIO:
    r = _http_get(url); r.raise_for_status()
    img = Image.open(io.BytesIO(r.content)).convert("RGBA")
    face = img.crop((8,8,16,16)).resize((32,32), Image.NEAREST)
    try:
        hat = img.crop((40,8,48,16)).resize((32,32), Image.NEAREST)
        face.alpha_composite(hat)
    except Exception:
        pass
    buf = io.BytesIO(); face.save(buf, format="PNG"); buf.seek(0); return buf

def _compose_head_png_from_crafatar(uuid_nodash: str) -> io.BytesIO:
    r = _http_get(CRAFATAR_AVATAR.format(uuid=uuid_nodash))
    if r.ok: return io.BytesIO(r.content)
    r2 = _http_get(CRAFATAR_SKIN.format(uuid=uuid_nodash)); r2.raise_for_status()
    img = Image.open(io.BytesIO(r2.content)).convert("RGBA")
    face = img.crop((8,8,16,16)).resize((32,32), Image.NEAREST)
    try:
        hat = img.crop((40,8,48,16)).resize((32,32), Image.NEAREST)
        face.alpha_composite(hat)
    except Exception:
        pass
    buf = io.BytesIO(); face.save(buf, format="PNG"); buf.seek(0); return buf

@admin_bp.route("/gameservers/api/player-head")
@login_required
def api_player_head():
    name = (request.args.get("name") or "").strip()
    uuid = (request.args.get("uuid") or "").replace("-", "").strip()
    log = current_app.logger if current_app else logging.getLogger(__name__)
    if uuid and len(uuid) != 32:
        uuid = uuid.replace("-", "")
    if uuid and len(uuid) != 32:
        uuid = ""
    try:
        if not uuid and name:
            uuid = _cached_uuid_for_name(name) or ""
        if uuid:
            skin_url = _cached_skin_url_for_uuid(uuid)
            if skin_url:
                buf = _compose_head_png_from_skin_url(skin_url)
                return send_file(buf, mimetype="image/png", max_age=600)
    except Exception as e:
        log.warning("player-head Mojang fail (name=%s uuid=%s): %s", name, uuid, e)
    try:
        if uuid:
            buf = _compose_head_png_from_crafatar(uuid)
            return send_file(buf, mimetype="image/png", max_age=600)
    except Exception as e:
        log.warning("player-head Crafatar fail (uuid=%s): %s", uuid, e)
    img = Image.new("RGBA", (32,32), (60,75,92,255))
    bio = io.BytesIO(); img.save(bio, format="PNG"); bio.seek(0)
    return send_file(bio, mimetype="image/png", max_age=120)

# ===================== SSE: console stream =====================

@admin_bp.route("/gameservers/api/console/stream")
@login_required
def api_console_stream():
    """
    SSE-прокси: bridge (WS) -> браузер.
    Пропускаем кадры console.stream / bridge.log только для указанного realm.
    """
    realm = (request.args.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400

    BRIDGE_URL = os.getenv("SP_BRIDGE_URL", "ws://127.0.0.1:8765/ws")
    BRIDGE_TOKEN = os.getenv("SP_TOKEN", "SUPER_SECRET")
    BRIDGE_MAX_SIZE = int(os.getenv("SP_MAX_SIZE", "131072"))

    q: Queue = Queue(maxsize=256)
    STOP = object()

    def worker():
        async def run():
            headers = {"Authorization": f"Bearer {BRIDGE_TOKEN}"}
            try:
                async with websockets.connect(
                    BRIDGE_URL,
                    extra_headers=headers,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=BRIDGE_MAX_SIZE,
                ) as ws:
                    while True:
                        raw = await ws.recv()
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            continue
                        t = obj.get("type")
                        if t not in ("console.stream", "bridge.log", "console.out"):
                            continue
                        # realm может приезжать в корне, payload.realm или data.realm
                        r = (
                            obj.get("realm")
                            or (obj.get("payload") or {}).get("realm")
                            or (obj.get("data") or {}).get("realm")
                        )
                        if r != realm:
                            continue
                        payload = obj.get("payload") or obj
                        try:
                            q.put_nowait(payload)
                        except Exception:
                            pass
            except Exception as e:
                try:
                    q.put_nowait({"_err": str(e)})
                except Exception:
                    pass
            finally:
                try:
                    q.put_nowait(STOP)
                except Exception:
                    pass

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())
        loop.close()

    threading.Thread(target=worker, daemon=True).start()

    def gen():
        # рекомендуемое значение ретрая для SSE (милисекунды)
        yield "retry: 2000\n\n"
        try:
            while True:
                try:
                    item = q.get(timeout=20)
                except Empty:
                    # keep-alive комментарий, чтобы не рвались прокси
                    yield ": keepalive\n\n"
                    continue
                if item is STOP:
                    break
                if isinstance(item, dict) and "_err" in item:
                    yield f"event: err\ndata: {json.dumps(item, ensure_ascii=False)}\n\n"
                    continue
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
        except GeneratorExit:
            pass

    resp = current_app.response_class(gen(), mimetype="text/event-stream")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp

# ===================== LuckPerms API =====================

def _parse_bool(val) -> bool:
    if isinstance(val, bool):
        return val
    s = (str(val) or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")

@admin_bp.route("/gameservers/api/lp/web/open", methods=["POST"])
@login_required
def api_lp_web_open():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    try:
        data = lp_web_open(realm)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_web_open failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/web/apply", methods=["POST"])
@login_required
def api_lp_web_apply():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    code = (j.get("code") or "").strip()
    if not realm or not code:
        return jsonify({"ok": False, "error": "realm and code required"}), 400
    try:
        data = lp_web_apply(realm, code)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_web_apply failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/user/perm/add", methods=["POST"])
@login_required
def api_lp_user_perm_add():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    permission = (j.get("permission") or "").strip()
    value = _parse_bool(j.get("value", True))
    if not realm or not user or not permission:
        return jsonify({"ok": False, "error": "realm, user, permission required"}), 400
    try:
        data = lp_user_perm_add(realm, user, permission, value=value)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_user_perm_add failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/user/perm/remove", methods=["POST"])
@login_required
def api_lp_user_perm_remove():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    permission = (j.get("permission") or "").strip()
    if not realm or not user or not permission:
        return jsonify({"ok": False, "error": "realm, user, permission required"}), 400
    try:
        data = lp_user_perm_remove(realm, user, permission)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_user_perm_remove failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/user/group/add", methods=["POST"])
@login_required
def api_lp_user_group_add():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    group = (j.get("group") or "").strip()
    if not realm or not user or not group:
        return jsonify({"ok": False, "error": "realm, user, group required"}), 400
    try:
        data = lp_user_group_add(realm, user, group)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_user_group_add failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/user/group/remove", methods=["POST"])
@login_required
def api_lp_user_group_remove():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    group = (j.get("group") or "").strip()
    if not realm or not user or not group:
        return jsonify({"ok": False, "error": "realm, user, group required"}), 400
    try:
        data = lp_user_group_remove(realm, user, group)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_user_group_remove failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/group/perm/add", methods=["POST"])
@login_required
def api_lp_group_perm_add():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    group = (j.get("group") or "").strip()
    permission = (j.get("permission") or "").strip()
    value = _parse_bool(j.get("value", True))
    if not realm or not group or not permission:
        return jsonify({"ok": False, "error": "realm, group, permission required"}), 400
    try:
        data = lp_group_perm_add(realm, group, permission, value=value)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_group_perm_add failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/group/perm/remove", methods=["POST"])
@login_required
def api_lp_group_perm_remove():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    group = (j.get("group") or "").strip()
    permission = (j.get("permission") or "").strip()
    if not realm or not group or not permission:
        return jsonify({"ok": False, "error": "realm, group, permission required"}), 400
    try:
        data = lp_group_perm_remove(realm, group, permission)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_group_perm_remove failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/user/info")
@login_required
def api_lp_user_info():
    realm = (request.args.get("realm") or "").strip()
    user = (request.args.get("user") or "").strip()
    if not realm or not user:
        return jsonify({"ok": False, "error": "realm and user required"}), 400
    try:
        data = lp_user_info(realm, user)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_user_info failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/lp/group/info")
@login_required
def api_lp_group_info():
    realm = (request.args.get("realm") or "").strip()
    group = (request.args.get("group") or "").strip()
    if not realm or not group:
        return jsonify({"ok": False, "error": "realm and group required"}), 400
    try:
        data = lp_group_info(realm, group)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("lp_group_info failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

# ===================== JustPoints API =====================

@admin_bp.route("/gameservers/api/jp/balance")
@login_required
def api_jp_balance_get():
    realm = (request.args.get("realm") or "").strip()
    user = (request.args.get("user") or "").strip()
    if not realm or not user:
        return jsonify({"ok": False, "error": "realm and user required"}), 400
    try:
        data = jp_balance_get(realm, user)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("jp_balance_get failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

def _parse_amount(j) -> Optional[float]:
    try:
        return float(j.get("amount"))
    except Exception:
        return None

@admin_bp.route("/gameservers/api/jp/balance/set", methods=["POST"])
@login_required
def api_jp_balance_set():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    amount = _parse_amount(j)
    if not realm or not user or amount is None:
        return jsonify({"ok": False, "error": "realm, user, amount required"}), 400
    try:
        data = jp_balance_set(realm, user, amount)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("jp_balance_set failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/jp/balance/add", methods=["POST"])
@login_required
def api_jp_balance_add():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    amount = _parse_amount(j)
    if not realm or not user or amount is None:
        return jsonify({"ok": False, "error": "realm, user, amount required"}), 400
    try:
        data = jp_balance_add(realm, user, amount)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("jp_balance_add failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/jp/balance/take", methods=["POST"])
@login_required
def api_jp_balance_take():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    user = (j.get("user") or "").strip()
    amount = _parse_amount(j)
    if not realm or not user or amount is None:
        return jsonify({"ok": False, "error": "realm, user, amount required"}), 400
    try:
        data = jp_balance_take(realm, user, amount)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("jp_balance_take failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502
# ===================== HTML =====================

@admin_bp.route("/gameservers", endpoint="gameservers.index")
@login_required
def gameservers_index():
    return render_template("admin/gameservers/index.html")

@admin_bp.route("/gameservers/<realm>", endpoint="gameservers.section")
@login_required
def gameservers_section(realm: str):
    return render_template("admin/gameservers/section.html", realm=realm)
