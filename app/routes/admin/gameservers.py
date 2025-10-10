# app/routes/admin/gameservers.py
from __future__ import annotations

import os
import io
import json
import base64
import asyncio
import logging
import threading
import time
from functools import lru_cache
from queue import Queue, Empty
from typing import Optional, Dict, Any, Iterable

import requests
import websockets
from PIL import Image
from flask import render_template, jsonify, request, send_file, current_app

from ...decorators import login_required
from ...modules.bridge_client import (
    bridge_list, bridge_info, stats_query, console_exec, bridge_send,
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

# --- DB: stats storage (soft import with fallbacks) ---
try:
    # preferred names
    from ...database import (
        get_db_connection,
    )
    try:
        from ...database import (
            ensure_stats_schema,           # (conn) -> None
            stats_save_snapshot,           # (conn, realm, data) -> int snapshot_id
            stats_get_latest,              # (conn, realm) -> dict | None
            stats_get_series,              # (conn, realm, *, since_ts=None, limit=..., step_sec=None) -> list[dict]
        )
    except Exception:
        # alt names we might have used earlier
        from ...database import (
            init_stats_schema as ensure_stats_schema,            # type: ignore
            save_stats_snapshot as stats_save_snapshot,          # type: ignore
            get_stats_latest as stats_get_latest,                # type: ignore
            get_stats_series as stats_get_series,                # type: ignore
        )
except Exception:  # ultimate fallback when db module is absent
    get_db_connection = None  # type: ignore
    ensure_stats_schema = None  # type: ignore
    stats_save_snapshot = None  # type: ignore
    stats_get_latest = None  # type: ignore
    stats_get_series = None  # type: ignore

from . import admin_bp  # Blueprint всего админ-раздела

# ===================== HTML =====================

@admin_bp.route("/gameservers")
@login_required
def gameservers_index():
    return render_template("admin/gameservers/index.html")

@admin_bp.route("/gameservers/<realm>")
@login_required
def gameservers_section(realm: str):
    """
    Страница конкретного сервера.
    Данные для истории берутся из БД (/gameservers/api/stats/series).
    Если сервер офлайн — история доступна.
    """
    return render_template("admin/gameservers/section.html", realm=realm)

# ===================== helpers: client meta =====================

def _client_from_headers() -> Dict[str, Any]:
    h = request.headers
    # то, что шлёт фронт дополнительными заголовками
    tz = h.get("X-Client-TZ") or ""
    of = h.get("X-Client-Of") or ""
    lang = h.get("X-Client-Lang") or (h.get("Accept-Language") or "")
    return {
        "tz": tz,
        "tzOffsetMin": int(of) if (of and of.lstrip("+-").isdigit()) else None,
        "lang": lang.split(",")[0] if lang else "",
        "ua": h.get("User-Agent") or "",
        "ip": (h.get("X-Forwarded-For") or "").split(",")[0].strip() or request.remote_addr,
    }

def _client_from_query() -> Dict[str, Any]:
    q = request.args
    return {
        "tz": q.get("tz") or "",
        "tzOffsetMin": int(q.get("of")) if (q.get("of") and q.get("of").lstrip("+-").isdigit()) else None,
        "lang": (q.get("lang") or ""),
        "vp": (q.get("vp") or ""),  # "WxH@DPR"
        "sc": (q.get("sc") or ""),  # "WxH"
        "vis": (q.get("vis") or ""),
        "page": request.path,
        "ip": (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip() or request.remote_addr,
        "ua": request.headers.get("User-Agent") or "",
        "_source": "query",
    }

def _client_from_body(j: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    c = (j or {}).get("client") or {}
    # лёгкая нормализация
    return {
        "tz": c.get("tz") or "",
        "tzOffsetMin": c.get("tzOffsetMin"),
        "lang": c.get("lang") or "",
        "ua": c.get("ua") or (request.headers.get("User-Agent") or ""),
        "vp": c.get("vp"),
        "sc": c.get("sc"),
        "page": c.get("page") or request.path,
        "ref": c.get("ref") or "",
        "vis": c.get("vis") or "",
        "ts": c.get("ts"),
        "ip": (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip() or request.remote_addr,
        "_source": "body",
    }

def _client_meta(j: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Собираем метаданные клиента из body + заголовков (+ query для SSE)."""
    meta = _client_from_headers()
    if request.method == "GET" and request.args:
        meta.update({k: v for k, v in _client_from_query().items() if v not in (None, "", [])})
    if request.method == "POST":
        meta.update({k: v for k, v in _client_from_body(j).items() if v not in (None, "", [])})
    return meta

def _log_client(action: str, realm: str, meta: Dict[str, Any], extra: Optional[Dict[str, Any]] = None):
    try:
        current_app.logger.info(
            "client-meta action=%s realm=%s meta=%s extra=%s",
            action, realm, json.dumps(meta, ensure_ascii=False), json.dumps(extra or {}, ensure_ascii=False)
        )
    except Exception:
        pass

def _bridge_origin(action: str, realm: str, meta: Dict[str, Any], extra: Optional[Dict[str, Any]] = None):
    """Отправляем в бридж служебный кадр с происхождением действия (fire-and-forget)."""
    try:
        bridge_send({
            "type": "admin.origin",
            "realm": realm,
            "payload": {
                "realm": realm,
                "action": action,
                "client": meta,
                "extra": extra or {}
            }
        })
    except Exception:
        current_app.logger.exception("bridge admin.origin failed")

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
        # для совместимости: bridge_info() возвращает bridge.list.result
        if data.get("type") not in ("bridge.info.result", "bridge.list.result"):
            return jsonify({"ok": False, "error": data.get("error") or "bridge unavailable"}), 502
        return jsonify({"ok": True, "data": data.get("payload", {})})
    except Exception as e:
        current_app.logger.exception("bridge_info failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502

# --- server-side cache, чтобы не «мигал» список игроков между тяжёлыми/лёгкими кадрами
_LAST_STATS: Dict[str, Dict[str, Any]] = {}  # realm -> {"ts": float, "data": dict}
_CACHE_TTL = 30.0  # сек для «подмешивания» players_list/worlds из последнего heavy

def _merge_with_cache(realm: str, norm: Dict[str, Any]) -> Dict[str, Any]:
    """
    Если текущий нормализованный ответ пустой по players_list/worlds — попробуем дополнить из кэша
    (актуального по времени). Это устраняет «мигание» списка игроков.
    """
    now = time.time()
    cached = _LAST_STATS.get(realm) or {}

    def fresh(c) -> bool:
        return bool(c) and (now - c.get("ts", 0) <= _CACHE_TTL)

    out = dict(norm)

    # players_list fallback
    pl = (out.get("players_list") or [])
    online = ((out.get("players") or {}).get("online"))
    if (not pl) and isinstance(online, int) and online > 0 and fresh(cached):
        c_pl = (cached.get("data") or {}).get("players_list") or []
        if c_pl:
            out["players_list"] = c_pl

    # worlds fallback
    worlds = out.get("worlds") or []
    if (not worlds) and fresh(cached):
        c_worlds = (cached.get("data") or {}).get("worlds") or []
        if c_worlds:
            out["worlds"] = c_worlds

    # обновим кэш, если текущий ответ содержательнее
    has_heavy = bool(out.get("players_list")) or bool(out.get("worlds"))
    if has_heavy:
        _LAST_STATS[realm] = {"ts": now, "data": out}
    else:
        # если тяжёлого нет — не затираем хороший кэш, но обновим метрику players/heap и т.п.
        if fresh(cached):
            merged = dict(cached.get("data") or {})
            merged.update({k: v for k, v in out.items() if v not in (None, [], {})})
            _LAST_STATS[realm] = {"ts": cached.get("ts", now), "data": merged}
            out = merged

    return out

def _db_stats_enabled() -> bool:
    return bool(get_db_connection and stats_save_snapshot and stats_get_latest and stats_get_series)

def _try_save_stats_to_db(realm: str, norm: Dict[str, Any]) -> Optional[int]:
    """
    Пытаемся сохранить снимок статистики в БД (если функции доступны).
    Возвращает snapshot_id или None.
    """
    if not _db_stats_enabled():
        return None
    try:
        with get_db_connection() as conn:  # type: ignore[misc]
            if ensure_stats_schema:
                ensure_stats_schema(conn)  # type: ignore[misc]
            snap_id = stats_save_snapshot(conn, realm, norm)  # type: ignore[misc]
            conn.commit()
            return int(snap_id) if snap_id is not None else None
    except Exception as e:
        current_app.logger.warning("stats DB save failed realm=%s: %s", realm, e)
        try:
            conn.rollback()  # type: ignore[name-defined]
        except Exception:
            pass
        return None

@admin_bp.route("/gameservers/api/stats")
@login_required
def api_stats():
    realm = (request.args.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    try:
        raw = stats_query(realm)
        if raw.get("type") == "bridge.error":
            # при ошибке бриджа попробуем вернуть последний снимок из БД, если есть
            if _db_stats_enabled():
                try:
                    with get_db_connection() as conn:  # type: ignore[misc]
                        latest = stats_get_latest(conn, realm)  # type: ignore[misc]
                    if latest:
                        return jsonify({"ok": True, "data": latest, "source": "db-cache"}), 200
                except Exception:
                    pass
            return jsonify({"ok": False, "error": raw.get("error") or "bridge error"}), 502

        norm = normalize_server_stats(raw)
        # Подмешаем players_list/worlds из кэша при необходимости
        norm = _merge_with_cache(realm, norm)

        # Сохраняем слепок в БД (best-effort)
        snap_id = _try_save_stats_to_db(realm, norm)
        if snap_id is not None:
            norm = dict(norm)
            norm["_snapshot_id"] = snap_id

        return jsonify({"ok": True, "data": norm})
    except Exception as e:
        current_app.logger.exception("stats_query failed: %s", e)
        # fallback к БД, если возможно
        if _db_stats_enabled():
            try:
                with get_db_connection() as conn:  # type: ignore[misc]
                    latest = stats_get_latest(conn, realm)  # type: ignore[misc]
                if latest:
                    return jsonify({"ok": True, "data": latest, "source": "db-cache"}), 200
            except Exception:
                pass
        return jsonify({"ok": False, "error": "bridge error"}), 502

@admin_bp.route("/gameservers/api/stats/latest")
@login_required
def api_stats_latest_from_db():
    """
    Возвращает последний сохранённый снимок статистики по серверу из БД.
    """
    realm = (request.args.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    if not _db_stats_enabled():
        return jsonify({"ok": False, "error": "db stats are not configured"}), 501
    try:
        with get_db_connection() as conn:  # type: ignore[misc]
            if ensure_stats_schema:
                ensure_stats_schema(conn)  # type: ignore[misc]
            latest = stats_get_latest(conn, realm)  # type: ignore[misc]
        if not latest:
            return jsonify({"ok": True, "data": None})
        return jsonify({"ok": True, "data": latest})
    except Exception as e:
        current_app.logger.exception("stats_latest db failed: %s", e)
        return jsonify({"ok": False, "error": "db error"}), 500

@admin_bp.route("/gameservers/api/stats/series")
@login_required
def api_stats_series_from_db():
    """
    История метрик из БД для построения графиков / «кликабельных плиток».
    Параметры:
      realm: обязательный
      minutes: сколько минут назад (по умолчанию 180)
      step: шаг агрегации в секундах (например, 60/120/300). По умолчанию 60.
      limit: максимальное число точек (бэкап-ограничение), по умолчанию 720.
      fields: CSV из известных ключей (players_online, tps_1m, mspt, heap_used, cpu_system_load, cpu_process_load, etc)
    """
    realm = (request.args.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    if not _db_stats_enabled():
        return jsonify({"ok": False, "error": "db stats are not configured"}), 501

    def _int(v, dv):
        try:
            return int(v)
        except Exception:
            return dv

    minutes = _int(request.args.get("minutes"), 180)
    step = _int(request.args.get("step"), 60)
    limit = _int(request.args.get("limit"), 720)
    fields_raw = (request.args.get("fields") or "").strip()
    fields: Optional[Iterable[str]] = None
    if fields_raw:
        raw_fields = [f.strip() for f in fields_raw.split(",") if f.strip()]
        # Маппинг алиасов, которые может прислать фронт
        alias = {
            "cpu_system_load": "cpu_sys",
            "cpu_process_load": "cpu_proc",
        }
        fields = [alias.get(f, f) for f in raw_fields]

    since_ts = int(time.time() - minutes * 60)

    try:
        with get_db_connection() as conn:  # type: ignore[misc]
            if ensure_stats_schema:
                ensure_stats_schema(conn)  # type: ignore[misc]
            series = stats_get_series(  # type: ignore[misc]
                conn, realm, since_ts=since_ts, step_sec=step, limit=limit, fields=fields
            )
        return jsonify({"ok": True, "data": series})
    except Exception as e:
        current_app.logger.exception("stats_series db failed: %s", e)
        return jsonify({"ok": False, "error": "db error"}), 500

@admin_bp.route("/gameservers/api/console", methods=["POST"])
@login_required
def api_console():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    cmd = (j.get("cmd") or "").strip()
    if not realm or not cmd:
        return jsonify({"ok": False, "error": "realm and cmd required"}), 400
    meta = _client_meta(j)
    _log_client("console.exec", realm, meta, {"cmd_preview": cmd[:120]})
    _bridge_origin("console.exec", realm, meta, {"cmd_preview": cmd[:120]})
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
    meta = _client_meta(j)
    _log_client("maintenance.toggle", realm, meta, {"enabled": enabled})
    _bridge_origin("maintenance.toggle", realm, meta, {"enabled": enabled, "message": kick_message[:160]})
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
    op = (j.get("op") or j.get("action") or "add").strip().lower()

    # ---- normalize users ----
    player_single = (j.get("player") or j.get("user") or "").strip()
    raw_many = j.get("players") or j.get("users")

    if isinstance(raw_many, (list, tuple, set)):
        users = [str(x).strip() for x in raw_many if str(x or "").strip()]
    elif player_single:
        users = [player_single]
    else:
        users = []

    # ---- validation ----
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    if op not in ("add", "remove", "list"):
        return jsonify({"ok": False, "error": "op must be add|remove|list"}), 400
    if op in ("add", "remove") and not users:
        return jsonify({"ok": False, "error": "player or players required"}), 400

    meta = _client_meta(j)
    _log_client("maintenance.whitelist", realm, meta, {"op": op, "players": users})
    _bridge_origin("maintenance.whitelist", realm, meta, {"op": op, "players": users})

    try:
        # Для list пользователей передавать не нужно
        payload_users = None if op == "list" else users
        resp = maintenance_whitelist(realm, op, payload_users)

        # Если вдруг прокинулся прямой bridge.error — отдаём 502
        if isinstance(resp, dict) and resp.get("type") == "bridge.error":
            return jsonify({"ok": False, "error": resp.get("error") or "bridge error"}), 502

        ok = bool(resp.get("ok", True))
        return jsonify({"ok": ok, "data": resp})
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
    bio = io.BytesIO(); face.save(bio, format="PNG"); bio.seek(0); return bio

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
    Пропускаем кадры console.stream / bridge.log / console.out только для указанного realm.
    При подключении отправляем в бридж кадр admin.hello с данными клиента.
    """
    realm = (request.args.get("realm") or "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400

    BRIDGE_URL = os.getenv("SP_BRIDGE_URL", "ws://127.0.0.1:8765/ws")
    BRIDGE_TOKEN = os.getenv("SP_TOKEN", "SUPER_SECRET")
    BRIDGE_MAX_SIZE = int(os.getenv("SP_MAX_SIZE", "131072"))

    # метаданные клиента из query/headers
    client_meta = _client_meta()
    _log_client("console.stream.open", realm, client_meta)
    # продублируем в бридж отдельным кадром (fire-and-forget) на всякий случай
    _bridge_origin("console.stream.open", realm, client_meta)

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
                    # помечаем admin-сессию и передаём метаданные клиента
                    try:
                        await ws.send(json.dumps({
                            "type": "admin.hello",
                            "realm": realm,
                            "payload": {"realm": realm, "client": client_meta}
                        }, ensure_ascii=False))
                    except Exception:
                        pass

                    while True:
                        raw = await ws.recv()
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            continue
                        t = obj.get("type")
                        if t not in ("console.stream", "bridge.log", "console.out"):
                            continue
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
        yield "retry: 2000\n\n"
        try:
            while True:
                try:
                    item = q.get(timeout=20)
                except Empty:
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
    meta = _client_meta(j)
    _log_client("lp.web.open", realm, meta)
    _bridge_origin("lp.web.open", realm, meta)
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
    meta = _client_meta(j)
    _log_client("lp.web.apply", realm, meta)
    _bridge_origin("lp.web.apply", realm, meta, {"code_preview": code[:12]})
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
    meta = _client_meta(j)
    _log_client("lp.user.perm.add", realm, meta, {"user": user, "permission": permission, "value": value})
    _bridge_origin("lp.user.perm.add", realm, meta, {"user": user, "permission": permission, "value": value})
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
    meta = _client_meta(j)
    _log_client("lp.user.perm.remove", realm, meta, {"user": user, "permission": permission})
    _bridge_origin("lp.user.perm.remove", realm, meta, {"user": user, "permission": permission})
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
    meta = _client_meta(j)
    _log_client("lp.user.group.add", realm, meta, {"user": user, "group": group})
    _bridge_origin("lp.user.group.add", realm, meta, {"user": user, "group": group})
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
    meta = _client_meta(j)
    _log_client("lp.user.group.remove", realm, meta, {"user": user, "group": group})
    _bridge_origin("lp.user.group.remove", realm, meta, {"user": user, "group": group})
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
    meta = _client_meta(j)
    _log_client("lp.group.perm.add", realm, meta, {"group": group, "permission": permission, "value": value})
    _bridge_origin("lp.group.perm.add", realm, meta, {"group": group, "permission": permission, "value": value})
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
    meta = _client_meta(j)
    _log_client("lp.group.perm.remove", realm, meta, {"group": group, "permission": permission})
    _bridge_origin("lp.group.perm.remove", realm, meta, {"group": group, "permission": permission})
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
    meta = _client_meta()
    _log_client("lp.user.info", realm, meta, {"user": user})
    _bridge_origin("lp.user.info", realm, meta, {"user": user})
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
    meta = _client_meta()
    _log_client("lp.group.info", realm, meta, {"group": group})
    _bridge_origin("lp.group.info", realm, meta, {"group": group})
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
    meta = _client_meta()
    _log_client("jp.balance.get", realm, meta, {"user": user})
    _bridge_origin("jp.balance.get", realm, meta, {"user": user})
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
    meta = _client_meta(j)
    _log_client("jp.balance.set", realm, meta, {"user": user, "amount": amount})
    _bridge_origin("jp.balance.set", realm, meta, {"user": user, "amount": amount})
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
    meta = _client_meta(j)
    _log_client("jp.balance.add", realm, meta, {"user": user, "amount": amount})
    _bridge_origin("jp.balance.add", realm, meta, {"user": user, "amount": amount})
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
    meta = _client_meta(j)
    _log_client("jp.balance.take", realm, meta, {"user": user, "amount": amount})
    _bridge_origin("jp.balance.take", realm, meta, {"user": user, "amount": amount})
    try:
        data = jp_balance_take(realm, user, amount)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("jp_balance_take failed: %s", e)
        return jsonify({"ok": False, "error": "bridge error"}), 502
