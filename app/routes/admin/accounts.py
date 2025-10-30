# panel/app/routes/admin/accounts.py
from __future__ import annotations

import os
import time
from typing import Set, List, Optional, Dict, Any

from flask import Blueprint, render_template, jsonify, request, current_app, session

from ...decorators import login_required
from ...database import get_authme_connection, MySQLConnection

# --- LuckPerms roles (поддержка разных реализаций модуля) ---
try:
    from ...modules.luckperms_repo import effective_roles_for_uuids  # type: ignore
except Exception:
    effective_roles_for_uuids = None  # type: ignore

try:
    from ...modules.luckperms_repo import roles_for_uuids  # type: ignore
except Exception:
    try:
        from ...modules.luckperms_repo import roles_from_user_permissions as roles_for_uuids  # type: ignore
    except Exception:
        roles_for_uuids = None  # type: ignore

# --- points_repo может отсутствовать ---
try:
    from ...modules.points_repo import get_points_by_uuid, set_points_by_uuid  # type: ignore
except Exception:
    def get_points_by_uuid(_uuid: str, *, key: str = "rubs"):
        return None
    def set_points_by_uuid(_uuid: str, _new: float, *, key: str = "rubs"):
        raise RuntimeError("points_repo is missing; create app/modules/points_repo.py")

# --- EasyPayments (история донатов) ---
try:
    from ...modules.easypayments_repo import donations_by_uuid  # type: ignore
except Exception:
    def donations_by_uuid(_uuid: str, limit: int = 200):
        return []

# --- LiteBans ---
try:
    from ...modules.litebans_repo import (  # type: ignore
        is_banned, get_active_ban, get_bans, get_mutes, get_warnings, get_kicks, get_history
    )
    try:
        from ...modules.litebans_repo import _conn as _lb_conn  # type: ignore
    except Exception:
        _lb_conn = None
except Exception:
    def is_banned(_uuid: str) -> bool: return False
    def get_active_ban(_uuid: str): return None
    def get_bans(_uuid: str, limit: int = 50, include_inactive: bool = True): return []
    def get_mutes(_uuid: str, limit: int = 50, include_inactive: bool = True): return []
    def get_warnings(_uuid: str, limit: int = 50): return []
    def get_kicks(_uuid: str, limit: int = 50): return []
    def get_history(_uuid: str, limit: int = 100): return []
    _lb_conn = None

# --- онлайн-мост (опционален) ---
try:
    from ...modules.bridge_client import BridgeClient  # type: ignore
except Exception:
    class BridgeClient:  # type: ignore
        def is_online(self, **_):
            return None

bp = Blueprint("accounts", __name__, url_prefix="/accounts")

POINTS_KEY = (os.getenv("POINTS_KEY") or "rubs").strip()
POINTS_EDITOR_NAME = (os.getenv("POINTS_EDITOR_NAME") or os.getenv("HBusiwshu9whsd") or "").strip()
POINTS_EDIT_ALL = (os.getenv("POINTS_EDIT_ALL") or "0").lower() in ("1", "true", "yes")
POINTS_EDIT_ROLES = [x.strip().lower() for x in (os.getenv("POINTS_EDIT_ROLES") or "admin,superadmin").split(",") if x.strip()]

LB_SERVER_ORIGIN = os.getenv("LB_SERVER_ORIGIN", "panel")
LB_ACTOR_UUID_DEFAULT = os.getenv("LB_ACTOR_UUID", "00000000-0000-0000-0000-000000000000")

# --- AuthMe schema helpers ---
AUTHME_DB = (os.getenv("AUTHME_NAME") or os.getenv("AUTHME_DB") or "").strip(" `")

def _q(name: str) -> str:
    return f"`{name}`"

def _qtbl(table: str) -> str:
    if AUTHME_DB:
        return f"{_q(AUTHME_DB)}.{_q(table)}"
    return _q(table)

# ---------- HTML ----------
@bp.route("/")
@login_required
def index():
    return render_template("admin/accounts/index.html")

# ---------- helpers ----------
def _current_schema(conn) -> Optional[str]:
    try:
        row = conn.query_one("SELECT DATABASE() AS db")
        return (row or {}).get("db") or (AUTHME_DB or None)
    except Exception:
        return AUTHME_DB or None

def _table_candidates() -> List[str]:
    prefer = (os.environ.get("AUTHME_TABLE") or "").strip()
    base = ["mc_auth_accounts", "authme", "accounts", "users"]
    out: List[str] = []
    if prefer:
        out.append(prefer)
    for x in base:
        if x not in out:
            out.append(x)
    return out

def _table_exists(conn, name: str) -> bool:
    db = _current_schema(conn)
    try:
        row = conn.query_one(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1",
            (db, name),
        )
        if row:
            return True
    except Exception:
        pass
    try:
        rows = conn.query_all("SHOW TABLES LIKE ?", (name,))
        return len(rows) > 0
    except Exception:
        return False

def _pick_table(conn) -> Optional[str]:
    for t in _table_candidates():
        if _table_exists(conn, t):
            return t
    return None

def _cols(conn, table: str) -> Set[str]:
    try:
        rows = conn.query_all(f"SHOW COLUMNS FROM {_qtbl(table)}")
        cols: Set[str] = set()
        for r in rows:
            if isinstance(r, dict):
                cols.add((r.get("Field") or r.get("field") or "").strip())
            else:
                cols.add(str(r[0]).strip())
        return {c for c in cols if c}
    except Exception:
        return set()

def _first_present(cols: Set[str], *variants: str) -> Optional[str]:
    for v in variants:
        if v and v in cols:
            return v
    return None

def _norm_ts(v) -> Optional[int]:
    try:
        t = int(v)
    except Exception:
        return None
    return t if t > 10_000_000_000 else t * 1000  # сек → мс

def _dash(uuid_any: str) -> str:
    s = (uuid_any or "").replace("-", "").lower()
    if len(s) != 32:
        return (uuid_any or "").lower()
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"

# ---- auth helpers (из сессии) ----
def _session_str(*keys: str) -> str:
    for k in keys:
        v = session.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _session_bool(*keys: str) -> bool:
    for k in keys:
        v = session.get(k)
        if isinstance(v, bool) and v:
            return True
        if isinstance(v, (int,)) and v == 1:
            return True
        if isinstance(v, str) and v.lower() in ("1", "true", "yes", "on"):
            return True
    return False

def _current_username() -> str:
    return _session_str("username", "user", "login", "name", "nickname", "nick", "account", "account_name")

def _current_role_lower() -> str:
    return (_session_str("role", "user_role") or "").lower()

def _has_admin_flag() -> bool:
    if _session_bool("is_superadmin", "superadmin", "is_admin", "admin"):
        return True
    role = _current_role_lower()
    return role in set(POINTS_EDIT_ROLES)

def _can_edit_points() -> bool:
    if POINTS_EDIT_ALL:
        return True
    if _has_admin_flag():
        return True
    u = (_current_username() or "").lower()
    if POINTS_EDITOR_NAME and u and u == POINTS_EDITOR_NAME.lower():
        return True
    return False

def _can_ban() -> bool:
    return _has_admin_flag()

def _map_columns(cols: Set[str]) -> Dict[str, Optional[str]]:
    return {
        "name": _first_present(cols, "realname", "username", "player_name", "name"),
        "uuid": _first_present(cols, "uuid", "unique_id"),
        "email": _first_present(cols, "email"),
        "ip": _first_present(cols, "ip"),
        "lastip": _first_present(cols, "lastip", "last_ip"),
        "regdate": _first_present(cols, "regdate", "created_at", "created", "register_date"),
        "lastlogin": _first_present(cols, "lastlogin", "last_session_start", "last_quit", "last_login"),
        "premium": _first_present(cols, "isPremium", "premium", "mojang"),
        "id": _first_present(cols, "id"),
    }

def _fetch_single_account(*, uuid: Optional[str] = None, name: Optional[str] = None) -> Optional[dict]:
    conn = get_authme_connection()
    table = _pick_table(conn)
    if not table:
        return None
    cols_set = _cols(conn, table)
    m = _map_columns(cols_set)

    row: Optional[Dict[str, Any]] = None
    if uuid and m["uuid"]:
        row = conn.query_one(f"SELECT * FROM {_qtbl(table)} WHERE {_q(m['uuid'])} = ? LIMIT 1", (_dash(uuid),))
    elif name and m["name"]:
        row = conn.query_one(f"SELECT * FROM {_qtbl(table)} WHERE {_q(m['name'])} = ? LIMIT 1", (name,))
    if not row:
        return None

    def gv(key: str, default=None):
        c = m.get(key)
        return row.get(c) if c else default

    data: Dict[str, Any] = {
        "name": gv("name"),
        "uuid": _dash(gv("uuid") or (uuid or "")) or None,
        "email": gv("email"),
        "ip": gv("ip") or gv("lastip"),
        "regdate": _norm_ts(gv("regdate")),
        "lastlogin": _norm_ts(gv("lastlogin")),
        "premium": bool(gv("premium")) if m.get("premium") else None,
    }

    try:
        u = data.get("uuid")
        role = None
        if u:
            role_map: Dict[str, str] = {}
            if effective_roles_for_uuids:
                try:
                    role_map = effective_roles_for_uuids([u]) or {}  # type: ignore
                except Exception:
                    role_map = {}
            if not role_map and roles_for_uuids:
                try:
                    role_map = roles_for_uuids([u]) or {}  # type: ignore
                except Exception:
                    role_map = {}
            role = role_map.get(u) if role_map else None
        data["role"] = role or "default"
    except Exception:
        data["role"] = "default"

    return data

def _online_status(uuid: Optional[str], name: Optional[str]) -> Optional[bool]:
    try:
        bc = BridgeClient()
        if uuid:
            return bool(bc.is_online(uuid=uuid))
        if name:
            return bool(bc.is_online(name=name))
    except Exception:
        return None
    return None

# ---------- API ----------
@bp.get("/api/search")
@login_required
def api_search():
    q = (request.args.get("q") or "").strip()
    limit = min(max(int(request.args.get("limit") or 50), 1), 200)

    try:
        conn = get_authme_connection()
    except Exception as e:
        current_app.logger.warning("AUTHME connect failed: %s", e)
        return jsonify({"ok": False, "error": f"Auth DB connection failed: {e}"}), 503

    table = _pick_table(conn)
    if not table:
        return jsonify({"ok": False, "error": "No suitable Auth table found (try AUTHME_TABLE=mc_auth_accounts)"}), 404

    cols = _cols(conn, table)
    if not cols:
        return jsonify({"ok": False, "error": f"Cannot read columns of `{table}`"}), 500

    m = _map_columns(cols)

    select_parts: List[str] = []
    if m["name"]:      select_parts.append(f"{_q(m['name'])} AS {_q('name')}")
    if m["uuid"]:      select_parts.append(f"{_q(m['uuid'])} AS {_q('uuid')}")
    if m["email"]:     select_parts.append(f"{_q(m['email'])} AS {_q('email')}")
    if m["ip"]:        select_parts.append(f"{_q(m['ip'])} AS {_q('ip')}")
    if m["lastip"]:    select_parts.append(f"{_q(m['lastip'])} AS {_q('lastip')}")
    if m["regdate"]:   select_parts.append(f"{_q(m['regdate'])} AS {_q('regdate')}")
    if m["lastlogin"]: select_parts.append(f"{_q(m['lastlogin'])} AS {_q('lastlogin')}")
    if m["premium"]:   select_parts.append(f"{_q(m['premium'])} AS {_q('premium')}")
    if not select_parts:
        select_parts = ["*"]

    where = ""
    params: List[str] = []
    if q:
        like_fields = [c for c in (m["name"], m["uuid"], m["email"], m["ip"], m["lastip"]) if c]
        if like_fields:
            where = "WHERE " + " OR ".join(f"{_q(c)} LIKE ?" for c in like_fields)
            params = [f"%{q}%"] * len(like_fields)

    order_candidates = [m["lastlogin"], m["regdate"], m["id"], m["uuid"], m["name"]]
    order_by = next((x for x in order_candidates if x), None)

    sql = f"SELECT {', '.join(select_parts)} FROM {_qtbl(table)} {where}"
    if order_by:
        sql += f" ORDER BY {_q(order_by)} DESC"
    sql += f" LIMIT {int(limit)}"  # LIMIT как литерал (только после валидации int!)

    try:
        rows = conn.query_all(sql, params)
    except Exception as e:
        current_app.logger.exception("Auth query failed: %s", e)
        return jsonify({"ok": False, "error": "Query failed"}), 500

    for r in rows:
        if "lastlogin" in r and r["lastlogin"] is not None:
            r["lastlogin"] = _norm_ts(r["lastlogin"])
        if "regdate" in r and r["regdate"] is not None:
            r["regdate"] = _norm_ts(r["regdate"])

    uuids_dashed = [_dash(r.get("uuid") or "") for r in rows if r.get("uuid")]
    roles_map: Dict[str, str] = {}
    try:
        if effective_roles_for_uuids:
            roles_map = effective_roles_for_uuids(uuids_dashed) or {}  # type: ignore
        if not roles_map and roles_for_uuids:
            roles_map = roles_for_uuids(uuids_dashed) or {}  # type: ignore
    except Exception as e:
        current_app.logger.warning("LP roles resolve failed, fallback: %s", e)
        try:
            if roles_for_uuids:
                roles_map = roles_for_uuids(uuids_dashed) or {}  # type: ignore
        except Exception:
            roles_map = {}

    for r in rows:
        u = _dash(r.get("uuid") or "")
        r["role"] = roles_map.get(u) or "default"

    return jsonify({"ok": True, "data": rows})

@bp.get("/api/details")
@login_required
def api_details():
    uuid = request.args.get("uuid") or ""
    name = request.args.get("name") or ""
    acc = _fetch_single_account(uuid=uuid, name=name)
    if not acc:
        return jsonify({"ok": False, "error": "Account not found"}), 404

    points: Optional[float] = None
    try:
        if acc.get("uuid"):
            points = get_points_by_uuid(acc["uuid"], key=POINTS_KEY)
    except Exception as e:
        current_app.logger.warning("points fetch failed: %s", e)
        points = None
    if points is None:
        points = 0.0

    online = _online_status(acc.get("uuid"), acc.get("name"))

    donations = []
    try:
        if acc.get("uuid"):
            donations = donations_by_uuid(acc["uuid"], limit=200) or []
    except Exception as e:
        current_app.logger.warning("donations fetch failed: %s", e)
        donations = []

    lb_active = None
    lb_is_banned = False
    try:
        if acc.get("uuid"):
            lb_is_banned = bool(is_banned(acc["uuid"]))
            lb_active = get_active_ban(acc["uuid"]) if lb_is_banned else None
    except Exception as e:
        current_app.logger.warning("litebans fetch failed: %s", e)
        lb_active = None
        lb_is_banned = False

    return jsonify({
        "ok": True,
        "data": {
            **acc,
            "points_key": POINTS_KEY,
            "points": points,
            "online": online,
            "can_edit_points": _can_edit_points(),
            "can_ban": _can_ban(),
            "ban": {"is_banned": lb_is_banned, "active": lb_active},
            "donations": donations,
        },
    })

# ---------------- LiteBans: бан/разбан ----------------
def _litebans_conn() -> Optional[MySQLConnection]:
    if _lb_conn is None:
        return None
    try:
        return _lb_conn()  # type: ignore
    except Exception as e:
        current_app.logger.warning("litebans connect failed: %s", e)
        return None

@bp.post("/api/ban")
@login_required
def api_ban():
    if not _can_ban():
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    js = request.get_json(silent=True) or {}
    uuid = (_dash(js.get("uuid") or "") or "").strip()
    reason = (js.get("reason") or "Banned by an operator").strip()
    duration_seconds = int(js.get("duration_seconds") or 0)
    silent = bool(js.get("silent") or False)
    ipban = bool(js.get("ipban") or False)

    if not uuid or len(uuid) < 32:
        return jsonify({"ok": False, "error": "uuid is required"}), 400

    try:
        if is_banned(uuid):
            return jsonify({"ok": True, "data": {"already": True, "active": get_active_ban(uuid)}})
    except Exception:
        pass

    conn = _litebans_conn()
    if conn is None:
        return jsonify({"ok": False, "error": "LiteBans connection not available"}), 500

    now_ms = int(time.time() * 1000)
    until_ms = 0 if duration_seconds <= 0 else now_ms + duration_seconds * 1000
    actor_name = _current_username() or "CONSOLE"
    actor_uuid = LB_ACTOR_UUID_DEFAULT

    try:
        conn.execute(
            "INSERT INTO `litebans_bans` "
            "(`uuid`,`ip`,`reason`,`banned_by_uuid`,`banned_by_name`,`removed_by_uuid`,`removed_by_name`,"
            " `removed_by_reason`,`removed_by_date`,`time`,`until`,`template`,`server_scope`,`server_origin`,"
            " `silent`,`ipban`,`ipban_wildcard`,`active`) "
            "VALUES (?,?,?,?,?,NULL,NULL,NULL,NULL,?,?,?,NULL,?, ?, ?, ?, 1)",
            (
                uuid, None, reason, actor_uuid, actor_name,
                now_ms, until_ms, 255, LB_SERVER_ORIGIN,
                1 if silent else 0, 1 if ipban else 0, 0,
            ),
        )
        conn.commit()
        active = get_active_ban(uuid)
        return jsonify({"ok": True, "data": {"banned": True, "active": active}})
    except Exception as e:
        current_app.logger.exception("litebans ban insert failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "Ban failed"}), 500

@bp.post("/api/unban")
@login_required
def api_unban():
    if not _can_ban():
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    js = request.get_json(silent=True) or {}
    uuid = (_dash(js.get("uuid") or "") or "").strip()
    reason = (js.get("reason") or "Unbanned by an operator").strip()

    if not uuid or len(uuid) < 32:
        return jsonify({"ok": False, "error": "uuid is required"}), 400

    conn = _litebans_conn()
    if conn is None:
        return jsonify({"ok": False, "error": "LiteBans connection not available"}), 500

    actor_name = _current_username() or "CONSOLE"
    actor_uuid = LB_ACTOR_UUID_DEFAULT

    try:
        conn.execute(
            "UPDATE `litebans_bans` "
            "SET `active` = 0, `removed_by_uuid` = ?, `removed_by_name` = ?, "
            "    `removed_by_reason` = ?, `removed_by_date` = NOW() "
            "WHERE `uuid` = ? AND `active` = 1",
            (actor_uuid, actor_name, reason, uuid),
        )
        conn.commit()
        return jsonify({"ok": True, "data": {"unbanned": True, "is_banned": bool(is_banned(uuid))}})
    except Exception as e:
        current_app.logger.exception("litebans unban failed: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "Unban failed"}), 500

# ---------- register under /admin ----------
from . import admin_bp
admin_bp.register_blueprint(bp)
