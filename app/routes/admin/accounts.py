# panel/app/routes/admin/accounts.py
from __future__ import annotations

import os
from typing import Set, List, Optional

from flask import Blueprint, render_template, jsonify, request, current_app, session

from ...decorators import login_required
from ...database import get_authme_connection
from ...modules.luckperms_repo import (
    roles_for_uuids,            # быстрый фолбэк (primary_group)
    effective_roles_for_uuids,  # «живые» роли: контексты + веса
)

# --- points_repo может отсутствовать: даём заглушки, чтобы список аккаунтов грузился ---
try:
    from ...modules.points_repo import get_points_by_uuid, set_points_by_uuid  # type: ignore
except Exception:
    def get_points_by_uuid(_uuid: str, *, key: str = "rubs"):
        return None
    def set_points_by_uuid(_uuid: str, _new: float, *, key: str = "rubs"):
        raise RuntimeError("points_repo is missing; create app/modules/points_repo.py")

# --- онлайн-мост (тоже опционален) ---
try:
    from ...modules.bridge_client import BridgeClient  # type: ignore
except Exception:
    class BridgeClient:  # type: ignore
        def is_online(self, **_):
            return None

bp = Blueprint("accounts", __name__, url_prefix="/accounts")

POINTS_KEY = os.getenv("POINTS_KEY", "rubs").strip()
POINTS_EDITOR_NAME = os.getenv("HBusiwshu9whsd", "").strip()  # имя пользователя, кто может редактировать


# ---------- HTML ----------
@bp.route("/")
@login_required
def index():
    return render_template("admin/accounts/index.html")


# ---------- helpers ----------
def _current_schema(conn) -> Optional[str]:
    try:
        row = conn.query_one("SELECT DATABASE() AS db")
        return (row or {}).get("db")
    except Exception:
        return None


def _table_candidates() -> List[str]:
    """
    Порядок важен: сначала ENV, затем типичные варианты.
    Добавлены: mc_auth_accounts (ваша), authme/accounts/users (классика).
    """
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
    # 1) Надёжно через information_schema
    try:
        row = conn.query_one(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ? LIMIT 1",
            (db, name),
        )
        if row:
            return True
    except Exception:
        pass
    # 2) Фолбэк
    try:
        rows = conn.query_all("SHOW TABLES LIKE ?", (name,))
        return len(rows) > 0
    except Exception:
        return False


def _pick_table(conn) -> str | None:
    for t in _table_candidates():
        if _table_exists(conn, t):
            return t
    return None


def _cols(conn, table: str) -> Set[str]:
    try:
        rows = conn.query_all(f"SHOW COLUMNS FROM `{table}`")
        return {r["Field"] for r in rows}
    except Exception:
        return set()


def _first_present(cols: Set[str], *variants) -> str | None:
    for v in variants:
        if v and v in cols:
            return v
    return None


def _norm_ts(v):
    """int -> ms (если похоже на секунды). Иначе None."""
    try:
        t = int(v)
    except Exception:
        return None
    return t if t > 10_000_000_000 else t * 1000


def _dash(uuid_any: str) -> str:
    s = (uuid_any or "").replace("-", "").lower()
    if len(s) != 32:
        return (uuid_any or "").lower()
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


def _current_username() -> str:
    """Право редактирования по имени пользователя в сессии."""
    return (
        (session.get("username") or "").strip()
        or (session.get("user") or "").strip()
        or (session.get("login") or "").strip()
        or (session.get("name") or "").strip()
    )


def _can_edit_points() -> bool:
    u = _current_username()
    return bool(u and POINTS_EDITOR_NAME and u == POINTS_EDITOR_NAME)


def _map_columns(cols: Set[str]) -> dict:
    """Единый мэппинг под разные схемы AuthMe/кастомные."""
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
    """
    Точная выборка по uuid (приоритет) или имени. Возвращает унифицированный dict.
    """
    conn = get_authme_connection()
    table = _pick_table(conn)
    if not table:
        return None
    cols_set = _cols(conn, table)
    m = _map_columns(cols_set)

    row = None
    if uuid and m["uuid"]:
        row = conn.query_one(f"SELECT * FROM `{table}` WHERE `{m['uuid']}` = ? LIMIT 1", (_dash(uuid),))
    elif name and m["name"]:
        row = conn.query_one(f"SELECT * FROM `{table}` WHERE `{m['name']}` = ? LIMIT 1", (name,))
    if not row:
        return None

    def gv(key, default=None):
        c = m.get(key)
        return row.get(c) if c else default

    data = {
        "name": gv("name"),
        "uuid": _dash(gv("uuid") or (uuid or "")) or None,
        "email": gv("email"),
        "ip": gv("ip") or gv("lastip"),
        "regdate": _norm_ts(gv("regdate")),
        "lastlogin": _norm_ts(gv("lastlogin")),
        "premium": bool(gv("premium")) if m.get("premium") else None,
    }

    # роль: «живая» → primary → default
    try:
        u = data.get("uuid")
        role = None
        if u:
            role_map = effective_roles_for_uuids([u]) or roles_for_uuids([u])
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
    """
    GET /admin/accounts/api/search?q=nick&limit=50
    - q пустой -> «последние» записи (по lastlogin|regdate|id).
    - Автодетект схемы (mc_auth_accounts/authme/accounts/users).
    - Роли: сначала «живые» (LP контексты + веса), иначе primary_group.
    """
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

    # SELECT с алиасами, чтобы фронту было удобно
    select_parts = []
    if m["name"]:      select_parts.append(f"`{m['name']}` AS `name`")
    if m["uuid"]:      select_parts.append(f"`{m['uuid']}` AS `uuid`")
    if m["email"]:     select_parts.append(f"`{m['email']}` AS `email`")
    if m["ip"]:        select_parts.append(f"`{m['ip']}` AS `ip`")
    if m["lastip"]:    select_parts.append(f"`{m['lastip']}` AS `lastip`")
    if m["regdate"]:   select_parts.append(f"`{m['regdate']}` AS `regdate`")
    if m["lastlogin"]: select_parts.append(f"`{m['lastlogin']}` AS `lastlogin`")
    if m["premium"]:   select_parts.append(f"`{m['premium']}` AS `premium`")
    if not select_parts:
        select_parts = ["*"]

    # WHERE
    where = ""
    params: List[str] = []
    if q:
        like_fields = [c for c in (m["name"], m["uuid"], m["email"], m["ip"], m["lastip"]) if c]
        if like_fields:
            where = "WHERE " + " OR ".join(f"`{c}` LIKE ?" for c in like_fields)
            params = [f"%{q}%"] * len(like_fields)

    # ORDER BY — «самые свежие сверху»
    order_by = m["lastlogin"] or m["regdate"] or "id"
    sql = f"SELECT {', '.join(select_parts)} FROM `{table}` {where} ORDER BY `{order_by}` DESC LIMIT {limit}"

    try:
        rows = conn.query_all(sql, params)
    except Exception as e:
        current_app.logger.exception("Auth query failed: %s", e)
        return jsonify({"ok": False, "error": "Query failed"}), 500

    # нормализуем таймстемпы (в ms)
    for r in rows:
        if "lastlogin" in r and r["lastlogin"] is not None:
            r["lastlogin"] = _norm_ts(r["lastlogin"])
        if "regdate" in r and r["regdate"] is not None:
            r["regdate"] = _norm_ts(r["regdate"])

    # подтягиваем роли из LuckPerms
    uuids_dashed = [_dash(r.get("uuid") or "") for r in rows if r.get("uuid")]
    roles_map = {}
    try:
        roles_map = effective_roles_for_uuids(uuids_dashed) or {}
        if not roles_map:
            roles_map = roles_for_uuids(uuids_dashed) or {}
    except Exception as e:
        current_app.logger.warning("LP roles resolve failed, fallback to primary_group: %s", e)
        try:
            roles_map = roles_for_uuids(uuids_dashed) or {}
        except Exception:
            roles_map = {}

    for r in rows:
        u = _dash(r.get("uuid") or "")
        r["role"] = roles_map.get(u) or "default"

    return jsonify({"ok": True, "data": rows})


@bp.get("/api/details")
@login_required
def api_details():
    """Детальная карточка: account + role + points + online + can_edit."""
    uuid = request.args.get("uuid") or ""
    name = request.args.get("name") or ""
    acc = _fetch_single_account(uuid=uuid, name=name)
    if not acc:
        return jsonify({"ok": False, "error": "Account not found"}), 404

    # points (0.0 если записи нет)
    points = None
    try:
        if acc.get("uuid"):
            points = get_points_by_uuid(acc["uuid"], key=POINTS_KEY)
    except Exception as e:
        current_app.logger.warning("points fetch failed: %s", e)
        points = None
    if points is None:
        points = 0.0

    # online
    online = _online_status(acc.get("uuid"), acc.get("name"))

    return jsonify(
        {
            "ok": True,
            "data": {
                **acc,
                "points_key": POINTS_KEY,
                "points": points,
                "online": online,  # True/False/None
                "can_edit_points": _can_edit_points(),
            },
        }
    )


@bp.post("/api/points")
@login_required
def api_points_update():
    """Обновление баланса донатной валюты."""
    if not _can_edit_points():
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    js = request.get_json(silent=True) or {}
    uuid = js.get("uuid")
    key = (js.get("key") or POINTS_KEY).strip()
    points = js.get("points")

    if not uuid or points is None:
        return jsonify({"ok": False, "error": "uuid and points are required"}), 400

    try:
        new_value = set_points_by_uuid(_dash(uuid), float(points), key=key)
        return jsonify({"ok": True, "data": {"uuid": _dash(uuid), "key": key, "points": new_value}})
    except Exception as e:
        current_app.logger.exception("points update failed: %s", e)
        return jsonify({"ok": False, "error": "Points update failed"}), 500


# ---------- register under /admin ----------
from . import admin_bp
admin_bp.register_blueprint(bp)
