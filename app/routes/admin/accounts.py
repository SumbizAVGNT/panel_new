# panel/app/routes/admin/accounts.py
from __future__ import annotations

import os
from typing import Set, List, Optional

from flask import Blueprint, render_template, jsonify, request, current_app

from ...decorators import login_required
from ...database import get_authme_connection
from ...modules.luckperms_repo import (
    roles_for_uuids,            # быстрый фолбэк (primary_group)
    effective_roles_for_uuids,  # «живые» роли: контексты + веса
)

bp = Blueprint("accounts", __name__, url_prefix="/accounts")


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


# ---------- API ----------
@bp.get("/api/search")
@login_required
def api_search():
    """
    GET /admin/accounts/api/search?q=nick&limit=50
    - q пустой -> «последние» записи (по lastlogin|regdate|id).
    - Автодетект схемы (mc_auth_accounts/authme/accounts/users).
    - Роли: сначала пробуем «живые» (LuckPerms контексты + веса),
      если что-то пошло не так — берём primary_group.
    """
    q = (request.args.get("q") or "").strip()
    limit = min(max(int(request.args.get("limit") or 50), 1), 200)

    # подключаемся к БД авторизации
    try:
        conn = get_authme_connection()
    except Exception as e:
        current_app.logger.warning("AUTHME connect failed: %s", e)
        return jsonify({"ok": False, "error": f"Auth DB connection failed: {e}"}), 503

    # выбираем таблицу
    table = _pick_table(conn)
    if not table:
        return jsonify({"ok": False, "error": "No suitable Auth table found (try AUTHME_TABLE=mc_auth_accounts)"}), 404

    cols = _cols(conn, table)
    if not cols:
        return jsonify({"ok": False, "error": f"Cannot read columns of `{table}`"}), 500

    # мэппинг колонок под разные схемы
    col_name      = _first_present(cols, "realname", "username", "player_name", "name")
    col_uuid      = _first_present(cols, "uuid", "unique_id")
    col_email     = _first_present(cols, "email")
    col_ip        = _first_present(cols, "ip")               # некоторым AuthMe хватает одного ip
    col_lastip    = _first_present(cols, "lastip", "last_ip")
    col_reg       = _first_present(cols, "regdate", "created_at", "created", "register_date")
    col_lastlogin = _first_present(cols, "lastlogin", "last_session_start", "last_quit", "last_login")
    col_premium   = _first_present(cols, "isPremium", "premium", "mojang")

    # SELECT с алиасами, чтобы фронту было удобно
    select_parts = []
    if col_name:      select_parts.append(f"`{col_name}` AS `name`")
    if col_uuid:      select_parts.append(f"`{col_uuid}` AS `uuid`")
    if col_email:     select_parts.append(f"`{col_email}` AS `email`")
    if col_ip:        select_parts.append(f"`{col_ip}` AS `ip`")
    if col_lastip:    select_parts.append(f"`{col_lastip}` AS `lastip`")
    if col_reg:       select_parts.append(f"`{col_reg}` AS `regdate`")
    if col_lastlogin: select_parts.append(f"`{col_lastlogin}` AS `lastlogin`")
    if col_premium:   select_parts.append(f"`{col_premium}` AS `premium`")
    if not select_parts:
        select_parts = ["*"]

    # WHERE по нескольким полям
    where = ""
    params: List[str] = []
    if q:
        like_fields = [c for c in (col_name, col_uuid, col_email, col_ip, col_lastip) if c]
        if like_fields:
            where = "WHERE " + " OR ".join(f"`{c}` LIKE ?" for c in like_fields)
            params = [f"%{q}%"] * len(like_fields)

    # ORDER BY — «самые свежие сверху»
    order_by = col_lastlogin or col_reg or "id"
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
    uuids_dashed = [ _dash(r.get("uuid") or "") for r in rows if r.get("uuid") ]
    roles_map = {}
    try:
        # предпочитаем «живые» роли (контексты + веса)
        roles_map = effective_roles_for_uuids(uuids_dashed)
        if not roles_map:
            roles_map = roles_for_uuids(uuids_dashed)
    except Exception as e:
        current_app.logger.warning("LP roles resolve failed, fallback to primary_group: %s", e)
        try:
            roles_map = roles_for_uuids(uuids_dashed)
        except Exception:
            roles_map = {}

    for r in rows:
        u = _dash(r.get("uuid") or "")
        r["role"] = roles_map.get(u) or "default"

    return jsonify({"ok": True, "data": rows})


# ---------- register under /admin ----------
from . import admin_bp
admin_bp.register_blueprint(bp)
