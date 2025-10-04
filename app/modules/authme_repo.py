from __future__ import annotations

import os
from typing import Optional, Dict, Set

from ..database import get_authme_connection

# По умолчанию ваша таблица
AUTHME_TABLE = os.getenv("AUTHME_TABLE", "mc_auth_accounts").strip() or "mc_auth_accounts"

def _q(name: str) -> str:
    return f"`{name}`"

def _tbl() -> str:
    return _q(AUTHME_TABLE)

def _columns(conn) -> Set[str]:
    rows = conn.query_all(f"SHOW COLUMNS FROM {_tbl()}")
    return {r["Field"] for r in rows}

def _first_present(cols: Set[str], *variants: str) -> Optional[str]:
    for v in variants:
        if v and v in cols:
            return v
    return None

def _resolve_map(conn) -> Dict[str, Optional[str]]:
    """
    Подбираем названия колонок под mc_auth_accounts и типичные AuthMe.
    """
    cols = _columns(conn)
    name_col   = _first_present(cols, "player_name", "realname", "username", "name")
    uuid_col   = _first_present(cols, "unique_id", "uuid")
    lastip_col = _first_present(cols, "last_ip", "lastip", "ip")
    quit_col   = _first_present(cols, "last_quit", "lastlogin", "last_login")
    start_col  = _first_present(cols, "last_session_start", "regdate", "created_at")
    return {"name": name_col, "uuid": uuid_col, "lastip": lastip_col, "last_quit": quit_col, "last_start": start_col}

def _clean_uuid(u: str) -> str:
    return (u or "").replace("-", "").strip().lower()

def find_by_name(name: str) -> Optional[dict]:
    if not name: return None
    with get_authme_connection() as conn:
        m = _resolve_map(conn)
        if not m["name"]: return None
        return conn.query_one(f"SELECT * FROM {_tbl()} WHERE {_q(m['name'])} = ? LIMIT 1", (name,))

def find_by_uuid(uuid_nodash: str) -> Optional[dict]:
    u = _clean_uuid(uuid_nodash)
    if not u: return None
    with get_authme_connection() as conn:
        m = _resolve_map(conn)
        if not m["uuid"]: return None
        return conn.query_one(f"SELECT * FROM {_tbl()} WHERE {_q(m['uuid'])} = ? LIMIT 1", (u,))

def last_sessions(limit: int = 20) -> list[dict]:
    limit = max(1, int(limit))
    with get_authme_connection() as conn:
        m = _resolve_map(conn)
        select_parts = []
        if m["name"]:       select_parts.append(f"{_q(m['name'])} AS player_name")
        if m["uuid"]:       select_parts.append(f"{_q(m['uuid'])} AS unique_id")
        if m["lastip"]:     select_parts.append(f"{_q(m['lastip'])} AS last_ip")
        if m["last_quit"]:  select_parts.append(f"{_q(m['last_quit'])} AS last_quit")
        if m["last_start"]: select_parts.append(f"{_q(m['last_start'])} AS last_session_start")
        if not select_parts: select_parts = ["*"]
        order = m["last_start"] or m["last_quit"] or "id"
        sql = f"SELECT {', '.join(select_parts)} FROM {_tbl()} ORDER BY {_q(order)} DESC LIMIT {limit}"
        return conn.query_all(sql)
