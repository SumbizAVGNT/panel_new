# app/modules/litebans_repo.py
from __future__ import annotations
from typing import Optional, List, Dict, Any, Callable
import os
import pymysql

# why: используем общую обёртку для совместимости с '?' плейсхолдерами и DictCursor
from ..database import MySQLConnection


# ---- connection -------------------------------------------------------------
def _bool_env(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    v = str(val).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def _conn() -> MySQLConnection:
    """
    Отдельное подключение к LiteBans.
    why: LiteBans часто лежит в своей БД; берём LITEBANS_* либо падаем на DB_*.
    """
    host = os.getenv("LITEBANS_HOST", os.getenv("DB_HOST", "127.0.0.1"))
    port = int(os.getenv("LITEBANS_PORT", os.getenv("DB_PORT", "3306")))
    user = os.getenv("LITEBANS_USER", os.getenv("DB_USER", "root"))
    password = os.getenv("LITEBANS_PASSWORD", os.getenv("DB_PASSWORD", ""))
    # по дампу у тебя имя БД: litebansBD
    database = os.getenv("LITEBANS_NAME", "litebansBD")

    use_ssl = _bool_env(os.getenv("LITEBANS_SSL", os.getenv("DB_SSL", "0")))
    ssl_ca = os.getenv("LITEBANS_SSL_CA", os.getenv("DB_SSL_CA"))

    kwargs = dict(
        host=host, port=port, user=user, password=password, database=database,
        charset="utf8mb4", autocommit=False, cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10, read_timeout=30, write_timeout=30
    )
    if use_ssl:
        kwargs["ssl"] = {"ca": ssl_ca} if (ssl_ca and str(ssl_ca).strip()) else {}
    return MySQLConnection(pymysql.connect(**kwargs))


# ---- helpers ---------------------------------------------------------------
def _bit_to_bool(v: Any) -> Optional[bool]:
    """why: PyMySQL BIT(1) → bytes; приводим к bool безопасно."""
    if v is None:
        return None
    if isinstance(v, (bool, int)):
        return bool(v)
    if isinstance(v, (bytes, bytearray)):
        return bool(int.from_bytes(v, "big"))
    s = str(v).strip()
    if s in {"1", "true", "True", "b'\\x01'"}:
        return True
    if s in {"0", "false", "False", "b'\\x00'"}:
        return False
    return None

def _as_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _row_ban(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r.get("id"),
        "uuid": r.get("uuid"),
        "ip": r.get("ip"),
        "reason": r.get("reason"),
        "actor_uuid": r.get("banned_by_uuid"),
        "actor_name": r.get("banned_by_name"),
        "removed_by_uuid": r.get("removed_by_uuid"),
        "removed_by_name": r.get("removed_by_name"),
        "removed_by_reason": r.get("removed_by_reason"),
        "removed_by_date": r.get("removed_by_date").isoformat() if r.get("removed_by_date") else None,
        "time_ms": _as_int(r.get("time")),
        "until_ms": _as_int(r.get("until")),
        "server_scope": r.get("server_scope"),
        "server_origin": r.get("server_origin"),
        "silent": _bit_to_bool(r.get("silent")),
        "ipban": _bit_to_bool(r.get("ipban")),
        "ipban_wildcard": _bit_to_bool(r.get("ipban_wildcard")),
        "active": _bit_to_bool(r.get("active")),
    }

def _row_mute(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r.get("id"),
        "uuid": r.get("uuid"),
        "ip": r.get("ip"),
        "reason": r.get("reason"),
        "actor_uuid": r.get("banned_by_uuid"),
        "actor_name": r.get("banned_by_name"),
        "removed_by_uuid": r.get("removed_by_uuid"),
        "removed_by_name": r.get("removed_by_name"),
        "removed_by_reason": r.get("removed_by_reason"),
        "removed_by_date": r.get("removed_by_date").isoformat() if r.get("removed_by_date") else None,
        "time_ms": _as_int(r.get("time")),
        "until_ms": _as_int(r.get("until")),
        "server_scope": r.get("server_scope"),
        "server_origin": r.get("server_origin"),
        "silent": _bit_to_bool(r.get("silent")),
        "ipban": _bit_to_bool(r.get("ipban")),
        "ipban_wildcard": _bit_to_bool(r.get("ipban_wildcard")),
        "active": _bit_to_bool(r.get("active")),
    }

def _row_warn(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r.get("id"),
        "uuid": r.get("uuid"),
        "ip": r.get("ip"),
        "reason": r.get("reason"),
        "actor_uuid": r.get("banned_by_uuid"),
        "actor_name": r.get("banned_by_name"),
        "removed_by_uuid": r.get("removed_by_uuid"),
        "removed_by_name": r.get("removed_by_name"),
        "removed_by_reason": r.get("removed_by_reason"),
        "removed_by_date": r.get("removed_by_date").isoformat() if r.get("removed_by_date") else None,
        "time_ms": _as_int(r.get("time")),
        "until_ms": _as_int(r.get("until")),
        "server_scope": r.get("server_scope"),
        "server_origin": r.get("server_origin"),
        "silent": _bit_to_bool(r.get("silent")),
        "ipban": _bit_to_bool(r.get("ipban")),
        "ipban_wildcard": _bit_to_bool(r.get("ipban_wildcard")),
        "active": _bit_to_bool(r.get("active")),
        "warned": _bit_to_bool(r.get("warned")),
    }

def _row_kick(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r.get("id"),
        "uuid": r.get("uuid"),
        "ip": r.get("ip"),
        "reason": r.get("reason"),
        "actor_uuid": r.get("banned_by_uuid"),
        "actor_name": r.get("banned_by_name"),
        "time_ms": _as_int(r.get("time")),
        "until_ms": _as_int(r.get("until")),
        "server_scope": r.get("server_scope"),
        "server_origin": r.get("server_origin"),
        "silent": _bit_to_bool(r.get("silent")),
        "ipban": _bit_to_bool(r.get("ipban")),
        "ipban_wildcard": _bit_to_bool(r.get("ipban_wildcard")),
        "active": _bit_to_bool(r.get("active")),
    }

def _row_hist(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": r.get("id"),
        "date": r.get("date").isoformat() if r.get("date") else None,
        "name": r.get("name"),
        "uuid": r.get("uuid"),
        "ip": r.get("ip"),
    }

def _select_many(sql: str, params: tuple, mapper: Callable[[Dict[str, Any]], Dict[str, Any]]) -> List[Dict[str, Any]]:
    with _conn() as conn:
        rows = conn.query_all(sql, params)
        return [mapper(r) for r in rows]

def _select_one(sql: str, params: tuple, mapper: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    with _conn() as conn:
        r = conn.query_one(sql, params)
        return mapper(r) if r else None


# ---- public API ------------------------------------------------------------
def is_banned(uuid: str) -> bool:
    """Активный бан по uuid."""
    b = get_active_ban(uuid)
    return bool(b and b.get("active") is True)

def is_muted(uuid: str) -> bool:
    """Активный мут по uuid."""
    m = get_active_mute(uuid)
    return bool(m and m.get("active") is True)

def get_active_ban(uuid: str) -> Optional[Dict[str, Any]]:
    sql = (
        "SELECT * FROM `litebans_bans` "
        "WHERE `uuid` = ? AND `active` = 1 "
        "ORDER BY `id` DESC LIMIT 1"
    )
    return _select_one(sql, (uuid,), _row_ban)

def get_active_mute(uuid: str) -> Optional[Dict[str, Any]]:
    sql = (
        "SELECT * FROM `litebans_mutes` "
        "WHERE `uuid` = ? AND `active` = 1 "
        "ORDER BY `id` DESC LIMIT 1"
    )
    return _select_one(sql, (uuid,), _row_mute)

def get_bans(uuid: str, limit: int = 50, include_inactive: bool = True) -> List[Dict[str, Any]]:
    where = "`uuid` = ?" + ("" if include_inactive else " AND `active` = 1")
    sql = f"SELECT * FROM `litebans_bans` WHERE {where} ORDER BY `time` DESC LIMIT ?"
    return _select_many(sql, (uuid, limit), _row_ban)

def get_mutes(uuid: str, limit: int = 50, include_inactive: bool = True) -> List[Dict[str, Any]]:
    where = "`uuid` = ?" + ("" if include_inactive else " AND `active` = 1")
    sql = f"SELECT * FROM `litebans_mutes` WHERE {where} ORDER BY `time` DESC LIMIT ?"
    return _select_many(sql, (uuid, limit), _row_mute)

def get_warnings(uuid: str, limit: int = 50) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM `litebans_warnings` WHERE `uuid` = ? ORDER BY `time` DESC LIMIT ?"
    return _select_many(sql, (uuid, limit), _row_warn)

def get_kicks(uuid: str, limit: int = 50) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM `litebans_kicks` WHERE `uuid` = ? ORDER BY `time` DESC LIMIT ?"
    return _select_many(sql, (uuid, limit), _row_kick)

def get_history(uuid: str, limit: int = 100) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM `litebans_history` WHERE `uuid` = ? ORDER BY `date` DESC LIMIT ?"
    return _select_many(sql, (uuid, limit), _row_hist)


# ---- optional: by IP (может пригодиться) -----------------------------------
def get_bans_by_ip(ip: str, limit: int = 50, include_inactive: bool = True) -> List[Dict[str, Any]]:
    where = "`ip` = ?" + ("" if include_inactive else " AND `active` = 1")
    sql = f"SELECT * FROM `litebans_bans` WHERE {where} ORDER BY `time` DESC LIMIT ?"
    return _select_many(sql, (ip, limit), _row_ban)

def get_mutes_by_ip(ip: str, limit: int = 50, include_inactive: bool = True) -> List[Dict[str, Any]]:
    where = "`ip` = ?" + ("" if include_inactive else " AND `active` = 1")
    sql = f"SELECT * FROM `litebans_mutes` WHERE {where} ORDER BY `time` DESC LIMIT ?"
    return _select_many(sql, (ip, limit), _row_mute)
