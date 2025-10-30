from __future__ import annotations
import os
from typing import Iterable, Dict, List

from ..database import get_luckperms_connection

# ====== ENV ======
LP_PREFIX = (os.getenv("LUCKPERMS_TABLE_PREFIX") or os.getenv("LUCKPERMS_PREFIX") or "luckperms_").strip("`")
LP_SERVER = (os.getenv("LUCKPERMS_SERVER") or "global").strip()

def _q(name: str) -> str:
    return f"`{name}`"

def _tbl(core: str) -> str:
    return _q(f"{LP_PREFIX}{core}")

def _to_dashed(u: str) -> str:
    """UUID → dashed (LuckPerms хранит dashed)."""
    u = (u or "").replace("-", "").lower()
    if len(u) == 32:
        return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"
    return u

def roles_from_user_permissions(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Берёт роль игрока из luckperms_user_permissions (permission='group.<role>'),
    без обращения к primary_group.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["%s"] * len(uu))
    where_server = "(server IS NULL OR server='' OR server='global' OR server=%s)"
    where_world  = "(world IS NULL OR world='')"

    sql = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS role "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND {where_server} AND {where_world} "
        f"  AND uuid IN ({ph})"
    )

    try:
        with get_luckperms_connection() as conn:
            rows = conn.query_all(sql, [LP_SERVER] + uu)
    except Exception as e:
        print("❌ Ошибка запроса:", e)
        return {}

    result: Dict[str, str] = {}
    for r in rows:
        uuid = r.get("uuid")
        role = r.get("role")
        if uuid and role:
            # Если у игрока несколько записей, оставляем самую «высокую» по алфавиту
            if uuid not in result or role > result[uuid]:
                result[uuid] = role
    return result

__all__ = ["roles_from_user_permissions"]
