# app/modules/luckperms_repo.py
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

# ---------- упрощённый вариант (только user_permissions) ----------
def roles_from_user_permissions(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Возвращает первую попавшуюся роль из luckperms_user_permissions (permission='group.<role>')
    с учётом контекстов server ∈ {NULL,'', 'global', LUCKPERMS_SERVER} и world ∈ {NULL,''}.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    where_server = "(server IS NULL OR server='' OR server='global' OR server=?)"
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
    except Exception:
        return {}

    out: Dict[str, str] = {}
    for r in rows:
        u = r.get("uuid")
        g = (r.get("role") or "").strip()
        if u and g and u not in out:
            out[u] = g
    return out

# ---------- полноценный вариант (user_permissions + group_permissions/веса) ----------
def effective_roles_for_uuids(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Вычисляет группу как самую «тяжёлую» из parent-узлов (group.*),
    учитывая контекст server ∈ {'global', LUCKPERMS_SERVER, NULL, ''} и world ∈ {NULL,''}.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    where_server = "(server IS NULL OR server='' OR server='global' OR server=?)"
    where_world  = "(world IS NULL OR world='')"

    sql_parents = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS grp "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND {where_server} AND {where_world} "
        f"  AND uuid IN ({ph})"
    )

    sql_weights = (
        f"SELECT name, MAX(CAST(SUBSTRING(permission, 8) AS UNSIGNED)) AS w "
        f"FROM {_tbl('group_permissions')} "
        f"WHERE permission LIKE 'weight.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND {where_server} AND {where_world} "
        f"GROUP BY name"
    )

    try:
        with get_luckperms_connection() as conn:
            # родители
            rows = conn.query_all(sql_parents, [LP_SERVER] + uu)
            parents: Dict[str, List[str]] = {}
            for r in rows:
                g = (r.get("grp") or "").strip()
                u = r.get("uuid")
                if u and g:
                    parents.setdefault(u, []).append(g)

            # веса
            wrows = conn.query_all(sql_weights, [LP_SERVER])
            weights = {r["name"]: int(r["w"] or 0) for r in wrows}
    except Exception:
        # на любой сбой — безопасный минимум
        return roles_from_user_permissions(uu)

    # выбор лучшей по весу (tie-break по имени)
    out: Dict[str, str] = {}
    for u in uu:
        gs = parents.get(u, [])
        if not gs:
            continue
        best = max(gs, key=lambda g: (weights.get(g, 0), g))
        out[u] = best
    # если у кого-то нет parent-узлов — можно докинуть упрощённый вариант
    missing = [u for u in uu if u not in out]
    if missing:
        out.update({k: v for k, v in roles_from_user_permissions(missing).items() if k not in out})
    return out

# ---------- алиасы и экспорт ----------
roles_for_uuids = effective_roles_for_uuids

__all__ = [
    "roles_from_user_permissions",
    "effective_roles_for_uuids",
    "roles_for_uuids",
]
