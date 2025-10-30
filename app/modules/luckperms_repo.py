# app/modules/luckperms_repo.py
from __future__ import annotations
import os
from typing import Iterable, Dict, List

from ..database import get_luckperms_connection

# ====== ENV ======
LP_PREFIX = (os.getenv("LUCKPERMS_TABLE_PREFIX") or os.getenv("LUCKPERMS_PREFIX") or "luckperms_").strip("`")

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

# ---------- простой вариант: берём первую попавшуюся group.* без фильтра по server/world ----------
def roles_from_user_permissions(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Возвращает роль из luckperms_user_permissions (permission='group.<role>')
    БЕЗ фильтров по server/world — чтобы ничего не потерять.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    sql = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS role "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND uuid IN ({ph})"
    )

    try:
        with get_luckperms_connection() as conn:
            rows = conn.query_all(sql, uu)
    except Exception:
        return {}

    out: Dict[str, str] = {}
    for r in rows:
        u = r.get("uuid")
        g = (r.get("role") or "").strip()
        if u and g and u not in out:
            out[u] = g
    return out

# ---------- полноценный вариант: выбираем «самую тяжёлую» группу по weight ----------
def effective_roles_for_uuids(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Вычисляет группу как самую «тяжёлую» из parent-узлов (group.*),
    веса берём из luckperms_group_permissions (permission='weight.N').
    Фильтров по server/world НЕТ намеренно (бывает разная конфигурация).
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))

    sql_parents = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS grp "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND uuid IN ({ph})"
    )

    # Веса берём максимально возможные по каждой группе
    sql_weights = (
        f"SELECT name, MAX(CAST(SUBSTRING(permission, 8) AS UNSIGNED)) AS w "
        f"FROM {_tbl('group_permissions')} "
        f"WHERE permission LIKE 'weight.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"GROUP BY name"
    )

    try:
        with get_luckperms_connection() as conn:
            rows = conn.query_all(sql_parents, uu)
            parents: Dict[str, List[str]] = {}
            for r in rows:
                g = (r.get("grp") or "").strip()
                u = r.get("uuid")
                if u and g:
                    parents.setdefault(u, []).append(g)

            wrows = conn.query_all(sql_weights)
            weights = {r["name"]: int(r["w"] or 0) for r in wrows}
    except Exception:
        # На любой сбой — простой путь
        return roles_from_user_permissions(uu)

    out: Dict[str, str] = {}
    for u in uu:
        gs = parents.get(u, [])
        if not gs:
            continue
        best = max(gs, key=lambda g: (weights.get(g, 0), g))  # tie-break по имени
        out[u] = best

    # Доклеим тех, у кого родителей не нашли — простым способом
    missing = [u for u in uu if u not in out]
    if missing:
        out.update({k: v for k, v in roles_from_user_permissions(missing).items() if k not in out})
    return out

# ---------- алиас под старое имя ----------
roles_for_uuids = effective_roles_for_uuids

__all__ = [
    "roles_from_user_permissions",
    "effective_roles_for_uuids",
    "roles_for_uuids",
]
