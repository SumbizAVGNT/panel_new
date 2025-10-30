# app/modules/luckperms_repo.py
from __future__ import annotations
import os
from typing import Iterable, Dict, List

from ..database import get_luckperms_connection

# ====== ENV ======
LP_DB     = (os.getenv("LUCKPERMS_NAME") or os.getenv("LUCKPERMS_DB") or "").strip(" `")
LP_PREFIX = (os.getenv("LUCKPERMS_TABLE_PREFIX") or os.getenv("LUCKPERMS_PREFIX") or "luckperms_").strip("`")

def _q(name: str) -> str:
    return f"`{name}`"

def _tbl(core: str) -> str:
    """Имя таблицы с префиксом и (если задано) со схемой: `db`.`luckperms_<core>`."""
    table = f"{LP_PREFIX}{core}"
    if LP_DB:
        return f"{_q(LP_DB)}.{_q(table)}"
    return _q(table)

def _to_dashed(u: str) -> str:
    u = (u or "").replace("-", "").lower()
    if len(u) == 32:
        return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"
    return u

# ---------- простой вариант: первая попавшаяся group.* ----------
def roles_from_user_permissions(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Возвращает роль из luckperms_user_permissions (permission LIKE 'group.%').
    ВАЖНО: LIKE параметризован (чтобы % не ломал python-форматирование драйвера).
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    sql = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS role "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE ? "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND uuid IN ({ph})"
    )
    params = ["group.%"] + uu

    try:
        with get_luckperms_connection() as conn:
            rows = conn.query_all(sql, params)
    except Exception:
        return {}

    out: Dict[str, str] = {}
    for r in rows:
        u = r.get("uuid")
        g = (r.get("role") or "").strip()
        if u and g and u not in out:
            out[u] = g
    return out

# ---------- «тяжёлая» роль по весам ----------
def effective_roles_for_uuids(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Выбирает «самую тяжёлую» группу из user_permissions (group.*),
    веса берёт из group_permissions (permission LIKE 'weight.%').
    Все LIKE — через параметры.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    sql_parents = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS grp "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE ? "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND uuid IN ({ph})"
    )
    params_parents = ["group.%"] + uu

    sql_weights = (
        f"SELECT name, MAX(CAST(SUBSTRING(permission, 8) AS UNSIGNED)) AS w "
        f"FROM {_tbl('group_permissions')} "
        f"WHERE permission LIKE ? "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"GROUP BY name"
    )
    params_weights = ["weight.%"]

    try:
        with get_luckperms_connection() as conn:
            rows = conn.query_all(sql_parents, params_parents)
            parents: Dict[str, List[str]] = {}
            for r in rows:
                g = (r.get("grp") or "").strip()
                u = r.get("uuid")
                if u and g:
                    parents.setdefault(u, []).append(g)

            wrows = conn.query_all(sql_weights, params_weights)
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

# алиас под старое имя
roles_for_uuids = effective_roles_for_uuids

__all__ = ["roles_from_user_permissions", "effective_roles_for_uuids", "roles_for_uuids"]
