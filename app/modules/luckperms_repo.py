# app/modules/luckperms_repo.py
from __future__ import annotations
import os
from typing import Iterable, Dict, List, Tuple

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

def _world_predicate() -> str:
    # ВАЖНО: у тебя world часто 'global' — учитываем его.
    return "(world IS NULL OR world='' OR world='global')"

def _server_predicate() -> str:
    return "(server IS NULL OR server='' OR server='global' OR server=?)"

# ---------- упрощённый вариант (только user_permissions) ----------
def roles_from_user_permissions(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Возвращает роль из luckperms_user_permissions (permission='group.<role>')
    с учётом контекстов: server ∈ {NULL,'','global', LUCKPERMS_SERVER}, world ∈ {NULL,'','global'}.
    Если ничего не нашли — повторяем запрос без world-предиката.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    where_server = _server_predicate()
    where_world  = _world_predicate()

    sql_base = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS role "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND {{server}} AND {{world}} "
        f"  AND uuid IN ({ph})"
    )

    def _run(query: str, params: List[str]) -> List[dict]:
        with get_luckperms_connection() as conn:
            return conn.query_all(query, params)

    # 1) с фильтром по world
    try:
        sql1 = sql_base.format(server=where_server, world=where_world)
        rows = _run(sql1, [LP_SERVER] + uu)
        if not rows:
            # 2) без фильтра по world (некоторые инсталляции его не задают)
            sql2 = sql_base.format(server=where_server, world="(1=1)")
            rows = _run(sql2, [LP_SERVER] + uu)
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
    учитывая:
      server ∈ {NULL,'','global', LUCKPERMS_SERVER}
      world  ∈ {NULL,'','global'}
    Если родителей нет — пробуем упрощённый вариант.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    ph = ",".join(["?"] * len(uu))
    where_server = _server_predicate()
    where_world  = _world_predicate()

    sql_parents_tpl = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS grp "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND {{server}} AND {{world}} "
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

    def _run_parents(world_filter: bool) -> List[dict]:
        with get_luckperms_connection() as conn:
            sql = sql_parents_tpl.format(server=where_server, world=(where_world if world_filter else "(1=1)"))
            return conn.query_all(sql, [LP_SERVER] + uu)

    try:
        # 1) родители с world-предикатом
        rows = _run_parents(world_filter=True)
        # 2) если пусто — без world-предиката
        if not rows:
            rows = _run_parents(world_filter=False)

        parents: Dict[str, List[str]] = {}
        for r in rows:
            g = (r.get("grp") or "").strip()
            u = r.get("uuid")
            if u and g:
                parents.setdefault(u, []).append(g)

        with get_luckperms_connection() as conn:
            wrows = conn.query_all(sql_weights, [LP_SERVER])
            weights = {r["name"]: int(r["w"] or 0) for r in wrows}
    except Exception:
        # Если что-то не так — попробуем простой путь
        return roles_from_user_permissions(uu)

    out: Dict[str, str] = {}
    for u in uu:
        gs = parents.get(u, [])
        if not gs:
            continue
        best = max(gs, key=lambda g: (weights.get(g, 0), g))
        out[u] = best

    # доклеим тех, у кого не нашли родителей
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
