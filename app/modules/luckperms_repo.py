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

def effective_roles_for_uuids(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Вычисляет роль как «самую тяжёлую» группу из user_permissions (узлы group.*),
    по весам из group_permissions (permission=weight.N), с контекстами:
      - server ∈ {NULL, '', 'global', LUCKPERMS_SERVER}
      - world  ∈ {NULL, ''}
    Если у игрока нет parent-узлов — роль не присваивается (без fallback'а на primary_group).
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    # placeholders: %s, %s, ...  (для PyMySQL и подобных драйверов)
    ph = ",".join(["%s"] * len(uu))

    where_server = "(server IS NULL OR server='' OR server='global' OR server=%s)"
    where_world  = "(world IS NULL OR world='')"

    # 1) все parent-узлы group.<name> у пользователей
    sql_parents = (
        f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS grp "
        f"FROM {_tbl('user_permissions')} "
        f"WHERE permission LIKE 'group.%' "
        f"  AND (value=1 OR value='1' OR value='true') "
        f"  AND {where_server} AND {where_world} "
        f"  AND uuid IN ({ph})"
    )

    # 2) веса групп (берём максимальный скоуп-совместимый вес)
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
            rows = conn.query_all(sql_parents, [LP_SERVER] + uu)
            parents: Dict[str, List[str]] = {}
            for r in rows:
                g = (r.get("grp") or "").strip()
                u = r.get("uuid")
                if g and u:
                    parents.setdefault(u, []).append(g)

            wrows = conn.query_all(sql_weights, [LP_SERVER])
            weights = {r["name"]: int(r["w"] or 0) for r in wrows}
    except Exception as e:
        # Если база недоступна/другая ошибка — вернём пусто (без primary fallback)
        return {}

    # 3) выбираем лучшую группу по весу (тай-брейк — по имени)
    result: Dict[str, str] = {}
    for u in uu:
        gs = parents.get(u, [])
        if not gs:
            continue
        best = max(gs, key=lambda g: (weights.get(g, 0), g))
        result[u] = best
    return result

# Удобные алиасы
roles_for_uuids = effective_roles_for_uuids

__all__ = ["effective_roles_for_uuids", "roles_for_uuids"]
