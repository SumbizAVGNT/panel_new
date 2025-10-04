from __future__ import annotations

import os
from typing import Iterable, Dict, List

from ..database import get_luckperms_connection

# ====== ENV ======
# Префикс таблиц LuckPerms (по умолчанию luckperms_)
LP_PREFIX = (
    os.getenv("LUCKPERMS_TABLE_PREFIX")
    or os.getenv("LUCKPERMS_PREFIX")
    or "luckperms_"
).strip("`")

# Имя сервера из config.yml LuckPerms (server: ...)
# Для глобальных узлов оставляй "global"
LP_SERVER = (os.getenv("LUCKPERMS_SERVER") or "global").strip()

def _q(name: str) -> str:
    return f"`{name}`"

def _tbl(core: str) -> str:
    return _q(f"{LP_PREFIX}{core}")

def _to_dashed(u: str) -> str:
    """Приводим UUID к dashed-формату (LP хранит так)."""
    u = (u or "").replace("-", "").lower()
    if len(u) == 32:
        return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"
    return u

# ---------- fallback: primary_group ----------
def roles_from_lp_primary(uuids: Iterable[str]) -> Dict[str, str]:
    """Берём роль из luckperms_players.primary_group (быстро и надёжно)."""
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}
    placeholders = ",".join(["?"] * len(uu))
    sql = f"SELECT uuid, primary_group FROM {_tbl('players')} WHERE uuid IN ({placeholders})"
    try:
        with get_luckperms_connection() as conn:
            rows = conn.query_all(sql, uu)
    except Exception:
        return {}
    return {r["uuid"]: r["primary_group"] for r in rows if r.get("primary_group")}

# ---------- актуальная роль по узлам и весам ----------
def effective_roles_for_uuids(uuids: Iterable[str]) -> Dict[str, str]:
    """
    Вычисляет группу как самую «тяжёлую» из parent-узлов (group.*),
    учитывая контекст server ∈ {'global', LUCKPERMS_SERVER} и пустой world.
    Если parent-узлов нет — докидывает primary_group.
    """
    uu = [_to_dashed(u) for u in uuids if u]
    if not uu:
        return {}

    try:
        with get_luckperms_connection() as conn:
            ph = ",".join(["?"] * len(uu))

            where_server = "(server IS NULL OR server='' OR server='global' OR server=?)"
            where_world  = "(world IS NULL OR world='')"

            # 1) Все назначенные игрокам группы (узлы group.<name>)
            sql_parents = (
                f"SELECT uuid, SUBSTRING_INDEX(permission,'.',-1) AS grp "
                f"FROM {_tbl('user_permissions')} "
                f"WHERE permission LIKE 'group.%' "
                f"AND (value=1 OR value='1' OR value='true') "
                f"AND {where_server} AND {where_world} "
                f"AND uuid IN ({ph})"
            )
            rows = conn.query_all(sql_parents, [LP_SERVER] + uu)
            parents: Dict[str, List[str]] = {}
            for r in rows:
                g = (r.get("grp") or "").strip()
                if g:
                    parents.setdefault(r["uuid"], []).append(g)

            # 2) Веса групп (weight.N) — берём максимальный из всех server='global'|LP_SERVER
            sql_weights = (
                f"SELECT name, MAX(CAST(SUBSTRING(permission, 8) AS UNSIGNED)) AS w "
                f"FROM {_tbl('group_permissions')} "
                f"WHERE permission LIKE 'weight.%' "
                f"AND (value=1 OR value='1' OR value='true') "
                f"AND {where_server} AND {where_world} "
                f"GROUP BY name"
            )
            wrows = conn.query_all(sql_weights, [LP_SERVER])
            weights = {r["name"]: int(r["w"] or 0) for r in wrows}
    except Exception:
        # если что-то пошло не так — безопасно упадём на primary_group
        return roles_from_lp_primary(uu)

    # 3) Выбираем лучшую группу по весу (тай-брейк — по имени)
    result: Dict[str, str] = {}
    for u in uu:
        gs = parents.get(u, [])
        if gs:
            best = max(gs, key=lambda g: (weights.get(g, 0), g))
            result[u] = best

    # 4) Для тех, у кого parent-узлов нет — подставим primary_group
    missing = [u for u in uu if u not in result]
    if missing:
        result.update(roles_from_lp_primary(missing))

    return result

# --------- обратная совместимость имён (чтобы старые импорты не падали) ---------
# Старое имя в вашем коде
roles_for_uuids = effective_roles_for_uuids
# Иногда использовали такое имя
primary_roles_for_uuids = roles_from_lp_primary

__all__ = [
    "effective_roles_for_uuids",
    "roles_from_lp_primary",
    "roles_for_uuids",
    "primary_roles_for_uuids",
]
