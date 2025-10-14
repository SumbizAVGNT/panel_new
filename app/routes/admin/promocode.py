# app/routes/admin/promocode.py
from __future__ import annotations

import os
import re
import json
import random
from typing import Optional, Iterable, List, Tuple

from flask import Blueprint, render_template, request, jsonify, current_app

from ...decorators import login_required
from ...database import get_db_connection, MySQLConnection, init_db
from . import admin_bp

bp = Blueprint("promocode", __name__, url_prefix="/promocode")

# =========================================================
# utils
# =========================================================

def _ok(data=None, **kw):
    out = {"ok": True}
    if data is not None:
        out["data"] = data
    out.update(kw)
    return jsonify(out)

def _err(msg: str, **kw):
    out = {"ok": False, "error": str(msg)}
    out.update(kw)
    return jsonify(out), 400

def _rand_code(n: int = 10) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(max(4, n)))

def _rows(conn: MySQLConnection, sql: str, params: Iterable = ()):
    return conn.query_all(sql, params)

def _row(conn: MySQLConnection, sql: str, params: Iterable = ()):
    return conn.query_one(sql, params)

_JSONC_LINE = re.compile(r"^\s*//.*$", re.MULTILINE)
_JSONC_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)

def _read_jsonc(text: str):
    """Разобрать JSON, допускающий комментарии // и /* ... */."""
    text = _JSONC_BLOCK.sub("", _JSONC_LINE.sub("", text))
    return json.loads(text or "[]")

def _try_open_candidates(candidates: List[str]) -> Tuple[Optional[str], Optional[list]]:
    """
    Пытается открыть JSON/JSONC по списку относительных путей.
    Возвращает (abs_path, data) или (None, None).
    """
    tried: List[str] = []
    # 1) Через open_resource — ищет внутри пакета/blueprint
    for rel in candidates:
        try:
            with current_app.open_resource(rel) as f:
                data = _read_jsonc(f.read().decode("utf-8"))
                abs_path = os.path.join(current_app.root_path, rel)
                return abs_path, data
        except FileNotFoundError:
            tried.append(f"open_resource:{rel}")
        except Exception as e:
            current_app.logger.warning("Items file load error (open_resource %s): %s", rel, e)

    # 2) Абсолютные варианты
    roots = [
        current_app.root_path,
        getattr(current_app, "static_folder", None) or os.path.join(current_app.root_path, "static"),
        os.path.dirname(current_app.root_path),
    ]
    for rel in candidates:
        for base in roots:
            abs_path = os.path.join(base, rel)
            try:
                with open(abs_path, "r", encoding="utf-8") as f:
                    data = _read_jsonc(f.read())
                    return abs_path, data
            except FileNotFoundError:
                tried.append(abs_path)
            except Exception as e:
                current_app.logger.warning("Items file load error at %s: %s", abs_path, e)

    current_app.logger.warning("Items file not found. Tried %s", ", ".join(tried))
    return None, None

# -------- ленивое создание схемы (idempotent) --------
_SCHEMA_READY = False
def _ensure_schema(conn: MySQLConnection) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    try:
        init_db(conn)  # создаст promo_* таблицы, если их нет
        _SCHEMA_READY = True
    except Exception as e:
        current_app.logger.error("init_db failed: %s", e)

def _kit_items(conn: MySQLConnection, kit_id: int) -> List[dict]:
    items = _rows(
        conn,
        """
        SELECT id, namespace, item_id, amount, display_name,
               enchants_json, nbt_json, slot
        FROM promo_kit_items
        WHERE kit_id = ?
        ORDER BY COALESCE(slot, 999), id
        """,
        (kit_id,),
    )
    for it in items:
        # корректно разворачиваем *_json в объекты
        for col in ("enchants_json", "nbt_json"):
            try:
                it[col.replace("_json", "")] = json.loads(it.get(col) or "[]")
            except Exception:
                it[col.replace("_json", "")] = []
    return items

# =========================================================
# UI
# =========================================================

@bp.get("/")
@login_required
def ui_index():
    # полный редактор Promocodes & Kits
    return render_template("admin/gameservers/promocode/index.html")

# =========================================================
# API: items catalog
# =========================================================

@bp.get("/api/items/vanilla")
@login_required
def api_items_vanilla():
    """
    Возвращает список ванильных предметов 1.20.6: [{id, name, icon, ns='minecraft'}]
    """
    rel = "data/vanilla-items-1.20.6.json"
    candidates = [f"static/{rel}", rel]
    _abs, items = _try_open_candidates(candidates)
    if not items:
        items = []

    base = "/static/mc/1.20.6/items"
    out = []
    for it in items:
        iid = (it.get("id") or "").strip()
        if not iid:
            continue
        name = it.get("name") or iid.replace("_", " ").title()
        out.append({
            "id": iid,
            "name": name,
            "icon": f"{base}/{iid}.png",
            "ns": "minecraft",
        })
    return _ok(out)

@bp.get("/api/items/custom")
@login_required
def api_items_custom():
    """
    Кастомные предметы (ItemsAdder).
    Формат файла:
      [{"id":"itemsadder:my_sword","name":"My Sword"}, ...]
    """
    rel = "data/itemsadder-items.json"
    candidates = [f"static/{rel}", rel]
    _abs, items = _try_open_candidates(candidates)
    items = items or []

    base = "/static/mc/itemsadder"
    out = []
    for it in items:
        raw = (it.get("id") or "").strip()
        if not raw:
            continue
        if ":" in raw:
            ns, iid = raw.split(":", 1)
        else:
            ns, iid = "itemsadder", raw
        name = it.get("name") or iid.replace("_", " ").title()
        out.append({
            "id": iid,
            "name": name,
            "icon": f"{base}/{ns}.{iid}.png",
            "ns": ns,
            "full": f"{ns}:{iid}",
        })
    return _ok(out)

# =========================================================
# API: kits
# =========================================================

@bp.get("/api/kits/list")
@login_required
def api_kits_list():
    with get_db_connection() as conn:
        _ensure_schema(conn)
        kits = _rows(conn, "SELECT id, name, description, created_at FROM promo_kits ORDER BY id DESC")
        for k in kits:
            k["items"] = _kit_items(conn, k["id"])
        return _ok(kits)

@bp.post("/api/kits/save")
@login_required
def api_kits_save():
    """
    Сохранение кита. Формат JSON:
    {
      "id": null|<int>,
      "name": "...",
      "description": "...",
      "items": [
        {
          "namespace":"minecraft","item_id":"diamond_sword","amount":1,
          "display_name":"&bМеч", "enchants":[{"id":"sharpness","lvl":5}],
          "nbt": [], "slot": 0
        }, ...
      ]
    }
    """
    js = request.get_json(silent=True) or {}
    kit_id = js.get("id")
    name = (js.get("name") or "").strip()
    desc = (js.get("description") or "").strip()
    items = js.get("items") or []

    if not name:
        return _err("name is required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        if kit_id:
            conn.execute("UPDATE promo_kits SET name=?, description=? WHERE id=?", (name, desc, int(kit_id)))
            conn.execute("DELETE FROM promo_kit_items WHERE kit_id=?", (int(kit_id),))
            kid = int(kit_id)
        else:
            conn.execute("INSERT INTO promo_kits(name, description) VALUES (?,?)", (name, desc))
            conn.commit()
            kid = int(conn.lastrowid)

        # вставляем предметы
        ins_sql = """
        INSERT INTO promo_kit_items(kit_id, namespace, item_id, amount, display_name, enchants_json, nbt_json, slot)
        VALUES (?,?,?,?,?,?,?,?)
        """
        for idx, it in enumerate(items):
            ns = (it.get("namespace") or "minecraft").strip()
            iid = (it.get("item_id") or "").strip()
            if not iid:
                continue
            amount = int(it.get("amount") or 1)
            disp = it.get("display_name")
            ench = it.get("enchants") or it.get("enchantments") or []
            nbt  = it.get("nbt") or []
            slot = it.get("slot") if it.get("slot") is not None else idx
            conn.execute(
                ins_sql,
                (kid, ns, iid, amount, disp, json.dumps(ench, ensure_ascii=False), json.dumps(nbt, ensure_ascii=False), slot),
            )
        conn.commit()

        out = _row(conn, "SELECT id, name, description, created_at FROM promo_kits WHERE id=?", (kid,))
        out["items"] = _kit_items(conn, kid)
        return _ok(out)

@bp.post("/api/kits/delete")
@login_required
def api_kits_delete():
    js = request.get_json(silent=True) or {}
    kid = js.get("id")
    if not kid:
        return _err("id required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute("DELETE FROM promo_kits WHERE id=?", (int(kid),))
        conn.commit()
        return _ok({"deleted": int(kid)})

# =========================================================
# API: promo codes (CRUD + views)
# =========================================================

@bp.get("/api/codes/list")
@login_required
def api_codes_list():
    """
    Возвращает список промокодов + статистика.
    ?q= фильтр по коду
    """
    q = (request.args.get("q") or "").strip()
    where = ""
    params: list = []
    if q:
        where = "WHERE c.code LIKE ?"
        params.append(f"%{q}%")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        rows = _rows(
            conn,
            f"""
            SELECT c.id, c.code, c.enabled, c.amount, c.currency_key, c.realm,
                   c.uses_total, c.uses_left, c.per_player_uses, c.cooldown_seconds,
                   c.expires_at, c.kit_id, c.created_at,
                   COALESCE(r.cnt, 0) AS redemptions
            FROM promo_codes c
            LEFT JOIN (
              SELECT code_id, COUNT(*) AS cnt
              FROM promo_redemptions
              GROUP BY code_id
            ) r ON r.code_id = c.id
            {where}
            ORDER BY c.id DESC
            """,
            params,
        )
        return _ok(rows)

@bp.get("/api/codes/one")
@login_required
def api_codes_one():
    code_id = request.args.get("id")
    code = (request.args.get("code") or "").strip()
    if not code_id and not code:
        return _err("id or code required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        if code_id:
            row = _row(conn, "SELECT * FROM promo_codes WHERE id=?", (int(code_id),))
        else:
            row = _row(conn, "SELECT * FROM promo_codes WHERE code=?", (code,))
        if not row:
            return _err("not found", code=code, id=code_id)
        # группы/команды
        groups = _rows(conn, "SELECT id, group_name, temp_seconds, context_server, context_world, priority FROM promo_code_groups WHERE code_id=? ORDER BY priority ASC, id ASC", (row["id"],))
        cmds   = _rows(conn, "SELECT id, run_as, realm, command_text, run_delay_ms, priority FROM promo_code_cmds WHERE code_id=? ORDER BY priority ASC, id ASC", (row["id"],))
        row["groups"] = groups
        row["cmds"]   = cmds
        return _ok(row)

@bp.post("/api/codes/save")
@login_required
def api_codes_save():
    """
    Создание/редактирование промокода.
    ВАЖНО: uses_left не обнуляется при апдейте; при изменении uses_total корректируется дельтой.
    JSON:
    {
      "id": null|int,
      "code": "ABC123",
      "enabled": true,
      "amount": 100.0, "currency_key": "rubs",
      "realm": "anarchy",
      "uses_total": 10, "per_player_uses": 1, "cooldown_seconds": 0,
      "expires_at": "2025-12-31 23:59:59" | null,
      "kit_id": null|int
    }
    """
    js = request.get_json(silent=True) or {}
    code_id = js.get("id")
    code = (js.get("code") or "").strip()
    if not code:
        return _err("code is required")

    enabled = 1 if js.get("enabled", True) else 0
    amount = js.get("amount")
    currency_key = (js.get("currency_key") or None) or None
    realm = (js.get("realm") or "").strip() or None
    uses_total = int(js.get("uses_total") or 0)
    per_player = js.get("per_player_uses")
    per_player = int(per_player) if per_player is not None else None
    cooldown = js.get("cooldown_seconds")
    cooldown = int(cooldown) if cooldown is not None else None
    expires_at = js.get("expires_at")  # строка в формате '%Y-%m-%d %H:%M:%S' или None
    kit_id = js.get("kit_id")
    kit_id = int(kit_id) if kit_id is not None else None

    with get_db_connection() as conn:
        _ensure_schema(conn)

        # Вставка/апдейт с сохранением uses_left.
        # Логика:
        #  - при первом создании uses_left = uses_total
        #  - при апдейте: uses_left НЕ перезаписывается;
        #    если изменили uses_total, то:
        #      uses_left := GREATEST(0, LEAST(new_total, uses_left + (new_total - old_total)))
        sql = """
        INSERT INTO promo_codes (id, code, enabled, amount, currency_key, realm,
                                 uses_total, uses_left, per_player_uses, cooldown_seconds, expires_at, kit_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          code = VALUES(code),
          enabled = VALUES(enabled),
          amount = VALUES(amount),
          currency_key = VALUES(currency_key),
          realm = VALUES(realm),
          -- пересчёт uses_left с учётом дельты uses_total
          uses_left = GREATEST(0, LEAST(VALUES(uses_total), COALESCE(uses_left, 0) + (VALUES(uses_total) - uses_total))),
          uses_total = VALUES(uses_total),
          per_player_uses = VALUES(per_player_uses),
          cooldown_seconds = VALUES(cooldown_seconds),
          expires_at = VALUES(expires_at),
          kit_id = VALUES(kit_id)
        """
        params = (
            int(code_id) if code_id else None,
            code, enabled, amount, currency_key, realm,
            uses_total, uses_total, per_player, cooldown, expires_at, kit_id
        )
        conn.execute(sql, params)
        conn.commit()

        row = _row(conn, "SELECT * FROM promo_codes WHERE code=?", (code,))
        return _ok(row)

@bp.post("/api/codes/delete")
@login_required
def api_codes_delete():
    """
    Удаление промокода по id или code.
    Связанные promo_code_groups / promo_code_cmds / promo_redemptions должны удалиться каскадом (FK ON DELETE CASCADE).
    """
    js = request.get_json(silent=True) or {}
    code_id = js.get("id")
    code = (js.get("code") or "").strip() if js.get("code") is not None else None
    if not code_id and not code:
        return _err("id or code required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        if code_id:
            conn.execute("DELETE FROM promo_codes WHERE id=?", (int(code_id),))
        else:
            conn.execute("DELETE FROM promo_codes WHERE code=?", (code,))
        conn.commit()
        return _ok({"deleted": code_id or code})

# =========================================================
# API: groups (LuckPerms bindings per code)
# =========================================================

@bp.get("/api/groups/list")
@login_required
def api_groups_list():
    code_id = request.args.get("code_id")
    if not code_id:
        return _err("code_id required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        rows = _rows(
            conn,
            "SELECT id, group_name, temp_seconds, context_server, context_world, priority "
            "FROM promo_code_groups WHERE code_id=? ORDER BY priority ASC, id ASC",
            (int(code_id),)
        )
        return _ok(rows)

@bp.post("/api/groups/save")
@login_required
def api_groups_save():
    """
    Полная замена набора групп у промокода.
    JSON:
    {
      "code_id": 123,
      "groups": [
        {"group_name":"vip","temp_seconds":0,"context_server":null,"context_world":null,"priority":0},
        ...
      ]
    }
    """
    js = request.get_json(silent=True) or {}
    code_id = js.get("code_id")
    groups = js.get("groups") or []
    if not code_id:
        return _err("code_id required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute("DELETE FROM promo_code_groups WHERE code_id=?", (int(code_id),))
        ins = "INSERT INTO promo_code_groups(code_id, group_name, temp_seconds, context_server, context_world, priority) VALUES (?,?,?,?,?,?)"
        for idx, g in enumerate(groups):
            name = (g.get("group_name") or "").strip()
            if not name:
                continue
            temp = g.get("temp_seconds")
            temp = int(temp) if temp is not None else None
            sctx = (g.get("context_server") or None)
            wctx = (g.get("context_world") or None)
            prio = int(g.get("priority") if g.get("priority") is not None else idx)
            conn.execute(ins, (int(code_id), name, temp, sctx, wctx, prio))
        conn.commit()
        rows = _rows(conn, "SELECT * FROM promo_code_groups WHERE code_id=? ORDER BY priority ASC, id ASC", (int(code_id),))
        return _ok(rows)

@bp.post("/api/groups/delete")
@login_required
def api_groups_delete():
    js = request.get_json(silent=True) or {}
    gid = js.get("id")
    if not gid:
        return _err("id required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute("DELETE FROM promo_code_groups WHERE id=?", (int(gid),))
        conn.commit()
        return _ok({"deleted": int(gid)})

# =========================================================
# API: redemptions (players)
# =========================================================

@bp.get("/api/redemptions")
@login_required
def api_redemptions():
    """
    Список активаций игроков.
    Параметры:
      - code_id (int) | code (str) | player (like)
      - limit (int, default 100)
      - offset (int, default 0)
    """
    code_id = request.args.get("code_id")
    code = (request.args.get("code") or "").strip()
    player = (request.args.get("player") or "").strip()
    limit = max(1, min(500, int(request.args.get("limit") or 100)))
    offset = max(0, int(request.args.get("offset") or 0))

    where = []
    params: List = []
    with get_db_connection() as conn:
        _ensure_schema(conn)
        if code_id:
            where.append("r.code_id = ?")
            params.append(int(code_id))
        elif code:
            row = _row(conn, "SELECT id FROM promo_codes WHERE code=?", (code,))
            if row:
                where.append("r.code_id = ?")
                params.append(int(row["id"]))
            else:
                return _ok({"items": [], "total": 0})
        if player:
            where.append("(r.username LIKE ? OR r.uuid = ?)")
            params.extend([f"%{player}%", player])

        where_sql = "WHERE " + " AND ".join(where) if where else ""
        items = _rows(
            conn,
            f"""
            SELECT r.id, r.code_id, r.uuid, r.username, r.realm,
                   r.granted_amount, r.currency_key, r.kit_id,
                   r.ip, r.created_at
            FROM promo_redemptions r
            {where_sql}
            ORDER BY r.id DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        )
        total = _row(conn, f"SELECT COUNT(*) AS c FROM promo_redemptions r {where_sql}", params)["c"]
        return _ok({"items": items, "total": total})

# =========================================================

admin_bp.register_blueprint(bp)
