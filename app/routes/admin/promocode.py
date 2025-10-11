# app/routes/admin/promocode.py
from __future__ import annotations

import os
import re
import json
import random
from typing import Optional, Iterable, List, Tuple

from flask import Blueprint, render_template, request, jsonify, current_app

from ...decorators import login_required
from ...database import get_db_connection, MySQLConnection
from . import admin_bp

bp = Blueprint("promocode", __name__, url_prefix="/promocode")

# =========================================================
# utils
# =========================================================

def _rand_code(n: int = 10) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(max(4, n)))

def _json(v):
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "[]"

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
    Возвращает (abs_path, data) или (None, None). Пишет в лог все попытки.
    """
    tried: List[str] = []
    # 1) Через open_resource — ищет внутри пакета/blueprint
    for rel in candidates:
        try:
            with current_app.open_resource(rel) as f:
                data = _read_jsonc(f.read().decode("utf-8"))
                # вычислим физический путь для лога/иконок
                abs_path = os.path.join(current_app.root_path, rel)
                return abs_path, data
        except FileNotFoundError:
            tried.append(f"open_resource:{rel}")
        except Exception as e:
            current_app.logger.warning("Items file load error (open_resource %s): %s", rel, e)

    # 2) Прямые абсолютные варианты от root_path и static_folder
    roots = [
        current_app.root_path,
        getattr(current_app, "static_folder", None) or os.path.join(current_app.root_path, "static"),
        os.path.dirname(current_app.root_path),  # /app
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
    Ищем файл по нескольким путям и поддерживаем JSONC с комментариями.
    """
    # возможные относительные пути
    rel = "data/vanilla-items-1.20.6.json"
    candidates = [
        f"static/{rel}",   # app/static/data/...
        rel,               # app/data/... (на случай иной сборки)
    ]
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
    return jsonify({"ok": True, "data": out})

@bp.get("/api/items/custom")
@login_required
def api_items_custom():
    """
    Кастомные предметы из ItemsAdder (опционально).
    Формат (JSONC допустим):
      [{"id":"itemsadder:my_sword","name":"My Sword"}, ...]
    Иконки ожидаем в /static/mc/itemsadder/<ns>.<id>.png
    """
    rel = "data/itemsadder-items.json"
    candidates = [
        f"static/{rel}",
        rel,
    ]
    _abs, items = _try_open_candidates(candidates)
    if not items:
        items = []

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
    return jsonify({"ok": True, "data": out})

# =========================================================
# API: kits
# =========================================================

@bp.get("/api/kits/list")
@login_required
def api_kits_list():
    with get_db_connection() as conn:
        kits = _rows(conn, "SELECT id, name, description, created_at FROM promo_kits ORDER BY id DESC")
        for k in kits:
            items = _rows(
                conn,
                """
                SELECT id, namespace, item_id, amount, display_name,
                       enchants_json, nbt_json, slot
                FROM promo_kit_items
                WHERE kit_id = ?
                ORDER BY COALESCE(slot, 999), id
                """,
                (k["id"],),
            )
            for it in items:
                # enchants / nbt -> lists
                for col in ("enchants_json", "nbt_json"):
                    try:
                        it[col[:-5] + "s"] = json.loads(it[col] or "[]")
                    except Exception:
                        it[col[:-5] + "s"] = []
            k["items"] = items
        return jsonify({"ok": True, "data": kits})

@bp.post("/api/kits/save")
@login_required
def api_kits_save():
    js = request.get_json(silent=True) or {}
    kit_id = js.get("id")
    name = (js.get("name") or "").strip()
    desc = (js.get("description") or "").strip()
    items = js.get("items") or []

    if not name:
        return jsonify({"ok": False, "error": "name is required"}), 400

    with get_db_connection() as conn:
        if kit_id:
            conn.execute("UPDATE promo_kits SET name=?, description=? WHERE id=?", (name, desc, int(kit_id)))
            conn.execute("DELETE FROM promo_kit_items WHERE kit_id=?", (int(kit_id),))
            kid = int(kit_id)
        else:
            conn.execute("INSERT INTO promo_kits(name, description) VALUES (?,?)", (name, desc))
            conn.commit()
            kid = int(conn.lastrowid)

        bulk = []
        pos = 0
        for it in items:
            ns = (it.get("ns") or it.get("namespace") or "minecraft").strip()
            iid = (it.get("id") or it.get("item_id") or "").strip()
            if not iid:
                continue
            amount = int(it.get("amount") or 1)
            display_name = (it.get("display_name") or "").strip() or None
            ench = _json(it.get("enchants") or [])
            nbt = _json(it.get("nbt") or [])
            slot = it.get("slot")
            try:
                slot = int(slot) if slot is not None else None
            except Exception:
                slot = None
            bulk.append((kid, ns, iid, amount, display_name, ench, nbt, slot if slot is not None else pos))
            pos += 1

        if bulk:
            conn.executemany(
                """
                INSERT INTO promo_kit_items(
                  kit_id, namespace, item_id, amount, display_name, enchants_json, nbt_json, slot
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                bulk,
            )
        conn.commit()
        return jsonify({"ok": True, "data": {"id": kid}})

@bp.post("/api/kits/delete")
@login_required
def api_kits_delete():
    js = request.get_json(silent=True) or {}
    kit_id = js.get("id")
    if not kit_id:
        return jsonify({"ok": False, "error": "id is required"}), 400
    with get_db_connection() as conn:
        conn.execute("DELETE FROM promo_kit_items WHERE kit_id=?", (int(kit_id),))
        conn.execute("DELETE FROM promo_kits WHERE id=?", (int(kit_id),))
        conn.commit()
    return jsonify({"ok": True})

# =========================================================
# API: promocodes
# =========================================================

@bp.post("/api/promo/create")
@login_required
def api_promo_create():
    js = request.get_json(silent=True) or {}
    code = (js.get("code") or "").strip().upper() or _rand_code()
    amount = float(js.get("amount") or 0)
    currency = (js.get("currency_key") or os.getenv("POINTS_KEY", "rubs")).strip()
    realm = (js.get("realm") or "").strip() or None
    kit_id = js.get("kit_id")
    uses = int(js.get("uses") or 1)
    expires_at = (js.get("expires_at") or "").strip() or None
    created_by = "admin"  # todo: взять из сессии

    if amount <= 0 and not kit_id:
        return jsonify({"ok": False, "error": "amount > 0 or kit_id required"}), 400

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO promo_codes(code, amount, currency_key, realm, kit_id, uses_total, uses_left, expires_at, created_by)
            VALUES (?,?,?,?,?,?,?, ?, ?)
            ON DUPLICATE KEY UPDATE
              amount=VALUES(amount),
              currency_key=VALUES(currency_key),
              realm=VALUES(realm),
              kit_id=VALUES(kit_id),
              uses_total=VALUES(uses_total),
              uses_left=VALUES(uses_left),
              expires_at=VALUES(expires_at),
              created_by=VALUES(created_by)
            """,
            (code, amount, currency, realm, int(kit_id) if kit_id else None, uses, uses, expires_at, created_by),
        )
        conn.commit()
    return jsonify({"ok": True, "data": {"code": code}})

@bp.get("/api/promo/list")
@login_required
def api_promo_list():
    with get_db_connection() as conn:
        rows = _rows(
            conn,
            """
            SELECT p.id, p.code, p.amount, p.currency_key, p.realm, p.kit_id,
                   p.uses_total, p.uses_left, p.expires_at, p.created_by, p.created_at,
                   k.name AS kit_name
            FROM promo_codes p
            LEFT JOIN promo_kits k ON k.id = p.kit_id
            ORDER BY p.id DESC
            LIMIT 100
            """,
        )
    return jsonify({"ok": True, "data": rows})

# Регистрация под /admin
admin_bp.register_blueprint(bp)
