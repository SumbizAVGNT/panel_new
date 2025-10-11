from __future__ import annotations

import os
import json
import random
import string
from typing import Optional, Iterable

from flask import Blueprint, render_template, request, jsonify, current_app, send_from_directory

from ...decorators import login_required
from ...database import get_db_connection, MySQLConnection
from . import admin_bp

bp = Blueprint("promocode", __name__, url_prefix="/promocode")

# --------- utils ----------
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

# --------- HTML ----------
@bp.get("/")
@login_required
def ui_index():
    return render_template("admin/promocode/index.html")

# --------- API: items catalog ----------
@bp.get("/api/items/vanilla")
@login_required
def api_items_vanilla():
    """
    Возвращает список ванильных предметов 1.20.6: [{id, name, icon}]
    Источник — статический JSON: /static/data/vanilla-items-1.20.6.json
    Формат файла:
    [
      {"id":"diamond_sword","name":"Diamond Sword"},
      {"id":"netherite_pickaxe","name":"Netherite Pickaxe"},
      ...
    ]
    """
    try:
        path = current_app.root_path
        fpath = os.path.join(path, "static", "data", "vanilla-items-1.20.6.json")
        with open(fpath, "r", encoding="utf-8") as f:
            items = json.load(f) or []
    except Exception as e:
        current_app.logger.warning("vanilla items file missing: %s", e)
        items = []

    base = "/static/mc/1.20.6/items"
    out = []
    for it in items:
        iid = (it.get("id") or "").strip()
        if not iid:
            continue
        out.append({
            "id": iid,
            "name": it.get("name") or iid.replace("_", " ").title(),
            "icon": f"{base}/{iid}.png",
            "ns": "minecraft",
        })
    return jsonify({"ok": True, "data": out})

@bp.get("/api/items/custom")
@login_required
def api_items_custom():
    """
    Кастомные предметы из ItemsAdder (опционально).
    Ожидается файл /static/data/itemsadder-items.json:
    [{"id":"itemsadder:my_sword","name":"My Sword"}, ...]
    Если нет — вернём пусто.
    """
    try:
        path = current_app.root_path
        fpath = os.path.join(path, "static", "data", "itemsadder-items.json")
        with open(fpath, "r", encoding="utf-8") as f:
            items = json.load(f) or []
    except Exception:
        items = []

    base = "/static/mc/itemsadder"
    out = []
    for it in items:
        nid = (it.get("id") or "").strip()
        if not nid:
            continue
        # split ns:id
        if ":" in nid:
            ns, iid = nid.split(":", 1)
        else:
            ns, iid = "itemsadder", nid
        out.append({
            "id": iid,
            "name": it.get("name") or iid.replace("_", " ").title(),
            "icon": f"{base}/{ns}.{iid}.png",
            "ns": ns,
            "full": f"{ns}:{iid}",
        })
    return jsonify({"ok": True, "data": out})

# --------- API: kits ----------
@bp.get("/api/kits/list")
@login_required
def api_kits_list():
    with get_db_connection() as conn:
        kits = _rows(conn, "SELECT id, name, description, created_at FROM promo_kits ORDER BY id DESC")
        for k in kits:
            items = _rows(conn, "SELECT id, namespace, item_id, amount, display_name, enchants_json, nbt_json, slot FROM promo_kit_items WHERE kit_id = ? ORDER BY COALESCE(slot, 999), id", (k["id"],))
            for it in items:
                for key in ("enchants_json", "nbt_json"):
                    try:
                        it[key[:-5]+"s"] = json.loads(it[key] or "[]")
                    except Exception:
                        it[key[:-5]+"s"] = []
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
                "INSERT INTO promo_kit_items(kit_id, namespace, item_id, amount, display_name, enchants_json, nbt_json, slot) "
                "VALUES (?,?,?,?,?,?,?,?)",
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

# --------- API: promocodes ----------
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
    created_by = "admin"  # возьми из сессии, если нужно

    if amount <= 0 and not kit_id:
        return jsonify({"ok": False, "error": "amount > 0 or kit_id required"}), 400

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO promo_codes(code, amount, currency_key, realm, kit_id, uses_total, uses_left, expires_at, created_by)
            VALUES (?,?,?,?,?,?,?, ?, ?)
            ON DUPLICATE KEY UPDATE amount=VALUES(amount), currency_key=VALUES(currency_key),
                realm=VALUES(realm), kit_id=VALUES(kit_id), uses_total=VALUES(uses_total),
                uses_left=VALUES(uses_left), expires_at=VALUES(expires_at), created_by=VALUES(created_by)
            """,
            (code, amount, currency, realm, int(kit_id) if kit_id else None, uses, uses, expires_at, created_by),
        )
        conn.commit()
    return jsonify({"ok": True, "data": {"code": code}})

@bp.get("/api/promo/list")
@login_required
def api_promo_list():
    with get_db_connection() as conn:
        rows = _rows(conn, """
        SELECT p.id, p.code, p.amount, p.currency_key, p.realm, p.kit_id, p.uses_total, p.uses_left, p.expires_at, p.created_by, p.created_at,
               k.name AS kit_name
        FROM promo_codes p
        LEFT JOIN promo_kits k ON k.id = p.kit_id
        ORDER BY p.id DESC
        LIMIT 100
        """)
    return jsonify({"ok": True, "data": rows})

# регистрируем под /admin
admin_bp.register_blueprint(bp)
