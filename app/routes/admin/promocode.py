# app/routes/admin/promocode.py
from __future__ import annotations

import os
import re
import json
import random
from typing import Iterable, List

from flask import Blueprint, render_template, request, jsonify, current_app

from ...decorators import login_required
from ...database import get_db_connection, MySQLConnection
from . import admin_bp

bp = Blueprint("promocode", __name__, url_prefix="/promocode")

# --------- utils ----------
def _rand_code(n: int = 10) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(max(4, n)))

def _rows(conn: MySQLConnection, sql: str, params: Iterable = ()):
    return conn.query_all(sql, params)

def _first_existing(paths: List[str]) -> str | None:
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return None

def _strip_json_comments(text: str) -> str:
    # Убираем // ... и /* ... */ для “человеческих” списков
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"^\s*//.*?$", "", text, flags=re.M)
    return text

def _load_items_from_static(rel_path: str) -> list[dict]:
    """
    Пытаемся открыть файл:
      1) через current_app.open_resource('static/...') — самый надёжный вариант
      2) через current_app.static_folder / root_path (fallback)
    Возвращаем [] при ошибке (и логируем причину).
    """
    # 1) Надёжный путь: открыть ресурс относительно корня приложения
    # rel_path сюда передаём вида "data/vanilla-items-1.20.6.json"
    resource_candidates = [
        os.path.join("static", rel_path),   # "static/data/xxx.json"
        rel_path,                           # "data/xxx.json" (на случай, если лежит рядом с app/)
    ]
    for res in resource_candidates:
        try:
            with current_app.open_resource(res, "r", encoding="utf-8") as f:
                raw = f.read()
            return json.loads(_strip_json_comments(raw)) or []
        except FileNotFoundError:
            continue
        except Exception as e:
            current_app.logger.warning("Items file load error via open_resource '%s': %s", res, e)
            return []

    # 2) Fallback: пробуем абсолютные пути
    file_candidates = [
        os.path.join(current_app.static_folder or "", rel_path),
        os.path.join(current_app.root_path, "static", rel_path),
        os.path.join(os.path.dirname(current_app.root_path), "static", rel_path),
    ]
    for p in file_candidates:
        try:
            with open(p, "r", encoding="utf-8") as f:
                raw = f.read()
            return json.loads(_strip_json_comments(raw)) or []
        except FileNotFoundError:
            continue
        except Exception as e:
            current_app.logger.warning("Items file load error at %s: %s", p, e)
            return []

    current_app.logger.warning(
        "Items file not found. Tried open_resource: %s; and files: %s",
        ", ".join(resource_candidates),
        ", ".join(file_candidates),
    )
    return []


# --------- HTML ----------
@bp.get("/")
@login_required
def ui_index():
    # корректный путь шаблона (включает partials и скрипты)
    return render_template("admin/gameservers/promocode/index.html")

# --------- API: items catalog ----------
@bp.get("/api/items/vanilla")
@login_required
def api_items_vanilla():
    """
    Список ванильных предметов 1.20.6: [{id, name, icon, ns}]
    Берётся из static/data/vanilla-items-1.20.6.json
    """
    items = _load_items_from_static(os.path.join("data", "vanilla-items-1.20.6.json"))

    base = "/static/mc/1.20.6/items"
    out = []
    for it in items:
        iid = (it.get("id") or "").strip()
        if not iid:
            continue
        out.append({
            "id": iid,
            "name": (it.get("name") or "").strip() or iid.replace("_", " ").title(),
            "icon": f"{base}/{iid}.png",
            "ns": "minecraft",
        })
    return jsonify({"ok": True, "data": out})

@bp.get("/api/items/custom")
@login_required
def api_items_custom():
    """
    Кастомные предметы из ItemsAdder.
    Файл: static/data/itemsadder-items.json
    Элементы вида {"id":"itemsadder:my_sword","name":"My Sword"}
    """
    items = _load_items_from_static(os.path.join("data", "itemsadder-items.json"))

    base = "/static/mc/itemsadder"
    out = []
    for it in items:
        full = (it.get("id") or "").strip()
        if not full:
            continue
        if ":" in full:
            ns, iid = full.split(":", 1)
        else:
            ns, iid = "itemsadder", full
        out.append({
            "id": iid,
            "name": (it.get("name") or "").strip() or iid.replace("_", " ").title(),
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
        kits = _rows(conn, """
            SELECT id, name, description, created_at
            FROM promo_kits
            ORDER BY id DESC
        """)
        for k in kits:
            items = _rows(conn, """
                SELECT id, namespace, item_id, amount, display_name, enchants_json, nbt_json, slot
                FROM promo_kit_items
                WHERE kit_id = ?
                ORDER BY COALESCE(slot, 999), id
            """, (k["id"],))
            for it in items:
                try:
                    it["enchants"] = json.loads(it.get("enchants_json") or "[]")
                except Exception:
                    it["enchants"] = []
                try:
                    it["nbt"] = json.loads(it.get("nbt_json") or "[]")
                except Exception:
                    it["nbt"] = []
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
            ench = json.dumps(it.get("enchants") or [], ensure_ascii=False)
            nbt = json.dumps(it.get("nbt") or [], ensure_ascii=False)
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
    try:
        amount = float(js.get("amount") or 0)
    except Exception:
        amount = 0.0
    currency = (js.get("currency_key") or os.getenv("POINTS_KEY", "rubs")).strip()
    realm = (js.get("realm") or "").strip() or None
    kit_id = js.get("kit_id")
    uses = int(js.get("uses") or 1)
    expires_at = (js.get("expires_at") or "").strip() or None
    created_by = "admin"  # при желании подставь из сессии

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

# Регистрируем под /admin
admin_bp.register_blueprint(bp)
