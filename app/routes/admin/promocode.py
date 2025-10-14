# app/routes/admin/promocode.py
from __future__ import annotations

import os
import re
import json
import random
from datetime import datetime
from typing import Optional, Iterable, List, Tuple

from flask import Blueprint, render_template, request, jsonify, current_app, g, session

from ...decorators import login_required
from ...database import get_db_connection, MySQLConnection, init_db
from . import admin_bp

bp = Blueprint("promocode", __name__, url_prefix="/promocode")

# =========================================================
# utils
# =========================================================

def _ok(data=None, **extra):
    res = {"ok": True}
    if data is not None:
        res["data"] = data
    res.update(extra)
    return jsonify(res)

def _err(msg: str, code: int = 400, **extra):
    res = {"ok": False, "error": msg}
    if extra:
        res.update(extra)
    return jsonify(res), code

def _actor_name() -> str:
    # пробуем вытащить того, кто создаёт код
    for key in ("username", "name", "login"):
        if isinstance(getattr(g, "user", None), dict) and key in g.user:
            return str(g.user[key])
        if key in session:
            return str(session[key])
    # иногда g.user может быть объектом с .username
    u = getattr(g, "user", None)
    if u is not None:
        for key in ("username", "name", "login"):
            if hasattr(u, key):
                return str(getattr(u, key))
    return "admin"

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
        for col in ("enchants_json", "nbt_json"):
            try:
                it[col[:-5] + "s"] = json.loads(it[col] or "[]")
            except Exception:
                it[col[:-5] + "s"] = []
    return items

# -------- ленивое создание схемы (idempotent) --------
_SCHEMA_READY = False
def _ensure_schema(conn: MySQLConnection) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    try:
        init_db(conn)  # создаст promo_* таблицы и остальную схему панели, если их нет
        _SCHEMA_READY = True
    except Exception as e:
        current_app.logger.error("init_db failed: %s", e)
        # даже если не удалось — пусть дальнейшая операция покажет понятную ошибку

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
    return _ok(out)

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

        bulk = []
        pos = 0
        for it in items:
            ns = (it.get("ns") or it.get("namespace") or "minecraft").strip()
            iid = (it.get("id") or it.get("item_id") or "").strip()
            if not iid:
                continue
            # поддержим и amount, и qty
            amount = int(it.get("amount") or it.get("qty") or 1)
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
        # совместимость: id и в data, и на корне
        return _ok({"id": kid}, id=kid)

@bp.post("/api/kits/delete")
@login_required
def api_kits_delete():
    js = request.get_json(silent=True) or {}
    kit_id = js.get("id")
    if not kit_id:
        return _err("id is required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute("DELETE FROM promo_kit_items WHERE kit_id=?", (int(kit_id),))
        conn.execute("DELETE FROM promo_kits WHERE id=?", (int(kit_id),))
        conn.commit()
    return _ok()

# =========================================================
# API: promocodes (CRUD + preview/redeem)
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
    created_by = _actor_name()

    if amount <= 0 and not kit_id:
        return _err("amount > 0 or kit_id required")

    # валидация kit_id (если дан)
    with get_db_connection() as conn:
        _ensure_schema(conn)
        if kit_id:
            kit = _row(conn, "SELECT id FROM promo_kits WHERE id=?", (int(kit_id),))
            if not kit:
                return _err("kit_id not found")

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
    return _ok({"code": code})

@bp.post("/api/promo/bulk_create")
@login_required
def api_promo_bulk_create():
    """
    Массовое создание однотипных кодов.
    Вход: {prefix?, amount?, currency_key?, realm?, kit_id?, uses?, expires_at?, count, length?}
    """
    js = request.get_json(silent=True) or {}
    count = max(1, int(js.get("count") or 1))
    length = max(6, int(js.get("length") or 10))
    prefix = (js.get("prefix") or "").strip().upper()
    amount = float(js.get("amount") or 0)
    currency = (js.get("currency_key") or os.getenv("POINTS_KEY", "rubs")).strip()
    realm = (js.get("realm") or "").strip() or None
    kit_id = js.get("kit_id")
    uses = int(js.get("uses") or 1)
    expires_at = (js.get("expires_at") or "").strip() or None
    created_by = _actor_name()

    if amount <= 0 and not kit_id:
        return _err("amount > 0 or kit_id required")

    out_codes: List[str] = []
    with get_db_connection() as conn:
        _ensure_schema(conn)
        if kit_id:
            kit = _row(conn, "SELECT id FROM promo_kits WHERE id=?", (int(kit_id),))
            if not kit:
                return _err("kit_id not found")

        # пробуем сгенерировать N уникальных
        attempts = 0
        while len(out_codes) < count and attempts < count * 10:
            attempts += 1
            code = (prefix + _rand_code(length)).upper()
            try:
                conn.execute(
                    """
                    INSERT INTO promo_codes(code, amount, currency_key, realm, kit_id, uses_total, uses_left, expires_at, created_by)
                    VALUES (?,?,?,?,?,?,?, ?, ?)
                    """,
                    (code, amount, currency, realm, int(kit_id) if kit_id else None, uses, uses, expires_at, created_by),
                )
                out_codes.append(code)
            except Exception:
                # дубликат — пробуем следующий
                pass
        conn.commit()
    return _ok({"count": len(out_codes), "codes": out_codes})

@bp.post("/api/promo/delete")
@login_required
def api_promo_delete():
    js = request.get_json(silent=True) or {}
    code = (js.get("code") or "").strip().upper()
    pid = js.get("id")
    if not code and not pid:
        return _err("code or id required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        if pid:
            conn.execute("DELETE FROM promo_codes WHERE id=?", (int(pid),))
        else:
            conn.execute("DELETE FROM promo_codes WHERE code=?", (code,))
        conn.commit()
    return _ok()

@bp.get("/api/promo/info")
@login_required
def api_promo_info():
    """Инфо по коду + предметы набора (если есть)."""
    code = (request.args.get("code") or "").strip().upper()
    if not code:
        return _err("code required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        p = _row(
            conn,
            """
            SELECT p.*, k.name AS kit_name
            FROM promo_codes p
            LEFT JOIN promo_kits k ON k.id = p.kit_id
            WHERE p.code = ?
            """,
            (code,),
        )
        if not p:
            return _err("code not found", 404)
        p["expired"] = bool(p.get("expires_at") and str(p["expires_at"]) and datetime.utcnow() > datetime.fromisoformat(str(p["expires_at"]).replace(" ", "T")))
        p["kit_items"] = _kit_items(conn, int(p["kit_id"])) if p.get("kit_id") else []
        return _ok(p)

@bp.get("/api/promo/list")
@login_required
def api_promo_list():
    with get_db_connection() as conn:
        _ensure_schema(conn)
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
    return _ok(rows)

# =========================================================
# API: redemptions (preview + redeem + history)
# =========================================================

def _load_code_for_update(conn: MySQLConnection, code: str) -> Optional[dict]:
    # берём строку под блокировку до конца транзакции
    return _row(conn, "SELECT * FROM promo_codes WHERE code=? FOR UPDATE", (code,))

def _validate_code_row(p: dict, *, realm: Optional[str]) -> Optional[str]:
    # возвращает текст ошибки или None если всё ок
    if not p:
        return "code not found"
    if int(p.get("uses_left") or 0) <= 0:
        return "no uses left"
    if p.get("expires_at"):
        try:
            exp = datetime.fromisoformat(str(p["expires_at"]).replace(" ", "T"))
            if datetime.utcnow() > exp:
                return "code expired"
        except Exception:
            pass
    if p.get("realm") and realm and str(p["realm"]).strip() != str(realm).strip():
        return "code is restricted to another realm"
    return None

@bp.post("/api/promo/redeem_preview")
@login_required
def api_promo_redeem_preview():
    """
    Проверка: можно ли применить код и что именно он выдаст.
    Вход: {code, username?, uuid?, realm?}
    Выход: {code_id, amount, currency_key, kit_id, kit_items, will_expire, uses_left_after?}
    """
    js = request.get_json(silent=True) or {}
    code = (js.get("code") or "").strip().upper()
    realm = (js.get("realm") or "").strip() or None
    if not code:
        return _err("code required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        p = _row(conn, "SELECT * FROM promo_codes WHERE code=?", (code,))
        err = _validate_code_row(p, realm=realm)
        if err:
            return _err(err, 400, code=code)

        out = {
            "code_id": int(p["id"]),
            "code": code,
            "amount": float(p["amount"] or 0),
            "currency_key": p["currency_key"],
            "kit_id": p.get("kit_id"),
            "uses_left": int(p.get("uses_left") or 0),
            "uses_total": int(p.get("uses_total") or 0),
            "realm": p.get("realm"),
            "expires_at": p.get("expires_at"),
            "kit_items": _kit_items(conn, int(p["kit_id"])) if p.get("kit_id") else [],
        }
        out["uses_left_after"] = max(0, int(out["uses_left"]) - 1)
        return _ok(out)

@bp.post("/api/promo/redeem")
@login_required
def api_promo_redeem():
    """
    Погашение кода (без реальной выдачи валюты/лутбоксов — только учёт).
    Вход: {code, username?, uuid, realm?, ip?}
    Логика:
      - SELECT ... FOR UPDATE
      - валидация (uses_left>0, не истёк, realm если задан)
      - INSERT promo_redemptions(...)
      - UPDATE promo_codes SET uses_left=uses_left-1
    """
    js = request.get_json(silent=True) or {}
    code = (js.get("code") or "").strip().upper()
    uuid = (js.get("uuid") or "").strip()
    username = (js.get("username") or "").strip() or None
    realm = (js.get("realm") or "").strip() or None
    ip = (js.get("ip") or request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()

    if not code:
        return _err("code required")
    if not uuid:
        return _err("uuid required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        # блокируем строку
        p = _load_code_for_update(conn, code)
        err = _validate_code_row(p, realm=realm)
        if err:
            return _err(err, 400, code=code)

        code_id = int(p["id"])
        amount = float(p.get("amount") or 0.0)
        currency_key = p.get("currency_key")
        kit_id = p.get("kit_id")
        kit_items = _kit_items(conn, int(kit_id)) if kit_id else []

        # запись о редемпшене
        conn.execute(
            """
            INSERT INTO promo_redemptions(code_id, uuid, username, realm, granted_amount, kit_id, granted_items_json, ip)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                code_id, uuid, username, realm,
                amount if amount > 0 else None,
                int(kit_id) if kit_id else None,
                _json(kit_items),
                ip or None,
            ),
        )
        # уменьшаем uses_left
        conn.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE id = ? AND uses_left > 0", (code_id,))
        conn.commit()

        return _ok({
            "code": code,
            "code_id": code_id,
            "uses_left": int(p["uses_left"]) - 1,
            "amount": amount,
            "currency_key": currency_key,
            "kit_id": kit_id,
            "kit_items": kit_items,
        })

@bp.get("/api/promo/redemptions")
@login_required
def api_promo_redemptions():
    """
    История погашений.
    Параметры: code?, uuid?, username?, limit?
    """
    code = (request.args.get("code") or "").strip().upper()
    uuid = (request.args.get("uuid") or "").strip()
    username = (request.args.get("username") or "").strip()
    limit = max(1, min(500, int(request.args.get("limit") or 100)))

    where = []
    params: List[object] = []
    if code:
        where.append("r.code_id = (SELECT id FROM promo_codes WHERE code = ?)")
        params.append(code)
    if uuid:
        where.append("r.uuid = ?")
        params.append(uuid)
    if username:
        where.append("r.username = ?")
        params.append(username)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with get_db_connection() as conn:
        _ensure_schema(conn)
        rows = _rows(
            conn,
            f"""
            SELECT r.id, r.code_id, (SELECT code FROM promo_codes WHERE id=r.code_id) AS code,
                   r.uuid, r.username, r.realm, r.granted_amount, r.kit_id, r.ip, r.created_at
            FROM promo_redemptions r
            {where_sql}
            ORDER BY r.id DESC
            LIMIT ?
            """,
            (*params, limit),
        )
    return _ok(rows)

# Регистрация под /admin
admin_bp.register_blueprint(bp)

# ===== Совместимые алиасы под /admin/gameservers/promocode =====
# UI может дергать эти пути — маппим их на те же view-функции

# UI
admin_bp.add_url_rule(
    "/gameservers/promocode", view_func=ui_index,
    methods=["GET"], endpoint="gameservers_promocode_index_no_slash"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/", view_func=ui_index,
    methods=["GET"], endpoint="gameservers_promocode_index"
)

# items
admin_bp.add_url_rule(
    "/gameservers/promocode/api/items/vanilla",
    view_func=api_items_vanilla, methods=["GET"],
    endpoint="gameservers_promocode_api_items_vanilla"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/items/custom",
    view_func=api_items_custom, methods=["GET"],
    endpoint="gameservers_promocode_api_items_custom"
)

# kits
admin_bp.add_url_rule(
    "/gameservers/promocode/api/kits/list",
    view_func=api_kits_list, methods=["GET"],
    endpoint="gameservers_promocode_api_kits_list"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/kits/save",
    view_func=api_kits_save, methods=["POST"],
    endpoint="gameservers_promocode_api_kits_save"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/kits/delete",
    view_func=api_kits_delete, methods=["POST"],
    endpoint="gameservers_promocode_api_kits_delete"
)

# promo
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/create",
    view_func=api_promo_create, methods=["POST"],
    endpoint="gameservers_promocode_api_promo_create"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/bulk_create",
    view_func=api_promo_bulk_create, methods=["POST"],
    endpoint="gameservers_promocode_api_promo_bulk_create"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/delete",
    view_func=api_promo_delete, methods=["POST"],
    endpoint="gameservers_promocode_api_promo_delete"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/info",
    view_func=api_promo_info, methods=["GET"],
    endpoint="gameservers_promocode_api_promo_info"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/list",
    view_func=api_promo_list, methods=["GET"],
    endpoint="gameservers_promocode_api_promo_list"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/redeem_preview",
    view_func=api_promo_redeem_preview, methods=["POST"],
    endpoint="gameservers_promocode_api_promo_redeem_preview"
)
admin_bp.add_url_rule(
    "/gameservers/promocode/api/promo/redeem",
    view_func=api_promo_redeem, methods=["POST"],
    endpoint="gameservers_promocode_api_promo_redeem"
)
