
from flask import Blueprint, render_template, request, jsonify, current_app, g, session
from flask_login import login_required
import os
import json
import random
import re
from datetime import datetime
from typing import Optional, Iterable, Any, Sequence, Dict, List, Tuple

from ...database import get_db_connection, MySQLConnection, init_db

bp = Blueprint("admin_gameservers_promocode", __name__)

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
    for key in ("username", "name", "login"):
        if isinstance(getattr(g, "user", None), dict) and key in g.user:
            return str(g.user[key])
        if key in session:
            return str(session[key])
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
    text = _JSONC_BLOCK.sub("", _JSONC_LINE.sub("", text))
    return json.loads(text or "[]")

def _try_open_candidates(candidates: List[str]) -> Tuple[Optional[str], Optional[list]]:
    tried: List[str] = []
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

# -------- schema guard --------
_SCHEMA_READY = False
def _ensure_schema(conn: MySQLConnection) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    try:
        init_db(conn)
        _ensure_groups_table(conn)
        _SCHEMA_READY = True
    except Exception as e:
        current_app.logger.error("init_db failed: %s", e)

def _ensure_groups_table(conn: MySQLConnection) -> None:
    # максимально безопасно: IF NOT EXISTS
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promo_groups (
          id INT AUTO_INCREMENT PRIMARY KEY,
          code_id INT NOT NULL,
          group_name VARCHAR(191) NOT NULL,
          temp_seconds INT NULL,
          context_server VARCHAR(64) NULL,
          context_world  VARCHAR(64) NULL,
          priority INT NOT NULL DEFAULT 1,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          INDEX idx_code (code_id),
          CONSTRAINT fk_groups_code FOREIGN KEY (code_id) REFERENCES promo_codes(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    conn.commit()

# =========================================================
# UI
# =========================================================

@bp.get("/")
@login_required
def ui_index():
    return render_template("admin/gameservers/promocode/index.html")

# =========================================================
# API: items catalog
# =========================================================

@bp.get("/api/items/vanilla")
@login_required
def api_items_vanilla():
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
        out.append({"id": iid, "name": name, "icon": f"{base}/{iid}.png", "ns": "minecraft"})
    return _ok(out)

@bp.get("/api/items/custom")
@login_required
def api_items_custom():
    rel = "data/itemsadder-items.json"
    candidates = [f"static/{rel}", rel]
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
        out.append({"id": iid, "name": name, "icon": f"{base}/{ns}.{iid}.png", "ns": ns, "full": f"{ns}:{iid}"})
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
# API: promocodes (CRUD + details)
# =========================================================

@bp.post("/api/promo/create")
@login_required
def api_promo_create():
    js = request.get_json(silent=True) or {}
    code = (js.get("code") or "").strip().upper() or _rand_code()
    fields = {
        "code": code,
        "enabled": int(js.get("enabled") or 1),
        "realm": (js.get("realm") or "").strip() or None,
        "expires_at": (js.get("expires_at") or "").strip() or None,
        "amount": js.get("amount"),
        "currency_key": (js.get("currency_key") or "").strip() or None,
        "uses_total": js.get("uses_total"),
        "per_player_uses": js.get("per_player_uses"),
        "cooldown_seconds": js.get("cooldown_seconds"),
        "kit_id": js.get("kit_id"),
        "created_by": _actor_name(),
    }
    cols, vals = zip(*fields.items())
    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute(
            f"""
            INSERT INTO promo_codes({','.join(cols)})
            VALUES ({','.join('?' for _ in cols)})
            """,
            tuple(vals),
        )
        conn.commit()
        pid = conn.lastrowid
    return _ok({"id": pid, "code": code}, id=pid, code=code)

@bp.post("/api/promo/update")
@login_required
def api_promo_update():
    js = request.get_json(silent=True) or {}
    pid = js.get("id")
    if not pid:
        return _err("id required")
    fields = {
        "code": (js.get("code") or "").strip().upper(),
        "enabled": int(js.get("enabled") or 1),
        "realm": (js.get("realm") or "").strip() or None,
        "expires_at": (js.get("expires_at") or "").strip() or None,
        "amount": js.get("amount"),
        "currency_key": (js.get("currency_key") or "").strip() or None,
        "uses_total": js.get("uses_total"),
        "per_player_uses": js.get("per_player_uses"),
        "cooldown_seconds": js.get("cooldown_seconds"),
        "kit_id": js.get("kit_id"),
    }
    sets, params = [], []
    for k, v in fields.items():
        if v is None and k in ("uses_total", "per_player_uses", "cooldown_seconds", "kit_id", "expires_at", "realm", "currency_key"):
            sets.append(f"{k}=NULL")
        elif v is not None and v != "":
            sets.append(f"{k}=?")
            params.append(v)
    if not sets:
        return _ok()
    params.append(int(pid))
    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute(f"UPDATE promo_codes SET {', '.join(sets)} WHERE id=?", tuple(params))
        conn.commit()
    return _ok()

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

@bp.get("/api/promo/details")
@login_required
def api_promo_details():
    """Детальная инфа по id (или code), вместе с именем кита."""
    pid = request.args.get("id")
    code = (request.args.get("code") or "").strip().upper()
    if not pid and not code:
        return _err("id or code required")

    with get_db_connection() as conn:
        _ensure_schema(conn)
        if pid:
            p = _row(
                conn,
                """
                SELECT p.*, k.name AS kit_name
                FROM promo_codes p
                LEFT JOIN promo_kits k ON k.id = p.kit_id
                WHERE p.id=?
                """,
                (int(pid),),
            )
        else:
            p = _row(
                conn,
                """
                SELECT p.*, k.name AS kit_name
                FROM promo_codes p
                LEFT JOIN promo_kits k ON k.id = p.kit_id
                WHERE p.code=?
                """,
                (code,),
            )
        if not p:
            return _err("not found", 404)
        return _ok(p)

@bp.get("/api/promo/info")
@login_required
def api_promo_info():
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
                   p.enabled, p.uses_total, p.uses_left, p.expires_at, p.created_by, p.created_at,
                   k.name AS kit_name
            FROM promo_codes p
            LEFT JOIN promo_kits k ON k.id = p.kit_id
            ORDER BY p.id DESC
            LIMIT 500
            """,
        )
    return _ok(rows)