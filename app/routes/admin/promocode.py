# app/routes/admin/promocode.py
from __future__ import annotations

import os
import re
import json
import random
from datetime import datetime
from typing import Optional, Iterable, List, Tuple, Dict

from flask import Blueprint, render_template, request, jsonify, current_app, g, session

from ...decorators import login_required
from ...database import (
    get_db_connection,
    get_default_connection,
    MySQLConnection,
    init_db,
    get_luckperms_connection,
)
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
        try:
            it["enchants"] = json.loads(it.get("enchants_json") or "[]")
        except Exception:
            it["enchants"] = []
        try:
            it["nbt"] = json.loads(it.get("nbt_json") or "[]")
        except Exception:
            it["nbt"] = []
    return items

# -------- schema guard --------
_SCHEMA_READY = False
def _ensure_schema(conn: MySQLConnection) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    try:
        init_db(conn)  # создаёт все таблицы, включая promo_kits / promo_kit_items / promo_codes / promo_code_groups ...
        _SCHEMA_READY = True
    except Exception as e:
        current_app.logger.error("init_db failed: %s", e)

# =========================================================
# LuckPerms helpers / config
# =========================================================

LP_PREFIX = (
    os.getenv("LUCKPERMS_TABLE_PREFIX")
    or os.getenv("LUCKPERMS_PREFIX")
    or "luckperms_"
).strip("`")

def _lp_tbl(core: str) -> str:
    return f"`{LP_PREFIX}{core}`"

def _to_dashed_uuid(u: str) -> str:
    u = (u or "").replace("-", "").lower()
    if len(u) == 32:
        return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"
    return u

def _promo_groups(conn: MySQLConnection, code_id: int) -> List[dict]:
    return _rows(
        conn,
        """
        SELECT group_name,
               temp_seconds,
               COALESCE(NULLIF(context_server,''),'global') AS context_server,
               COALESCE(NULLIF(context_world,''),'global')  AS context_world,
               priority
        FROM promo_code_groups
        WHERE code_id = ?
        ORDER BY priority ASC, id ASC
        """,
        (int(code_id),),
    )

def _lp_apply_groups(uuid: str, groups: List[dict]) -> int:
    """Добавляет узлы group.<name> пользователю в LuckPerms (временные/перманентные, с контекстами)."""
    import time
    if not uuid or not groups:
        return 0
    uu = _to_dashed_uuid(uuid)
    now = int(time.time())

    rows = []
    for g in groups:
        name = (g.get("group_name") or "").strip()
        if not name:
            continue
        server = (g.get("context_server") or "global").strip() or "global"
        world  = (g.get("context_world")  or "global").strip() or "global"
        temp   = g.get("temp_seconds")
        try:
            expiry = int(now + int(temp)) if temp is not None and str(temp) != "" else 0
        except Exception:
            expiry = 0
        rows.append((uu, f"group.{name}", 1, server, world, expiry, ""))

    if not rows:
        return 0

    try:
        with get_luckperms_connection() as lpc:
            lpc.executemany(
                f"""
                INSERT INTO {_lp_tbl('user_permissions')}
                (uuid, permission, value, server, world, expiry, contexts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
    except Exception as e:
        current_app.logger.error("LuckPerms apply failed: %s", e)
        return 0

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
    """
    Create/Update kit + replace items atomically.
    Body: { id?: int, name: str, description?: str, items: [
      {namespace, item_id, amount, display_name, enchants, nbt, slot}
    ] }
    """
    js = request.get_json(silent=True) or {}
    kit_id = js.get("id")
    name = (js.get("name") or "").strip()
    description = (js.get("description") or "").strip() or None
    raw_items = js.get("items") or []

    if not name:
        return jsonify({"ok": False, "error": "name is required"}), 400

    with get_default_connection() as conn:
        _ensure_schema(conn)

        # 1) upsert kit
        if kit_id:
            conn.execute(
                "UPDATE `promo_kits` SET `name` = ?, `description` = ? WHERE `id` = ?",
                (name, description, int(kit_id)),
            )
            row = conn.query_one("SELECT `id` FROM `promo_kits` WHERE `id` = ? LIMIT 1", (int(kit_id),))
            if not row:
                conn.execute(
                    "INSERT INTO `promo_kits` (`id`, `name`, `description`) VALUES (?, ?, ?)",
                    (int(kit_id), name, description),
                )
        else:
            conn.execute(
                "INSERT INTO `promo_kits` (`name`, `description`) VALUES (?, ?)",
                (name, description),
            )
            rid = conn.query_one("SELECT LAST_INSERT_ID() AS id") or {}
            kit_id = int(rid.get("id") or 0)

        if not kit_id:
            return _err("Failed to obtain kit id", 500)

        # 2) replace items
        conn.execute("DELETE FROM `promo_kit_items` WHERE `kit_id` = ?", (int(kit_id),))

        items_params = []
        for it in raw_items:
            if not it:
                continue
            ns = (it.get("namespace") or it.get("ns") or "minecraft").strip().lower()
            iid = str(it.get("item_id") or it.get("id") or "").strip()
            if not iid:
                continue
            amount = int(it.get("amount") or it.get("qty") or 1)
            if amount < 1:
                amount = 1
            if amount > 64:
                amount = 64
            dname = (it.get("display_name") or it.get("display") or "").strip() or None
            ench = it.get("enchants") or it.get("enchants_json") or []
            nbt  = it.get("nbt") or it.get("nbt_json") or []
            try:
                ench_json = json.dumps(ench, ensure_ascii=False)
            except Exception:
                ench_json = "[]"
            try:
                nbt_json = json.dumps(nbt, ensure_ascii=False)
            except Exception:
                nbt_json = "[]"
            slot = it.get("slot")
            try:
                slot = int(slot)
            except Exception:
                slot = None
            if slot is None or slot < 0 or slot > 26:
                slot = len(items_params)
                if slot > 26:
                    break

            items_params.append((
                int(kit_id), slot, ns, iid, amount, dname, ench_json, nbt_json
            ))

        if items_params:
            conn.executemany(
                "INSERT INTO `promo_kit_items` "
                "(`kit_id`,`slot`,`namespace`,`item_id`,`amount`,`display_name`,`enchants_json`,`nbt_json`) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                items_params
            )

        return jsonify({"ok": True, "data": {"id": int(kit_id), "name": name}})

@bp.post("/api/kits/delete")
@login_required
def api_kits_delete():
    js = request.get_json(silent=True) or {}
    kit_id = js.get("id")
    if not kit_id:
        return jsonify({"ok": False, "error": "id is required"}), 400

    with get_default_connection() as conn:
        _ensure_schema(conn)
        conn.execute("DELETE FROM `promo_kit_items` WHERE `kit_id` = ?", (int(kit_id),))
        conn.execute("DELETE FROM `promo_kits` WHERE `id` = ?", (int(kit_id),))
        return jsonify({"ok": True, "data": True})

# =========================================================
# API: promocodes (CRUD + details)
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
        pid = int(_row(conn, "SELECT id FROM promo_codes WHERE code=?", (code,))["id"])
    return _ok({"id": pid, "code": code})

@bp.post("/api/promo/update")
@login_required
def api_promo_update():
    js = request.get_json(silent=True) or {}
    pid = js.get("id")
    if not pid:
        return _err("id required")
    fields: Dict[str, object] = {
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
            conn.execute("DELETE FROM promo_code_groups WHERE code_id=?", (int(pid),))
            conn.execute("DELETE FROM promo_codes WHERE id=?", (int(pid),))
        else:
            conn.execute(
                "DELETE FROM promo_code_groups WHERE code_id=(SELECT id FROM promo_codes WHERE code=? LIMIT 1)",
                (code,),
            )
            conn.execute("DELETE FROM promo_codes WHERE code=?", (code,))
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

# =========================================================
# API: LuckPerms groups for a code
# =========================================================

@bp.get("/api/promo/groups/list")
@login_required
def api_promo_groups_list():
    code_id = request.args.get("code_id", type=int)
    if not code_id:
        return _err("code_id required")
    with get_db_connection() as conn:
        _ensure_schema(conn)
        rows = _rows(
            conn,
            """
            SELECT id, code_id, group_name, temp_seconds, context_server, context_world, priority, created_at
            FROM promo_code_groups
            WHERE code_id=?
            ORDER BY priority ASC, id ASC
            """,
            (code_id,),
        )
    return _ok(rows)

@bp.post("/api/promo/groups/save")
@login_required
def api_promo_groups_save():
    js = request.get_json(silent=True) or {}
    code_id = js.get("code_id")
    groups = js.get("groups") or []
    if not code_id:
        return _err("code_id required")

    bulk = []
    for i, g in enumerate(groups, start=1):
        name = (g.get("group_name") or "").strip()
        if not name:
            continue
        if len(name) > 36:
            name = name[:36]
        tmp = g.get("temp_seconds")
        try:
            tmp = int(tmp) if tmp is not None and str(tmp) != "" else None
        except Exception:
            tmp = None
        srv = (g.get("context_server") or "").strip() or "global"
        wrd = (g.get("context_world") or "").strip() or "global"
        pr  = g.get("priority")
        try:
            pr = int(pr) if pr is not None and str(pr) != "" else i - 1
        except Exception:
            pr = i - 1
        bulk.append((int(code_id), name, tmp, srv, wrd, pr))

    with get_db_connection() as conn:
        _ensure_schema(conn)
        conn.execute("DELETE FROM promo_code_groups WHERE code_id=?", (int(code_id),))
        if bulk:
            conn.executemany(
                """
                INSERT INTO promo_code_groups(code_id, group_name, temp_seconds, context_server, context_world, priority)
                VALUES (?,?,?,?,?,?)
                """,
                bulk,
            )
    return _ok({"count": len(bulk)})

# =========================================================
# API: redemptions (preview + redeem + history)
# =========================================================

def _load_code_for_update(conn: MySQLConnection, code: str) -> Optional[dict]:
    return _row(conn, "SELECT * FROM promo_codes WHERE code=? FOR UPDATE", (code,))

def _validate_code_row(p: dict, *, realm: Optional[str]) -> Optional[str]:
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
        # превью LuckPerms-групп
        out["groups"] = _promo_groups(conn, int(p["id"]))
        return _ok(out)

@bp.post("/api/promo/redeem")
@login_required
def api_promo_redeem():
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

        p = _load_code_for_update(conn, code)
        err = _validate_code_row(p, realm=realm)
        if err:
            return _err(err, 400, code=code)

        code_id = int(p["id"])
        amount = float(p.get("amount") or 0.0)
        currency_key = p.get("currency_key")
        kit_id = p.get("kit_id")
        kit_items = _kit_items(conn, int(kit_id)) if kit_id else []

        # применим группы LP
        groups = _promo_groups(conn, code_id)
        lp_applied = _lp_apply_groups(uuid, groups)

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
        conn.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE id = ? AND uses_left > 0", (code_id,))

        return _ok({
            "code": code,
            "code_id": code_id,
            "uses_left": int(p["uses_left"]) - 1,
            "amount": amount,
            "currency_key": currency_key,
            "kit_id": kit_id,
            "kit_items": kit_items,
            "groups": groups,
            "lp_applied": lp_applied,
        })

@bp.get("/api/promo/redemptions")
@login_required
def api_promo_redemptions():
    code = (request.args.get("code") or "").strip().upper()
    code_id = request.args.get("code_id", type=int)
    uuid = (request.args.get("uuid") or "").strip()
    username = (request.args.get("username") or "").strip()
    q = (request.args.get("q") or "").strip()
    limit = max(1, min(500, int(request.args.get("limit") or 100)))

    where = []
    params: List[object] = []
    if code_id:
        where.append("r.code_id = ?")
        params.append(code_id)
    elif code:
        where.append("r.code_id = (SELECT id FROM promo_codes WHERE code = ?)")
        params.append(code)
    if uuid:
        where.append("r.uuid = ?")
        params.append(uuid)
    if username:
        where.append("r.username = ?")
        params.append(username)
    if q:
        where.append("(r.username LIKE ? OR r.uuid LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])

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

# короткий алиас под фронтовой путь /api/promo/reds
@bp.get("/api/promo/reds")
@login_required
def api_promo_reds_alias():
    return api_promo_redemptions()

# =========================================================
# Register under /admin
# =========================================================

admin_bp.register_blueprint(bp)

# UI aliases
admin_bp.add_url_rule("/gameservers/promocode", view_func=ui_index, methods=["GET"], endpoint="gameservers_promocode_index_no_slash")
admin_bp.add_url_rule("/gameservers/promocode/", view_func=ui_index, methods=["GET"], endpoint="gameservers_promocode_index")

# items
admin_bp.add_url_rule("/gameservers/promocode/api/items/vanilla", view_func=api_items_vanilla, methods=["GET"], endpoint="gameservers_promocode_api_items_vanilla")
admin_bp.add_url_rule("/gameservers/promocode/api/items/custom",  view_func=api_items_custom,  methods=["GET"], endpoint="gameservers_promocode_api_items_custom")

# kits
admin_bp.add_url_rule("/gameservers/promocode/api/kits/list",   view_func=api_kits_list,   methods=["GET"],  endpoint="gameservers_promocode_api_kits_list")
admin_bp.add_url_rule("/gameservers/promocode/api/kits/save",   view_func=api_kits_save,   methods=["POST"], endpoint="gameservers_promocode_api_kits_save")
admin_bp.add_url_rule("/gameservers/promocode/api/kits/delete", view_func=api_kits_delete, methods=["POST"], endpoint="gameservers_promocode_api_kits_delete")

# promo CRUD + details
admin_bp.add_url_rule("/gameservers/promocode/api/promo/create",  view_func=api_promo_create,  methods=["POST"], endpoint="gameservers_promocode_api_promo_create")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/update",  view_func=api_promo_update,  methods=["POST"], endpoint="gameservers_promocode_api_promo_update")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/delete",  view_func=api_promo_delete,  methods=["POST"], endpoint="gameservers_promocode_api_promo_delete")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/details", view_func=api_promo_details, methods=["GET"],  endpoint="gameservers_promocode_api_promo_details")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/info",    view_func=api_promo_info,    methods=["GET"],  endpoint="gameservers_promocode_api_promo_info")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/list",    view_func=api_promo_list,    methods=["GET"],  endpoint="gameservers_promocode_api_promo_list")

# promo groups
admin_bp.add_url_rule("/gameservers/promocode/api/promo/groups/list", view_func=api_promo_groups_list, methods=["GET"],  endpoint="gameservers_promocode_api_promo_groups_list")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/groups/save", view_func=api_promo_groups_save, methods=["POST"], endpoint="gameservers_promocode_api_promo_groups_save")

# redemptions
admin_bp.add_url_rule("/gameservers/promocode/api/promo/redemptions", view_func=api_promo_redemptions, methods=["GET"], endpoint="gameservers_promocode_api_promo_redemptions")
admin_bp.add_url_rule("/gameservers/promocode/api/promo/reds",        view_func=api_promo_reds_alias,  methods=["GET"], endpoint="gameservers_promocode_api_promo_reds")
