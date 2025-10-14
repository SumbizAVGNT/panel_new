# app/services/promo.py
from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ..database import get_db_connection, MySQLConnection

# =============== Exceptions ===============

class PromoError(Exception): ...
class PromoNotFound(PromoError): ...
class PromoExpired(PromoError): ...
class PromoNoUsesLeft(PromoError): ...
class PromoRealmMismatch(PromoError): ...
class KitNotFound(PromoError): ...

# =============== Models (lightweight) ===============

@dataclass
class KitItem:
    id: int
    namespace: str
    item_id: str
    amount: int
    display_name: Optional[str]
    enchants: List[dict]
    nbt: List[dict]
    slot: Optional[int]

@dataclass
class Kit:
    id: int
    name: str
    description: Optional[str]
    created_at: Optional[str]
    items: List[KitItem]

@dataclass
class PromoCode:
    id: int
    code: str
    amount: float
    currency_key: str
    realm: Optional[str]
    kit_id: Optional[int]
    uses_total: int
    uses_left: int
    expires_at: Optional[str]
    created_by: Optional[str]
    created_at: Optional[str]

# =============== Helpers ===============

def _json_dump(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "[]"

def _json_load(s: Optional[str], default: Any) -> Any:
    try:
        return json.loads(s or "") if s else default
    except Exception:
        return default

def _as_dt_utc(s: Union[str, datetime]) -> datetime:
    if isinstance(s, datetime):
        return s
    return datetime.fromisoformat(str(s).replace(" ", "T"))

def _kit_items(conn: MySQLConnection, kit_id: int) -> List[KitItem]:
    rows = conn.query_all(
        """
        SELECT id, namespace, item_id, amount, display_name, enchants_json, nbt_json, slot
        FROM promo_kit_items
        WHERE kit_id=? ORDER BY COALESCE(slot, 999), id
        """,
        (kit_id,),
    )
    out: List[KitItem] = []
    for r in rows:
        out.append(
            KitItem(
                id=int(r["id"]),
                namespace=r["namespace"],
                item_id=r["item_id"],
                amount=int(r["amount"] or 1),
                display_name=r.get("display_name"),
                enchants=_json_load(r.get("enchants_json"), []),
                nbt=_json_load(r.get("nbt_json"), []),
                slot=(int(r["slot"]) if r.get("slot") is not None else None),
            )
        )
    return out

def _promo_row_to_model(r: dict) -> PromoCode:
    return PromoCode(
        id=int(r["id"]),
        code=r["code"],
        amount=float(r.get("amount") or 0),
        currency_key=r.get("currency_key") or os.getenv("POINTS_KEY", "rubs"),
        realm=r.get("realm"),
        kit_id=(int(r["kit_id"]) if r.get("kit_id") is not None else None),
        uses_total=int(r.get("uses_total") or 0),
        uses_left=int(r.get("uses_left") or 0),
        expires_at=(str(r["expires_at"]) if r.get("expires_at") else None),
        created_by=r.get("created_by"),
        created_at=(str(r["created_at"]) if r.get("created_at") else None),
    )

# =============== Service ===============

class PromoService:
    """
    Повторно используемый сервис для промокодов и китов.
    Можно вызывать без явного соединения: каждый метод откроет своё.
    Или передать открытый conn, чтобы батчить операции в одной транзакции.
    """

    # ---------- KITS ----------

    @staticmethod
    def list_kits(conn: Optional[MySQLConnection] = None) -> List[Kit]:
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            kits = conn.query_all("SELECT id, name, description, created_at FROM promo_kits ORDER BY id DESC")
            out: List[Kit] = []
            for k in kits:
                kid = int(k["id"])
                out.append(
                    Kit(
                        id=kid,
                        name=k["name"],
                        description=k.get("description"),
                        created_at=str(k.get("created_at")) if k.get("created_at") else None,
                        items=_kit_items(conn, kid),
                    )
                )
            return out
        finally:
            if own:
                conn.close()

    @staticmethod
    def save_kit(
        *,
        kit_id: Optional[int],
        name: str,
        description: Optional[str],
        items: Iterable[dict],
        conn: Optional[MySQLConnection] = None,
    ) -> int:
        if not name.strip():
            raise ValueError("name is required")
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            if kit_id:
                conn.execute("UPDATE promo_kits SET name=?, description=? WHERE id=?", (name, description or "", int(kit_id)))
                conn.execute("DELETE FROM promo_kit_items WHERE kit_id=?", (int(kit_id),))
                kid = int(kit_id)
            else:
                conn.execute("INSERT INTO promo_kits(name, description) VALUES (?,?)", (name, description or ""))
                conn.commit()
                kid = int(conn.lastrowid)

            bulk = []
            pos = 0
            for it in items or []:
                ns = (it.get("ns") or it.get("namespace") or "minecraft").strip()
                iid = (it.get("id") or it.get("item_id") or "").strip()
                if not iid:
                    continue
                amount = int(it.get("amount") or 1)
                display_name = (it.get("display_name") or "").strip() or None
                ench = _json_dump(it.get("enchants") or [])
                nbt = _json_dump(it.get("nbt") or [])
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
            return kid
        finally:
            if own:
                conn.close()

    @staticmethod
    def delete_kit(kit_id: int, conn: Optional[MySQLConnection] = None) -> None:
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            conn.execute("DELETE FROM promo_kit_items WHERE kit_id=?", (int(kit_id),))
            conn.execute("DELETE FROM promo_kits WHERE id=?", (int(kit_id),))
            conn.commit()
        finally:
            if own:
                conn.close()

    # ---------- PROMO CODES ----------

    @staticmethod
    def get_code(code: str, conn: Optional[MySQLConnection] = None) -> PromoCode:
        code = code.strip().upper()
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            r = conn.query_one("SELECT * FROM promo_codes WHERE code=?", (code,))
            if not r:
                raise PromoNotFound("code not found")
            return _promo_row_to_model(r)
        finally:
            if own:
                conn.close()

    @staticmethod
    def list_codes(limit: int = 100, conn: Optional[MySQLConnection] = None) -> List[PromoCode]:
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            rows = conn.query_all(
                """
                SELECT p.* FROM promo_codes p
                ORDER BY p.id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            return [_promo_row_to_model(r) for r in rows]
        finally:
            if own:
                conn.close()

    @staticmethod
    def create_code(
        *,
        code: str,
        amount: float = 0.0,
        currency_key: Optional[str] = None,
        realm: Optional[str] = None,
        kit_id: Optional[int] = None,
        uses: int = 1,
        expires_at: Optional[str] = None,
        created_by: Optional[str] = None,
        conn: Optional[MySQLConnection] = None,
    ) -> str:
        if (amount or 0) <= 0 and not kit_id:
            raise ValueError("amount > 0 or kit_id required")
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            if kit_id:
                exists = conn.query_one("SELECT id FROM promo_kits WHERE id=?", (int(kit_id),))
                if not exists:
                    raise KitNotFound("kit_id not found")
            currency = (currency_key or os.getenv("POINTS_KEY", "rubs")).strip()
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
                (code.strip().upper(), float(amount or 0), currency, realm, int(kit_id) if kit_id else None,
                 int(uses), int(uses), expires_at, created_by or "admin"),
            )
            conn.commit()
            return code.strip().upper()
        finally:
            if own:
                conn.close()

    @staticmethod
    def delete_code(*, code: Optional[str] = None, promo_id: Optional[int] = None,
                    conn: Optional[MySQLConnection] = None) -> None:
        if not code and not promo_id:
            raise ValueError("code or promo_id required")
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            if promo_id:
                conn.execute("DELETE FROM promo_codes WHERE id=?", (int(promo_id),))
            else:
                conn.execute("DELETE FROM promo_codes WHERE code=?", (code.strip().upper(),))
            conn.commit()
        finally:
            if own:
                conn.close()

    @staticmethod
    def bulk_create(
        *,
        count: int,
        make_code,  # callable: (index:int)->str  или None для авто A-Z/2-9
        amount: float = 0.0,
        currency_key: Optional[str] = None,
        realm: Optional[str] = None,
        kit_id: Optional[int] = None,
        uses: int = 1,
        expires_at: Optional[str] = None,
        created_by: Optional[str] = None,
        conn: Optional[MySQLConnection] = None,
    ) -> List[str]:
        """
        make_code: если None — будет автогенерация через _rand().
        """
        import random
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

        def _rand(n=10):
            return "".join(random.choice(alphabet) for _ in range(max(6, n)))

        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        out: List[str] = []
        try:
            if kit_id:
                exists = conn.query_one("SELECT id FROM promo_kits WHERE id=?", (int(kit_id),))
                if not exists:
                    raise KitNotFound("kit_id not found")
            currency = (currency_key or os.getenv("POINTS_KEY", "rubs")).strip()
            attempts = 0
            while len(out) < int(count):
                attempts += 1
                if attempts > count * 10:
                    break
                code = (make_code(len(out)) if make_code else _rand()).upper()
                try:
                    conn.execute(
                        """
                        INSERT INTO promo_codes(code, amount, currency_key, realm, kit_id, uses_total, uses_left, expires_at, created_by)
                        VALUES (?,?,?,?,?,?,?, ?, ?)
                        """,
                        (code, float(amount or 0), currency, realm, int(kit_id) if kit_id else None,
                         int(uses), int(uses), expires_at, created_by or "admin"),
                    )
                    out.append(code)
                except Exception:
                    # дубликат? пробуем дальше
                    continue
            conn.commit()
            return out
        finally:
            if own:
                conn.close()

    # ---------- Preview & Redeem ----------

    @staticmethod
    def preview(
        *,
        code: str,
        realm: Optional[str] = None,
        conn: Optional[MySQLConnection] = None,
    ) -> Dict[str, Any]:
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            p = conn.query_one("SELECT * FROM promo_codes WHERE code=?", (code.strip().upper(),))
            if not p:
                raise PromoNotFound("code not found")
            # валидации
            if int(p.get("uses_left") or 0) <= 0:
                raise PromoNoUsesLeft("no uses left")
            if p.get("expires_at"):
                exp = _as_dt_utc(p["expires_at"])
                if datetime.utcnow() > exp:
                    raise PromoExpired("code expired")
            if p.get("realm") and realm and str(p["realm"]).strip() != str(realm).strip():
                raise PromoRealmMismatch("code is restricted to another realm")

            kit_items = _kit_items(conn, int(p["kit_id"])) if p.get("kit_id") else []
            return {
                "code_id": int(p["id"]),
                "code": p["code"],
                "amount": float(p.get("amount") or 0),
                "currency_key": p.get("currency_key"),
                "kit_id": p.get("kit_id"),
                "uses_left": int(p.get("uses_left") or 0),
                "uses_total": int(p.get("uses_total") or 0),
                "realm": p.get("realm"),
                "expires_at": p.get("expires_at"),
                "kit_items": [item.__dict__ for item in kit_items],
                "uses_left_after": max(0, int(p.get("uses_left") or 0) - 1),
            }
        finally:
            if own:
                conn.close()

    @staticmethod
    def redeem(
        *,
        code: str,
        uuid: str,
        username: Optional[str] = None,
        realm: Optional[str] = None,
        ip: Optional[str] = None,
        conn: Optional[MySQLConnection] = None,
    ) -> Dict[str, Any]:
        if not code:
            raise ValueError("code required")
        if not uuid:
            raise ValueError("uuid required")

        own = False
        if conn is None:
            conn = get_db_connection()
            own = True

        try:
            # берём строку под блокировку
            p = conn.query_one("SELECT * FROM promo_codes WHERE code=? FOR UPDATE", (code.strip().upper(),))
            if not p:
                raise PromoNotFound("code not found")
            if int(p.get("uses_left") or 0) <= 0:
                raise PromoNoUsesLeft("no uses left")
            if p.get("expires_at"):
                exp = _as_dt_utc(p["expires_at"])
                if datetime.utcnow() > exp:
                    raise PromoExpired("code expired")
            if p.get("realm") and realm and str(p["realm"]).strip() != str(realm).strip():
                raise PromoRealmMismatch("code is restricted to another realm")

            code_id = int(p["id"])
            amount = float(p.get("amount") or 0.0)
            currency_key = p.get("currency_key")
            kit_id = (int(p["kit_id"]) if p.get("kit_id") is not None else None)
            kit_items = _kit_items(conn, int(kit_id)) if kit_id else []

            # запись редемпшена
            conn.execute(
                """
                INSERT INTO promo_redemptions(code_id, uuid, username, realm, granted_amount, kit_id, granted_items_json, ip)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    code_id, uuid, username, realm,
                    amount if amount > 0 else None,
                    kit_id,
                    _json_dump([i.__dict__ for i in kit_items]),
                    (ip or None),
                ),
            )
            # уменьшение uses_left
            conn.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE id=? AND uses_left > 0", (code_id,))
            conn.commit()

            return {
                "code": p["code"],
                "code_id": code_id,
                "uses_left": int(p["uses_left"]) - 1,
                "amount": amount,
                "currency_key": currency_key,
                "kit_id": kit_id,
                "kit_items": [i.__dict__ for i in kit_items],
            }
        finally:
            if own:
                conn.close()

    @staticmethod
    def list_redemptions(
        *,
        code: Optional[str] = None,
        uuid: Optional[str] = None,
        username: Optional[str] = None,
        limit: int = 100,
        conn: Optional[MySQLConnection] = None,
    ) -> List[Dict[str, Any]]:
        own = False
        if conn is None:
            conn = get_db_connection()
            own = True
        try:
            where = []
            params: List[Any] = []
            if code:
                where.append("r.code_id = (SELECT id FROM promo_codes WHERE code=?)")
                params.append(code.strip().upper())
            if uuid:
                where.append("r.uuid = ?")
                params.append(uuid.strip())
            if username:
                where.append("r.username = ?")
                params.append(username.strip())
            sql_where = ("WHERE " + " AND ".join(where)) if where else ""
            rows = conn.query_all(
                f"""
                SELECT r.id, r.code_id, (SELECT code FROM promo_codes WHERE id=r.code_id) AS code,
                       r.uuid, r.username, r.realm, r.granted_amount, r.kit_id, r.ip, r.created_at
                FROM promo_redemptions r
                {sql_where}
                ORDER BY r.id DESC
                LIMIT ?
                """,
                (*params, int(max(1, min(500, limit)))),
            )
            return rows
        finally:
            if own:
                conn.close()
