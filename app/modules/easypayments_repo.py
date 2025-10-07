# app/modules/easypayments_repo.py
from __future__ import annotations

from typing import List, Any, Dict
import os
import pymysql

# why: единый слой курсора + '?' плейсхолдеры как в проекте
from ..database import MySQLConnection


def _bool_env(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _conn() -> MySQLConnection:
    """
    Подключение к БД EasyPayments.
    EASYPAY_* перекрывают DB_*. По умолчанию имя БД — 'easypayments'.
    """
    host = os.getenv("EASYPAY_HOST", os.getenv("DB_HOST", "127.0.0.1"))
    port = int(os.getenv("EASYPAY_PORT", os.getenv("DB_PORT", "3306")))
    user = os.getenv("EASYPAY_USER", os.getenv("DB_USER", "root"))
    password = os.getenv("EASYPAY_PASSWORD", os.getenv("DB_PASSWORD", ""))
    database = os.getenv("EASYPAY_NAME", "easypayments")

    use_ssl = _bool_env(os.getenv("EASYPAY_SSL", os.getenv("DB_SSL", "0")))
    ssl_ca = os.getenv("EASYPAY_SSL_CA", os.getenv("DB_SSL_CA"))

    kwargs: Dict[str, Any] = dict(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )
    if use_ssl:
        kwargs["ssl"] = {"ca": ssl_ca} if (ssl_ca and str(ssl_ca).strip()) else {}

    return MySQLConnection(pymysql.connect(**kwargs))


def donations_by_uuid(uuid: str, limit: int = 200) -> List[dict]:
    """
    История покупок из EasyPayments по uuid.
    Возвращаем покупки с привязкой к платежу.
    """
    sql = (
        "SELECT pur.id              AS purchase_id, "
        "       pay.id              AS payment_id, "
        "       c.player_name       AS customer_name, "
        "       c.player_uuid       AS customer_uuid, "
        "       pay.server_id, "
        "       pay.created_at      AS payment_created_at, "
        "       pur.name            AS product_name, "
        "       pur.amount, "
        "       pur.cost, "
        "       pur.commands, "
        "       pur.responses, "
        "       pur.created_at      AS purchase_created_at "
        "FROM easypayments_customers c "
        "JOIN easypayments_payments  pay ON pay.customer_id = c.player_name "
        "JOIN easypayments_purchases pur ON pur.payment_id = pay.id "
        "WHERE c.player_uuid = ? "
        "ORDER BY pay.created_at DESC, pur.id ASC "
        "LIMIT ?"
    )
    with _conn() as conn:
        rows = conn.query_all(sql, (uuid, limit))
        # лёгкая нормализация типов
        for r in rows:
            try:
                r["amount"] = int(r.get("amount"))
            except Exception:
                pass
            try:
                r["cost"] = float(r.get("cost"))
            except Exception:
                pass
        return rows
