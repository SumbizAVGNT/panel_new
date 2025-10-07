# app/modules/points_repo.py
from __future__ import annotations

from typing import Optional
import time

from ..database import get_points_connection


def _now_ms() -> int:
    """Миллисекунды: нужен для pnts_syncing.LastEdited, чтобы сбросить кеш плагина."""
    return int(time.time() * 1000)


def get_points_by_uuid(uuid: str, *, key: str = "rubs") -> Optional[float]:
    """
    Текущее значение Points для (Uuid, Key) или None, если записи нет.
    """
    with get_points_connection() as conn:
        row = conn.query_one(
            "SELECT `Points` FROM `pnts_points` WHERE `Uuid` = ? AND `Key` = ? LIMIT 1",
            (uuid, key),
        )
        return float(row["Points"]) if row and "Points" in row else None


def set_points_by_uuid(uuid: str, new_points: float, *, key: str = "rubs") -> float:
    """
    Апсертом устанавливает Points для (Uuid, Key) и обновляет pnts_syncing.LastEdited.
    Это обязательно, иначе игровой плагин может не подхватить новые значения из-за кеша.
    """
    value = float(new_points)
    now = _now_ms()

    with get_points_connection() as conn:
        # upsert баланса
        conn.execute(
            "INSERT INTO `pnts_points` (`Uuid`, `Key`, `Points`) VALUES (?, ?, ?) "
            "ON DUPLICATE KEY UPDATE `Points` = VALUES(`Points`)",
            (uuid, key, value),
        )
        # сигнал синхронизации для плагина
        conn.execute(
            "INSERT INTO `pnts_syncing` (`Uuid`, `LastEdited`) VALUES (?, ?) "
            "ON DUPLICATE KEY UPDATE `LastEdited` = VALUES(`LastEdited`)",
            (uuid, now),
        )
        conn.commit()

    return value
