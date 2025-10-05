# app/modules/points_repo.py
from typing import Optional

# отдельный коннектор к points-БД
from ..database import get_points_connection


def get_points_by_uuid(uuid: str, *, key: str = "rubs") -> Optional[float]:
    """
    Текущее значение Points для (Uuid, Key) или None, если записи нет.
    """
    with get_points_connection() as conn:
        row = conn.query_one(
            "SELECT `Points` FROM `pnts_points` WHERE `Uuid` = ? AND `Key` = ? LIMIT 1",
            (uuid, key),
        )
        return float(row["Points"]) if row else None


def set_points_by_uuid(uuid: str, new_points: float, *, key: str = "rubs") -> float:
    """
    Апсертом устанавливает Points для (Uuid, Key) и возвращает сохранённое значение.
    """
    with get_points_connection() as conn:
        exists = conn.query_one(
            "SELECT 1 FROM `pnts_points` WHERE `Uuid` = ? AND `Key` = ? LIMIT 1",
            (uuid, key),
        ) is not None

        if exists:
            conn.execute(
                "UPDATE `pnts_points` SET `Points` = ? WHERE `Uuid` = ? AND `Key` = ?",
                (new_points, uuid, key),
            )
        else:
            conn.execute(
                "INSERT INTO `pnts_points` (`Uuid`, `Key`, `Points`) VALUES (?, ?, ?)",
                (uuid, key, new_points),
            )
        conn.commit()
        return float(new_points)
