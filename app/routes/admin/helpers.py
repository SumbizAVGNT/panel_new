# app/routes/admin/helpers.py
from __future__ import annotations

import os
import socket
import datetime
from functools import lru_cache
from typing import Optional, Dict, Any, List

from flask import current_app
from ...database import get_db_connection


# =========================
#   Files (SSH keys)
# =========================
def keys_dir() -> str:
    """
    Возвращает абсолютный путь к каталогу для приватных ключей:
      <project_root>/uploads/keys
    Каталог создаётся при необходимости.
    """
    base = os.path.dirname(current_app.root_path)  # корень проекта (на уровень выше app/)
    path = os.path.join(base, "uploads", "keys")
    os.makedirs(path, exist_ok=True)
    return path


# =========================
#   Network probes
# =========================
def tcp_ping(host: str, port: int, timeout: float = 3.0) -> bool:
    """
    Быстрая проверка TCP-доступности (без TLS/SSH): True, если connect() успешен.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ssh_uptime(
    host: str,
    port: int,
    username: str,
    password: Optional[str],
    ssh_key_path: Optional[str],
    *,
    timeout_connect: float = 6.0,
    timeout_cmd: float = 6.0,
) -> Optional[str]:
    """
    Пытается получить красивый аптайм по SSH через `uptime -p`.
    Возвращает строку вида 'up 1 hour, 5 minutes' либо None.

    Примечания:
      - Если нет paramiko — вернём None.
      - Сначала пробует ключ (RSA/Ed25519), затем пароль.
      - Всегда закрывает клиент в finally.
    """
    try:
        import paramiko  # optional
    except Exception:
        return None

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        # Подготовка pkey, если указан путь к ключу
        pkey = None
        if ssh_key_path:
            try:
                pkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            except Exception:
                try:
                    pkey = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
                except Exception:
                    pkey = None

        if pkey is not None:
            client.connect(
                hostname=host,
                port=port,
                username=username,
                pkey=pkey,
                timeout=timeout_connect,
                auth_timeout=timeout_connect,
                banner_timeout=timeout_connect,
            )
        else:
            client.connect(
                hostname=host,
                port=port,
                username=username,
                password=password or "",
                timeout=timeout_connect,
                auth_timeout=timeout_connect,
                banner_timeout=timeout_connect,
            )

        stdin, stdout, stderr = client.exec_command("uptime -p", timeout=timeout_cmd)
        out = stdout.read().decode("utf-8", "ignore").strip()
        # прочитаем stderr, чтобы не оставлять пайпы
        _ = stderr.read()
        return out or None
    except Exception:
        return None
    finally:
        try:
            client.close()
        except Exception:
            pass


# =========================
#   servers.last_status schema helpers
# =========================
@lru_cache(maxsize=1)
def servers_last_status_storage(db_path: str) -> Dict[str, Any]:
    """
    Узнаёт тип столбца servers.last_status.

    Возвращает структуру:
      { "kind": "enum" | "text" | "numeric", "allowed": [<enum values>]}
    Результат кешируется (в рамках процесса) для снижения нагрузки.
    """
    with get_db_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT DATA_TYPE, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'servers'
              AND COLUMN_NAME = 'last_status'
            """
        ).fetchone()

    if not row:
        return {"kind": "text", "allowed": []}

    data_type = (row.get("DATA_TYPE") or "").lower()
    col_type = (row.get("COLUMN_TYPE") or "").lower()

    # ENUM('online','offline',...)
    if data_type == "enum" or col_type.startswith("enum("):
        allowed: List[str] = []
        if "(" in col_type and ")" in col_type:
            inside = col_type[col_type.find("(") + 1 : col_type.rfind(")")]
            parts = [p.strip() for p in inside.split(",")]
            for p in parts:
                # значения в INFORMATION_SCHEMA идут в одинарных кавычках
                if p.startswith("'") and p.endswith("'"):
                    allowed.append(p[1:-1])
        return {"kind": "enum", "allowed": allowed}

    # VARCHAR/TEXT/CHAR...
    if any(k in data_type for k in ("char", "text", "varchar")):
        return {"kind": "text", "allowed": []}

    # Числовые — трактуем как 1/0
    return {"kind": "numeric", "allowed": []}


def pick_status_value_for_db(db_path: str, reachable: bool) -> object:
    """
    Возвращает значение, совместимое с типом servers.last_status:
      - numeric -> 1/0
      - enum    -> выбирает лучшее совпадение: 'online'/'offline', затем синонимы
      - text    -> 'online'/'offline'
    """
    info = servers_last_status_storage(db_path)

    if info["kind"] == "numeric":
        return 1 if reachable else 0

    desired_primary = "online" if reachable else "offline"
    desired_alt = "up" if reachable else "down"

    if info["kind"] == "enum":
        allowed = info.get("allowed", []) or []
        # 1) онлайн/офлайн
        if desired_primary in allowed:
            return desired_primary
        if desired_alt in allowed:
            return desired_alt
        # 2) общие синонимы
        synonyms_true = ["on", "true", "yes", "1", "alive", "running"]
        synonyms_false = ["off", "false", "no", "0", "dead", "stopped"]
        for c in (synonyms_true if reachable else synonyms_false):
            if c in allowed:
                return c
        # 3) fallback: берём первый вариант из ENUM, если он есть
        return allowed[0] if allowed else desired_primary

    # TEXT/VARCHAR
    return desired_primary


# =========================
#   misc
# =========================
def utc_now_str() -> str:
    """UTC now в читаемом формате для логов/полей last_checked."""
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
