from __future__ import annotations

import os
from typing import Optional, Iterable, Any, Sequence

# --- env (подхватываем заранее) ---
try:
    from dotenv import load_dotenv, find_dotenv
    _env = find_dotenv(usecwd=True)
    if _env:
        load_dotenv(_env)
except Exception:
    pass

import pymysql
from pymysql.cursors import DictCursor
from pymysql import err as mysql_err
from pymysql.err import IntegrityError

__all__ = [
    "get_db_connection",
    "get_authme_connection",
    "get_luckperms_connection",
    "init_db",
    "get_setting",
    "set_setting",
    "get_all_settings",
    "IntegrityError",
]

# =========================
#   Обёртка соединения
# =========================
class MySQLConnection:
    """
    Простая обёртка над PyMySQL с API, похожим на sqlite3.Connection.
    Заменяет плейсхолдеры '?' -> '%s'.
    """
    def __init__(self, conn: pymysql.connections.Connection):
        self._conn = conn
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            pass

    def __enter__(self) -> "MySQLConnection":
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            (self._conn.commit() if exc_type is None else self._conn.rollback())
        except Exception:
            pass

    def commit(self) -> None: self._conn.commit()
    def rollback(self) -> None: self._conn.rollback()

    def close(self) -> None:
        try: self._conn.close()
        except Exception: pass

    @staticmethod
    def _qmark_to_percent(sql: str) -> str:
        return sql.replace("?", "%s")

    def _cursor(self):
        return self._conn.cursor(DictCursor)

    def execute(self, sql: str, params: Iterable[Any] | None = None):
        cur = self._cursor()
        cur.execute(self._qmark_to_percent(sql), tuple(params or ()))
        return cur

    def executemany(self, sql: str, seq_of_params: Sequence[Iterable[Any]]):
        cur = self._cursor()
        cur.executemany(self._qmark_to_percent(sql), [tuple(p) for p in seq_of_params])
        return cur

    def executescript(self, script: str):
        cur = self._conn.cursor()
        try:
            for stmt in [s.strip() for s in script.split(";")]:
                if stmt:
                    cur.execute(stmt)
            self._conn.commit()
        finally:
            cur.close()

    def query_one(self, sql: str, params: Iterable[Any] | None = None) -> Optional[dict]:
        cur = self.execute(sql, params)
        try: return cur.fetchone()
        finally: cur.close()

    def query_all(self, sql: str, params: Iterable[Any] | None = None) -> list[dict]:
        cur = self.execute(sql, params)
        try: return list(cur.fetchall())
        finally: cur.close()

    @property
    def lastrowid(self) -> int:
        try: return int(self._conn.insert_id())
        except Exception: return 0


# =========================
#   Конфиг / коннекты
# =========================
def _env_or(prefix: str, key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Возвращаем {prefix}{key}. Fallback:
      - для AUTHME_: -> DB_{key}
      - для LUCKPERMS_: -> AUTHME_{key} -> DB_{key}
    Это полезно, если панельная БД локальная, а AuthMe/LuckPerms — на удалённом хосте.
    """
    val = os.environ.get(f"{prefix}{key}")

    # AUTHME_ берёт базовые доступы из DB_ если не заданы
    if val is None and prefix == "AUTHME_" and key in {"HOST", "PORT", "USER", "PASSWORD"}:
        val = os.environ.get(f"DB_{key}")

    # LUCKPERMS_ пробует AUTHME_, затем DB_
    if val is None and prefix == "LUCKPERMS_" and key in {"HOST", "PORT", "USER", "PASSWORD"}:
        val = os.environ.get(f"AUTHME_{key}") or os.environ.get(f"DB_{key}")

    return val if val is not None else default


def _load_env(prefix: str):
    # дефолтные имена БД
    if prefix == "AUTHME_":
        default_db = "authmedb"
    elif prefix == "LUCKPERMS_":
        default_db = "donate"
    else:
        default_db = "panel"

    host = _env_or(prefix, "HOST", "127.0.0.1")
    port = int(_env_or(prefix, "PORT", "3306"))
    user = _env_or(prefix, "USER", "root")
    password = _env_or(prefix, "PASSWORD", "")
    db = _env_or(prefix, "NAME", default_db)

    ssl_flag = (_env_or(prefix, "SSL", _env_or(prefix, "SSL_MODE", "0")) or "0").lower()
    use_ssl = ssl_flag in ("1", "true", "yes", "require")
    ssl_ca = _env_or(prefix, "SSL_CA", None)

    return host, port, user, password, db, use_ssl, ssl_ca


def _base_kwargs(host, port, user, password, database=None, use_ssl=False, ssl_ca=None):
    kwargs = dict(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )
    if use_ssl:
        kwargs["ssl"] = {"ca": ssl_ca} if (ssl_ca and str(ssl_ca).strip()) else {}
    return kwargs


def _connect_with_auto_create(prefix: str, create_if_missing: bool) -> pymysql.connections.Connection:
    host, port, user, password, db, use_ssl, ssl_ca = _load_env(prefix)

    try:
        return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))
    except mysql_err.OperationalError as e:
        if getattr(e, "args", [None])[0] != 1049 or not create_if_missing:
            raise

    # создаём БД (если разрешено)
    admin = pymysql.connect(**_base_kwargs(host, port, user, password, database=None, use_ssl=use_ssl, ssl_ca=ssl_ca))
    try:
        with admin.cursor() as c:
            c.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        admin.commit()
    finally:
        admin.close()

    return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))


def _mysql_connect(prefix: str, create_if_missing: bool) -> MySQLConnection:
    conn = _connect_with_auto_create(prefix, create_if_missing=create_if_missing)
    return MySQLConnection(conn)


def get_db_connection(_db_path: Optional[str] = None) -> MySQLConnection:
    """Основная БД панели (prefix=DB_), автосоздание разрешено."""
    return _mysql_connect(prefix="DB_", create_if_missing=True)


def get_authme_connection() -> MySQLConnection:
    """БД авторизации (prefix=AUTHME_), без автосоздания."""
    return _mysql_connect(prefix="AUTHME_", create_if_missing=False)


def get_luckperms_connection() -> MySQLConnection:
    """БД LuckPerms (prefix=LUCKPERMS_), без автосоздания."""
    return _mysql_connect(prefix="LUCKPERMS_", create_if_missing=False)


# =========================
#   Схема панели (совместимая)
# =========================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    discord_id VARCHAR(64) UNIQUE NULL,
    username VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role ENUM('pending','user','admin','superadmin') NOT NULL DEFAULT 'pending',
    is_superadmin TINYINT(1) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS settings (
    `key` VARCHAR(191) PRIMARY KEY,
    `value` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS bots (
    id INT AUTO_INCREMENT PRIMARY KEY,
    platform ENUM('discord','telegram','vk') NOT NULL,
    bot_id VARCHAR(128) NOT NULL,
    name VARCHAR(255) NOT NULL,
    avatar_url VARCHAR(512) NULL,
    token TEXT NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_platform_bot (platform, bot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS servers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(191) NOT NULL,
    host VARCHAR(191) NOT NULL,
    port INT NOT NULL DEFAULT 22,
    username VARCHAR(191) NOT NULL,
    password TEXT NULL,
    ssh_key_path VARCHAR(512) NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_status ENUM('online','offline','unknown') DEFAULT 'unknown',
    last_uptime VARCHAR(255) NULL,
    last_checked DATETIME NULL,
    UNIQUE KEY uniq_host_port_user (host, port, username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS server_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    server_id INT NOT NULL,
    cpu_pct DECIMAL(5,2) NULL,
    mem_used_gb DECIMAL(8,2) NULL,
    mem_total_gb DECIMAL(8,2) NULL,
    disk_used_gb DECIMAL(10,2) NULL,
    disk_total_gb DECIMAL(10,2) NULL,
    net_in_mbps DECIMAL(10,2) NULL,
    net_out_mbps DECIMAL(10,2) NULL,
    docker_running INT NULL,
    docker_names TEXT NULL,
    collected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_metrics_server_ts (server_id, collected_at),
    CONSTRAINT fk_metrics_server FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS news (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    body MEDIUMTEXT NOT NULL,
    author_id INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_news_created (created_at),
    CONSTRAINT fk_news_author FOREIGN KEY (author_id) REFERENCES users(id)
      ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content MEDIUMTEXT NOT NULL,
    author_id INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('draft','sent','failed') NOT NULL DEFAULT 'draft',
    embed_json MEDIUMTEXT NULL,
    attachment_file VARCHAR(512) NULL,
    attachment_name VARCHAR(255) NULL,
    attachment_mime VARCHAR(100) NULL,
    CONSTRAINT fk_posts_author FOREIGN KEY (author_id) REFERENCES users(id)
      ON DELETE SET NULL ON UPDATE CASCADE,
    INDEX idx_posts_created (created_at),
    INDEX idx_posts_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS post_targets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    post_id INT NOT NULL,
    platform ENUM('discord','telegram','vk') NOT NULL,
    bot_db_id INT NOT NULL,
    external_target_id VARCHAR(128) NULL,
    external_target_name VARCHAR(255) NULL,
    send_status ENUM('pending','sent','error') NOT NULL DEFAULT 'pending',
    response_json MEDIUMTEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pt_post FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    CONSTRAINT fk_pt_bot  FOREIGN KEY (bot_db_id) REFERENCES bots(id)  ON DELETE CASCADE,
    INDEX idx_pt_post (post_id),
    INDEX idx_pt_platform (platform),
    INDEX idx_pt_status (send_status),
    INDEX idx_pt_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

# =========================
#   Утилиты панели
# =========================
def init_db(conn: MySQLConnection) -> None:
    conn.executescript(SCHEMA_SQL)

def get_setting(conn: MySQLConnection, key: str, default=None):
    row = conn.query_one("SELECT `value` FROM settings WHERE `key` = ?", (key,))
    return row["value"] if row else default

def set_setting(conn: MySQLConnection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings(`key`,`value`) VALUES(?,?) "
        "ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)",
        (key, value),
    )
    conn.commit()

def get_all_settings(conn: MySQLConnection) -> dict[str, str]:
    rows = conn.query_all("SELECT `key`,`value` FROM settings")
    return {row["key"]: row["value"] for row in rows}
