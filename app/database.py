import os
from typing import Optional, Iterable, Any

# ✅ Гарантированно подхватываем .env ещё до чтения os.environ
try:
    from dotenv import load_dotenv, find_dotenv
    _env_path = find_dotenv(usecwd=True)
    if _env_path:
        load_dotenv(_env_path)  # загрузит из ближайшего .env в дереве
except Exception:
    # если python-dotenv не установлен — просто пропустим (но лучше установить)
    pass

import pymysql
from pymysql.cursors import DictCursor
from pymysql import err as mysql_err

# re-export, чтобы можно было ловить уникальные ошибки в коде
from pymysql.err import IntegrityError

__all__ = [
    "get_db_connection",
    "init_db",
    "get_setting",
    "set_setting",
    "get_all_settings",
    "IntegrityError",
]


# --- Обёртка над PyMySQL, имитирует интерфейс sqlite3.Connection ---
class MySQLConnection:
    def __init__(self, conn: pymysql.connections.Connection):
        self._conn = conn
        self._conn.ping(reconnect=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            try:
                self._conn.commit()
            finally:
                pass
        else:
            self._conn.rollback()

    def commit(self):
        self._conn.commit()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    @staticmethod
    def _qmark_to_percent(sql: str) -> str:
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: Iterable[Any] | None = None):
        sql2 = self._qmark_to_percent(sql)
        cur = self._conn.cursor(DictCursor)
        cur.execute(sql2, tuple(params or ()))
        return cur

    def executescript(self, script: str):
        for stmt in [s.strip() for s in script.split(";")]:
            if not stmt:
                continue
            self._conn.cursor().execute(stmt)
        self._conn.commit()


def _load_env():
    host = os.environ.get("DB_HOST", "127.0.0.1")
    port = int(os.environ.get("DB_PORT", "3306"))
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    db = os.environ.get("DB_NAME", "appdb")

    # TLS
    use_ssl = os.environ.get("DB_SSL", os.environ.get("DB_SSL_MODE", "0")) in ("1", "true", "TRUE")
    ssl_ca = os.environ.get("DB_SSL_CA")

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
        # Если известен CA — укажем его; если нет — передаём пустой dict,
        # это включает TLS без явной проверки CA (поведение зависит от сервера).
        if ssl_ca and str(ssl_ca).strip():
            kwargs["ssl"] = {"ca": ssl_ca}
        else:
            kwargs["ssl"] = {}  # <-- ВАЖНО: словарь, а не True
    return kwargs


def _connect_with_auto_create():
    host, port, user, password, db, use_ssl, ssl_ca = _load_env()

    # 1) пробуем сразу подключиться к DB_NAME
    try:
        return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))
    except mysql_err.OperationalError as e:
        if getattr(e, "args", [None])[0] != 1049:  # Unknown database
            raise

    # 2) если базы нет — создаём (нужны права)
    admin = pymysql.connect(**_base_kwargs(host, port, user, password, database=None, use_ssl=use_ssl, ssl_ca=ssl_ca))
    try:
        with admin.cursor() as c:
            c.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        admin.commit()
    finally:
        admin.close()

    # 3) повторное подключение
    return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))


def _mysql_connect() -> MySQLConnection:
    conn = _connect_with_auto_create()
    return MySQLConnection(conn)


def get_db_connection(_db_path: Optional[str] = None):
    return _mysql_connect()


SCHEMA_SQL = """
-- =========================
-- Users
-- =========================
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    discord_id VARCHAR(64) UNIQUE NULL,
    username VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role ENUM('pending','user','admin','superadmin') NOT NULL DEFAULT 'pending',
    is_superadmin TINYINT(1)
        GENERATED ALWAYS AS (CASE WHEN role='superadmin' THEN 1 ELSE 0 END) VIRTUAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================
-- Key-Value настройки (в т.ч. Discord, Chatwoot и пр.)
-- =========================
CREATE TABLE IF NOT EXISTS settings (
    `key`   VARCHAR(191) PRIMARY KEY,
    `value` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================
-- Боты соцсетей
-- =========================
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

-- =========================
-- Серверы (SSH)
-- =========================
CREATE TABLE IF NOT EXISTS servers (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(191) NOT NULL,
    host          VARCHAR(191) NOT NULL,
    port          INT NOT NULL DEFAULT 22,
    username      VARCHAR(191) NOT NULL,
    password      TEXT NULL,
    ssh_key_path  VARCHAR(512) NULL,
    added_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- согласовано с кодом: 'online' / 'offline' / 'unknown'
    last_status   ENUM('online','offline','unknown') DEFAULT 'unknown',
    last_uptime   VARCHAR(255) NULL,
    last_checked  DATETIME NULL,
    UNIQUE KEY uniq_host_port_user (host, port, username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Метрики серверов (внешний ключ -> servers)
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
    CONSTRAINT fk_metrics_server
        FOREIGN KEY (server_id) REFERENCES servers(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================
-- Лента новостей (опционально)
-- =========================
CREATE TABLE IF NOT EXISTS news (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    body MEDIUMTEXT NOT NULL,
    author_id INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_news_created (created_at),
    CONSTRAINT fk_news_author
      FOREIGN KEY (author_id) REFERENCES users(id)
      ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================
-- Пост-заготовка для мультипостинга
-- =========================
CREATE TABLE IF NOT EXISTS posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content MEDIUMTEXT NOT NULL,
    author_id INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('draft','sent','failed') NOT NULL DEFAULT 'draft',

    -- Embed и вложение
    embed_json JSON NULL,
    attachment_file VARCHAR(512) NULL,
    attachment_name VARCHAR(255) NULL,
    attachment_mime VARCHAR(100) NULL,

    CONSTRAINT fk_posts_author
      FOREIGN KEY (author_id) REFERENCES users(id)
      ON DELETE SET NULL ON UPDATE CASCADE,

    INDEX idx_posts_created (created_at),
    INDEX idx_posts_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================
-- Цели публикации поста
-- =========================
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




def init_db(conn: MySQLConnection):
    conn.executescript(SCHEMA_SQL)


def get_setting(conn: MySQLConnection, key: str, default=None):
    row = conn.execute("SELECT `value` FROM settings WHERE `key` = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn: MySQLConnection, key: str, value: str):
    conn.execute(
        "INSERT INTO settings(`key`,`value`) VALUES(?,?) "
        "ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)",
        (key, value),
    )
    conn.commit()


def get_all_settings(conn: MySQLConnection):
    cur = conn.execute("SELECT `key`,`value` FROM settings")
    return {row["key"]: row["value"] for row in cur.fetchall()}
