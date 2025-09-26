# panel/app/database.py
from __future__ import annotations

import os
from typing import Optional, Iterable, Any, Sequence

# ✅ Гарантированно подхватываем .env ещё до чтения os.environ
try:
    from dotenv import load_dotenv, find_dotenv
    _env_path = find_dotenv(usecwd=True)
    if _env_path:
        load_dotenv(_env_path)
except Exception:
    # если python-dotenv не установлен — просто пропустим
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


# =========================
#   Класс-обёртка соединения
# =========================
class MySQLConnection:
    """
    Обёртка над PyMySQL с интерфейсом, похожим на sqlite3.Connection.
    Автоконвертирует плейсхолдеры '?' → '%s'.
    """

    def __init__(self, conn: pymysql.connections.Connection):
        self._conn = conn
        # поддерживаем живое соединение (переподключение при необходимости)
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            pass

    # --- Контекстный менеджер ---
    def __enter__(self) -> "MySQLConnection":
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            try:
                self._conn.commit()
            except Exception:
                # не падаем в __exit__
                pass
        else:
            try:
                self._conn.rollback()
            except Exception:
                pass

    # --- Базовые методы транзакции/закрытия ---
    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    # --- Утилиты ---
    @staticmethod
    def _qmark_to_percent(sql: str) -> str:
        """
        Простая замена '?' на '%s'.
        Важно: не подходит для сложных SQL со знаками '?' внутри строк/JSON.
        В проекте мы используем простые выражения, поэтому достаточно.
        """
        return sql.replace("?", "%s")

    def _cursor(self):
        # Отдаём курсор как dict-строки
        return self._conn.cursor(DictCursor)

    # --- Выполнение запросов ---
    def execute(self, sql: str, params: Iterable[Any] | None = None):
        sql2 = self._qmark_to_percent(sql)
        cur = self._cursor()
        cur.execute(sql2, tuple(params or ()))
        return cur

    def executemany(self, sql: str, seq_of_params: Sequence[Iterable[Any]]):
        sql2 = self._qmark_to_percent(sql)
        cur = self._cursor()
        cur.executemany(sql2, [tuple(p) for p in seq_of_params])
        return cur

    def executescript(self, script: str):
        """
        Наивный разбор по ';'. Подходит для инициализации схемы.
        """
        cursor = self._conn.cursor()
        try:
            for stmt in [s.strip() for s in script.split(";")]:
                if not stmt:
                    continue
                cursor.execute(stmt)
            self._conn.commit()
        finally:
            cursor.close()

    # --- Удобные шорткаты ---
    def query_one(self, sql: str, params: Iterable[Any] | None = None) -> Optional[dict]:
        cur = self.execute(sql, params)
        try:
            return cur.fetchone()
        finally:
            cur.close()

    def query_all(self, sql: str, params: Iterable[Any] | None = None) -> list[dict]:
        cur = self.execute(sql, params)
        try:
            return list(cur.fetchall())
        finally:
            cur.close()

    @property
    def lastrowid(self) -> int:
        try:
            return int(self._conn.insert_id())
        except Exception:
            return 0


# =========================
#   Конфиг и подключение
# =========================
def _load_env():
    host = os.environ.get("DB_HOST", "127.0.0.1")
    port = int(os.environ.get("DB_PORT", "3306"))
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    db = os.environ.get("DB_NAME", "appdb")

    # TLS
    use_ssl = os.environ.get("DB_SSL", os.environ.get("DB_SSL_MODE", "0")).lower() in ("1", "true", "yes")
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
        # Если известен CA — укажем его; иначе — пустой dict включает TLS без строгой проверки
        kwargs["ssl"] = {"ca": ssl_ca} if (ssl_ca and str(ssl_ca).strip()) else {}
    return kwargs


def _connect_with_auto_create() -> pymysql.connections.Connection:
    host, port, user, password, db, use_ssl, ssl_ca = _load_env()

    # 1) Пытаемся подключиться сразу к целевой БД
    try:
        return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))
    except mysql_err.OperationalError as e:
        # 1049 — Unknown database
        if getattr(e, "args", [None])[0] != 1049:
            raise

    # 2) Если базы нет — создаём (нужны права у пользователя)
    admin = pymysql.connect(**_base_kwargs(host, port, user, password, database=None, use_ssl=use_ssl, ssl_ca=ssl_ca))
    try:
        with admin.cursor() as c:
            c.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        admin.commit()
    finally:
        admin.close()

    # 3) Повторное подключение к созданной БД
    return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))


def _mysql_connect() -> MySQLConnection:
    conn = _connect_with_auto_create()
    return MySQLConnection(conn)


def get_db_connection(_db_path: Optional[str] = None) -> MySQLConnection:
    """
    Поддерживаем старую сигнатуру (параметр игнорируется), чтобы код, где передавался DB_PATH,
    не ломался. Всегда возвращаем MySQLConnection.
    """
    return _mysql_connect()


# =========================
#   Схема БД
# =========================
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
-- Key-Value настройки
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
    last_status   ENUM('online','offline','unknown') DEFAULT 'unknown',
    last_uptime   VARCHAR(255) NULL,
    last_checked  DATETIME NULL,
    UNIQUE KEY uniq_host_port_user (host, port, username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Метрики серверов
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


# =========================
#   Утилиты и настройки
# =========================
def init_db(conn: MySQLConnection):
    """Применяет SCHEMA_SQL."""
    conn.executescript(SCHEMA_SQL)


def get_setting(conn: MySQLConnection, key: str, default=None):
    row = conn.query_one("SELECT `value` FROM settings WHERE `key` = ?", (key,))
    return row["value"] if row else default


def set_setting(conn: MySQLConnection, key: str, value: str):
    conn.execute(
        "INSERT INTO settings(`key`,`value`) VALUES(?,?) "
        "ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)",
        (key, value),
    )
    conn.commit()


def get_all_settings(conn: MySQLConnection) -> dict[str, str]:
    rows = conn.query_all("SELECT `key`,`value` FROM settings")
    return {row["key"]: row["value"] for row in rows}
