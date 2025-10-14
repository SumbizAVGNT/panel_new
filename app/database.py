# app/database.py
from __future__ import annotations

import os
import json
import time
from datetime import datetime
from typing import Optional, Iterable, Any, Sequence, Dict, List

# env
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
    # коннекторы
    "get_db_connection",
    "get_authme_connection",
    "get_luckperms_connection",
    "get_points_connection",
    "get_easypayments_connection",
    "get_litebans_connection",
    "get_bcases_connection",
    "get_leader_connection",
    # общая схема/настройки (panel)
    "init_db",
    "get_setting",
    "set_setting",
    "get_all_settings",
    # статистика (оригинальные имена)
    "init_stats_schema",
    "save_server_stats",
    "list_realms",
    "get_stats_recent",
    "get_stats_range",
    "get_stats_payloads_range",
    "get_stats_agg",
    "purge_old_stats",
    # статистика (совместимые имена, которых ждут роуты)
    "ensure_stats_schema",
    "stats_save_snapshot",
    "stats_get_latest",
    "stats_get_series",
    # доменные хелперы (LuckPerms / EasyPayments / LiteBans / Leaderboards / BCases)
    "lp_find_uuid",
    "lp_get_player",
    "lp_get_user_permissions",
    "lp_get_group_permissions",
    "ep_get_customer_by_name",
    "ep_get_payments_by_customer",
    "ep_get_purchases_by_payment",
    "lb_list_bans_by_uuid",
    "lb_list_history_by_uuid",
    "leader_top_hours",
    "leader_top_balance",
    "leader_suffix_by_uuid",
    "bcases_list_for_uuid_or_name",
    # эксепшены
    "IntegrityError",
]

# -------------------------
# MySQL wrapper
# -------------------------
class MySQLConnection:
    """PyMySQL wrapper with sqlite-like API; converts '?' to '%s'."""
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


# -------------------------
# Config / connectors
# -------------------------
def _env_or(prefix: str, key: str, default: Optional[str] = None) -> Optional[str]:
    """
    {prefix}{key} with fallbacks:
      AUTHME_  -> DB_
      LUCKPERMS-> AUTHME_ -> DB_
      POINTS_/EASYPAYMENTS_/LITEBANS_/BCASES_/LEADER_ -> DB_
      SSL/SSL_CA also fallback to DB_.
      NAME falls back to DB_NAME for all prefixes.
    """
    val = os.environ.get(f"{prefix}{key}")

    if val is None and prefix == "AUTHME_" and key in {"HOST", "PORT", "USER", "PASSWORD"}:
        val = os.environ.get(f"DB_{key}")

    if val is None and prefix == "LUCKPERMS_" and key in {"HOST", "PORT", "USER", "PASSWORD"}:
        val = os.environ.get(f"AUTHME_{key}") or os.environ.get(f"DB_{key}")

    if val is None and prefix in {"POINTS_", "EASYPAYMENTS_", "LITEBANS_", "BCASES_", "LEADER_"} and key in {"HOST", "PORT", "USER", "PASSWORD"}:
        val = os.environ.get(f"DB_{key}")

    # NAME -> DB_NAME fallback (универсально)
    if val is None and key == "NAME":
        val = os.environ.get("DB_NAME")

    # SSL/SSL_MODE/SSL_CA fallback к DB_
    if val is None and key in {"SSL", "SSL_MODE", "SSL_CA"}:
        base = os.environ.get(f"{prefix}{key}")
        if base is None:
            val = os.environ.get(f"DB_{key}")

    return val if val is not None else default


def _load_env(prefix: str):
    # дефолтные имена БД, ближе к стандартным плагинам
    if prefix == "AUTHME_":
        default_db = "authme"
    elif prefix == "LUCKPERMS_":
        default_db = "luckperms"
    elif prefix == "POINTS_":
        default_db = "points"
    elif prefix == "EASYPAYMENTS_":
        default_db = "easypayments"
    elif prefix == "LITEBANS_":
        default_db = "litebans"
    elif prefix == "BCASES_":
        default_db = "bcases"
    elif prefix == "LEADER_":
        default_db = "leader"
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
        host=host, port=port, user=user, password=password, database=database,
        charset="utf8mb4", autocommit=False, cursorclass=DictCursor,
        connect_timeout=10, read_timeout=30, write_timeout=30,
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

    admin = pymysql.connect(**_base_kwargs(host, port, user, password, database=None, use_ssl=use_ssl, ssl_ca=ssl_ca))
    try:
        with admin.cursor() as c:
            c.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        admin.commit()
    finally:
        admin.close()

    return pymysql.connect(**_base_kwargs(host, port, user, password, database=db, use_ssl=use_ssl, ssl_ca=ssl_ca))


def _mysql_connect(prefix: str, create_if_missing: bool) -> MySQLConnection:
    return MySQLConnection(_connect_with_auto_create(prefix, create_if_missing=create_if_missing))


def get_db_connection(_db_path: Optional[str] = None) -> MySQLConnection:
    return _mysql_connect(prefix="DB_", create_if_missing=True)

def get_authme_connection() -> MySQLConnection:
    return _mysql_connect(prefix="AUTHME_", create_if_missing=False)

def get_luckperms_connection() -> MySQLConnection:
    return _mysql_connect(prefix="LUCKPERMS_", create_if_missing=False)

def get_points_connection() -> MySQLConnection:
    return _mysql_connect(prefix="POINTS_", create_if_missing=False)

def get_easypayments_connection() -> MySQLConnection:
    return _mysql_connect(prefix="EASYPAYMENTS_", create_if_missing=False)

def get_litebans_connection() -> MySQLConnection:
    return _mysql_connect(prefix="LITEBANS_", create_if_missing=False)

def get_bcases_connection() -> MySQLConnection:
    return _mysql_connect(prefix="BCASES_", create_if_missing=False)

def get_leader_connection() -> MySQLConnection:
    return _mysql_connect(prefix="LEADER_", create_if_missing=False)


# -------------------------
# Panel schema (optional)
# -------------------------
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

-- ===== PROMO / KITS — base =====
CREATE TABLE IF NOT EXISTS promo_kits (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(191) NOT NULL,
    description TEXT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_pkit_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS promo_kit_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    kit_id INT NOT NULL,
    namespace VARCHAR(64) NOT NULL DEFAULT 'minecraft',
    item_id VARCHAR(191) NOT NULL,
    amount INT NOT NULL DEFAULT 1,
    display_name VARCHAR(255) NULL,
    enchants_json MEDIUMTEXT NULL,  -- [{"id":"sharpness","lvl":5},...]
    nbt_json MEDIUMTEXT NULL,       -- произвольный NBT в JSON
    slot INT NULL,                  -- фиксированный слот, если нужен
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pkit_kit FOREIGN KEY (kit_id) REFERENCES promo_kits(id) ON DELETE CASCADE,
    INDEX idx_pkit_kit (kit_id),
    INDEX idx_pkit_slot (kit_id, slot)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ===== PROMO codes =====
CREATE TABLE IF NOT EXISTS promo_codes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(64) NOT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,

    -- Валюта/поинты (опционально)
    amount DECIMAL(12,2) NULL,
    currency_key VARCHAR(32) NULL,          -- напр. "rubs", "coins"

    -- LP / Киты / Реалм
    realm VARCHAR(191) NULL,                -- куда слать команды/какой контекст
    kit_id INT NULL,

    -- Ограничения
    uses_total INT NOT NULL DEFAULT 1,      -- общий лимит
    uses_left INT NOT NULL DEFAULT 1,
    per_player_uses INT NOT NULL DEFAULT 1, -- лимит на игрока
    cooldown_seconds INT NOT NULL DEFAULT 0,
    expires_at DATETIME NULL,

    -- Админские поля
    created_by VARCHAR(191) NULL,
    note VARCHAR(255) NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_promo_kit FOREIGN KEY (kit_id) REFERENCES promo_kits(id) ON DELETE SET NULL,
    UNIQUE KEY uq_promo_code_ci (code),
    INDEX idx_promo_expires (expires_at),
    INDEX idx_promo_uses_left (uses_left)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Много LP-групп на один промокод (перманент или временно)
CREATE TABLE IF NOT EXISTS promo_code_groups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code_id INT NOT NULL,
    group_name VARCHAR(36) NOT NULL,        -- имя группы LP
    temp_seconds INT NULL,                  -- NULL = перманент
    context_server VARCHAR(36) NOT NULL DEFAULT 'global',
    context_world  VARCHAR(64) NOT NULL DEFAULT 'global',
    priority INT NOT NULL DEFAULT 0,        -- порядок применения
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pcg_code FOREIGN KEY (code_id) REFERENCES promo_codes(id) ON DELETE CASCADE,
    INDEX idx_pcg_code (code_id),
    INDEX idx_pcg_order (code_id, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Набор произвольных команд на код (выполняются от CONSOLE/PLAYER)
CREATE TABLE IF NOT EXISTS promo_code_cmds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code_id INT NOT NULL,
    run_as ENUM('CONSOLE','PLAYER') NOT NULL DEFAULT 'CONSOLE',
    realm VARCHAR(191) NULL,                -- если надо отправлять в другой realm через мост
    command_text VARCHAR(500) NOT NULL,     -- без слеша, например: "lp user {player} parent add knight"
    run_delay_ms INT NOT NULL DEFAULT 0,    -- задержка перед выполнением
    priority INT NOT NULL DEFAULT 0,        -- порядок
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pcc_code FOREIGN KEY (code_id) REFERENCES promo_codes(id) ON DELETE CASCADE,
    INDEX idx_pcc_code (code_id),
    INDEX idx_pcc_order (code_id, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Лог погашений
CREATE TABLE IF NOT EXISTS promo_redemptions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    code_id INT NOT NULL,
    uuid VARCHAR(36) NOT NULL,
    username VARCHAR(191) NULL,
    realm VARCHAR(191) NULL,

    granted_amount DECIMAL(12,2) NULL,
    currency_key VARCHAR(32) NULL,          -- фактически применённая валюта
    kit_id INT NULL,

    granted_items_json MEDIUMTEXT NULL,     -- какие предметы реально выдали
    granted_groups_json MEDIUMTEXT NULL,    -- [{"group":"knight","temp_seconds":2592000},...]
    executed_cmds_json MEDIUMTEXT NULL,     -- список выполненных команд и статусов

    ip VARCHAR(64) NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_pred_code FOREIGN KEY (code_id) REFERENCES promo_codes(id) ON DELETE CASCADE,
    INDEX idx_pred_uuid (uuid),
    INDEX idx_pred_code (code_id),
    INDEX idx_pred_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

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


# ==========================
# Stats schema & API
# ==========================
STATS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS realms (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(191) NOT NULL UNIQUE,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS stats_samples (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    realm_id INT NOT NULL,
    collected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- агрегаты
    players_online INT NULL,
    players_max INT NULL,
    tps_1m DECIMAL(5,2) NULL,
    tps_5m DECIMAL(5,2) NULL,
    tps_15m DECIMAL(5,2) NULL,
    mspt DECIMAL(6,2) NULL,
    heap_used BIGINT NULL,
    heap_max BIGINT NULL,
    cpu_sys DECIMAL(6,3) NULL,
    cpu_proc DECIMAL(6,3) NULL,

    -- исходный нормализованный объект — целиком
    payload_json MEDIUMTEXT NULL,

    INDEX idx_stats_realm_ts (realm_id, collected_at),
    CONSTRAINT fk_stats_realm FOREIGN KEY (realm_id) REFERENCES realms(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

def init_stats_schema(conn: MySQLConnection) -> None:
    """Создать таблицы для статистики (idempotent)."""
    conn.executescript(STATS_SCHEMA_SQL)

def _realm_id(conn: MySQLConnection, name: str) -> int:
    """Вернёт id realm, создаст при необходимости."""
    name = (name or "").strip() or "default"
    row = conn.query_one("SELECT id FROM realms WHERE name = ?", (name,))
    if row:
        return int(row["id"])
    conn.execute(
        "INSERT INTO realms(name) VALUES (?) "
        "ON DUPLICATE KEY UPDATE name = VALUES(name)",
        (name,),
    )
    conn.commit()
    row = conn.query_one("SELECT id FROM realms WHERE name = ?", (name,))
    return int(row["id"])

def save_server_stats(conn: MySQLConnection, stats: dict, *, collected_at: datetime | None = None) -> int:
    """
    Сохранить результат normalize_server_stats(...).
    Возвращает id вставленной строки.
    """
    realm = (stats.get("realm") or "").strip() or "default"
    rid = _realm_id(conn, realm)

    players = stats.get("players") or {}
    tps = stats.get("tps") or {}
    heap = stats.get("heap") or {}
    osb = stats.get("os") or {}
    cpu = osb.get("cpu_load") or {}

    row = (
        rid,
        (collected_at or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S"),
        players.get("online"),
        players.get("max"),
        tps.get("1m"),
        tps.get("5m"),
        tps.get("15m"),
        (tps.get("mspt") if isinstance(tps, dict) else None) or stats.get("mspt"),
        heap.get("used"),
        heap.get("max"),
        cpu.get("system"),
        cpu.get("process"),
        json.dumps(stats, ensure_ascii=False),
    )

    conn.execute(
        """
        INSERT INTO stats_samples(
            realm_id, collected_at,
            players_online, players_max,
            tps_1m, tps_5m, tps_15m, mspt,
            heap_used, heap_max, cpu_sys, cpu_proc,
            payload_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        row,
    )
    conn.commit()
    return conn.lastrowid

def list_realms(conn: MySQLConnection) -> list[str]:
    rows = conn.query_all("SELECT name FROM realms ORDER BY name ASC")
    return [r["name"] for r in rows]

def get_stats_recent(conn: MySQLConnection, realm: str, limit: int = 100) -> list[dict]:
    """Последние N записей по realm (от новых к старым)."""
    rid = _realm_id(conn, realm)
    rows = conn.query_all(
        """
        SELECT id, collected_at, players_online, players_max,
               tps_1m, tps_5m, tps_15m, mspt,
               heap_used, heap_max, cpu_sys, cpu_proc
        FROM stats_samples
        WHERE realm_id = ?
        ORDER BY collected_at DESC
        LIMIT ?
        """,
        (rid, int(limit)),
    )
    return rows

def get_stats_range(conn: MySQLConnection, realm: str, start: datetime | str, end: datetime | str) -> list[dict]:
    """Все записи за интервал [start, end]."""
    rid = _realm_id(conn, realm)
    s = _as_dt(start).strftime("%Y-%m-%d %H:%M:%S")
    e = _as_dt(end).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.query_all(
        """
        SELECT id, collected_at, players_online, players_max,
               tps_1m, tps_5m, tps_15m, mspt,
               heap_used, heap_max, cpu_sys, cpu_proc
        FROM stats_samples
        WHERE realm_id = ? AND collected_at BETWEEN ? AND ?
        ORDER BY collected_at ASC
        """,
        (rid, s, e),
    )
    return rows

def get_stats_payloads_range(conn: MySQLConnection, realm: str, start: datetime | str, end: datetime | str) -> list[dict]:
    """
    То же, что get_stats_range, но с полным JSON (может быть тяжёлым).
    Возвращает список словарей с payload_json уже распарсенным.
    """
    rid = _realm_id(conn, realm)
    s = _as_dt(start).strftime("%Y-%m-%d %H:%M:%S")
    e = _as_dt(end).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.query_all(
        """
        SELECT id, collected_at, payload_json
        FROM stats_samples
        WHERE realm_id = ? AND collected_at BETWEEN ? AND ?
        ORDER BY collected_at ASC
        """,
        (rid, s, e),
    )
    for r in rows:
        try:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
        except Exception:
            r["payload"] = {}
    return rows

def get_stats_agg(conn: MySQLConnection, realm: str, minutes: int = 60) -> dict:
    """
    Сводная агрегация за последние N минут (avg/min/max основных метрик).
    """
    rid = _realm_id(conn, realm)
    rows = conn.query_all(
        """
        SELECT
          AVG(players_online) AS avg_players, MIN(players_online) AS min_players, MAX(players_online) AS max_players,
          AVG(tps_1m) AS avg_tps1, MIN(tps_1m) AS min_tps1, MAX(tps_1m) AS max_tps1,
          AVG(mspt) AS avg_mspt, MIN(mspt) AS min_mspt, MAX(mspt) AS max_mspt,
          AVG(cpu_sys) AS avg_cpu_sys, AVG(cpu_proc) AS avg_cpu_proc,
          AVG(heap_used) AS avg_heap_used, MAX(heap_used) AS peak_heap_used
        FROM stats_samples
        WHERE realm_id = ? AND collected_at >= (NOW() - INTERVAL ? MINUTE)
        """,
        (rid, int(minutes)),
    )
    return rows[0] if rows else {}

def purge_old_stats(conn: MySQLConnection, days: int = 30) -> int:
    """Удалить записи старше N дней. Возвращает число удалённых строк (если доступно)."""
    cur = conn.execute(
        "DELETE FROM stats_samples WHERE collected_at < (NOW() - INTERVAL ? DAY)",
        (int(days),),
    )
    conn.commit()
    try:
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        try:
            cur.close()
        except Exception:
            pass

# -------------
# utils
# -------------
def _as_dt(x: Any) -> datetime:
    if isinstance(x, datetime):
        return x
    s = str(x).replace("T", " ")
    return datetime.fromisoformat(s)

def _num(v):
    try:
        if v is None: return None
        if isinstance(v, (int, float)): return v
        s = str(v).strip()
        if s == "": return None
        if "." in s: return float(s)
        return int(s)
    except Exception:
        return None


# =========================================================
# Совместимые функции, которые ожидает gameservers.py
# =========================================================

def ensure_stats_schema(conn: MySQLConnection) -> None:
    init_stats_schema(conn)

def stats_save_snapshot(conn: MySQLConnection, realm: str, data: Dict[str, Any]) -> int:
    payload = dict(data or {})
    payload["realm"] = payload.get("realm") or realm
    if "ts" not in payload:
        try:
            payload["ts"] = int(time.time())
        except Exception:
            pass
    return save_server_stats(conn, payload)

def stats_get_latest(conn: MySQLConnection, realm: str) -> Optional[Dict[str, Any]]:
    rid = _realm_id(conn, realm)
    row = conn.query_one(
        """
        SELECT id, UNIX_TIMESTAMP(collected_at) AS ts_unix, players_online, players_max,
               tps_1m, tps_5m, tps_15m, mspt,
               heap_used, heap_max, cpu_sys, cpu_proc, payload_json
        FROM stats_samples
        WHERE realm_id = ?
        ORDER BY collected_at DESC, id DESC
        LIMIT 1
        """,
        (rid,),
    )
    if not row:
        return None

    if row.get("payload_json"):
        try:
            payload = json.loads(row["payload_json"] or "{}")
            if isinstance(payload, dict):
                payload = dict(payload)
                payload["realm"] = payload.get("realm") or realm
                payload["ts"] = payload.get("ts") or int(row.get("ts_unix") or time.time())
                return payload
        except Exception:
            pass

    return {
        "realm": realm,
        "ts": int(row.get("ts_unix") or time.time()),
        "mspt": _num(row.get("mspt")),
        "tps": {
            "1m": _num(row.get("tps_1m")),
            "5m": _num(row.get("tps_5m")),
            "15m": _num(row.get("tps_15m")),
            "mspt": _num(row.get("mspt")),
        },
        "players": {"online": _num(row.get("players_online")), "max": _num(row.get("players_max"))},
        "heap": {"used": _num(row.get("heap_used")), "max": _num(row.get("heap_max"))},
        "nonheap": {"used": None},
        "os": {"cpu_load": {"system": _num(row.get("cpu_sys")), "process": _num(row.get("cpu_proc"))}},
        "fs": {},
    }

def stats_get_series(
    conn: MySQLConnection,
    realm: str,
    *,
    since_ts: Optional[int] = None,
    limit: int = 720,
    step_sec: Optional[int] = 60,
    fields: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Возвращает [{"ts": <unix>, <field>: value, ...}, ...]
    Разрешённые поля:
      mspt, tps_1m, tps_5m, tps_15m, players_online,
      heap_used, heap_max, cpu_sys, cpu_proc
    """
    allowed = {
        "mspt","tps_1m","tps_5m","tps_15m","players_online",
        "heap_used","heap_max","cpu_sys","cpu_proc"
    }
    if fields:
        fset = [f for f in fields if f in allowed]
        if not fset:
            fset = ["tps_1m"]
    else:
        fset = ["tps_1m","tps_5m","tps_15m","players_online","mspt","cpu_sys","cpu_proc","heap_used","heap_max"]

    step = max(1, int(step_sec or 60))
    rid = _realm_id(conn, realm)

    where = ["realm_id = ?"]
    params: List[Any] = [rid]
    if since_ts:
        where.append("collected_at >= FROM_UNIXTIME(?)")
        params.append(int(since_ts))

    def agg_sql(col: str) -> str:
        if col in ("heap_max", "players_online"):
            return f"MAX({col}) AS {col}"
        return f"AVG({col}) AS {col}"

    bucket = f"(FLOOR(UNIX_TIMESTAMP(collected_at)/{step})*{step})"
    select_cols = [f"{bucket} AS ts"] + [agg_sql(c) for c in fset]

    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM stats_samples
        WHERE {" AND ".join(where)}
        GROUP BY {bucket}
        ORDER BY ts ASC
        LIMIT ?
    """
    params.append(int(limit or 720))
    rows = conn.query_all(sql, params) or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        item: Dict[str, Any] = {"ts": int(r["ts"])}
        for k in fset:
            item[k] = _num(r.get(k))
        if "cpu_sys" in item:
            item["cpu_system_load"] = item["cpu_sys"]
        if "cpu_proc" in item:
            item["cpu_process_load"] = item["cpu_proc"]
        out.append(item)
    return out


# =========================================================
# Доменные хелперы по дампу (LuckPerms / EasyPayments / LiteBans / AJLB / BCases)
# =========================================================

# ---------- LuckPerms (обычно отдельная БД) ----------
def lp_find_uuid(lp: MySQLConnection, username: str) -> Optional[str]:
    """Вернёт UUID по нику из luckperms_players."""
    row = lp.query_one("SELECT uuid FROM luckperms_players WHERE username = ? LIMIT 1", (username,))
    return (row or {}).get("uuid") if row else None

def lp_get_player(lp: MySQLConnection, uuid_or_name: str) -> Optional[dict]:
    """Инфо о игроке: uuid, username, primary_group + все его user_permissions."""
    uuid = uuid_or_name
    if "-" not in uuid_or_name or len(uuid_or_name) != 36:
        uuid = lp_find_uuid(lp, uuid_or_name) or ""
    if not uuid:
        return None
    p = lp.query_one("SELECT uuid, username, primary_group FROM luckperms_players WHERE uuid = ? LIMIT 1", (uuid,))
    if not p:
        return None
    perms = lp.query_all(
        "SELECT permission, value, server, world, expiry, contexts FROM luckperms_user_permissions WHERE uuid = ?",
        (uuid,),
    )
    p["permissions"] = perms
    return p

def lp_get_user_permissions(lp: MySQLConnection, uuid: str) -> List[dict]:
    return lp.query_all(
        "SELECT permission, value, server, world, expiry, contexts FROM luckperms_user_permissions WHERE uuid = ?",
        (uuid,),
    )

def lp_get_group_permissions(lp: MySQLConnection, group: str) -> List[dict]:
    return lp.query_all(
        "SELECT permission, value, server, world, expiry, contexts FROM luckperms_group_permissions WHERE name = ?",
        (group,),
    )

# ---------- EasyPayments ----------
def ep_get_customer_by_name(ep: MySQLConnection, username: str) -> Optional[dict]:
    """Возвращает карточку покупателя + его id (нужен для join'ов)."""
    return ep.query_one(
        "SELECT id, player_name, player_uuid, created_at, updated_at FROM easypayments_customers WHERE player_name = ?",
        (username,),
    )

def ep_get_payments_by_customer(ep: MySQLConnection, customer: Any) -> List[dict]:
    """
    Заказы покупателя. Аргумент может быть:
      - числовым id покупателя (int/str цифры)
      - ником игрока (мы найдём id через easypayments_customers)
    """
    customer_id: Optional[int] = None
    if isinstance(customer, int) or (isinstance(customer, str) and customer.isdigit()):
        customer_id = int(customer)
    else:
        row = ep_get_customer_by_name(ep, str(customer))
        if row and "id" in row:
            customer_id = int(row["id"])

    if customer_id is None:
        return []

    return ep.query_all(
        "SELECT * FROM easypayments_payments WHERE customer_id = ? ORDER BY created_at ASC",
        (customer_id,),
    )

def ep_get_purchases_by_payment(ep: MySQLConnection, payment_id: int) -> List[dict]:
    rows = ep.query_all(
        "SELECT * FROM easypayments_purchases WHERE payment_id = ? ORDER BY id ASC",
        (int(payment_id),),
    )
    # попытка распарсить JSON колонки
    for r in rows:
        for key in ("commands", "responses"):
            try:
                r[key] = json.loads(r.get(key) or "[]")
            except Exception:
                pass
    return rows

# ---------- LiteBans ----------
def lb_list_bans_by_uuid(lite: MySQLConnection, uuid: str, *, limit: int = 50) -> List[dict]:
    return lite.query_all(
        """
        SELECT id, uuid, ip, reason, banned_by_uuid, banned_by_name,
               removed_by_uuid, removed_by_name, removed_by_reason, removed_by_date,
               time, until, template, server_scope, server_origin,
               (silent+0) AS silent, (ipban+0) AS ipban, (ipban_wildcard+0) AS ipban_wildcard, (active+0) AS active
        FROM litebans_bans
        WHERE uuid = ?
        ORDER BY time DESC
        LIMIT ?
        """,
        (uuid, int(limit)),
    )

def lb_list_history_by_uuid(lite: MySQLConnection, uuid: str, *, limit: int = 50) -> List[dict]:
    return lite.query_all(
        """
        SELECT id, date, name, uuid, ip
        FROM litebans_history
        WHERE uuid = ?
        ORDER BY date DESC
        LIMIT ?
        """,
        (uuid, int(limit)),
    )

# ---------- AJ Leaderboards (leader) ----------
def leader_top_hours(leader: MySQLConnection, *, limit: int = 10) -> List[dict]:
    """Топ по наигранным часам (ajlb_statistic_hours_played.value)."""
    rows = leader.query_all(
        """
        SELECT id AS uuid, namecache AS username, value AS hours, suffixcache, prefixcache, displaynamecache
        FROM ajlb_statistic_hours_played
        ORDER BY CAST(value AS DECIMAL(65,2)) DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return rows

def leader_top_balance(leader: MySQLConnection, *, limit: int = 10) -> List[dict]:
    rows = leader.query_all(
        """
        SELECT id AS uuid, namecache AS username, value AS balance, suffixcache, prefixcache, displaynamecache
        FROM ajlb_vault_eco_balance
        ORDER BY CAST(value AS DECIMAL(65,2)) DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return rows

def leader_suffix_by_uuid(leader: MySQLConnection, uuid: str) -> Optional[str]:
    """Вытянуть кэшированный суффикс из ajlb_extras (placeholder=vault_ranksuffix)."""
    row = leader.query_one(
        "SELECT value FROM ajlb_extras WHERE id = ? AND placeholder = 'vault_ranksuffix' LIMIT 1",
        (uuid,),
    )
    return (row or {}).get("value") if row else None

# ---------- BCases ----------
def bcases_list_for_uuid_or_name(bc: MySQLConnection, uuid_or_name: str, *, limit: int = 100) -> List[dict]:
    """Строки из bcases_users по uuid или name (сортировка по issue_date)."""
    if "-" in uuid_or_name and len(uuid_or_name) == 36:
        rows = bc.query_all(
            """
            SELECT name, uuid, id AS case_id, issue_date, removal_date
            FROM bcases_users
            WHERE uuid = ?
            ORDER BY issue_date DESC
            LIMIT ?
            """,
            (uuid_or_name, int(limit)),
        )
    else:
        rows = bc.query_all(
            """
            SELECT name, uuid, id AS case_id, issue_date, removal_date
            FROM bcases_users
            WHERE name = ?
            ORDER BY issue_date DESC
            LIMIT ?
            """,
            (uuid_or_name, int(limit)),
        )
    return rows
