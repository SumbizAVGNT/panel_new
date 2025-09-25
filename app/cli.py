# app/cli.py
import click
from flask import current_app
from pymysql.err import OperationalError

from .database import get_db_connection, SCHEMA_SQL


def _apply_schema(conn) -> None:
    """
    Применяет батч CREATE/ALTER из SCHEMA_SQL.
    Ваша обёртка уже умеет executescript(sql) и сама разбивает по ';'.
    """
    conn.executescript(SCHEMA_SQL)


def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (table, column),
    )
    row = cur.fetchone()
    return row is not None


def _ensure_posts_extras(conn) -> None:
    """
    Добавляет недостающие поля для embed/attachments в таблицу posts.
    Пытается создать JSON; если не поддерживается — MEDIUMTEXT.
    Коммит не нужен: ваша обёртка выполняет DDL сразу.
    """
    # embed_json
    if not _column_exists(conn, "posts", "embed_json"):
        try:
            conn.execute("ALTER TABLE posts ADD COLUMN embed_json JSON NULL")
        except OperationalError:
            conn.execute("ALTER TABLE posts ADD COLUMN embed_json MEDIUMTEXT NULL")

    # attachment_file
    if not _column_exists(conn, "posts", "attachment_file"):
        conn.execute("ALTER TABLE posts ADD COLUMN attachment_file VARCHAR(512) NULL")

    # attachment_name
    if not _column_exists(conn, "posts", "attachment_name"):
        conn.execute("ALTER TABLE posts ADD COLUMN attachment_name VARCHAR(255) NULL")

    # attachment_mime
    if not _column_exists(conn, "posts", "attachment_mime"):
        conn.execute("ALTER TABLE posts ADD COLUMN attachment_mime VARCHAR(100) NULL")

    # при необходимости можно добавить индексы с проверкой, чтобы не падать на duplicate:
    # try: conn.execute("ALTER TABLE posts ADD INDEX idx_posts_created (created_at)")
    # except OperationalError: pass
    # try: conn.execute("ALTER TABLE posts ADD INDEX idx_posts_status (status)")
    # except OperationalError: pass


def register_cli(app):
    @app.cli.command("migrate")
    def migrate():
        """
        Применяет SCHEMA_SQL и гарантирует поля embed/attachments в posts.
        Запуск:  flask --app run.py migrate
        """
        db_path = current_app.config.get("DB_PATH")
        conn = get_db_connection(db_path)

        click.echo("⏳ Применяю SCHEMA_SQL ...")
        _apply_schema(conn)
        click.echo("✅ Базовая схема применена.")

        click.echo("⏳ Проверяю дополнительные поля posts (embed/attachments) ...")
        _ensure_posts_extras(conn)
        click.echo("✅ Поля posts обновлены.")

        click.echo("🎉 Миграция завершена.")
