# app/cli.py
from __future__ import annotations

import os
import click
from flask import current_app
from pymysql.err import OperationalError
from werkzeug.security import generate_password_hash

from .database import get_db_connection, SCHEMA_SQL


def _apply_schema(conn) -> None:
    """
    Применяет батч CREATE/ALTER из SCHEMA_SQL.
    Обёртка умеет executescript(sql) и сама разбивает по ';'.
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
    try:
        row = cur.fetchone()
        return row is not None
    finally:
        cur.close()


def _table_exists(conn, table: str) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
        """,
        (table,),
    )
    try:
        return cur.fetchone() is not None
    finally:
        cur.close()


def _commit_if_possible(conn) -> None:
    try:
        conn.commit()
    except Exception:
        # На случай автокоммита — тихо игнорируем.
        pass


def _ensure_posts_extras(conn) -> None:
    """
    Добавляет недостающие поля для embed/attachments в таблицу posts.
    Пытается создать JSON; если не поддерживается — MEDIUMTEXT.
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

    # Индексы (без падений при дубликатах)
    for ddl in (
        "ALTER TABLE posts ADD INDEX idx_posts_created (created_at)",
        "ALTER TABLE posts ADD INDEX idx_posts_status (status)",
    ):
        try:
            conn.execute(ddl)
        except OperationalError:
            pass

    _commit_if_possible(conn)


def _upsert_admin_user(conn, username: str, password: str, role: str):
    """
    Создаёт или обновляет пользователя с ролью admin/superadmin.
    Если пользователь уже есть — обновляет пароль (если есть столбец password_hash) и роль.
    Возвращает (action_str, user_id|None).
    """
    if not _table_exists(conn, "users"):
        raise RuntimeError(
            "Таблица 'users' не найдена. Сначала выполните миграцию: flask --app run.py migrate"
        )

    has_pwd_col = _column_exists(conn, "users", "password_hash")
    if not _column_exists(conn, "users", "role"):
        raise RuntimeError("В таблице 'users' отсутствует столбец 'role'.")

    # Ищем по username
    cur = conn.execute("SELECT id, role FROM users WHERE username=%s LIMIT 1", (username,))
    row = cur.fetchone()
    cur.close()

    pwd_hash = generate_password_hash(password) if has_pwd_col else None

    if row:
        # Обновляем существующего
        if has_pwd_col and pwd_hash:
            conn.execute(
                "UPDATE users SET password_hash=%s, role=%s WHERE id=%s",
                (pwd_hash, role, row["id"]),
            )
        else:
            conn.execute(
                "UPDATE users SET role=%s WHERE id=%s",
                (role, row["id"]),
            )
        _commit_if_possible(conn)
        return "обновлён", row["id"]
    else:
        # Создаём нового
        if has_pwd_col:
            cur = conn.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                (username, pwd_hash, role),
            )
        else:
            cur = conn.execute(
                "INSERT INTO users (username, role) VALUES (%s, %s)",
                (username, role),
            )
        user_id = getattr(cur, "lastrowid", None)
        _commit_if_possible(conn)
        try:
            cur.close()
        except Exception:
            pass
        return "создан", user_id


def register_cli(app):
    @app.cli.command("migrate")
    def migrate():
        """
        Применяет SCHEMA_SQL и гарантирует поля embed/attachments в posts.
        Запуск:  flask --app run.py migrate
        """
        db_path = current_app.config.get("DB_PATH")
        with get_db_connection(db_path) as conn:
            click.echo("⏳ Применяю SCHEMA_SQL ...")
            _apply_schema(conn)
            click.echo("✅ Базовая схема применена.")

            click.echo("⏳ Проверяю дополнительные поля posts (embed/attachments) ...")
            _ensure_posts_extras(conn)
            click.echo("✅ Поля posts обновлены.")

        click.echo("🎉 Миграция завершена.")

    @app.cli.command("init-db")
    def init_db():
        """
        Синоним migrate (на случай привычного названия).
        """
        migrate.callback()  # переиспользуем логику

    @app.cli.command("create-admin")
    @click.option("--username", "-u", prompt="Логин", help="Имя пользователя (username).")
    @click.option(
        "--password",
        "-p",
        prompt=True,
        hide_input=True,
        confirmation_prompt=True,
        help="Пароль для входа.",
    )
    @click.option(
        "--role",
        "-r",
        type=click.Choice(["admin", "superadmin"], case_sensitive=False),
        default="admin",
        show_default=True,
        help="Роль, которую назначить пользователю.",
    )
    def create_admin(username: str, password: str, role: str):
        """
        Создаёт пользователя с ролью admin/superadmin или обновляет существующего.
        Примеры:
          flask --app run.py create-admin -u admin
          flask --app run.py create-admin -u boss -r superadmin
        """
        db_path = current_app.config.get("DB_PATH")
        with get_db_connection(db_path) as conn:
            click.echo("⏳ Создаю/обновляю администратора ...")
            try:
                action, user_id = _upsert_admin_user(conn, username.strip(), password, role.lower())
            except Exception as e:
                raise click.ClickException(str(e))

        tail = f" (id={user_id})" if user_id is not None else ""
        click.echo(f"✅ Пользователь '{username}' {action} с ролью '{role}'{tail}.")

    @app.cli.command("bootstrap")
    def bootstrap():
        """
        Полный первичный прогон:
          1) миграция схемы
          2) создание/обновление супер-админа из .env (если заданы ADMIN_USERNAME / ADMIN_PASSWORD)

        Пример:
          FLASK_APP=run.py flask bootstrap
        """
        # 1) migrate
        migrate.callback()

        # 2) admin from .env
        env_user = os.environ.get("ADMIN_USERNAME")
        env_pass = os.environ.get("ADMIN_PASSWORD")
        role = "superadmin"

        if env_user and env_pass:
            db_path = current_app.config.get("DB_PATH")
            with get_db_connection(db_path) as conn:
                click.echo("⏳ Создаю/обновляю суперпользователя из .env ...")
                try:
                    action, user_id = _upsert_admin_user(conn, env_user.strip(), env_pass, role)
                except Exception as e:
                    raise click.ClickException(str(e))
                tail = f" (id={user_id})" if user_id is not None else ""
                click.echo(f"✅ Пользователь '{env_user}' {action} с ролью '{role}'{tail}.")
        else:
            click.echo("ℹ️  ADMIN_USERNAME/ADMIN_PASSWORD в .env не заданы — шаг с админом пропущен.")
