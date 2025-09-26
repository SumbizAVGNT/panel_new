# app/__init__.py
import os
from datetime import timedelta
from secrets import token_urlsafe

from dotenv import load_dotenv
from flask import Flask, g, session
from jinja2 import FileSystemLoader  # <-- добавили

from .routes.news import news_bp
from .cli import register_cli


def create_app():
    # Подхватываем .env из корня проекта
    load_dotenv()

    app = Flask(__name__, template_folder="templates", static_folder="static")

    # --- Secret key ---
    app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32))

    # --- Безопасность сессий ---
    session_cookie_secure = os.environ.get('SESSION_COOKIE_SECURE', '0') == '1'

    # --- Путь к БД (для совместимости, фактически не используется в MySQL) ---
    raw_db_path = os.environ.get('DB_PATH', os.path.join(app.instance_path, 'database.db'))
    if not os.path.isabs(raw_db_path):
        raw_db_path = os.path.join(app.root_path, raw_db_path)
    os.makedirs(os.path.dirname(raw_db_path), exist_ok=True)
    os.makedirs(app.instance_path, exist_ok=True)

    # --- Конфиг приложения ---
    app.config.update(
        DISCORD_CLIENT_ID=os.environ.get('DISCORD_CLIENT_ID', ''),
        DISCORD_CLIENT_SECRET=os.environ.get('DISCORD_CLIENT_SECRET', ''),
        DISCORD_REDIRECT_URI=os.environ.get('DISCORD_REDIRECT_URI', 'http://127.0.0.1:5000/discord/callback'),

        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=session_cookie_secure,
        SESSION_COOKIE_SAMESITE='Lax',
        PERMANENT_SESSION_LIFETIME=timedelta(hours=int(os.environ.get('SESSION_HOURS', '12') or 12)),

        DB_PATH=raw_db_path,  # игнорируется MySQL-обёрткой, оставлено для совместимости
    )

    # --- ДОБАВЛЯЕМ layout/ в поиск шаблонов ---
    # Теперь {% extends "base.html" %} найдёт файл в app/templates/layout/base.html
    try:
        loader: FileSystemLoader = app.jinja_loader  # Flask по умолчанию FileSystemLoader
        layout_path = os.path.join(app.root_path, 'templates', 'layout')
        if hasattr(loader, "searchpath") and layout_path not in loader.searchpath:
            loader.searchpath.append(layout_path)
    except Exception:
        # тихо игнорируем — в стандартной конфигурации выше достаточно
        pass

    # --- Авто-инициализация схемы в MySQL + «автобутстрап» суперпользователя из .env ---
    from .database import get_db_connection, init_db

    def _ensure_schema_and_bootstrap():
        from werkzeug.security import generate_password_hash

        with get_db_connection(None) as conn:
            # 1) схема (CREATE TABLE IF NOT EXISTS …)
            init_db(conn)

            # 2) если в .env заданы ADMIN_USERNAME/ADMIN_PASSWORD — создаём/обновляем суперпользователя
            env_user = (os.environ.get("ADMIN_USERNAME") or "").strip()
            env_pass = (os.environ.get("ADMIN_PASSWORD") or "").strip()
            if not (env_user and env_pass):
                return

            # убеждаемся, что таблица users существует
            row = conn.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'users'
                """
            ).fetchone()
            if not row:
                return

            count_row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
            total = int(count_row["c"] if count_row and "c" in count_row else 0)

            pwd_hash = generate_password_hash(env_pass)

            if total == 0:
                conn.execute(
                    "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                    (env_user, pwd_hash, "superadmin"),
                )
            else:
                u = conn.execute(
                    "SELECT id FROM users WHERE username = ? LIMIT 1",
                    (env_user,),
                ).fetchone()
                if u:
                    conn.execute(
                        "UPDATE users SET password_hash = ?, role = 'superadmin' WHERE id = ?",
                        (pwd_hash, u["id"]),
                    )
                else:
                    conn.execute(
                        "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                        (env_user, pwd_hash, "superadmin"),
                    )
            try:
                conn.commit()
            except Exception:
                pass

    _ensure_schema_and_bootstrap()

    # --- CLI команды ---
    register_cli(app)

    # --- DB helpers (g.db) ---
    from .database import get_db_connection as _get_conn

    @app.before_request
    def _open_db():
        if getattr(g, 'db', None) is None:
            g.db = _get_conn(app.config['DB_PATH'])

    @app.teardown_appcontext
    def _close_db(_exc):
        db = getattr(g, 'db', None)
        if db is not None:
            db.close()

    # --- Простой CSRF-токен для форм ---
    @app.context_processor
    def inject_csrf():
        token = session.get('_csrf')
        if not token:
            token = token_urlsafe(32)
            session['_csrf'] = token
        return {'csrf_token': token}

    # --- Роуты ---
    from .routes.auth import auth_bp
    from .routes.dashboard import dashboard_bp
    from .routes.admin import admin_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(news_bp)

    return app
