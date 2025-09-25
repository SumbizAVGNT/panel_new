# app/__init__.py
import os
from datetime import timedelta
from secrets import token_urlsafe

from dotenv import load_dotenv
from flask import Flask, g, session
from .routes.news import news_bp
from .cli import register_cli


def create_app():
    # Подхватываем .env из корня проекта
    load_dotenv()

    app = Flask(__name__)

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
        PERMANENT_SESSION_LIFETIME=timedelta(hours=12),

        DB_PATH=raw_db_path,  # игнорируется, оставлено для совместимости
    )

    # --- Авто-инициализация схемы в MySQL ---
    from .database import get_db_connection, init_db
    def _ensure_schema():
        conn = get_db_connection(None)
        try:
            init_db(conn)  # CREATE TABLE IF NOT EXISTS — безопасно
        finally:
            conn.close()
    _ensure_schema()

    # --- CLI команды ---
    register_cli(app)

    # --- DB helpers (g.db) ---
    from .database import get_db_connection as _get_conn

    @app.before_request
    def _open_db():
        if getattr(g, 'db', None) is None:
            # параметр остаётся, но внутри игнорируется и подключение идёт по ENV
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
