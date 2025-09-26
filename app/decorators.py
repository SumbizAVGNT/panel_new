# panel/app/decorators.py
from __future__ import annotations

from functools import wraps
from typing import Tuple, Optional

from flask import g, session, redirect, url_for, flash, current_app

from .database import get_db_connection


def _conn() -> Tuple[object, bool]:
    """
    Возвращает (conn, need_close):
    - если в g уже есть соединение (например, создано в before_request) — используем его;
    - иначе создаём временное через get_db_connection() и потом закроем сами.
    """
    if getattr(g, "db", None) is not None:
        return g.db, False
    # get_db_connection сам читает конфиг (MySQL/SQLite), параметр не нужен
    conn = get_db_connection()
    return conn, True


def _fetch_user(conn, user_id: int) -> Optional[dict]:
    """
    Универсально получает пользователя по id.
    Делает попытку с синтаксисом SQLite ('?'), если драйвер не принимает — повторяет с MySQL ('%s').
    Возвращает словарь-строку (row) или None.
    """
    # Предпочитаем кросс-совместимый способ: попробуем с '?'
    sql_q = "SELECT id, username, role, discord_id FROM users WHERE id = ?"
    sql_p = "SELECT id, username, role, discord_id FROM users WHERE id = %s"

    try:
        row = conn.execute(sql_q, (user_id,)).fetchone()
        return row
    except Exception:
        # Повторяем в стиле MySQL
        cur = conn.execute(sql_p, (user_id,))
        row = cur.fetchone()
        return row


def _ensure_logged_in():
    """Общий guard: требует наличие user_id в сессии, иначе редирект на логин."""
    if "user_id" not in session:
        flash("Please log in", "warning")
        return redirect(url_for("auth.login"))
    return None


def _require_role(allowed_roles: Tuple[str, ...]):
    """
    Фабрика декораторов по ролям.
    allowed_roles: кортеж допустимых ролей, напр. ('admin', 'superadmin') или ('superadmin',)
    """
    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            # 1) Должен быть залогинен
            need_login = _ensure_logged_in()
            if need_login:
                return need_login

            # 2) Берём соединение (g.db или временное)
            conn, need_close = _conn()
            row = None
            try:
                row = _fetch_user(conn, session["user_id"])
                if row:
                    # Сохраняем в g для шаблонов (base.html и др.)
                    g.user = row
            finally:
                if need_close:
                    try:
                        conn.close()
                    except Exception:
                        pass

            # 3) Сессия может быть битой / пользователь удалён
            if not row:
                session.clear()
                flash("Session expired. Please log in again.", "error")
                return redirect(url_for("auth.login"))

            # 4) Проверка роли
            user_role = (row.get("role") if hasattr(row, "get") else row["role"])
            if allowed_roles and user_role not in allowed_roles:
                flash("Access denied", "error")
                return redirect(url_for("dashboard.dashboard"))

            # 5) Всё ок — пускаем
            return view(*args, **kwargs)

        return wrapper
    return decorator


def login_required(view):
    """
    Требует только факт логина (без проверки роли).
    """
    @wraps(view)
    def wrapper(*args, **kwargs):
        need_login = _ensure_logged_in()
        if need_login:
            return need_login
        return view(*args, **kwargs)
    return wrapper


# Доступ для admin и superadmin
admin_required = _require_role(("admin", "superadmin"))

# Доступ только для superadmin
superadmin_required = _require_role(("superadmin",))
