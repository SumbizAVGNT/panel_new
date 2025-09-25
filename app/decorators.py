from functools import wraps
from flask import g, session, redirect, url_for, flash, current_app
from .database import get_db_connection


def _conn():
    """
    Возвращает текущее соединение (g.db), либо создаёт временное к DB_PATH.
    Вызывающий не обязан закрывать g.db, временное соединение закрываем сами.
    """
    if getattr(g, "db", None) is not None:
        return g.db, False  # (conn, need_close)
    conn = get_db_connection(current_app.config["DB_PATH"])
    return conn, True


def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in", "warning")
            return redirect(url_for("auth.login"))
        return view(*args, **kwargs)
    return wrapper


def admin_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in", "warning")
            return redirect(url_for("auth.login"))

        conn, need_close = _conn()
        try:
            row = conn.execute(
                "SELECT id, username, role, discord_id FROM users WHERE id = ?",
                (session["user_id"],)
            ).fetchone()
            if row:
                # полезно для base.html
                g.user = row
        finally:
            if need_close:
                conn.close()

        if not row:
            session.clear()
            flash("Session expired. Please log in again.", "error")
            return redirect(url_for("auth.login"))

        if row["role"] not in ("admin", "superadmin"):
            flash("Access denied", "error")
            return redirect(url_for("dashboard.dashboard"))

        return view(*args, **kwargs)
    return wrapper


def superadmin_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in", "warning")
            return redirect(url_for("auth.login"))

        conn, need_close = _conn()
        try:
            row = conn.execute(
                "SELECT id, username, role, discord_id FROM users WHERE id = ?",
                (session["user_id"],)
            ).fetchone()
            if row:
                g.user = row
        finally:
            if need_close:
                conn.close()

        if not row:
            session.clear()
            flash("Session expired. Please log in again.", "error")
            return redirect(url_for("auth.login"))

        if row["role"] != "superadmin":
            flash("Access denied", "error")
            return redirect(url_for("dashboard.dashboard"))

        return view(*args, **kwargs)
    return wrapper
