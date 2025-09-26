# app/routes/admin/users.py
from __future__ import annotations

from flask import render_template, request, redirect, url_for, flash, session, current_app
from werkzeug.security import generate_password_hash

from ...database import get_db_connection, IntegrityError
from ...decorators import superadmin_required
from . import admin_bp
from .admin_common import ALLOWED_ROLES, check_csrf


@admin_bp.route("/users")
@superadmin_required
def admin_users():
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        users = conn.execute(
            """
            SELECT id, username, discord_id, role, is_superadmin, created_at
            FROM users
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()
    # ВАЖНО: указываем подпапку admin/ для шаблона
    return render_template("admin/users.html", users=users)


@admin_bp.route("/users/add", methods=["POST"])
@superadmin_required
def add_user():
    check_csrf()
    username   = (request.form.get("username") or "").strip()
    password   = request.form.get("password")
    discord_id = (request.form.get("discord_id") or "").strip() or None
    role       = request.form.get("role", "user")

    if role not in ALLOWED_ROLES:
        flash("Invalid role", "error")
        return redirect(url_for("admin.admin_users"))

    if username and password:
        try:
            with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
                conn.execute(
                    "INSERT INTO users (username, password_hash, discord_id, role) VALUES (?, ?, ?, ?)",
                    (username, generate_password_hash(password), discord_id, role),
                )
            flash("User created", "success")
        except IntegrityError:
            flash("Username or Discord ID already exists", "error")
        return redirect(url_for("admin.admin_users"))

    if discord_id:
        gen_username = f"discord_{discord_id}"
        try:
            with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
                conn.execute(
                    "INSERT INTO users (username, password_hash, discord_id, role) VALUES (?, ?, ?, ?)",
                    (gen_username, "discord_oauth", discord_id, role),
                )
            flash("Discord user added", "success")
        except IntegrityError:
            flash("Discord ID or generated username already exists", "error")
        return redirect(url_for("admin.admin_users"))

    flash("Provide username+password or Discord ID", "error")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/users/delete/<int:user_id>", methods=["POST"])
@superadmin_required
def delete_user(user_id: int):
    check_csrf()
    if user_id == session.get("user_id"):
        flash("You cannot delete yourself", "error")
        return redirect(url_for("admin.admin_users"))
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    flash("User deleted", "success")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/users/role/<int:user_id>", methods=["POST"])
@superadmin_required
def update_user_role(user_id: int):
    check_csrf()
    new_role = (request.form.get("role") or "").strip()
    if new_role not in ALLOWED_ROLES:
        flash("Invalid role", "error")
        return redirect(url_for("admin.admin_users"))
    if session.get("user_id") == user_id and new_role != "superadmin":
        flash("You cannot change your own role to non-superadmin", "error")
        return redirect(url_for("admin.admin_users"))
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute("UPDATE users SET role = ? WHERE id = ?", (new_role, user_id))
    flash("Role updated", "success")
    return redirect(url_for("admin.admin_users"))
