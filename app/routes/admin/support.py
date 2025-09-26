# app/routes/admin/support.py
from __future__ import annotations

from flask import redirect, url_for, flash, jsonify, current_app
from ...database import get_db_connection, get_setting, set_setting
from ...decorators import superadmin_required
from . import admin_bp
from .admin_common import check_csrf, probe_chatwoot

@admin_bp.get("/settings/support-status")
@superadmin_required
def support_status():
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        base  = get_setting(conn, "CHATWOOT_BASE_URL", "")
        token = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "")
        client = get_setting(conn, "CHATWOOT_CLIENT", "")
        uid = get_setting(conn, "CHATWOOT_UID", "")
    if not (base.strip() and token.strip()):
        return jsonify(ok=False, reason="Not configured")
    ok, reason = probe_chatwoot(base, token, client or None, uid or None)
    return jsonify(ok=ok, reason=reason)

@admin_bp.post("/settings/support-test")
@superadmin_required
def support_test():
    check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        base   = get_setting(conn, "CHATWOOT_BASE_URL", "") or ""
        token  = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "") or ""
        client = get_setting(conn, "CHATWOOT_CLIENT", "") or ""
        uid    = get_setting(conn, "CHATWOOT_UID", "") or ""

    if not (base.strip() and token.strip()):
        flash("Support (Chatwoot) is not configured: set Base URL and Access Token.", "warning")
        return redirect(url_for("admin.settings"))

    ok, reason = probe_chatwoot(base, token, client or None, uid or None)
    if ok:
        flash("Support (Chatwoot) connection OK.", "success")
    else:
        flash(f"Support (Chatwoot) check failed: {reason}", "error")
    return redirect(url_for("admin.settings"))

@admin_bp.post("/settings/support-disconnect")
@superadmin_required
def support_disconnect():
    check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute(
            "DELETE FROM settings WHERE `key` IN (?, ?, ?, ?)",
            ("CHATWOOT_BASE_URL", "CHATWOOT_ACCESS_TOKEN", "CHATWOOT_CLIENT", "CHATWOOT_UID"),
        )
    flash("Support (Chatwoot) disconnected: credentials removed.", "success")
    return redirect(url_for("admin.settings"))
