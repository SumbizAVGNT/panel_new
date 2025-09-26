# app/routes/admin/settings.py
from __future__ import annotations

from flask import render_template, request, redirect, url_for, flash, session, current_app, jsonify

from ...database import get_db_connection, get_setting, set_setting
from ...decorators import superadmin_required
from . import admin_bp
from .admin_common import check_csrf, probe_oauth_status, probe_chatwoot


@admin_bp.route("/settings", methods=["GET", "POST"])
@superadmin_required
def settings():
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        if request.method == "POST":
            check_csrf()
            # General
            set_setting(conn, "SITE_TITLE", (request.form.get("SITE_TITLE") or "").strip())
            set_setting(conn, "DISCORD_REDIRECT_URI", (request.form.get("DISCORD_REDIRECT_URI") or "").strip())

            # OAuth2 edit
            cid = request.form.get("DISCORD_CLIENT_ID")
            if cid is not None:
                set_setting(conn, "DISCORD_CLIENT_ID", (cid or "").strip())
            csec = (request.form.get("DISCORD_CLIENT_SECRET") or "").strip()
            if csec:
                set_setting(conn, "DISCORD_CLIENT_SECRET", csec)

            # Security
            set_setting(conn, "REQUIRE_LOGIN", "1" if request.form.get("REQUIRE_LOGIN") in ("1", "true", "on") else "0")
            set_setting(conn, "SESSION_HOURS", (request.form.get("SESSION_HOURS") or "12").strip())

            # Appearance
            set_setting(conn, "THEME", (request.form.get("THEME") or "dark").strip())
            set_setting(conn, "ACCENT", (request.form.get("ACCENT") or "#5865F2").strip())

            # Logs
            set_setting(conn, "LOG_RETENTION_DAYS", (request.form.get("LOG_RETENTION_DAYS") or "14").strip())
            set_setting(conn, "LOG_LEVEL", (request.form.get("LOG_LEVEL") or "INFO").strip())

            # Support (Chatwoot)
            cw_base = request.form.get("CHATWOOT_BASE_URL")
            cw_token = request.form.get("CHATWOOT_ACCESS_TOKEN")
            cw_client = request.form.get("CHATWOOT_CLIENT")
            cw_uid = request.form.get("CHATWOOT_UID")

            if cw_base is not None:
                set_setting(conn, "CHATWOOT_BASE_URL", (cw_base or "").strip())
            if cw_token is not None:
                set_setting(conn, "CHATWOOT_ACCESS_TOKEN", (cw_token or "").strip())
            if cw_client is not None:
                set_setting(conn, "CHATWOOT_CLIENT", (cw_client or "").strip())
            if cw_uid is not None:
                set_setting(conn, "CHATWOOT_UID", (cw_uid or "").strip())

            # Немедленная проверка
            if (cw_base is not None) or (cw_token is not None):
                base_saved = get_setting(conn, "CHATWOOT_BASE_URL", "")
                token_saved = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "")
                client_saved = get_setting(conn, "CHATWOOT_CLIENT", "")
                uid_saved = get_setting(conn, "CHATWOOT_UID", "")
                ok, reason = probe_chatwoot(base_saved, token_saved, client_saved or None, uid_saved or None)
                if ok:
                    flash("Support (Chatwoot) connected.", "success")
                else:
                    flash(f"Support (Chatwoot) misconfigured: {reason}", "warning")

            flash("Settings saved", "success")
            return redirect(url_for("admin.settings"))

        # GET
        site_title    = get_setting(conn, "SITE_TITLE", "MoonRein")
        client_id     = get_setting(conn, "DISCORD_CLIENT_ID", "")
        client_secret = get_setting(conn, "DISCORD_CLIENT_SECRET", None)
        redirect_uri  = get_setting(conn, "DISCORD_REDIRECT_URI", "http://127.0.0.1:5000/discord/callback")
        require_login = get_setting(conn, "REQUIRE_LOGIN", "1") == "1"
        session_hours = int(get_setting(conn, "SESSION_HOURS", "12") or 12)
        theme         = get_setting(conn, "THEME", "dark")
        accent        = get_setting(conn, "ACCENT", "#5865F2")
        log_days      = int(get_setting(conn, "LOG_RETENTION_DAYS", "14") or 14)
        log_level     = get_setting(conn, "LOG_LEVEL", "INFO")

        # Support (Chatwoot)
        cw_base    = get_setting(conn, "CHATWOOT_BASE_URL", "")
        cw_token   = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "")
        cw_client  = get_setting(conn, "CHATWOOT_CLIENT", "")
        cw_uid     = get_setting(conn, "CHATWOOT_UID", "")

        support_ok, support_reason = (False, None)
        if cw_base.strip() and cw_token.strip():
            support_ok, support_reason = probe_chatwoot(cw_base, cw_token, cw_client or None, cw_uid or None)

        # Bots
        bots = conn.execute(
            """
            SELECT id, platform, bot_id, name, avatar_url, active, created_at
            FROM bots
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()

        # Servers
        servers = conn.execute(
            """
            SELECT id, name, host, port, username, password, ssh_key_path,
                   last_status, last_uptime, last_checked, added_at
            FROM servers
            ORDER BY added_at DESC, id DESC
            """
        ).fetchall()

    oauth_ok, oauth_reason = False, None
    if (client_id or "").strip() and (client_secret or "").strip() and (redirect_uri or "").strip():
        oauth_ok, oauth_reason = probe_oauth_status(client_id, client_secret, redirect_uri)

    # ⬇⬇⬇ ключевая правка: путь к шаблону внутри папки admin
    return render_template(
        "admin/settings.html",
        site_title=site_title,
        client_id=client_id, has_secret=bool(client_secret), redirect_uri=redirect_uri,
        require_login=require_login, session_hours=session_hours,
        theme=theme, accent=accent, log_days=log_days, log_level=log_level,
        oauth_ok=oauth_ok, oauth_reason=oauth_reason,
        bots=bots,
        servers=servers,
        chatwoot_base=cw_base, chatwoot_token=bool(cw_token), chatwoot_client=cw_client, chatwoot_uid=cw_uid,
        support_ok=support_ok, support_reason=support_reason,
        csrf_token=session.get("_csrf"),
    )


@admin_bp.post("/settings/oauth2/disconnect")
@superadmin_required
def oauth_disconnect():
    from ...database import get_db_connection  # локальный импорт, чтобы не плодить циклы
    check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute(
            "DELETE FROM settings WHERE `key` IN (?, ?, ?)",
            ("DISCORD_CLIENT_ID", "DISCORD_CLIENT_SECRET", "DISCORD_REDIRECT_URI"),
        )
    return redirect(url_for("admin.settings"))


@admin_bp.get("/settings/oauth-status")
@superadmin_required
def oauth_status():
    from ...database import get_setting, get_db_connection
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        cid   = get_setting(conn, "DISCORD_CLIENT_ID", "") or ""
        csec  = get_setting(conn, "DISCORD_CLIENT_SECRET", "") or ""
        redir = get_setting(conn, "DISCORD_REDIRECT_URI", "") or ""
    if not (cid.strip() and csec.strip() and redir.strip()):
        return jsonify(ok=False, reason="Not configured")
    ok, reason = probe_oauth_status(cid, csec, redir)
    return jsonify(ok=ok, reason=reason)
