# app/routes/admin.py
from __future__ import annotations

import os
import time
import socket
import datetime
from typing import Optional

import requests
from werkzeug.security import generate_password_hash
from werkzeug.utils import secure_filename
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, session, current_app, abort, jsonify
)

from ..database import get_db_connection, get_setting, set_setting, IntegrityError
from ..decorators import superadmin_required

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

ALLOWED_ROLES = {"pending", "user", "admin", "superadmin"}


# ---------------- CSRF ----------------
def _check_csrf() -> None:
    if request.form.get("csrf_token") != session.get("_csrf"):
        abort(400, description="CSRF token invalid")


# ---------------- Discord OAuth2 probe ----------------
def _probe_oauth_status(client_id: str, client_secret: str, redirect_uri: str):
    """
    Быстрая лайв-проверка валидности пары client_id/secret/redirect_uri.
    Ожидаем 400 invalid_grant (значит креды валидны, код фейковый) или 401 при неверных кредах.
    """
    try:
        r = requests.post(
            "https://discord.com/api/oauth2/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "authorization_code",
                "code": "dummy",
                "redirect_uri": redirect_uri,
            },
            timeout=7,
        )
        ctype = r.headers.get("content-type", "")
        if r.status_code == 400 and ctype.startswith("application/json"):
            try:
                if (r.json() or {}).get("error") == "invalid_grant":
                    return True, None
            except Exception:
                pass
        if r.status_code == 401:
            return False, "Invalid Client ID/Secret"
        return False, f"Discord responded: {r.status_code}"
    except requests.Timeout:
        return False, "Network: timeout"
    except requests.SSLError as e:
        return False, f"Network: SSL error ({e})"
    except requests.ConnectionError as e:
        return False, f"Network: connection error ({e})"
    except requests.RequestException as e:
        return False, f"Network: {e.__class__.__name__} ({e})"


# ---------- Chatwoot (Support) helpers ----------
def _normalize_base(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _probe_chatwoot(base_url: str, access_token: str, client: str | None = None, uid: str | None = None):
    """
    Проверка Chatwoot по REST API.
    Делаем GET на /api/v1/accounts с токеном; ожидаем 200/OK и JSON.
    """
    if not base_url or not access_token:
        return False, "Base URL and Access Token required"

    base = _normalize_base(base_url)
    test_url = f"{base}/api/v1/accounts"

    headers = {
        "Accept": "application/json",
        # Chatwoot понимает оба варианта — оставим оба:
        "Authorization": f"Bearer {access_token}",
        "api_access_token": access_token,
    }
    if client:
        headers["X-Chatwoot-Client"] = client
    if uid:
        headers["X-Chatwoot-Uid"] = uid

    try:
        r = requests.get(test_url, headers=headers, timeout=8)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
            return True, None
        if r.status_code in (401, 403):
            return False, "Unauthorized (check token)"
        return False, f"Chatwoot responded: {r.status_code}"
    except requests.Timeout:
        return False, "Network: timeout"
    except requests.RequestException as e:
        return False, f"Network: {e.__class__.__name__} ({e})"


# ===================== USERS =====================
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
    return render_template("admin_users.html", users=users)


@admin_bp.route("/users/add", methods=["POST"])
@superadmin_required
def add_user():
    _check_csrf()
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
    _check_csrf()
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
    _check_csrf()
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


# ===================== SETTINGS / OAUTH2 / BOTS / SERVERS / SUPPORT =====================
@admin_bp.route("/settings", methods=["GET", "POST"])
@superadmin_required
def settings():
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        if request.method == "POST":
            _check_csrf()
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

            # --- Support (Chatwoot) ---
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

            # Если передали base/token — сразу проверим и сообщим результат
            if (cw_base is not None) or (cw_token is not None):
                base_saved = get_setting(conn, "CHATWOOT_BASE_URL", "")
                token_saved = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "")
                client_saved = get_setting(conn, "CHATWOOT_CLIENT", "")
                uid_saved = get_setting(conn, "CHATWOOT_UID", "")
                ok, reason = _probe_chatwoot(base_saved, token_saved, client_saved or None, uid_saved or None)
                if ok:
                    flash("Support (Chatwoot) connected.", "success")
                else:
                    flash(f"Support (Chatwoot) misconfigured: {reason}", "warning")

            flash("Settings saved", "success")
            return redirect(url_for("admin.settings"))

        # GET — читаем настройки
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
            support_ok, support_reason = _probe_chatwoot(cw_base, cw_token, cw_client or None, cw_uid or None)

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

    # Статус OAuth2
    oauth_ok, oauth_reason = False, None
    if (client_id or "").strip() and (client_secret or "").strip() and (redirect_uri or "").strip():
        oauth_ok, oauth_reason = _probe_oauth_status(client_id, client_secret, redirect_uri)

    return render_template(
        "settings.html",
        site_title=site_title,
        client_id=client_id, has_secret=bool(client_secret), redirect_uri=redirect_uri,
        require_login=require_login, session_hours=session_hours,
        theme=theme, accent=accent, log_days=log_days, log_level=log_level,
        oauth_ok=oauth_ok, oauth_reason=oauth_reason,
        bots=bots,
        servers=servers,
        # Support (Chatwoot) для шаблона
        chatwoot_base=cw_base, chatwoot_token=bool(cw_token), chatwoot_client=cw_client, chatwoot_uid=cw_uid,
        support_ok=support_ok, support_reason=support_reason,
        csrf_token=session.get("_csrf"),
    )


@admin_bp.post("/settings/oauth2/disconnect")
@superadmin_required
def oauth_disconnect():
    _check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute(
            "DELETE FROM settings WHERE `key` IN (?, ?, ?)",
            ("DISCORD_CLIENT_ID", "DISCORD_CLIENT_SECRET", "DISCORD_REDIRECT_URI"),
        )
    flash("Discord OAuth2 disconnected (credentials removed).", "success")
    return redirect(url_for("admin.settings"))


@admin_bp.get("/settings/oauth-status")
@superadmin_required
def oauth_status():
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        cid   = get_setting(conn, "DISCORD_CLIENT_ID", "") or ""
        csec  = get_setting(conn, "DISCORD_CLIENT_SECRET", "") or ""
        redir = get_setting(conn, "DISCORD_REDIRECT_URI", "") or ""
    if not (cid.strip() and csec.strip() and redir.strip()):
        return jsonify(ok=False, reason="Not configured")
    ok, reason = _probe_oauth_status(cid, csec, redir)
    return jsonify(ok=ok, reason=reason)


@admin_bp.get("/settings/support-status")
@superadmin_required
def support_status():
    """AJAX-проверка Chatwoot (Support)."""
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        base  = get_setting(conn, "CHATWOOT_BASE_URL", "")
        token = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "")
        client = get_setting(conn, "CHATWOOT_CLIENT", "")
        uid = get_setting(conn, "CHATWOOT_UID", "")
    if not (base.strip() and token.strip()):
        return jsonify(ok=False, reason="Not configured")
    ok, reason = _probe_chatwoot(base, token, client or None, uid or None)
    return jsonify(ok=ok, reason=reason)


# ---------------- BOTS ----------------
def _fetch_bot_info(platform: str, token: str):
    """
    Вернёт (bot_id, name, avatar_url) для платформ: discord/telegram/vk.
    """
    platform = platform.lower()

    if platform == "discord":
        r = requests.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": f"Bot {token}"},
            timeout=7,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Discord: {r.status_code} {r.text[:200]}")
        j = r.json()
        bot_id = str(j["id"])
        name = j.get("username") or j.get("global_name") or f"discord_{bot_id}"
        avatar = j.get("avatar")
        avatar_url = f"https://cdn.discordapp.com/avatars/{bot_id}/{avatar}.png?size=128" if avatar else None
        return bot_id, name, avatar_url

    if platform == "telegram":
        r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=7)
        if r.status_code != 200 or not (r.json().get("ok")):
            raise RuntimeError(f"Telegram: {r.status_code} {r.text[:200]}")
        me = r.json()["result"]
        bot_id = str(me["id"])
        name = me.get("first_name") or me.get("username") or f"telegram_{bot_id}"
        avatar_url = None
        try:
            photos = requests.get(
                f"https://api.telegram.org/bot{token}/getUserProfilePhotos",
                params={"user_id": bot_id, "limit": 1},
                timeout=7,
            ).json()
            if photos.get("ok") and photos["result"]["total_count"] > 0:
                file_id = photos["result"]["photos"][0][0]["file_id"]
                file = requests.get(
                    f"https://api.telegram.org/bot{token}/getFile",
                    params={"file_id": file_id},
                    timeout=7,
                ).json()
                if file.get("ok"):
                    file_path = file["result"]["file_path"]
                    avatar_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
        except Exception:
            pass
        return bot_id, name, avatar_url

    if platform == "vk":
        r = requests.get(
            "https://api.vk.com/method/groups.getById",
            params={"access_token": token, "v": "5.131", "fields": "name,screen_name,photo_100"},
            timeout=7,
        )
        j = r.json()
        if "error" in j:
            raise RuntimeError(f"VK: {j['error'].get('error_msg','error')}")
        info = j["response"][0]
        bot_id = str(info["id"])
        name = info.get("name") or info.get("screen_name") or f"vk_{bot_id}"
        avatar_url = info.get("photo_100")
        return bot_id, name, avatar_url

    raise ValueError("Unsupported platform")


@admin_bp.post("/settings/bots/add")
@superadmin_required
def bots_add():
    _check_csrf()
    platform = (request.form.get("platform") or "").lower().strip()
    token    = (request.form.get("token") or "").strip()
    if platform not in ("discord", "telegram", "vk"):
        flash("Choose platform: discord / telegram / vk", "error")
        return redirect(url_for("admin.settings"))
    if not token:
        flash("Token is required", "error")
        return redirect(url_for("admin.settings"))

    try:
        bot_id, name, avatar_url = _fetch_bot_info(platform, token)
    except Exception as e:
        flash(f"Failed to add {platform} bot: {e}", "error")
        return redirect(url_for("admin.settings"))

    try:
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute(
                "INSERT INTO bots(platform, bot_id, name, avatar_url, token, active) VALUES(?,?,?,?,?,1) "
                "ON DUPLICATE KEY UPDATE name=VALUES(name), avatar_url=VALUES(avatar_url), token=VALUES(token), active=1",
                (platform, bot_id, name, avatar_url, token),
            )
        flash(f"{platform.capitalize()} bot “{name}” added/updated", "success")
    except IntegrityError:
        flash("Bot already exists", "error")
    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/bots/<int:bot_pk>/delete")
@superadmin_required
def bots_delete(bot_pk: int):
    _check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute("DELETE FROM bots WHERE id = ?", (bot_pk,))
    flash("Bot removed", "success")
    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/bots/<int:bot_pk>/refresh")
@superadmin_required
def bots_refresh(bot_pk: int):
    _check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        row = conn.execute("SELECT id, platform, token FROM bots WHERE id = ?", (bot_pk,)).fetchone()
    if not row:
        flash("Bot not found", "error")
        return redirect(url_for("admin.settings"))
    try:
        _, name, avatar_url = _fetch_bot_info(row["platform"], row["token"])
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute("UPDATE bots SET name = ?, avatar_url = ? WHERE id = ?", (name, avatar_url, bot_pk))
        flash("Bot info refreshed", "success")
    except Exception as e:
        flash(f"Refresh failed: {e}", "error")
    return redirect(url_for("admin.settings"))


# ===================== SERVERS (add/delete/check + upload key) =====================
def _keys_dir() -> str:
    base = os.path.dirname(current_app.root_path)  # корень проекта
    path = os.path.join(base, "uploads", "keys")
    os.makedirs(path, exist_ok=True)
    return path


@admin_bp.post("/settings/servers/add")
@superadmin_required
def servers_add():
    _check_csrf()
    name     = (request.form.get("name") or "").strip() or None
    host     = (request.form.get("host") or "").strip()
    port     = request.form.get("port", type=int) or 22
    username = (request.form.get("username") or "").strip() or None
    password = (request.form.get("password") or "").strip() or None
    key_file = request.files.get("ssh_key")

    if not (host and username):
        flash("Fill host and username", "error")
        return redirect(url_for("admin.settings"))

    ssh_key_path = None
    if key_file and key_file.filename:
        fname = f"{int(time.time())}_{secure_filename(key_file.filename)}"
        dest = os.path.join(_keys_dir(), fname)
        key_file.save(dest)
        ssh_key_path = dest

    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute(
            """
            INSERT INTO servers(name, host, port, username, password, ssh_key_path)
            VALUES (?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              password=VALUES(password),
              ssh_key_path=VALUES(ssh_key_path)
            """,
            (name, host, port, username, password, ssh_key_path),
        )

    flash("Server added", "success")
    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/servers/<int:server_id>/delete")
@superadmin_required
def servers_delete(server_id: int):
    _check_csrf()
    # удалим ключ с диска (если есть)
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        row = conn.execute("SELECT ssh_key_path FROM servers WHERE id = ?", (server_id,)).fetchone()
    if row and row["ssh_key_path"]:
        try:
            os.remove(row["ssh_key_path"])
        except Exception:
            pass

    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute("DELETE FROM servers WHERE id = ?", (server_id,))
    flash("Server removed", "success")
    return redirect(url_for("admin.settings"))


def _tcp_ping(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _ssh_uptime(host: str, port: int, username: str, password: Optional[str], ssh_key_path: Optional[str]) -> Optional[str]:
    """
    Пытается получить красивый uptime по SSH: `uptime -p`.
    Возвращает строку или None. Не фейлит, если нет paramiko.
    """
    try:
        import paramiko  # optional
    except Exception:
        return None

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if ssh_key_path:
            key = None
            try:
                key = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            except Exception:
                try:
                    key = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
                except Exception:
                    key = None
            if key is not None:
                client.connect(hostname=host, port=port, username=username, pkey=key, timeout=5)
            else:
                client.connect(hostname=host, port=port, username=username, password=password or "", timeout=5)
        else:
            client.connect(hostname=host, port=port, username=username, password=password or "", timeout=5)

        stdin, stdout, stderr = client.exec_command("uptime -p", timeout=5)
        out = stdout.read().decode("utf-8", "ignore").strip()
        stderr.read()  # consume
        client.close()
        if out:
            return out
    except Exception:
        return None
    return None


# --- helpers to detect servers.last_status storage and allowed values ---
def _servers_last_status_storage():
    """
    Возвращает dict с информацией о поле last_status:
    {
      "kind": "enum" | "text" | "numeric",
      "allowed": ["online","offline","unknown"]  # только для enum, иначе []
    }
    """
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        row = conn.execute(
            """
            SELECT DATA_TYPE, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'servers'
              AND COLUMN_NAME = 'last_status'
            """
        ).fetchone()

    if not row:
        return {"kind": "text", "allowed": []}

    data_type = (row["DATA_TYPE"] or "").lower()
    # В некоторых драйверах может быть только COLUMN_TYPE / column_type
    col_type = (row.get("COLUMN_TYPE") if hasattr(row, "get") else None) or ""
    col_type = col_type.lower()

    if data_type == "enum" or col_type.startswith("enum("):
        allowed = []
        if "(" in col_type and ")" in col_type:
            inside = col_type[col_type.find("(") + 1: col_type.rfind(")")]
            parts = [p.strip() for p in inside.split(",")]
            for p in parts:
                if p.startswith("'") and p.endswith("'") and len(p) >= 2:
                    allowed.append(p[1:-1])
        return {"kind": "enum", "allowed": allowed}

    if any(k in data_type for k in ("char", "text", "varchar")):
        return {"kind": "text", "allowed": []}

    return {"kind": "numeric", "allowed": []}


def _pick_status_value_for_db(reachable: bool) -> object:
    """
    Возвращает корректное значение для записи в servers.last_status
    с учётом текущей схемы (enum/text/numeric) и разрешённых enum-значений.
    """
    info = _servers_last_status_storage()
    if info["kind"] == "numeric":
        return 1 if reachable else 0

    desired_primary = "online" if reachable else "offline"
    desired_alt     = "up" if reachable else "down"

    if info["kind"] == "enum":
        allowed = info["allowed"]
        if desired_primary in allowed:
            return desired_primary
        if desired_alt in allowed:
            return desired_alt
        candidates_true  = ["on", "true", "yes", "1"]
        candidates_false = ["off", "false", "no", "0"]
        for c in (candidates_true if reachable else candidates_false):
            if c in allowed:
                return c
        return allowed[0] if allowed else desired_primary

    return desired_primary


@admin_bp.post("/settings/servers/<int:server_id>/check")
@superadmin_required
def servers_check(server_id: int):
    """
    Проверка доступности сервера и запись last_status/last_uptime/last_checked
    с учётом фактического типа колонки last_status (ENUM/Text/Numeric).
    """
    _check_csrf()

    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        s = conn.execute(
            """
            SELECT id, host, port, username, password, ssh_key_path
            FROM servers WHERE id = ?
            """,
            (server_id,),
        ).fetchone()

    if not s:
        flash("Server not found", "error")
        return redirect(url_for("admin.settings"))

    host = s["host"]
    port = int(s["port"] or 22)
    username = s["username"]
    password = s["password"]
    ssh_key_path = s["ssh_key_path"]

    reachable = _tcp_ping(host, port)
    status_val = _pick_status_value_for_db(reachable)

    uptime = None
    if reachable and username:
        uptime = _ssh_uptime(host, port, username, password, ssh_key_path)

    now_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute(
                """
                UPDATE servers
                SET last_status = ?, last_uptime = ?, last_checked = ?
                WHERE id = ?
                """,
                (status_val, uptime, now_utc, server_id),
            )
    except Exception as e:
        info = _servers_last_status_storage()
        if info["kind"] == "enum":
            alt_val = ("up" if reachable else "down")
            try:
                with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
                    conn.execute(
                        """
                        UPDATE servers
                        SET last_status = ?, last_uptime = ?, last_checked = ?
                        WHERE id = ?
                        """,
                        (alt_val, uptime, now_utc, server_id),
                    )
            except Exception:
                flash(f"Update failed for last_status: {e}", "error")
                return redirect(url_for("admin.settings"))
        else:
            flash(f"Update failed for last_status: {e}", "error")
            return redirect(url_for("admin.settings"))

    if reachable:
        msg = f"Server {host}:{port} is ONLINE"
        if uptime:
            msg += f" (uptime: {uptime})"
        flash(msg, "success")
    else:
        flash(f"Server {host}:{port} is OFFLINE or unreachable", "warning")

    return redirect(url_for("admin.settings"))

@admin_bp.post("/settings/support-test")
@superadmin_required
def support_test():
    """Кнопка 'Test' в настройках Support (Chatwoot): проверяем текущие сохранённые креды."""
    _check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        base   = get_setting(conn, "CHATWOOT_BASE_URL", "") or ""
        token  = get_setting(conn, "CHATWOOT_ACCESS_TOKEN", "") or ""
        client = get_setting(conn, "CHATWOOT_CLIENT", "") or ""
        uid    = get_setting(conn, "CHATWOOT_UID", "") or ""

    if not (base.strip() and token.strip()):
        flash("Support (Chatwoot) is not configured: set Base URL and Access Token.", "warning")
        return redirect(url_for("admin.settings"))

    ok, reason = _probe_chatwoot(base, token, client or None, uid or None)
    if ok:
        flash("Support (Chatwoot) connection OK.", "success")
    else:
        flash(f"Support (Chatwoot) check failed: {reason}", "error")

    return redirect(url_for("admin.settings"))

@admin_bp.post("/settings/support-disconnect")
@superadmin_required
def support_disconnect():
    """Сброс настроек Chatwoot (Support) из settings."""
    _check_csrf()
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute(
            "DELETE FROM settings WHERE `key` IN (?, ?, ?, ?)",
            ("CHATWOOT_BASE_URL", "CHATWOOT_ACCESS_TOKEN", "CHATWOOT_CLIENT", "CHATWOOT_UID"),
        )
    flash("Support (Chatwoot) disconnected: credentials removed.", "success")
    return redirect(url_for("admin.settings"))