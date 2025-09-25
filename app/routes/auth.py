# app/routes/auth.py
from flask import Blueprint, render_template, request, redirect, url_for, session, flash, g, current_app
import requests, secrets
from werkzeug.security import check_password_hash
from ..database import get_setting, get_db_connection
from werkzeug.routing import BuildError
from ..decorators import login_required

auth_bp = Blueprint("auth", __name__, url_prefix="")

TIMEOUT = (5, 10)  # connect, read


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("dashboard.dashboard"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        if not username or not password:
            flash("Username and password are required", "error")
            return redirect(url_for("auth.login"))

        # используем открытое соединение g.db
        user = g.db.execute(
            "SELECT id, password_hash, role FROM users WHERE username = ?",
            (username,),
        ).fetchone()

        if user and check_password_hash(user["password_hash"], password):
            if user["role"] == "pending":
                flash("Your account is awaiting admin approval", "warning")
                return redirect(url_for("auth.login"))

            session.clear()
            session["user_id"] = user["id"]
            session["role"] = user["role"]
            flash("Login successful!", "success")
            return redirect(url_for("dashboard.dashboard"))

        flash("Invalid credentials", "error")

    return render_template("login.html")


@auth_bp.route("/discord/login")
def discord_login():
    state = secrets.token_urlsafe(24)
    session["oauth_state"] = state

    client_id = get_setting(g.db, "DISCORD_CLIENT_ID", "")
    redirect_uri = get_setting(g.db, "DISCORD_REDIRECT_URI", "")

    from urllib.parse import urlencode
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "identify",
        "state": state,
    }
    return redirect(f"https://discord.com/api/oauth2/authorize?{urlencode(params)}")


@auth_bp.route("/discord/callback")
def discord_callback():
    # 1) Проверяем наличие кода (или ошибку от Discord)
    if (err := request.args.get("error")):
        flash(f"Discord authentication denied: {err}", "error")
        return redirect(url_for("auth.login"))

    code = request.args.get("code")
    if not code:
        flash("Discord auth: missing 'code' parameter.", "error")
        return redirect(url_for("auth.login"))

    # 2) Читаем OAuth-настройки из таблицы settings
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        client_id     = get_setting(conn, "DISCORD_CLIENT_ID", "")
        client_secret = get_setting(conn, "DISCORD_CLIENT_SECRET", "")
        redirect_uri  = get_setting(conn, "DISCORD_REDIRECT_URI", "http://127.0.0.1:5000/discord/callback")

    if not client_id or not client_secret or not redirect_uri:
        flash("Discord OAuth2 is not configured.", "error")
        return redirect(url_for("admin.settings"))

    # 3) Обмениваем code → access_token
    try:
        token_resp = requests.post(
            "https://discord.com/api/oauth2/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
            timeout=10,
        )
    except requests.Timeout:
        flash("Discord authentication failed (timeout).", "error")
        return redirect(url_for("auth.login"))
    except requests.SSLError as e:
        flash(f"Discord authentication failed (SSL): {e}", "error")
        return redirect(url_for("auth.login"))
    except requests.ConnectionError as e:
        flash(f"Discord authentication failed (connection): {e}", "error")
        return redirect(url_for("auth.login"))
    except requests.RequestException as e:
        flash(f"Discord authentication failed (network): {e}", "error")
        return redirect(url_for("auth.login"))

    if token_resp.status_code != 200:
        # Покажем точную причину от Discord (401=invalid_client, 400=invalid_grant и т.д.)
        try:
            data = token_resp.json()
            detail = data.get("error_description") or data.get("error") or token_resp.text
        except Exception:
            detail = token_resp.text
        flash(f"Discord authentication failed ({token_resp.status_code}): {detail}", "error")
        return redirect(url_for("auth.login"))

    token_json = token_resp.json()
    access_token = token_json.get("access_token")
    if not access_token:
        flash("Discord authentication failed: no access_token.", "error")
        return redirect(url_for("auth.login"))

    # 4) Забираем профиль пользователя
    try:
        me_resp = requests.get(
            "https://discord.com/api/users/@me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
    except requests.RequestException as e:
        flash(f"Discord profile fetch failed: {e}", "error")
        return redirect(url_for("auth.login"))

    if me_resp.status_code != 200:
        flash(f"Discord profile fetch failed ({me_resp.status_code}).", "error")
        return redirect(url_for("auth.login"))

    me = me_resp.json()
    discord_id = str(me.get("id"))
    display_name = (me.get("global_name") or me.get("username") or f"discord_{discord_id}").strip()

    # 5) Логиним/создаём пользователя
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        row = conn.execute(
            "SELECT id, username, role FROM users WHERE discord_id = ?",
            (discord_id,),
        ).fetchone()

        if row:
            user_id = row["id"]
            username = row["username"]
            role = row["role"]
            # Мягко обновим username, если был автосгенерирован
            if username.startswith("discord_") and display_name and display_name != username:
                try:
                    conn.execute("UPDATE users SET username = ? WHERE id = ?", (display_name, user_id))
                    username = display_name
                except Exception:
                    pass
        else:
            username = display_name or f"discord_{discord_id}"
            conn.execute(
                "INSERT INTO users (username, password_hash, discord_id, role) VALUES (?, ?, ?, ?)",
                (username, "discord_oauth", discord_id, "user"),
            )
            user_id = conn.execute("SELECT LAST_INSERT_ID() AS id").fetchone()["id"]
            role = "user"

    # 6) Сессия
    session["user_id"] = user_id
    session["username"] = username
    session["role"] = role

    # 7) Редиректим на существующий эндпоинт дашборда
    try:
        dest = url_for("dashboard.index")
    except BuildError:
        try:
            dest = url_for("dashboard.dashboard")  # у тебя так называется
        except BuildError:
            dest = "/"

    flash("Successfully signed in with Discord.", "success")
    return redirect(dest)


@auth_bp.route("/logout")
@login_required
def logout():
    session.clear()
    flash("Logged out successfully", "success")
    return redirect(url_for("auth.login"))
