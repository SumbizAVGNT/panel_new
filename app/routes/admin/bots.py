# app/routes/admin/bots.py
from __future__ import annotations

import html
import re
from typing import Tuple, Optional

import requests
from flask import request, redirect, url_for, flash, current_app

from ...database import get_db_connection, IntegrityError
from ...decorators import superadmin_required
from . import admin_bp
from .admin_common import check_csrf

# ---- Network defaults ----
HTTP_TIMEOUT = 8  # seconds
USER_AGENT = "MoonRein/1.0 (+bots-admin)"
_JSON_CT = re.compile(r"^application/(?:json|problem\+json)(?:;|$)", re.I)


def _is_json(resp: requests.Response) -> bool:
    return _JSON_CT.match(resp.headers.get("content-type", "")) is not None


def _trim(s: str, n: int = 200) -> str:
    s = s or ""
    if len(s) > n:
        return s[:n] + "…"
    return s


def _discord_bot_info(token: str) -> Tuple[str, str, Optional[str]]:
    """
    Возвращает (bot_id, name, avatar_url) по Discord Bot Token.
    """
    try:
        r = requests.get(
            "https://discord.com/api/v10/users/@me",
            headers={"Authorization": f"Bot {token}", "User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
        )
    except requests.Timeout:
        raise RuntimeError("Discord: timeout")
    except requests.RequestException as e:
        raise RuntimeError(f"Discord: network error ({e.__class__.__name__})")

    if r.status_code != 200:
        text = r.text if not _is_json(r) else (r.json() or {})
        raise RuntimeError(f"Discord: {r.status_code} {_trim(str(text))}")

    j = r.json()
    bot_id = str(j.get("id") or "")
    if not bot_id:
        raise RuntimeError("Discord: empty id in response")

    # global_name чаще человекочитаемый, но может отсутствовать
    name = j.get("global_name") or j.get("username") or f"discord_{bot_id}"
    avatar = j.get("avatar")
    avatar_url = f"https://cdn.discordapp.com/avatars/{bot_id}/{avatar}.png?size=128" if avatar else None
    return bot_id, name, avatar_url


def _telegram_bot_info(token: str) -> Tuple[str, str, Optional[str]]:
    """
    Возвращает (bot_id, name, avatar_url) по Telegram Bot Token.
    Аккуратно вытаскивает аватар через getUserProfilePhotos -> getFile.
    """
    base = f"https://api.telegram.org/bot{token}"
    try:
        r = requests.get(f"{base}/getMe", headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
    except requests.Timeout:
        raise RuntimeError("Telegram: timeout")
    except requests.RequestException as e:
        raise RuntimeError(f"Telegram: network error ({e.__class__.__name__})")

    if r.status_code != 200 or (not _is_json(r)):
        raise RuntimeError(f"Telegram: {r.status_code} {_trim(r.text)}")

    me = r.json()
    if not me.get("ok"):
        raise RuntimeError(f"Telegram: {_trim(str(me))}")

    res = me.get("result") or {}
    bot_id = str(res.get("id") or "")
    if not bot_id:
        raise RuntimeError("Telegram: empty id in response")

    name = res.get("first_name") or res.get("username") or f"telegram_{bot_id}"

    # Попробуем подтянуть аватар (best-effort)
    avatar_url = None
    try:
        photos = requests.get(
            f"{base}/getUserProfilePhotos",
            params={"user_id": bot_id, "limit": 1},
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
        )
        if photos.status_code == 200 and _is_json(photos):
            pj = photos.json()
            if pj.get("ok") and (pj["result"].get("total_count", 0) > 0):
                file_id = pj["result"]["photos"][0][0]["file_id"]
                gf = requests.get(
                    f"{base}/getFile",
                    params={"file_id": file_id},
                    headers={"User-Agent": USER_AGENT},
                    timeout=HTTP_TIMEOUT,
                )
                if gf.status_code == 200 and _is_json(gf):
                    fj = gf.json()
                    if fj.get("ok"):
                        file_path = fj["result"].get("file_path")
                        if file_path:
                            # В отличие от getFile, сам файл скачивается по /file/botTOKEN/<file_path>
                            avatar_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    except Exception:
        # аватар — необязателен, молча пропустим любые ошибки
        pass

    return bot_id, name, avatar_url


def _vk_group_info(token: str) -> Tuple[str, str, Optional[str]]:
    """
    Возвращает (group_id, name, avatar_url) по VK group token.
    """
    try:
        r = requests.get(
            "https://api.vk.com/method/groups.getById",
            params={"access_token": token, "v": "5.131", "fields": "name,screen_name,photo_100"},
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
        )
    except requests.Timeout:
        raise RuntimeError("VK: timeout")
    except requests.RequestException as e:
        raise RuntimeError(f"VK: network error ({e.__class__.__name__})")

    if not _is_json(r):
        raise RuntimeError(f"VK: {r.status_code} {_trim(r.text)}")

    j = r.json()
    if "error" in j:
        msg = j["error"].get("error_msg", "error")
        raise RuntimeError(f"VK: {msg}")

    arr = j.get("response") or []
    if not arr:
        raise RuntimeError("VK: empty response")
    info = arr[0]
    bot_id = str(info.get("id") or "")
    if not bot_id:
        raise RuntimeError("VK: empty id in response")

    name = info.get("name") or info.get("screen_name") or f"vk_{bot_id}"
    avatar_url = info.get("photo_100")
    return bot_id, name, avatar_url


def _fetch_bot_info(platform: str, token: str) -> Tuple[str, str, Optional[str]]:
    """
    Унифицированный выбор платформы, возвращает (bot_id, name, avatar_url).
    """
    plat = (platform or "").strip().lower()
    if plat == "discord":
        return _discord_bot_info(token)
    if plat == "telegram":
        return _telegram_bot_info(token)
    if plat == "vk":
        return _vk_group_info(token)
    raise ValueError("Unsupported platform")


# ---------------- Routes ----------------

@admin_bp.post("/settings/bots/add")
@superadmin_required
def bots_add():
    check_csrf()

    platform = (request.form.get("platform") or "").lower().strip()
    token = (request.form.get("token") or "").strip()

    if platform not in ("discord", "telegram", "vk"):
        flash("Choose platform: discord / telegram / vk", "error")
        return redirect(url_for("admin.settings"))

    if not token:
        flash("Token is required", "error")
        return redirect(url_for("admin.settings"))

    # Пытаемся получить информацию о боте
    try:
        bot_id, name, avatar_url = _fetch_bot_info(platform, token)
    except Exception as e:
        flash(f"Failed to add {platform} bot: {html.escape(str(e))}", "error")
        return redirect(url_for("admin.settings"))

    # Upsert в таблицу bots
    try:
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute(
                """
                INSERT INTO bots(platform, bot_id, name, avatar_url, token, active)
                VALUES(?,?,?,?,?,1)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name),
                    avatar_url=VALUES(avatar_url),
                    token=VALUES(token),
                    active=1
                """,
                (platform, bot_id, name, avatar_url, token),
            )
        flash(f"{platform.capitalize()} bot “{name}” added/updated", "success")
    except IntegrityError:
        # На случай уникального конфликта по (platform, bot_id) — маловероятно из-за ON DUP KEY, но оставим
        flash("Bot already exists", "error")
    except Exception as e:
        flash(f"Database error: {html.escape(str(e))}", "error")

    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/bots/<int:bot_pk>/delete")
@superadmin_required
def bots_delete(bot_pk: int):
    check_csrf()
    try:
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute("DELETE FROM bots WHERE id = ?", (bot_pk,))
        flash("Bot removed", "success")
    except Exception as e:
        flash(f"Delete failed: {html.escape(str(e))}", "error")
    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/bots/<int:bot_pk>/refresh")
@superadmin_required
def bots_refresh(bot_pk: int):
    check_csrf()

    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        row = conn.execute(
            "SELECT id, platform, token FROM bots WHERE id = ?",
            (bot_pk,),
        ).fetchone()

    if not row:
        flash("Bot not found", "error")
        return redirect(url_for("admin.settings"))

    try:
        _bot_id, name, avatar_url = _fetch_bot_info(row["platform"], row["token"])
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute(
                "UPDATE bots SET name = ?, avatar_url = ? WHERE id = ?",
                (name, avatar_url, bot_pk),
            )
        flash("Bot info refreshed", "success")
    except Exception as e:
        flash(f"Refresh failed: {html.escape(str(e))}", "error")

    return redirect(url_for("admin.settings"))
