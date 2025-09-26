# app/routes/news/api.py
from __future__ import annotations

import json
from flask import request, jsonify, url_for, current_app

import nextcord

from ...database import get_db_connection
from ...decorators import superadmin_required, login_required
from . import news_bp
from .common import get_bot_token, cache_get, cache_put, run_async
from .nextcord_helpers import fetch_guilds_via_nextcord, fetch_channels_via_nextcord
from .senders import discord_send_message, telegram_send_message
from .tg_helpers import tg_get_updates_brief, tg_get_chat


# =========================
# Discord helper APIs
# =========================

@news_bp.get("/api/discord/guilds/<int:bot_db_id>")
@superadmin_required
def api_discord_guilds(bot_db_id: int):
    """
    Возвращает гильдии, где виден бот (gateway через nextcord).
    Результат кэшируется на TTL (см. common).
    """
    token = get_bot_token(bot_db_id, "discord")

    cached = cache_get("guilds", bot_db_id)
    if cached is not None:
        return jsonify(ok=True, guilds=cached)

    try:
        guilds = run_async(fetch_guilds_via_nextcord(token))
        if not guilds:
            return jsonify(
                ok=False,
                reason="No guilds visible. Invite the bot and grant 'View Channels'."
            ), 200
    except nextcord.LoginFailure as e:
        return jsonify(ok=False, reason=f"Login failed: {e}"), 200
    except Exception as e:
        return jsonify(ok=False, reason=f"gateway error: {e}"), 200

    cache_put("guilds", bot_db_id, guilds)
    return jsonify(ok=True, guilds=guilds)


@news_bp.get("/api/discord/channels/<int:bot_db_id>")
@superadmin_required
def api_discord_channels(bot_db_id: int):
    """
    По гильдии возвращает список текстовых/новостных каналов.
    ?guild_id=<int>
    """
    token = get_bot_token(bot_db_id, "discord")
    guild_id = request.args.get("guild_id", type=int)
    if not guild_id:
        return jsonify(ok=False, reason="guild_id required"), 400

    key = (bot_db_id, guild_id)
    cached = cache_get("channels", key)
    if cached is not None:
        return jsonify(ok=True, channels=cached)

    try:
        channels = run_async(fetch_channels_via_nextcord(token, guild_id))
        if not channels:
            return jsonify(
                ok=False,
                reason="No text/announcement channels or missing permissions"
            ), 200
    except nextcord.LoginFailure as e:
        return jsonify(ok=False, reason=f"Login failed: {e}"), 200
    except Exception as e:
        return jsonify(ok=False, reason=f"gateway error: {e}"), 200

    cache_put("channels", key, channels)
    return jsonify(ok=True, channels=channels)


@news_bp.get("/api/discord/test/<int:bot_db_id>")
@superadmin_required
def api_discord_test(bot_db_id: int):
    """Быстрый тест gateway-подключения: вернёт до 5 гильдий."""
    token = get_bot_token(bot_db_id, "discord")
    try:
        guilds = run_async(fetch_guilds_via_nextcord(token))
        return jsonify(ok=True, count=len(guilds), guilds=guilds[:5])
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 200


@news_bp.post("/api/discord/ping")
@superadmin_required
def api_discord_ping():
    """
    Быстрая проверка отправки:
    form: bot_db_id, channel_id, text? (по умолчанию "MoonRein test ✅")
    """
    bot_db_id = request.form.get("bot_db_id", type=int)
    channel_id = (request.form.get("channel_id") or "").strip()
    text = (request.form.get("text") or "MoonRein test ✅")

    if not bot_db_id or not channel_id:
        return jsonify(ok=False, reason="bot_db_id and channel_id required"), 400

    token = get_bot_token(bot_db_id, "discord")
    try:
        discord_send_message(
            bot_token=token,
            channel_id=channel_id,
            content=text,
            embed_payload=None,
            attachment_path=None,
            attachment_name=None,
            attachment_mime=None,
        )
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 200


# =========================
# Post JSON (modal preview)
# =========================

@news_bp.get("/api/post/<int:post_id>")
@login_required
def api_post(post_id: int):
    """
    JSON представление поста для предпросмотра (модалка в списке).
    """
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        p = conn.execute(
            """
            SELECT id, title, content, embed_json,
                   attachment_file, attachment_name, attachment_mime,
                   created_at
            FROM posts WHERE id = ?
            """,
            (post_id,),
        ).fetchone()

    if not p:
        return jsonify(ok=False, reason="Not found"), 404

    post = dict(p)
    if post.get("embed_json"):
        try:
            if isinstance(post["embed_json"], (bytes, bytearray)):
                post["embed_json"] = post["embed_json"].decode("utf-8", "ignore")
            if isinstance(post["embed_json"], str):
                post["embed_json"] = json.loads(post["embed_json"])
        except Exception:
            post["embed_json"] = None

    post["attachment_url"] = (
        url_for("news.file", filename=post["attachment_file"])
        if post.get("attachment_file") else None
    )
    return jsonify(ok=True, post=post)


# =========================
# Telegram helper APIs
# =========================

@news_bp.get("/api/telegram/chats/<int:bot_db_id>")
@superadmin_required
def api_telegram_chats(bot_db_id: int):
    """
    Возвращает список чатов, где бот замечен по getUpdates (tg_helpers).
    Если у бота webhook — список может быть пустым.
    ?force=1 — игнорирует кэш (TTL см. common).
    """
    force = request.args.get("force", type=int) == 1
    token = get_bot_token(bot_db_id, "telegram")

    if not force:
        cached = cache_get("tg_chats", bot_db_id)
        if cached is not None:
            return jsonify(ok=True, chats=cached)

    try:
        chats = tg_get_updates_brief(token)
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 200

    cache_put("tg_chats", bot_db_id, chats)
    return jsonify(ok=True, chats=chats)


@news_bp.get("/api/telegram/resolve/<int:bot_db_id>")
@superadmin_required
def api_telegram_resolve(bot_db_id: int):
    """
    Резолвит чат по q (@username или ID) через getChat.
    ?q=@name|123456
    """
    q = request.args.get("q", "", type=str).strip()
    if not q:
        return jsonify(ok=False, reason="q required"), 400

    token = get_bot_token(bot_db_id, "telegram")
    try:
        item = tg_get_chat(token, q)
        if not item:
            return jsonify(ok=False, reason="Not found"), 200
        return jsonify(ok=True, chat=item)
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 200


@news_bp.post("/api/telegram/ping")
@superadmin_required
def api_telegram_ping():
    """
    Быстрая проверка отправки:
    form: bot_db_id, chat, text?  (chat — ID или @username)
    """
    bot_db_id = request.form.get("bot_db_id", type=int)
    chat = (request.form.get("chat") or "").strip()
    text = (request.form.get("text") or "MoonRein TG test ✅")

    if not bot_db_id or not chat:
        return jsonify(ok=False, reason="bot_db_id and chat required"), 400

    token = get_bot_token(bot_db_id, "telegram")
    try:
        telegram_send_message(token, chat, text)
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 200
