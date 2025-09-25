# app/routes/news.py
# News + New Post (multi-publish) with Discord guild/channel discovery via nextcord
from __future__ import annotations

import os
import json
import mimetypes
import asyncio
import time
import random
from typing import List, Dict, Any, Optional

import requests
import nextcord
from werkzeug.utils import secure_filename
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    current_app,
    jsonify,
    abort,
    send_from_directory,
)

from ..database import get_db_connection
from ..decorators import superadmin_required, login_required

news_bp = Blueprint("news", __name__, url_prefix="/news")

# -------------------------- Uploads config --------------------------

ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "gif", "webp"}
MAX_UPLOAD_MB = 8  # лимит Discord для вложений в большинстве случаев
UPLOAD_DIR: Optional[str] = None


@news_bp.before_app_request
def _ensure_upload_dir():
    """Создаём папку uploads один раз рядом с проектом."""
    global UPLOAD_DIR
    if UPLOAD_DIR:
        return
    root = current_app.root_path       # .../app
    base = os.path.dirname(root)       # корень проекта
    up = os.path.join(base, "uploads")
    os.makedirs(up, exist_ok=True)
    UPLOAD_DIR = up


def _allowed_image(filename: str) -> bool:
    ext = (filename.rsplit(".", 1)[-1] or "").lower()
    return ext in ALLOWED_IMAGE_EXT


# -------------------------- CSRF helpers --------------------------

def _csrf_ok() -> bool:
    return request.form.get("csrf_token") == session.get("_csrf")


def _must_csrf() -> None:
    if not _csrf_ok():
        abort(400, description="CSRF token invalid")


# -------------------------- In-memory cache for Discord gateway lookups --------------------------

_CACHE_TTL = 60  # seconds
_cache: Dict[str, Dict[Any, Any]] = {
    "guilds": {},    # {bot_db_id: (ts, [{"id": "...","name":"..."}])}
    "channels": {},  # {(bot_db_id, guild_id): (ts, [{"id":"...","name":"..."}])}
}


def _run_async(coro):
    # безопасный вызов asyncio.run
    return asyncio.run(coro)


# -------------------------- nextcord helpers --------------------------

async def _fetch_guilds_via_nextcord(bot_token: str) -> List[Dict[str, str]]:
    intents = nextcord.Intents.none()
    intents.guilds = True
    client = nextcord.Client(intents=intents)
    result: List[Dict[str, str]] = []

    @client.event
    async def on_ready():
        nonlocal result
        result = [{"id": str(g.id), "name": g.name} for g in client.guilds]
        await client.close()

    await client.start(bot_token, reconnect=False)
    return result


async def _fetch_channels_via_nextcord(bot_token: str, guild_id: int) -> List[Dict[str, str]]:
    intents = nextcord.Intents.none()
    intents.guilds = True
    client = nextcord.Client(intents=intents)
    result: List[Dict[str, str]] = []

    @client.event
    async def on_ready():
        nonlocal result
        g = client.get_guild(guild_id)
        chans: List[Dict[str, str]] = []
        if g:
            for ch in g.channels:
                try:
                    if isinstance(ch, nextcord.TextChannel):
                        ch_type = getattr(ch, "type", None)
                        news_type = getattr(nextcord.ChannelType, "news", None)
                        if ch_type in (nextcord.ChannelType.text, news_type):
                            chans.append({"id": str(ch.id), "name": ch.name})
                except Exception:
                    continue
        result = sorted(chans, key=lambda c: c["name"].lower())
        await client.close()

    await client.start(bot_token, reconnect=False)
    return result


# -------------------------- DB helpers --------------------------

def _get_bot_token(bot_db_id: int, platform: str) -> str:
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        row = conn.execute(
            "SELECT token, platform FROM bots WHERE id = ?",
            (bot_db_id,),
        ).fetchone()
    if not row or row["platform"] != platform:
        abort(404, description="Bot not found")
    return row["token"]


# -------------------------- Pages --------------------------

@news_bp.get("/")
@login_required
def index():
    """List posts with their publish targets."""
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        posts = conn.execute(
            """
            SELECT
              p.id, p.title, p.status, p.created_at,
              p.content, p.embed_json,
              p.attachment_file, p.attachment_name, p.attachment_mime,
              u.username AS author
            FROM posts p
            LEFT JOIN users u ON u.id = p.author_id
            ORDER BY p.created_at DESC, p.id DESC
            """
        ).fetchall()

        if not posts:
            return render_template(
                "news_list.html",
                items=[],
                targets_by_post={},
                can_edit=(session.get("role") == "superadmin"),
            )

        post_ids = [p["id"] for p in posts]
        placeholders = ",".join("?" for _ in post_ids)

        rows = conn.execute(
            f"""
            SELECT pt.post_id,
                   pt.platform,
                   pt.external_target_id,
                   pt.external_target_name,
                   pt.send_status,
                   b.name  AS bot_name
            FROM post_targets pt
            JOIN bots b ON b.id = pt.bot_db_id
            WHERE pt.post_id IN ({placeholders})
            ORDER BY pt.id
            """,
            post_ids,
        ).fetchall()

    # сформируем цели по посту
    targets_by_post: Dict[int, list] = {}
    for r in rows:
        targets_by_post.setdefault(r["post_id"], []).append(r)

    # подготовим данные для модалки (attachment_url)
    items = []
    for p in posts:
        d = dict(p)
        if d.get("attachment_file"):
            d["attachment_url"] = url_for("news.file", filename=d["attachment_file"])
        else:
            d["attachment_url"] = None
        items.append(d)

    return render_template(
        "news_list.html",
        items=items,
        targets_by_post=targets_by_post,
        can_edit=(session.get("role") == "superadmin"),
    )


@news_bp.route("/new", methods=["GET", "POST"])
@superadmin_required
def new_post():
    """Create post + select targets (Discord/Telegram/VK) with optional embed & attachment."""
    if request.method == "POST":
        _must_csrf()

        title = (request.form.get("title") or "").strip()
        content = (request.form.get("content") or "").strip()
        if not title or not content:
            flash("Title and content are required.", "error")
            return redirect(url_for("news.new_post"))

        # ---- Embed fields from form
        e_title = (request.form.get("embed_title") or "").strip()
        e_desc = (request.form.get("embed_desc") or "").strip()
        e_color = (request.form.get("embed_color") or "").strip()
        e_img_url = (request.form.get("embed_image_url") or "").strip()

        file = request.files.get("embed_image_file")

        # auto-enable embed if any field present or file uploaded
        embed_enabled_auto = any([e_title, e_desc, e_color, e_img_url]) or (file and file.filename)

        embed_payload: Optional[Dict[str, Any]] = None
        attach_rel_path: Optional[str] = None
        attach_name: Optional[str] = None
        attach_mime: Optional[str] = None

        # ---- Attachment handling (image)
        if file and file.filename:
            filename = secure_filename(file.filename)
            if not _allowed_image(filename):
                flash("Only image files are allowed (png/jpg/jpeg/gif/webp).", "error")
                return redirect(url_for("news.new_post"))

            # size check
            file.stream.seek(0, os.SEEK_END)
            size_mb = file.stream.tell() / (1024 * 1024)
            file.stream.seek(0)
            if size_mb > MAX_UPLOAD_MB:
                flash(f"Image is too large (> {MAX_UPLOAD_MB} MB).", "error")
                return redirect(url_for("news.new_post"))

            stored = f"{int(time.time())}_{random.randint(1000,9999)}_{filename}"
            dest = os.path.join(UPLOAD_DIR, stored)
            file.save(dest)

            attach_rel_path = stored
            attach_name = filename
            attach_mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        # ---- Build embed if needed
        if embed_enabled_auto:
            color_int: Optional[int] = None
            if e_color:
                try:
                    color_int = int(e_color.lstrip("#"), 16)
                except Exception:
                    color_int = None

            embed_payload = {}
            if e_title:
                embed_payload["title"] = e_title
            if e_desc:
                embed_payload["description"] = e_desc
            if color_int is not None:
                embed_payload["color"] = color_int

            if attach_rel_path:
                # файл имеет приоритет
                embed_payload["image"] = {"url": f"attachment://{attach_name or attach_rel_path}"}
            elif e_img_url:
                embed_payload["image"] = {"url": e_img_url}

            embed_payload = {k: v for k, v in embed_payload.items() if v not in (None, "", {})}
            if not embed_payload:
                embed_payload = None

        # ---- Persist post and targets
        with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
            cur = conn.execute(
                "INSERT INTO posts(title, content, author_id, embed_json, attachment_file, attachment_name, attachment_mime) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    title,
                    content,
                    session.get("user_id"),
                    json.dumps(embed_payload) if embed_payload else None,
                    attach_rel_path,
                    attach_name,
                    attach_mime,
                ),
            )
            post_id = cur.lastrowid

            targets = []

            # Discord
            if request.form.get("send_discord") == "on":
                bot_id = request.form.get("discord_bot_id")
                channel_id = request.form.get("discord_channel_id")
                manual_channel = (request.form.get("discord_channel_manual") or "").strip()
                if manual_channel:
                    channel_id = manual_channel
                if not (bot_id and channel_id):
                    flash("Discord: choose bot and channel (or enter Channel ID).", "error")
                    return redirect(url_for("news.new_post"))
                targets.append(("discord", int(bot_id), channel_id, None))

            # Telegram
            if request.form.get("send_telegram") == "on":
                tg_bot_id = request.form.get("telegram_bot_id")
                chat_id = (request.form.get("telegram_chat_id") or "").strip()
                if not (tg_bot_id and chat_id):
                    flash("Telegram: choose bot and set chat id / @username.", "error")
                    return redirect(url_for("news.new_post"))
                targets.append(("telegram", int(tg_bot_id), chat_id, None))

            # VK
            if request.form.get("send_vk") == "on":
                vk_bot_id = request.form.get("vk_bot_id")
                peer_id = (request.form.get("vk_peer_id") or "").strip()
                if not (vk_bot_id and peer_id):
                    flash("VK: choose bot and set peer id.", "error")
                    return redirect(url_for("news.new_post"))
                targets.append(("vk", int(vk_bot_id), peer_id, None))

            for plat, bot_db_id, ext_id, ext_name in targets:
                conn.execute(
                    """
                    INSERT INTO post_targets
                      (post_id, platform, bot_db_id, external_target_id, external_target_name)
                    VALUES (?,?,?,?,?)
                    """,
                    (post_id, plat, bot_db_id, ext_id, ext_name),
                )

        flash("Post created.", "success")
        return redirect(url_for("news.publish", post_id=post_id))

    # GET: form needs active bots
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        bots = conn.execute(
            "SELECT id, platform, name FROM bots WHERE active=1 ORDER BY platform, name"
        ).fetchall()
    discord_bots = [b for b in bots if b["platform"] == "discord"]
    telegram_bots = [b for b in bots if b["platform"] == "telegram"]
    vk_bots = [b for b in bots if b["platform"] == "vk"]

    return render_template(
        "new_post.html",
        discord_bots=discord_bots,
        telegram_bots=telegram_bots,
        vk_bots=vk_bots,
        csrf_token=session.get("_csrf"),
    )


@news_bp.get("/publish/<int:post_id>")
@superadmin_required
def publish(post_id: int):
    """Send post to all pending targets."""
    sent_ok = 0
    sent_err = 0

    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        post = conn.execute(
            "SELECT id, title, content, embed_json, attachment_file, attachment_name, attachment_mime "
            "FROM posts WHERE id = ?",
            (post_id,),
        ).fetchone()
        if not post:
            abort(404)

        targets = conn.execute(
            """
            SELECT pt.id, pt.platform, pt.external_target_id,
                   b.id AS bot_id, b.token
            FROM post_targets pt
            JOIN bots b ON b.id = pt.bot_db_id
            WHERE pt.post_id = ? AND pt.send_status = 'pending'
            ORDER BY pt.id
            """,
            (post_id,),
        ).fetchall()

    embed_payload = json.loads(post["embed_json"]) if post["embed_json"] else None
    attach_path = os.path.join(UPLOAD_DIR, post["attachment_file"]) if post["attachment_file"] else None
    attach_name = post["attachment_name"]
    attach_mime = post["attachment_mime"]

    for t in targets:
        try:
            if t["platform"] == "discord":
                _discord_send_message(
                    t["token"],
                    t["external_target_id"],
                    post["content"],
                    embed_payload,
                    attach_path,
                    attach_name,
                    attach_mime,
                )
            elif t["platform"] == "telegram":
                _telegram_send_message(t["token"], t["external_target_id"], post["content"])
            elif t["platform"] == "vk":
                _vk_send_message(t["token"], t["external_target_id"], post["content"])
            status = "sent"
            response = "{}"
            sent_ok += 1
        except Exception as e:
            status = "error"
            response = str(e)
            sent_err += 1

        with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
            conn.execute(
                "UPDATE post_targets SET send_status = ?, response_json = ? WHERE id = ?",
                (status, response, t["id"]),
            )

    with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
        conn.execute(
            "UPDATE posts SET status = ? WHERE id = ?",
            ("sent" if sent_err == 0 else "failed", post_id),
        )

    flash(f"Published: {sent_ok} sent, {sent_err} failed.", "success" if sent_err == 0 else "warning")
    return redirect(url_for("news.index"))


# -------------------------- Discord helper APIs (guilds & channels) --------------------------

@news_bp.get("/api/discord/guilds/<int:bot_db_id>")
@superadmin_required
def api_discord_guilds(bot_db_id: int):
    token = _get_bot_token(bot_db_id, "discord")

    cached = _cache["guilds"].get(bot_db_id)
    if cached and (time.time() - cached[0] < _CACHE_TTL):
        return jsonify(ok=True, guilds=cached[1])

    try:
        guilds = _run_async(_fetch_guilds_via_nextcord(token))
        if not guilds:
            reason = "No guilds visible. Invite the bot to your server and grant 'View Channels'."
            return jsonify(ok=False, reason=reason), 200
    except nextcord.LoginFailure as e:
        return jsonify(ok=False, reason=f"Login failed: {e}"), 200
    except Exception as e:
        return jsonify(ok=False, reason=f"gateway error: {e}"), 200

    _cache["guilds"][bot_db_id] = (time.time(), guilds)
    return jsonify(ok=True, guilds=guilds)


@news_bp.get("/api/discord/channels/<int:bot_db_id>")
@superadmin_required
def api_discord_channels(bot_db_id: int):
    token = _get_bot_token(bot_db_id, "discord")
    guild_id = request.args.get("guild_id", type=int)
    if not guild_id:
        return jsonify(ok=False, reason="guild_id required"), 200

    key = (bot_db_id, guild_id)
    cached = _cache["channels"].get(key)
    if cached and (time.time() - cached[0] < _CACHE_TTL):
        return jsonify(ok=True, channels=cached[1])

    try:
        channels = _run_async(_fetch_channels_via_nextcord(token, guild_id))
        if not channels:
            return jsonify(ok=False, reason="No text/announcement channels or missing permissions"), 200
    except nextcord.LoginFailure as e:
        return jsonify(ok=False, reason=f"Login failed: {e}"), 200
    except Exception as e:
        return jsonify(ok=False, reason=f"gateway error: {e}"), 200

    _cache["channels"][key] = (time.time(), channels)
    return jsonify(ok=True, channels=channels)


@news_bp.get("/api/discord/test/<int:bot_db_id>")
@superadmin_required
def api_discord_test(bot_db_id: int):
    token = _get_bot_token(bot_db_id, "discord")
    try:
        guilds = _run_async(_fetch_guilds_via_nextcord(token))
        return jsonify(ok=True, count=len(guilds), guilds=guilds[:5])
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 200


# -------------------------- Post JSON for modal preview --------------------------

@news_bp.get("/api/post/<int:post_id>")
@login_required
def api_post(post_id: int):
    """Возвращает JSON-представление поста для модалки."""
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
            post["embed_json"] = json.loads(post["embed_json"])
        except Exception:
            post["embed_json"] = None
    post["attachment_url"] = (
        url_for("news.file", filename=post["attachment_file"])
        if post.get("attachment_file") else None
    )
    return jsonify(ok=True, post=post)


# -------------------------- Serve uploaded files --------------------------

@news_bp.get("/file/<path:filename>")
@login_required
def file(filename: str):
    """Безопасная раздача загруженных файлов (из каталога uploads)."""
    # минимальная защита от traversal
    filename = os.path.basename(filename)
    if not filename:
        abort(404)
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=False, max_age=3600)


# -------------------------- Platform senders --------------------------

def _discord_send_message(
    bot_token: str,
    channel_id: str,
    content: str,
    embed_payload: Optional[Dict[str, Any]] = None,
    attachment_path: Optional[str] = None,
    attachment_name: Optional[str] = None,
    attachment_mime: Optional[str] = None,
) -> None:
    """
    Отправляет сообщение и (опционально) embed + файл в Discord.
    Если есть файл — multipart: files[0] + payload_json + attachments.
    Если файла нет — JSON c embeds.
    """
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    headers = {"Authorization": f"Bot {bot_token}"}

    payload: Dict[str, Any] = {"content": content if content is not None else ""}

    if embed_payload:
        embed = {k: v for k, v in embed_payload.items() if v not in (None, "", {})}
        if embed:
            payload["embeds"] = [embed]

    if attachment_path and os.path.exists(attachment_path):
        filename = attachment_name or os.path.basename(attachment_path)
        mime = attachment_mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"

        # Свяжем embed с вложением, если image ещё не указан
        if "embeds" in payload:
            emb0 = payload["embeds"][0]
            if "image" not in emb0:
                emb0["image"] = {"url": f"attachment://{filename}"}

        payload.setdefault("attachments", []).append({"id": 0, "filename": filename})

        with open(attachment_path, "rb") as f:
            files = {
                "files[0]": (filename, f, mime),
                "payload_json": (None, json.dumps(payload), "application/json"),
            }
            r = requests.post(url, headers=headers, files=files, timeout=30)
    else:
        r = requests.post(url, headers=headers, json=payload, timeout=20)

    if r.status_code not in (200, 201):
        raise RuntimeError(f"Discord send error {r.status_code}: {r.text[:400]}")


def _telegram_send_message(bot_token: str, chat_id: str, content: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data={"chat_id": chat_id, "text": content},
        timeout=20,
    )
    try:
        j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    except Exception:
        j = {}
    if not j.get("ok"):
        raise RuntimeError(f"Telegram send error: {r.text[:200]}")


def _vk_send_message(group_token: str, peer_id: str, content: str) -> None:
    r = requests.post(
        "https://api.vk.com/method/messages.send",
        data={
            "access_token": group_token,
            "v": "5.131",
            "peer_id": peer_id,
            "random_id": random.randint(1, 2**31 - 1),
            "message": content,
        },
        timeout=20,
    )
    j = r.json()
    if "error" in j:
        raise RuntimeError(f"VK send error: {j['error'].get('error_msg', 'error')}")
