# app/routes/news/views.py
from __future__ import annotations

import os
import json
import mimetypes
import time
import random
from typing import Dict, Any, Optional, List

from werkzeug.utils import secure_filename
from flask import render_template, request, redirect, url_for, flash, session, current_app, abort

from ...database import get_db_connection
from ...decorators import superadmin_required, login_required
from . import news_bp
from .common import get_upload_dir, allowed_image, must_csrf, MAX_UPLOAD_MB


# ========= helpers =========

def _render(template: str, **ctx):
    """
    Унифицируем путь к шаблонам news/* (учитываем новую структуру templates).
    """
    # Все шаблоны страницы новостей лежат в app/templates/news/*.html
    # base — в app/templates/layout/base.html и подключается в самих шаблонах через {% extends "layout/base.html" %}
    return render_template(f"news/{template}", **ctx)


def _build_embed_payload(e_title: str, e_desc: str, e_color: str,
                         e_img_url: str | None,
                         attach_name: str | None) -> Optional[Dict[str, Any]]:
    """
    Собирает Discord-embed payload (dict), учитывая:
      - title / description
      - color (hex -> int)
      - image (приоритет: прикреплённый файл -> URL)
    """
    has_any = any([e_title, e_desc, e_color, e_img_url, attach_name])
    if not has_any:
        return None

    # color: "#5865F2" => 0x5865F2
    color_int: Optional[int] = None
    e_color = (e_color or "").strip()
    if e_color:
        try:
            color_int = int(e_color.lstrip("#"), 16)
        except Exception:
            color_int = None

    payload: Dict[str, Any] = {}
    if e_title:
        payload["title"] = e_title
    if e_desc:
        payload["description"] = e_desc
    if color_int is not None:
        payload["color"] = color_int

    # Картинка: если есть прикреплённый файл — attachment://..., иначе URL
    if attach_name:
        payload["image"] = {"url": f"attachment://{attach_name}"}
    elif e_img_url:
        payload["image"] = {"url": e_img_url}

    # выкидываем пустые значения
    payload = {k: v for k, v in payload.items() if v not in (None, "", {}, [])}
    return payload or None


def _save_image(file_storage) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Сохраняет загруженное изображение в /uploads, проверяет расширение и размер.
    Возвращает (relative_path, original_name, mime) или (None, None, None).
    """
    if not file_storage or not file_storage.filename:
        return None, None, None

    filename = secure_filename(file_storage.filename)
    if not filename:
        return None, None, None

    if not allowed_image(filename):
        raise ValueError("Only image files are allowed (png/jpg/jpeg/gif/webp).")

    # size check
    file_storage.stream.seek(0, os.SEEK_END)
    size_mb = file_storage.stream.tell() / (1024 * 1024)
    file_storage.stream.seek(0)
    if size_mb > MAX_UPLOAD_MB:
        raise ValueError(f"Image is too large (> {MAX_UPLOAD_MB} MB).")

    stored = f"{int(time.time())}_{random.randint(1000, 9999)}_{filename}"
    dest = os.path.join(get_upload_dir(), stored)
    file_storage.save(dest)

    mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return stored, filename, mime


# ========= routes =========

@news_bp.get("/")
@login_required
def index():
    """Список постов с целями публикации."""
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
            return _render(
                "list.html",
                items=[],
                targets_by_post={},
                can_edit=(session.get("role") == "superadmin"),
                csrf_token=session.get("_csrf"),
            )

        post_ids: List[int] = [p["id"] for p in posts]
        placeholders = ",".join("?" for _ in post_ids)

        rows = conn.execute(
            f"""
            SELECT
                pt.id,               -- id цели (нужно для reset из UI)
                pt.post_id,
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

    targets_by_post: Dict[int, list] = {}
    for r in rows:
        targets_by_post.setdefault(r["post_id"], []).append(r)

    items = []
    for p in posts:
        d = dict(p)
        d["attachment_url"] = (
            url_for("news.file", filename=d["attachment_file"])
            if d.get("attachment_file") else None
        )
        items.append(d)

    return _render(
        "list.html",
        items=items,
        targets_by_post=targets_by_post,
        can_edit=(session.get("role") == "superadmin"),
        csrf_token=session.get("_csrf"),
    )


@news_bp.route("/new", methods=["GET", "POST"])
@superadmin_required
def new_post():
    """
    Создание поста + выбор целей (Discord/Telegram/VK) + опциональный embed и вложение.
    Серверные проверки синхронизированы с UI:
      - title и content обязательны
      - лимит content <= 2000
    """
    if request.method == "POST":
        must_csrf()

        title = (request.form.get("title") or "").strip()
        content = (request.form.get("content") or "").strip()

        if not title or not content:
            flash("Title and content are required.", "error")
            return redirect(url_for("news.new_post"))
        if len(content) > 2000:
            flash("Content exceeds 2000 characters.", "error")
            return redirect(url_for("news.new_post"))

        # ---- Embed fields
        e_title   = (request.form.get("embed_title") or "").strip()
        e_desc    = (request.form.get("embed_desc") or "").strip()
        e_color   = (request.form.get("embed_color") or "").strip()
        e_img_url = (request.form.get("embed_image_url") or "").strip()

        # ---- Attachment (image)
        file = request.files.get("embed_image_file")
        try:
            attach_rel_path, attach_name, attach_mime = _save_image(file)
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("news.new_post"))

        # ---- Embed payload (attachment имеет приоритет над URL)
        embed_payload = _build_embed_payload(
            e_title, e_desc, e_color,
            e_img_url or None,
            attach_name or None
        )

        # ---- Persist post + targets
        with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
            cur = conn.execute(
                """
                INSERT INTO posts
                  (title, content, author_id, embed_json, attachment_file, attachment_name, attachment_mime)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    title,
                    content,
                    session.get("user_id"),
                    json.dumps(embed_payload, ensure_ascii=False) if embed_payload else None,
                    attach_rel_path,
                    attach_name,
                    attach_mime,
                ),
            )
            post_id = cur.lastrowid

            targets: list[tuple[str, int, str, Optional[str]]] = []

            # ---- Discord
            if request.form.get("send_discord") == "on":
                bot_id = request.form.get("discord_bot_id")
                channel_id = (request.form.get("discord_channel_id") or "").strip()
                manual_channel = (request.form.get("discord_channel_manual") or "").strip()
                if manual_channel:
                    channel_id = manual_channel
                if not (bot_id and channel_id):
                    flash("Discord: choose bot and channel (or enter Channel ID).", "error")
                    return redirect(url_for("news.new_post"))
                # channel_id может быть и numeric, и строкой — не режем
                targets.append(("discord", int(bot_id), channel_id, None))

            # ---- Telegram
            if request.form.get("send_telegram") == "on":
                tg_bot_id = request.form.get("telegram_bot_id")
                chat_id = (request.form.get("telegram_chat_id") or "").strip()
                if not (tg_bot_id and chat_id):
                    flash("Telegram: choose bot and set chat id / @username.", "error")
                    return redirect(url_for("news.new_post"))
                targets.append(("telegram", int(tg_bot_id), chat_id, None))

            # ---- VK
            if request.form.get("send_vk") == "on":
                vk_bot_id = request.form.get("vk_bot_id")
                peer_id = (request.form.get("vk_peer_id") or "").strip()
                if not (vk_bot_id and peer_id):
                    flash("VK: choose bot and set peer id.", "error")
                    return redirect(url_for("news.new_post"))
                targets.append(("vk", int(vk_bot_id), peer_id, None))

            # Сохраняем цели
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

    # GET: список ботов по платформам для селекторов
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        bots = conn.execute(
            "SELECT id, platform, name FROM bots WHERE active=1 ORDER BY platform, name"
        ).fetchall()

    discord_bots = [b for b in bots if b["platform"] == "discord"]
    telegram_bots = [b for b in bots if b["platform"] == "telegram"]
    vk_bots = [b for b in bots if b["platform"] == "vk"]

    return _render(
        "new.html",
        discord_bots=discord_bots,
        telegram_bots=telegram_bots,
        vk_bots=vk_bots,
        csrf_token=session.get("_csrf"),
    )
