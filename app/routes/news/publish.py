# app/routes/news/publish.py
from __future__ import annotations

import json
import os
import mimetypes
from typing import Optional, Tuple

from flask import redirect, url_for, flash, current_app, abort, request

from ...database import get_db_connection
from ...decorators import superadmin_required
from . import news_bp
from .common import get_upload_dir, must_csrf
from .senders import discord_send_message, telegram_send_message, vk_send_message


def _safe_embed(raw: Optional[str]) -> Optional[dict]:
    """Парсит JSON embed из БД и возвращает dict | None без исключений."""
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and obj:
            # Чистим пустые поля, чтобы не слать пустышку
            return {k: v for k, v in obj.items() if v not in (None, "", {}, [])}
        return None
    except Exception:
        return None


def _resolve_attachment(path_rel: Optional[str],
                        db_name: Optional[str],
                        db_mime: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (abs_path|None, name|None, mime|None), если файл существует.
    Имя берём из БД, если задано, иначе — basename файла.
    MIME берём из БД, иначе — угадываем по имени.
    """
    if not path_rel:
        return None, None, None

    abs_path = os.path.join(get_upload_dir(), path_rel)
    if not os.path.isfile(abs_path):
        return None, None, None

    name = db_name or os.path.basename(abs_path)
    mime = db_mime or mimetypes.guess_type(name)[0] or "application/octet-stream"
    return abs_path, name, mime


def _is_meaningful_discord_message(text: str,
                                   embed: Optional[dict],
                                   attach_path: Optional[str]) -> bool:
    """
    Для Discord сообщение должно содержать хотя бы что-то:
    текст ИЛИ эмбед ИЛИ вложение.
    """
    t = (text or "").strip()
    has_text = len(t) > 0
    has_embed = bool(embed)
    has_file = bool(attach_path)
    return has_text or has_embed or has_file


@news_bp.get("/publish/<int:post_id>")
@superadmin_required
def publish(post_id: int):
    """Отправляет пост во все цели со статусом 'pending'. Обновляет статусы и сам пост."""
    sent_ok = 0
    sent_err = 0

    # 1) Забираем пост и цели одним коннектом (чтение)
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        post = conn.execute(
            """
            SELECT id, title, content, embed_json, attachment_file, attachment_name, attachment_mime
            FROM posts WHERE id = ?
            """,
            (post_id,),
        ).fetchone()
        if not post:
            abort(404, description="Post not found")

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

    if not targets:
        flash("No pending targets for this post.", "info")
        return redirect(url_for("news.index"))

    # 2) Подготовка данных поста
    content = post["content"] or ""
    embed_payload = _safe_embed(post["embed_json"])
    attach_path, attach_name, attach_mime = _resolve_attachment(
        post.get("attachment_file"),
        post.get("attachment_name"),
        post.get("attachment_mime"),
    )

    # 3) Отправка в каждую цель
    for t in targets:
        status = "sent"
        response = "{}"
        try:
            platform = t["platform"]
            token = t["token"]
            target_id = t["external_target_id"]

            if platform == "discord":
                # Discord не принимает полностью пустые сообщения
                if not _is_meaningful_discord_message(content, embed_payload, attach_path):
                    raise RuntimeError("Discord: empty message (no text, no embed, no attachment)")

                discord_send_message(
                    token,
                    target_id,
                    content,
                    embed_payload,
                    attach_path,
                    attach_name,
                    attach_mime,
                )

            elif platform == "telegram":
                # Пока отправляем только текст (даже если есть файл/эмбед — игнорируем для TG)
                # При необходимости можно расширить до sendPhoto/sendDocument.
                telegram_send_message(token, target_id, content)

            elif platform == "vk":
                # Аналогично — только текст.
                vk_send_message(token, target_id, content)

            else:
                raise RuntimeError(f"Unsupported platform: {platform}")

            sent_ok += 1

        except Exception as e:
            status = "error"
            response = (str(e) or "error")[:1000]
            sent_err += 1

        # 4) Фиксируем результат по цели
        with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
            conn.execute(
                "UPDATE post_targets SET send_status = ?, response_json = ? WHERE id = ?",
                (status, response, t["id"]),
            )

    # 5) Итоговый статус поста
    with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
        conn.execute(
            "UPDATE posts SET status = ? WHERE id = ?",
            ("sent" if sent_err == 0 else "failed", post_id),
        )

    flash(
        f"Published: {sent_ok} sent, {sent_err} failed.",
        "success" if sent_err == 0 else "warning",
    )
    return redirect(url_for("news.index"))


@news_bp.post("/target/<int:target_id>/reset")
@superadmin_required
def reset_target(target_id: int):
    """Ставит одной цели send_status='pending' и сразу редиректит на публикацию конкретного поста."""
    must_csrf()
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        row = conn.execute(
            "SELECT id, post_id FROM post_targets WHERE id = ?",
            (target_id,),
        ).fetchone()
        if not row:
            abort(404, description="Target not found")
        post_id = row["post_id"]
        with conn:
            conn.execute(
                "UPDATE post_targets SET send_status = 'pending', response_json = NULL WHERE id = ?",
                (target_id,),
            )

    flash("Target reset to pending.", "info")
    return redirect(url_for("news.publish", post_id=post_id))


@news_bp.post("/publish/reset-all/<int:post_id>")
@superadmin_required
def reset_all_targets(post_id: int):
    """
    Ставит всем целям поста send_status='pending' и переводит сам пост обратно в 'draft'
    (в схеме posts.status нет значения 'pending').
    """
    must_csrf()
    with get_db_connection(current_app.config.get("DB_PATH")) as conn, conn:
        conn.execute(
            "UPDATE post_targets SET send_status = 'pending', response_json = NULL WHERE post_id = ?",
            (post_id,),
        )
        conn.execute("UPDATE posts SET status = 'draft' WHERE id = ?", (post_id,))
    flash("All targets reset to pending. Post is back to draft.", "info")
    return redirect(url_for("news.index"))
