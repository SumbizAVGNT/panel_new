from __future__ import annotations

import os
import json
import mimetypes
import random
import time
from typing import Optional, Dict, Any, Callable, List

import requests

# Коды, при которых есть смысл повторить запрос
TRANSIENT_CODES = {429, 500, 502, 503, 504}

# Пределы платформ (на сегодня)
DISCORD_LIMIT = 2000
TELEGRAM_LIMIT = 4096  # для текста
VK_LIMIT = 4096

# ===== Общие утилиты =====

def request_with_retry(
    func: Callable[[], requests.Response],
    attempts: int = 4,
    base_sleep: float = 0.6
) -> requests.Response:
    """
    Универсальный ретрай: 429/5xx — ждём и повторяем с экспоненциальной задержкой,
    учитывая Retry-After если он есть.
    """
    last_exc = None
    for i in range(attempts):
        try:
            r = func()
            if r.status_code in TRANSIENT_CODES:
                ra = r.headers.get("Retry-After")
                sleep = float(ra) if ra else (base_sleep * (2 ** i))
                time.sleep(min(sleep, 5.0))
                continue
            return r
        except requests.RequestException as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** i))
    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_retry: exceeded attempts")


def _chunk_text(text: str, limit: int) -> List[str]:
    """
    Делит текст на чанки по limit символов, стараясь резать по \n или пробелу.
    Если ничего подходящего — жестко режет.
    """
    s = text or ""
    if len(s) <= limit:
        return [s]

    chunks: List[str] = []
    i = 0
    n = len(s)
    while i < n:
        end = min(i + limit, n)
        if end < n:
            window = s[i:end]
            cut = max(window.rfind("\n"), window.rfind(" "))
            if cut != -1 and cut >= limit // 2:
                end = i + cut
        chunks.append(s[i:end].rstrip())
        i = end
    return [c for c in chunks if c]


# ===== Discord =====

def _discord_post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> requests.Response:
    return requests.post(url, headers=headers, json=payload, timeout=20)


def _discord_post_multipart(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    attachment_path: str,
    filename: str,
    mime: str,
) -> requests.Response:
    with open(attachment_path, "rb") as f:
        files = {
            "files[0]": (filename, f, mime),
            "payload_json": (None, json.dumps(payload, ensure_ascii=False), "application/json"),
        }
        return requests.post(url, headers=headers, files=files, timeout=30)


def discord_send_message(
    bot_token: str,
    channel_id: str,
    content: str,
    embed_payload: Optional[Dict[str, Any]] = None,
    attachment_path: Optional[str] = None,
    attachment_name: Optional[str] = None,
    attachment_mime: Optional[str] = None,
) -> None:
    # (1) Валидация и подготовка
    channel_id = (channel_id or "").strip()
    if not channel_id.isdigit():
        raise RuntimeError("Discord: invalid channel_id (must be numeric)")

    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    headers = {"Authorization": f"Bot {bot_token}"}

    # (2) Нормализуем embed
    embed: Optional[Dict[str, Any]] = None
    if embed_payload:
        embed = {k: v for k, v in embed_payload.items() if v not in (None, "", {}, [])}

    # (3) Вложение
    attach_exists = bool(attachment_path and os.path.exists(attachment_path))
    filename = attachment_name or (os.path.basename(attachment_path) if attachment_path else None)
    mime = attachment_mime or (mimetypes.guess_type(filename or "")[0] if filename else None) or "application/octet-stream"

    # Если есть embed и файл, а image не задан — привяжем картинку через attachment://
    if attach_exists and embed is not None and "image" not in embed and filename:
        embed = dict(embed)
        embed["image"] = {"url": f"attachment://{filename}"}

    # (4) Чанкование текста
    chunks = _chunk_text(content or "", DISCORD_LIMIT) or [""]

    # ВАЖНО: если только файл (без текста и без embed) — Discord требует контент.
    if attach_exists and not embed and (len(chunks) == 1 and not chunks[0]):
        chunks[0] = "\u200b"  # zero-width space

    # (5) Первый запрос (возможен multipart)
    payload_first: Dict[str, Any] = {"content": chunks[0]}
    if embed:
        payload_first["embeds"] = [embed]

    if attach_exists and filename:
        # <— КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: id как СТРОКА "0", а не число 0
        payload_first.setdefault("attachments", []).append({"id": "0", "filename": filename})

        def do_multipart():
            with open(attachment_path, "rb") as f:
                files = {
                    "files[0]": (filename, f, mime),
                    "payload_json": (None, json.dumps(payload_first, ensure_ascii=False), "application/json"),
                }
                return requests.post(url, headers=headers, files=files, timeout=30)

        r = request_with_retry(do_multipart)
    else:
        r = request_with_retry(lambda: requests.post(url, headers=headers, json=payload_first, timeout=20))

    if r.status_code not in (200, 201):
        ctype = r.headers.get("content-type", "")
        try:
            body = json.dumps(r.json(), ensure_ascii=False) if ctype.startswith("application/json") else (r.text or "")[:500]
        except Exception:
            body = (r.text or "")[:500]
        raise RuntimeError(f"Discord send error {r.status_code}: {body}")

    # (6) Остальные чанки (без файла/эмбеда)
    if len(chunks) > 1:
        for part in chunks[1:]:
            r2 = request_with_retry(lambda: requests.post(
                url, headers=headers, json={"content": part}, timeout=20
            ))
            if r2.status_code not in (200, 201):
                ctype2 = r2.headers.get("content-type", "")
                try:
                    body2 = json.dumps(r2.json(), ensure_ascii=False) if ctype2.startswith("application/json") else (r2.text or "")[:500]
                except Exception:
                    body2 = (r2.text or "")[:500]
                raise RuntimeError(f"Discord send error (part) {r2.status_code}: {body2}")


# ===== Telegram =====

_MD2_SPECIALS = r'_*[]()~`>#+-=|{}.!'
def _escape_md2(s: str) -> str:
    """Мягкое экранирование под MarkdownV2."""
    out = []
    for ch in s:
        out.append("\\" + ch if ch in _MD2_SPECIALS else ch)
    return "".join(out)


def telegram_send_message(
    bot_token: str,
    chat_id: str,
    content: str,
    *,
    parse_mode: Optional[str] = "MarkdownV2",
    disable_web_page_preview: Optional[bool] = None
) -> None:
    """
    Отправляет сообщение в Telegram.
    - По умолчанию MarkdownV2 с мягким экранированием.
    - Длинные сообщения (>4096) делим на части.
    """
    text = content or ""
    text_to_send = _escape_md2(text) if parse_mode == "MarkdownV2" else text

    chunks = _chunk_text(text_to_send, TELEGRAM_LIMIT) or [""]

    base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def _send_one(part: str) -> requests.Response:
        data = {"chat_id": chat_id, "text": part}
        if parse_mode:
            data["parse_mode"] = parse_mode
        if disable_web_page_preview is not None:
            data["disable_web_page_preview"] = "true" if disable_web_page_preview else "false"
        return requests.post(base_url, data=data, timeout=20)

    for i, part in enumerate(chunks):
        r = request_with_retry(lambda: _send_one(part))
        try:
            j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        except Exception:
            j = {}
        if not j.get("ok"):
            if parse_mode and i == 0:
                r2 = request_with_retry(lambda: requests.post(base_url, data={"chat_id": chat_id, "text": part}, timeout=20))
                try:
                    j2 = r2.json() if r2.headers.get("content-type", "").startswith("application/json") else {}
                except Exception:
                    j2 = {}
                if not j2.get("ok"):
                    raise RuntimeError(f"Telegram send error: {r2.text[:300]}")
            else:
                raise RuntimeError(f"Telegram send error: {r.text[:300]}")


# ===== VK =====

def vk_send_message(group_token: str, peer_id: str, content: str) -> None:
    """
    Отправляет сообщение во VK (messages.send).
    Если текст длиннее лимита — делим на части и отправляем по очереди.
    """
    chunks = _chunk_text(content or "", VK_LIMIT) or [""]

    def do_send(part: str) -> requests.Response:
        return requests.post(
            "https://api.vk.com/method/messages.send",
            data={
                "access_token": group_token,
                "v": "5.131",
                "peer_id": peer_id,
                "random_id": random.randint(1, 2**31 - 1),
                "message": part,
            },
            timeout=20,
        )

    for part in chunks:
        r = request_with_retry(lambda: do_send(part))
        try:
            j = r.json()
        except Exception:
            j = {}
        if "error" in j:
            raise RuntimeError(f"VK send error: {j['error'].get('error_msg', 'error')}")
