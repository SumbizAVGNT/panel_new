# app/routes/news/tg_helpers.py

from __future__ import annotations
from typing import Dict, Any, List, Optional, Callable
import time
import random

# --- Пытаемся использовать python-telegram-bot (v20+) ---
_PTBDISABLED = False
try:
    import telegram
    from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError
    from telegram.request import HTTPXRequest
    _HAVE_PTB = True
except Exception:
    _HAVE_PTB = False

import requests

TG_API = "https://api.telegram.org"

# ====== Общие утилиты (ретраи) ======

_TRANSIENT_CODES = {429, 500, 502, 503, 504}

def _with_retry(
    fn: Callable[[], Any],
    attempts: int = 4,
    base_delay: float = 0.6,
) -> Any:
    last_exc = None
    for i in range(attempts):
        try:
            return fn()
        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            code = resp.status_code if resp is not None else None
            if code in _TRANSIENT_CODES:
                ra = resp.headers.get("Retry-After") if resp is not None else None
                delay = float(ra) if ra else base_delay * (2 ** i)
                time.sleep(min(delay, 5.0))
                last_exc = e
                continue
            raise
        except (requests.Timeout, requests.ConnectionError) as e:
            time.sleep(base_delay * (2 ** i))
            last_exc = e
            continue
        except Exception as e:
            last_exc = e
            break
    if last_exc:
        raise last_exc


# ====== HTTP (fallback) реализация Bot API ======

def _tg_request_http(bot_token: str, method: str, params: Dict[str, Any] | None = None, timeout: float = 10.0) -> Dict[str, Any]:
    url = f"{TG_API}/bot{bot_token}/{method}"
    def do():
        r = requests.get(url, params=params or {}, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        if not j.get("ok"):
            raise requests.HTTPError(f"Telegram API {method} error", response=r)
        return j
    return _with_retry(do)

def _updates_to_brief_http(updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chats: Dict[int, Dict[str, Any]] = {}

    def add_chat(ch: Dict[str, Any]):
        if not ch:
            return
        cid = ch.get("id")
        if not isinstance(cid, int):
            return
        title = ch.get("title") or ch.get("username") or ""
        ctype = ch.get("type") or ""
        username = ch.get("username")
        chats[cid] = {
            "id": str(cid),
            "title": title or f"{ctype}:{cid}",
            "type": ctype,
            "username": username,
        }

    for u in updates:
        for key in ("message", "edited_message", "channel_post"):
            if key in u and u[key] and "chat" in u[key]:
                add_chat(u[key]["chat"])
        if "my_chat_member" in u and u["my_chat_member"]:
            add_chat(u["my_chat_member"].get("chat"))
        if "chat_join_request" in u and u["chat_join_request"]:
            add_chat(u["chat_join_request"].get("chat"))

    arr = list(chats.values())
    arr.sort(key=lambda x: (x.get("title") or "").lower())
    return arr


# ====== PTB реализация ======

def _make_ptb_bot(bot_token: str) -> "telegram.Bot":
    """
    Создаёт Bot с аккуратными таймаутами. Требует python-telegram-bot v20+.
    """
    # HTTPXRequest позволяет задать таймауты/пул
    req = HTTPXRequest(
        connect_timeout=5.0,
        read_timeout=10.0,
        pool_timeout=5.0,
        # можно поднять параллелизм, если когда-нибудь понадобится:
        # pool_limits=...
    )
    return telegram.Bot(token=bot_token, request=req)

def _with_retry_ptb(fn: Callable[[], Any], attempts: int = 4, base_delay: float = 0.6) -> Any:
    last_exc = None
    for i in range(attempts):
        try:
            return fn()
        except RetryAfter as e:
            delay = getattr(e, "retry_after", None)
            time.sleep(min(float(delay or (base_delay * (2 ** i))), 5.0))
            last_exc = e
            continue
        except (TimedOut, NetworkError) as e:
            time.sleep(base_delay * (2 ** i))
            last_exc = e
            continue
        except TelegramError as e:
            # логические ошибки (chat not found, forbidden) — не ретраим
            last_exc = e
            break
        except Exception as e:
            last_exc = e
            break
    if last_exc:
        raise last_exc

def _updates_to_brief_ptb(updates: List["telegram.Update"]) -> List[Dict[str, Any]]:
    chats: Dict[int, Dict[str, Any]] = {}

    def add_chat(chat: Optional["telegram.Chat"]):
        if not chat:
            return
        cid = chat.id
        if not isinstance(cid, int):
            return
        title = (chat.title or chat.username or "") if hasattr(chat, "title") else (chat.username or "")
        ctype = chat.type or ""
        username = getattr(chat, "username", None)
        chats[cid] = {
            "id": str(cid),
            "title": title or f"{ctype}:{cid}",
            "type": ctype,
            "username": username,
        }

    for u in updates:
        if u.message:
            add_chat(u.message.chat)
        if u.edited_message:
            add_chat(u.edited_message.chat)
        if u.channel_post:
            add_chat(u.channel_post.chat)
        if u.my_chat_member:
            add_chat(u.my_chat_member.chat)
        if u.chat_join_request:
            add_chat(u.chat_join_request.chat)

    arr = list(chats.values())
    arr.sort(key=lambda x: (x.get("title") or "").lower())
    return arr


# ====== Публичный API (те же функции, что и были) ======

def tg_get_updates_brief(bot_token: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Возвращает список чатов, где бот был замечен (по getUpdates).
    PTB приоритетно; если PTB недоступен — HTTP fallback.
    ВАЖНО: если у бота включён webhook, getUpdates обычно пустой — это ограничение Bot API.
    """
    if _HAVE_PTB and not _PTBDISABLED:
        bot = _make_ptb_bot(bot_token)
        allowed = ["message", "edited_message", "channel_post", "my_chat_member", "chat_join_request"]
        updates = _with_retry_ptb(lambda: bot.get_updates(limit=limit, allowed_updates=allowed))
        return _updates_to_brief_ptb(updates)

    # Fallback: HTTP
    j = _tg_request_http(bot_token, "getUpdates", {
        "limit": limit,
        "allowed_updates": ["message","edited_message","channel_post","my_chat_member","chat_join_request"]
    })
    updates = j.get("result", []) or []
    return _updates_to_brief_http(updates)


def tg_get_chat(bot_token: str, q: str) -> Optional[Dict[str, Any]]:
    """
    Резолвит чат по @username или numeric ID.
    """
    q = (q or "").strip()
    if not q:
        return None

    if _HAVE_PTB and not _PTBDISABLED:
        bot = _make_ptb_bot(bot_token)
        chat = _with_retry_ptb(lambda: bot.get_chat(q))
        if not chat:
            return None
        title = (getattr(chat, "title", None) or getattr(chat, "username", None) or "")
        ctype = chat.type or ""
        username = getattr(chat, "username", None)
        cid = chat.id
        if not isinstance(cid, int):
            return None
        return {
            "id": str(cid),
            "title": title or f"{ctype}:{cid}",
            "type": ctype,
            "username": username,
        }

    # Fallback: HTTP
    j = _tg_request_http(bot_token, "getChat", {"chat_id": q})
    ch = j.get("result") or {}
    cid = ch.get("id")
    if not isinstance(cid, int):
        return None
    title = ch.get("title") or ch.get("username") or ""
    ctype = ch.get("type") or ""
    username = ch.get("username")
    return {"id": str(cid), "title": title or f"{ctype}:{cid}", "type": ctype, "username": username}


# ====== Доп. заметки по списку “всех групп где есть бот” ======
# К сожалению, Telegram Bot API не предоставляет универсального метода
# “перечислить все чаты, где состоит бот”. Надёжные варианты:
# 1) getUpdates (как здесь) — только если бот без webhook и у чатов были события.
# 2) Хранить чаты в своей БД при первом событии/успешной отправке (рекомендуется).
# 3) Использовать MTProto-клиента (Telethon) с api_id/api_hash — это уже не Bot API.
#
# Поэтому текущая реализация делает максимум возможного через Bot API и даёт
# удобный fallback на HTTP, чтобы всё работало “из коробки”.
