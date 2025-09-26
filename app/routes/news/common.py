# app/routes/news/common.py
from __future__ import annotations

import os
import io
import time
import json
import uuid
import mmap
import errno
import asyncio
import mimetypes
import threading
from typing import Dict, Any, Optional, Tuple

from flask import current_app, request, session, abort
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from ...database import get_db_connection


# =========================
# Uploads (images/attachments)
# =========================

ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "gif", "webp"}
MAX_UPLOAD_MB = 8  # Discord-friendly default

_UPLOAD_DIR: Optional[str] = None


def get_upload_dir() -> str:
    """
    Единожды создаёт /uploads рядом с корнем проекта и возвращает путь.
    """
    global _UPLOAD_DIR
    if _UPLOAD_DIR:
        return _UPLOAD_DIR

    # <repo_root>/uploads
    repo_root = os.path.dirname(current_app.root_path)  # .../app -> repo root
    up = os.path.join(repo_root, "uploads")
    os.makedirs(up, exist_ok=True)
    _UPLOAD_DIR = up
    return up


def allowed_image(filename: str) -> bool:
    ext = (filename.rsplit(".", 1)[-1] or "").lower()
    return ext in ALLOWED_IMAGE_EXT


def _filesize_safe(fs: FileStorage) -> Optional[int]:
    """
    Пытаемся получить размер загружаемого файла без чтения его полностью в память.
    Возвращаем None, если оценить не удалось.
    """
    try:
        # Если в заголовках есть длина — используем её
        if hasattr(fs, "content_length") and fs.content_length is not None:
            return int(fs.content_length)
    except Exception:
        pass

    # Если поток — это реальный файл
    try:
        pos = fs.stream.tell()
        fs.stream.seek(0, os.SEEK_END)
        end = fs.stream.tell()
        fs.stream.seek(pos, os.SEEK_SET)
        return int(end)
    except Exception:
        pass

    return None


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _guess_mime(path: str, fallback: str = "application/octet-stream") -> str:
    m, _ = mimetypes.guess_type(path)
    return m or fallback


def save_upload_image(fs: FileStorage) -> Tuple[str, str, str]:
    """
    Сохраняет изображение, проверяя расширение и лимит размера.
    Возвращает (rel_path, filename, mime).
      rel_path — относительный путь внутри /uploads, который можно отдать через send_from_directory
    """
    if not fs or not fs.filename:
        raise ValueError("No file provided")

    fname = secure_filename(fs.filename)
    if not fname:
        raise ValueError("Invalid filename")

    if not allowed_image(fname):
        raise ValueError("Unsupported image type (allowed: png, jpg, jpeg, gif, webp)")

    size = _filesize_safe(fs)
    if size is not None and size > MAX_UPLOAD_MB * 1024 * 1024:
        raise ValueError(f"File too large (>{MAX_UPLOAD_MB} MB)")

    # Папка /uploads/images/YYYYMM
    base = get_upload_dir()
    sub = time.strftime("images/%Y%m")
    target_dir = os.path.join(base, sub)
    _ensure_dir(target_dir)

    ext = fname.rsplit(".", 1)[-1].lower()
    new_name = f"{int(time.time())}_{uuid.uuid4().hex[:8]}.{ext}"
    abs_path = os.path.join(target_dir, new_name)

    # Сохраняем поток
    fs.stream.seek(0)
    fs.save(abs_path)

    mime = _guess_mime(abs_path, "image/" + ("jpeg" if ext == "jpg" else ext))
    rel_path = os.path.join(sub, new_name).replace("\\", "/")
    return rel_path, new_name, mime


def save_upload_any(fs: FileStorage, bucket: str = "files") -> Tuple[str, str, str]:
    """
    Универсальное сохранение любого файла без проверки на картинку.
    Возвращает (rel_path, filename, mime).
    """
    if not fs or not fs.filename:
        raise ValueError("No file provided")

    fname = secure_filename(fs.filename)
    if not fname:
        raise ValueError("Invalid filename")

    size = _filesize_safe(fs)
    if size is not None and size > 25 * 1024 * 1024:
        # Произвольный лимит 25MB (можно вынести в настройки)
        raise ValueError("File too large (>25 MB)")

    base = get_upload_dir()
    sub = time.strftime(f"{bucket}/%Y%m")
    target_dir = os.path.join(base, sub)
    _ensure_dir(target_dir)

    # сохраняем оригинальное расширение
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else None
    new_name = f"{int(time.time())}_{uuid.uuid4().hex[:8]}" + (f".{ext}" if ext else "")
    abs_path = os.path.join(target_dir, new_name)

    fs.stream.seek(0)
    fs.save(abs_path)

    mime = _guess_mime(abs_path)
    rel_path = os.path.join(sub, new_name).replace("\\", "/")
    return rel_path, fname, mime


# =========================
# CSRF helpers
# =========================

def csrf_ok() -> bool:
    return request.form.get("csrf_token") == session.get("_csrf")


def must_csrf() -> None:
    if not csrf_ok():
        abort(400, description="CSRF token invalid")


# =========================
# Async runner
# =========================

def _run_coro_in_new_loop(coro):
    """
    Запускает корутину в отдельном потоке/цикле и возвращает результат синхронно.
    Используется, если уже есть активный event loop (например, в некоторых embed-окружениях).
    """
    result_container: Dict[str, Any] = {}
    exc_container: Dict[str, BaseException] = {}

    def _target():
        try:
            # новый loop и политика совместимая с Windows
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result_container["value"] = loop.run_until_complete(coro)
        except BaseException as e:  # noqa: BLE001
            exc_container["error"] = e
        finally:
            try:
                loop = asyncio.get_event_loop()
                loop.stop()
                loop.close()
            except Exception:
                pass

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join()

    if "error" in exc_container:
        raise exc_container["error"]  # пробрасываем оригинал
    return result_container.get("value")


def run_async(coro):
    """
    Безопасный вызов корутин из синхронного Flask-мира.
    - Если event loop НЕ запущен: используем asyncio.run(coro)
    - Если loop уже запущен (RuntimeError не бросается, get_running_loop возвращает loop):
      выполняем корутину в отдельном потоке/цикле и ждём результат.
    """
    try:
        # Если луп не запущен — вызов бросит RuntimeError
        asyncio.get_running_loop()
        running = True
    except RuntimeError:
        running = False

    if not running:
        return asyncio.run(coro)

    # Уже есть активный loop — уходим в изолированный поток/loop
    return _run_coro_in_new_loop(coro)


# =========================
# Lightweight cache (guilds/channels/telegram)
# =========================

# TTL можно переопределить в settings (таблица settings, ключ CACHE_TTL_SECONDS)
DEFAULT_CACHE_TTL = 60  # секунд

_cache: Dict[str, Dict[Any, Tuple[float, Any]]] = {
    "guilds": {},     # {bot_db_id: (ts, data)}
    "channels": {},   # {(bot_db_id, guild_id): (ts, data)}
    "tg_chats": {},   # {bot_db_id: (ts, data)}
}


def _cache_ttl() -> int:
    """
    Позволяет динамически переопределять TTL из БД (settings.CACHE_TTL_SECONDS).
    Возвращает число секунд, по умолчанию DEFAULT_CACHE_TTL.
    """
    try:
        with get_db_connection(current_app.config.get("DB_PATH")) as conn:
            row = conn.execute(
                "SELECT `value` FROM settings WHERE `key` = ?",
                ("CACHE_TTL_SECONDS",),
            ).fetchone()
        if row and str(row["value"]).strip().isdigit():
            v = int(row["value"])
            if 5 <= v <= 3600:
                return v
    except Exception:
        pass
    return DEFAULT_CACHE_TTL


def cache_get(space: str, key: Any):
    bucket = _cache.get(space)
    if not bucket:
        return None
    val = bucket.get(key)
    if not val:
        return None
    ts, data = val
    if (time.time() - ts) < _cache_ttl():
        return data
    # просрочено — удалим и вернём None
    try:
        del bucket[key]
    except Exception:
        pass
    return None


def cache_put(space: str, key: Any, data: Any):
    _cache.setdefault(space, {})[key] = (time.time(), data)


def cache_clear(space: Optional[str] = None):
    """
    Полезно для админки/отладки.
    """
    if space:
        _cache.pop(space, None)
    else:
        for k in list(_cache.keys()):
            _cache[k].clear()


# =========================
# DB helpers
# =========================

def get_bot_token(bot_db_id: int, platform: str) -> str:
    """
    Возвращает токен по id записи из bots только если платформа совпадает.
    """
    with get_db_connection(current_app.config.get("DB_PATH")) as conn:
        row = conn.execute(
            "SELECT token, platform FROM bots WHERE id = ?",
            (bot_db_id,),
        ).fetchone()
    if not row or (row["platform"] or "").lower() != platform.lower():
        abort(404, description="Bot not found")
    return row["token"]
