# app/routes/news/files.py
from __future__ import annotations

import os
from flask import abort
from flask import send_from_directory
from werkzeug.utils import safe_join  # защищает от обхода директорий

from ...decorators import login_required
from . import news_bp
from .common import get_upload_dir


@news_bp.get("/file/<path:filename>")
@login_required
def file(filename: str):
    """
    Безопасная раздача загруженных файлов из каталога /uploads.
    Поддерживает подкаталоги (например, images/202509/img.webp),
    защищает от path traversal и даёт кэширование.
    """
    if not filename:
        abort(404)

    # Базовая папка загрузок
    base_dir = get_upload_dir()  # например, <repo_root>/uploads

    # Безопасно склеиваем путь (вернёт None, если попытка выйти из base_dir)
    safe_path = safe_join(base_dir, filename)
    if not safe_path:
        abort(404)

    # Файл должен существовать на диске
    if not os.path.isfile(safe_path):
        abort(404)

    # Отдаём относительно base_dir.
    # В Flask 3.x параметр называется `path`, в 2.x — позиционный.
    rel_path = os.path.relpath(safe_path, base_dir).replace("\\", "/")

    try:
        # Flask 3.x
        return send_from_directory(
            base_dir,
            path=rel_path,
            as_attachment=False,
            max_age=3600,
            conditional=True,  # ETag/If-Modified-Since
        )
    except TypeError:
        # Flask 2.x — без именованного аргумента `path`
        return send_from_directory(
            base_dir,
            rel_path,
            as_attachment=False,
            max_age=3600,
            conditional=True,
        )
