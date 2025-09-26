# app/routes/news/__init__.py
from __future__ import annotations

from pathlib import Path
from flask import Blueprint

# Корень пакета app/
BASE_DIR = Path(__file__).resolve().parents[2]

news_bp = Blueprint(
    "news",
    __name__,
    url_prefix="/news",
    template_folder=str(BASE_DIR / "templates" / "news"),  # например: app/templates/news/
    static_folder=None,  # если понадобится своя статика: "static/news"
)

# Импорты регистрируют эндпоинты на news_bp — оставляем внизу
from . import views      # noqa: E402,F401
from . import publish    # noqa: E402,F401
from . import api        # noqa: E402,F401
from . import files      # noqa: E402,F401

__all__ = ["news_bp"]
