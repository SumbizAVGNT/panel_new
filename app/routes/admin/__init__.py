from __future__ import annotations

"""
Админский blueprint и подключение секций:
- users     — управление пользователями
- settings  — общие настройки, OAuth, логи
- bots      — добавление/обновление ботов
- servers   — список серверов и проверки
- support   — Chatwoot/Support интеграция
"""

from flask import Blueprint

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

__all__ = ["admin_bp"]

# ВАЖНО: импорты оставляем внизу, чтобы модули увидели уже созданный admin_bp
# и зарегистрировали на нём свои маршруты.
from . import users        # noqa: E402,F401
from . import settings     # noqa: E402,F401
from . import bots         # noqa: E402,F401
from . import servers      # noqa: E402,F401
from . import support      # noqa: E402,F401
from . import gameservers  # noqa: E402,F401
from . import accounts     # noqa: E402,F401
from . import promocode    # noqa: E402,F401  # NEW
