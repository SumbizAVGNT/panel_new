# run.py
from __future__ import annotations

import os
import sys
import signal
from pathlib import Path

# --- .env (опционально) ---
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # пакет может быть не установлен — это ок
    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False

load_dotenv()

# --- Flask app factory / CLI ---
from app import create_app  # noqa: E402
from app.cli import register_cli  # noqa: E402


def _bool_env(*keys: str, default: bool = False) -> bool:
    """True если любая из переменных == '1' / 'true' / 'yes' / 'on' (без регистра)."""
    truthy = {"1", "true", "yes", "on"}
    for k in keys:
        v = os.environ.get(k)
        if v is not None and v.strip().lower() in truthy:
            return True
    return default


def main() -> int:
    app = create_app()
    register_cli(app)

    # ---- конфиг запуска ----
    debug = _bool_env("FLASK_DEBUG", "DEBUG", default=False)
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5000"))

    # Потише Werkzeug в проде
    if not debug:
        import logging
        logging.getLogger("werkzeug").setLevel(logging.INFO)

    # Отслеживаем изменения в шаблонах при debug
    extra_files: list[str] = []
    if debug:
        # Найдём каталог templates рядом с app/
        root = Path(__file__).resolve().parent
        # Обычно шаблоны в ./app/templates или ./templates
        candidates = [root / "app" / "templates", root / "templates"]
        for p in candidates:
            if p.exists():
                # Добавляем все файлы, чтобы перезагружать по любому изменению
                extra_files = [str(f) for f in p.rglob("*") if f.is_file()]
                break

    # Красивый баннер
    def banner() -> None:
        url_local = f"http://127.0.0.1:{port}"
        try:
            # на Docker/WSL часто удобно подсказать 0.0.0.0
            url_all = f"http://{host}:{port}"
        except Exception:
            url_all = url_local
        print(
            "\n"
            "🚀 MoonRein panel\n"
            f"   Debug: {debug}\n"
            f"   Running on: {url_local}  (and {url_all})\n"
        )

    banner()

    # Корректное завершение по Ctrl+C в некоторых окружениях
    def _graceful_exit(_sig, _frame):
        print("\nShutting down…")
        sys.exit(0)

    signal.signal(signal.SIGINT, _graceful_exit)
    try:
        signal.signal(signal.SIGTERM, _graceful_exit)
    except Exception:
        pass

    app.run(
        host=host,
        port=port,
        debug=debug,
        use_reloader=debug,        # только в отладке
        extra_files=extra_files,   # следим за шаблонами
        threaded=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
f