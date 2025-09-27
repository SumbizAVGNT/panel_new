# app/routes/admin/gameservers.py
from __future__ import annotations

from flask import render_template, abort
from . import admin_bp  # этот blueprint уже есть в пакете admin
from ...decorators import admin_required  # если у вас только superadmin_required — замените здесь

# Список игровых серверов (пока статично, можно позже читать из БД)
SERVERS = [
    {
        "id": "anarxy_1",          # важен slug: совпадает с URL
        "name": "Anarxy #1",
        "game": "Minecraft (Paper)",
        "status": "online",        # online / offline / maintenance
        "players_online": 12,
        "players_max": 100,
        "host": "mc.example.com",
        "port": 25565,
        "tags": ["survival", "pvp", "economy"]
    }
]

SECTIONS = [
    ("players", "Players"),
    ("donate", "Donate"),
    ("logs", "Logs"),
    ("console", "Console"),
    ("errors", "Errors"),
    ("whitelist", "WhiteList"),
    ("oplist", "OpList"),
    ("maintenance", "Technical work"),
]

def find_server_or_404(server_id: str):
    for s in SERVERS:
        if s["id"] == server_id:
            return s
    abort(404, description="Server not found")


@admin_bp.get("/gameservers")
@admin_required
def gameservers_index():
    """Список игровых серверов."""
    return render_template(
        "admin/gameservers/index.html",
        servers=SERVERS,
    )


@admin_bp.get("/gameservers/<server_id>")
@admin_required
def gameservers_show(server_id: str):
    """Обзор конкретного сервера (промежуточная страница)."""
    server = find_server_or_404(server_id)
    return render_template(
        "admin/gameservers/anarxy_1.html" if server_id == "anarxy_1" else "admin/gameservers/overview.html",
        server=server,
        sections=SECTIONS,
    )


# Универсальный обработчик секций
@admin_bp.get("/gameservers/<server_id>/<section>")
@admin_required
def gameservers_section(server_id: str, section: str):
    """
    Роуты:
      players / donate / logs / console / errors / whitelist / oplist / maintenance
    """
    server = find_server_or_404(server_id)
    valid = {k: v for k, v in SECTIONS}
    if section not in valid:
        abort(404, description="Section not found")

    # Для простоты — один шаблон, который меняет контент по section
    return render_template(
        "admin/gameservers/section.html",
        server=server,
        sections=SECTIONS,
        section=section,
        section_title=valid[section],
    )
