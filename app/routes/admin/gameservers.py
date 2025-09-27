# app/routes/admin/gameservers.py
from __future__ import annotations

from flask import Blueprint, render_template, jsonify, request, redirect, url_for
from app.decorators import login_required
from app.modules.bridge_client import bridge_list, stats_query, console_exec

# корневой админский блюпринт
from . import admin_bp

# -------- вложенный блюпринт под /admin/gameservers --------
bp = Blueprint("gameservers", __name__, url_prefix="/gameservers")

# HTML
@bp.route("/")
@login_required
def index():
    return render_template("admin/gameservers/index.html")

@bp.route("/<realm>")
@login_required
def realm_page(realm: str):
    return render_template("admin/gameservers/section.html", realm=realm)

# API
@bp.route("/api/list")
@login_required
def api_list():
    try:
        data = bridge_list()
        return jsonify({"ok": True, "data": data.get("payload", {})})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@bp.route("/api/stats")
@login_required
def api_stats():
    realm = request.args.get("realm", "").strip()
    if not realm:
        return jsonify({"ok": False, "error": "realm required"}), 400
    try:
        data = stats_query(realm)
        return jsonify({"ok": True, "data": data.get("payload") or data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@bp.route("/api/console", methods=["POST"])
@login_required
def api_console():
    j = request.get_json(silent=True) or {}
    realm = (j.get("realm") or "").strip()
    cmd = (j.get("cmd") or "").strip()
    if not realm or not cmd:
        return jsonify({"ok": False, "error": "realm and cmd required"}), 400
    try:
        data = console_exec(realm, cmd)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ------------- ВАЖНО: регистрируем сначала под-блюпринт -------------
admin_bp.register_blueprint(bp)

# -------- затем объявляем алиасы для обратной совместимости --------
# Эти алиасы имеют те же пути, но будут добавлены ПОСЛЕ основных правил,
# поэтому на прямой заход URL попадёт в основные view, а старые endpoint-имена
# будут продолжать строить URL без 404.

@admin_bp.get("/gameservers", endpoint="gameservers_index")
@login_required
def _gs_index_alias():
    return render_template("admin/gameservers/index.html")

@admin_bp.get("/gameservers/<realm>", endpoint="gameservers_section")
@login_required
def _gs_section_alias(realm: str):
    return render_template("admin/gameservers/section.html", realm=realm)
