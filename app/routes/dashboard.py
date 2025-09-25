# app/routes/dashboard.py
from __future__ import annotations

import datetime
import random
from typing import List, Dict, Any

from flask import Blueprint, render_template, session, redirect, url_for, current_app, jsonify, request
from ..database import get_db_connection
from ..decorators import login_required

dashboard_bp = Blueprint('dashboard', __name__)


def _fetch_user(conn, user_id: int):
    return conn.execute(
        "SELECT username, role FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = ?
        """,
        (table_name,),
    ).fetchone()
    return bool(row)


def _fetch_servers_with_metrics(conn) -> List[Dict[str, Any]]:
    """Возвращает сервера + последние метрики если есть таблица server_metrics."""
    has_metrics = _table_exists(conn, "server_metrics")

    if has_metrics:
        rows = conn.execute(
            """
            SELECT s.id, s.name, s.host, s.port, s.username,
                   s.last_status, s.last_uptime, s.last_checked,
                   m.cpu_pct, m.mem_used_gb, m.mem_total_gb,
                   m.disk_used_gb, m.disk_total_gb,
                   m.net_in_mbps, m.net_out_mbps,
                   m.docker_running, m.docker_names, m.collected_at
            FROM servers s
            LEFT JOIN (
                SELECT t.*
                FROM server_metrics t
                JOIN (
                  SELECT server_id, MAX(collected_at) AS ts
                  FROM server_metrics
                  GROUP BY server_id
                ) last ON last.server_id = t.server_id AND last.ts = t.collected_at
            ) m ON m.server_id = s.id
            ORDER BY s.added_at DESC, s.id DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT s.id, s.name, s.host, s.port, s.username,
                   s.last_status, s.last_uptime, s.last_checked
            FROM servers s
            ORDER BY s.added_at DESC, s.id DESC
            """
        ).fetchall()

    servers: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        # нормализация docker_names -> список
        names_raw = d.get("docker_names")
        if names_raw is None:
            lst = []
        else:
            try:
                import json
                v = json.loads(names_raw)
                lst = v if isinstance(v, list) else [str(v)]
            except Exception:
                lst = [x.strip() for x in str(names_raw).split(",") if x.strip()]
        d["docker_names_list"] = lst
        servers.append(d)
    return servers


@dashboard_bp.route('/')
@login_required
def dashboard():
    with get_db_connection(current_app.config['DB_PATH']) as conn:
        user = _fetch_user(conn, session['user_id'])
        if not user:
            session.clear()
            return redirect(url_for('auth.login'))
        servers = _fetch_servers_with_metrics(conn)

    stats = {
        'sales': '97.6K',
        'avg_sessions': '2.7k',
        'sessions_change': '5.2',
        'cost': '$100000',
        'users': '100K',
        'retention': '90%',
        'duration': '1yr',
        'tickets': '16.3',
        'new_tickets': '29',
        'cricket_received': '97.5K',
        'completed_tickets': '83%',
        'response_time': '89 y less'
    }

    return render_template(
        'dashboard.html',
        stats=stats,
        username=user['username'],
        role=user['role'],
        servers=servers,
        now=datetime.datetime.utcnow()
    )


@dashboard_bp.get('/api/servers')
@login_required
def api_servers():
    """JSON для живого обновления карточек. ?demo=1 — сэмплирует метрики при их отсутствии."""
    demo = request.args.get('demo', '0') == '1'
    with get_db_connection(current_app.config['DB_PATH']) as conn:
        servers = _fetch_servers_with_metrics(conn)

    out = []
    for s in servers:
        # базовая информация
        item = {
            "id": s["id"],
            "name": s["name"],
            "host": s["host"],
            "port": s["port"],
            "username": s["username"],
            "status": s.get("last_status") or "unknown",
            "uptime": s.get("last_uptime") or None,
            "checked": s.get("last_checked") and str(s["last_checked"]) or None,
            "collected_at": s.get("collected_at") and str(s["collected_at"]) or None,
            "docker_running": s.get("docker_running"),
            "docker_names": s.get("docker_names_list") or [],
        }

        # реальные метрики если есть
        item["cpu_pct"] = s.get("cpu_pct")
        item["mem_used_gb"] = s.get("mem_used_gb")
        item["mem_total_gb"] = s.get("mem_total_gb")
        item["disk_used_gb"] = s.get("disk_used_gb")
        item["disk_total_gb"] = s.get("disk_total_gb")
        item["net_in_mbps"] = s.get("net_in_mbps")
        item["net_out_mbps"] = s.get("net_out_mbps")

        # DEMO: если метрик нет — подставим «живые» значения
        if demo:
            rng = random.Random(f"{s['id']}-{datetime.datetime.utcnow().minute}-{datetime.datetime.utcnow().second//5}")
            if item["cpu_pct"] is None:
                item["cpu_pct"] = round(rng.uniform(3, 78), 1)
            if item["mem_total_gb"] is None:
                total = rng.choice([4, 8, 16, 32])
                used = round(total * rng.uniform(0.25, 0.85), 1)
                item["mem_total_gb"] = total
                item["mem_used_gb"] = used
            if item["disk_total_gb"] is None:
                dt = rng.choice([80, 128, 256, 512, 1024])
                du = round(dt * rng.uniform(0.15, 0.9), 1)
                item["disk_total_gb"] = dt
                item["disk_used_gb"] = du
            if item["net_in_mbps"] is None:
                item["net_in_mbps"] = round(rng.uniform(0, 20), 1)
            if item["net_out_mbps"] is None:
                item["net_out_mbps"] = round(rng.uniform(0, 20), 1)
            if item["docker_running"] is None:
                item["docker_running"] = rng.randint(0, 6)
            if not item["docker_names"]:
                n = item["docker_running"] or 0
                item["docker_names"] = [f"svc-{i}" for i in range(1, n + 1)]

        # аватар (стабильно случайный для сервера)
        seed = f"{s['id']}-{s['name']}"
        item["avatar"] = f"https://api.dicebear.com/7.x/shapes/svg?seed={seed}&radius=8"

        out.append(item)

    return jsonify(ok=True, servers=out)
