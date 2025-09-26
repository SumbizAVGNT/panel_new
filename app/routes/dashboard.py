# app/routes/dashboard.py
from __future__ import annotations

import datetime as dt
import json
import random
from typing import Any, Dict, List, Optional

from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    current_app,
    jsonify,
    request,
)

from ..database import get_db_connection
from ..decorators import login_required

dashboard_bp = Blueprint("dashboard", __name__)


# ---------------------------- DB helpers ----------------------------

def _fetch_user(conn, user_id: int) -> Optional[Dict[str, Any]]:
    """Возвращает username/role по id или None."""
    row = conn.execute(
        "SELECT username, role FROM users WHERE id = ? LIMIT 1",
        (user_id,),
    ).fetchone()
    return dict(row) if row else None


def _table_exists(conn, table_name: str) -> bool:
    """Проверяет наличие таблицы в текущей БД."""
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
    """
    Возвращает список серверов; если есть server_metrics — подмешивает последние метрики.
    Поля docker_names преобразуются в список (из JSON/CSV/None).
    """
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

        # docker_names: нормализуем к списку
        names_raw = d.get("docker_names")
        if names_raw is None:
            lst: List[str] = []
        else:
            try:
                v = json.loads(names_raw)
                if isinstance(v, list):
                    lst = [str(x) for x in v]
                elif isinstance(v, str) and v.strip():
                    # мог быть JSON-стрингом с CSV внутри
                    lst = [s.strip() for s in v.split(",") if s.strip()]
                else:
                    lst = [str(v)]
            except Exception:
                lst = [s.strip() for s in str(names_raw).split(",") if s.strip()]
        d["docker_names_list"] = lst

        servers.append(d)

    return servers


# ---------------------------- Views ----------------------------

@dashboard_bp.route("/")
@login_required
def dashboard():
    now = dt.datetime.utcnow()
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        user = _fetch_user(conn, session["user_id"])
        if not user:
            session.clear()
            return redirect(url_for("auth.login"))
        servers = _fetch_servers_with_metrics(conn)

    # Заглушечные цифры для карточек (можно заменить реалом позже)
    stats = {
        "sales": "97.6K",
        "avg_sessions": "2.7k",
        "sessions_change": "5.2",
        "cost": "$100000",
        "users": "100K",
        "retention": "90%",
        "duration": "1yr",
        "tickets": "16.3",
        "new_tickets": "29",
        "cricket_received": "97.5K",
        "completed_tickets": "83%",
        "response_time": "89 y less",
    }

    return render_template(
        "dashboard.html",
        stats=stats,
        username=user["username"],
        role=user["role"],
        servers=servers,
        now=now,
    )


@dashboard_bp.get("/api/servers")
@login_required
def api_servers():
    """
    Возвращает JSON для живого обновления карточек серверов.
    Параметр ?demo=1 — синтетически заполняет метрики, если их нет.
    """
    demo = request.args.get("demo", "0") == "1"
    now = dt.datetime.utcnow()

    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        servers = _fetch_servers_with_metrics(conn)

    # стабильный «шум» для демо внутри 5-секундных бакетов времени
    bucket = now.replace(microsecond=0, second=(now.second // 5) * 5)
    bucket_key = f"{bucket.minute}-{bucket.second}"

    out: List[Dict[str, Any]] = []
    for s in servers:
        item: Dict[str, Any] = {
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

        # реальные метрики (если есть)
        item["cpu_pct"] = s.get("cpu_pct")
        item["mem_used_gb"] = s.get("mem_used_gb")
        item["mem_total_gb"] = s.get("mem_total_gb")
        item["disk_used_gb"] = s.get("disk_used_gb")
        item["disk_total_gb"] = s.get("disk_total_gb")
        item["net_in_mbps"] = s.get("net_in_mbps")
        item["net_out_mbps"] = s.get("net_out_mbps")

        # DEMO: подставим «живые» значения, если отсутствуют
        if demo:
            rng = random.Random(f"{s['id']}-{bucket_key}")

            if item["cpu_pct"] is None:
                item["cpu_pct"] = round(rng.uniform(3, 78), 1)

            if item["mem_total_gb"] is None:
                total = rng.choice([4, 8, 16, 32])
                used = round(total * rng.uniform(0.25, 0.85), 1)
                item["mem_total_gb"] = total
                item["mem_used_gb"] = used

            if item["disk_total_gb"] is None:
                dtot = rng.choice([80, 128, 256, 512, 1024])
                dusd = round(dtot * rng.uniform(0.15, 0.9), 1)
                item["disk_total_gb"] = dtot
                item["disk_used_gb"] = dusd

            if item["net_in_mbps"] is None:
                item["net_in_mbps"] = round(rng.uniform(0, 20), 1)
            if item["net_out_mbps"] is None:
                item["net_out_mbps"] = round(rng.uniform(0, 20), 1)

            if item["docker_running"] is None:
                item["docker_running"] = rng.randint(0, 6)
            if not item["docker_names"]:
                n = int(item["docker_running"] or 0)
                item["docker_names"] = [f"svc-{i}" for i in range(1, n + 1)]

        # Аватар (стабильно «случайный» для сервера)
        seed = f"{s['id']}-{s['name']}"
        item["avatar"] = f"https://api.dicebear.com/7.x/shapes/svg?seed={seed}&radius=8"

        out.append(item)

    return jsonify(ok=True, now=str(now), servers=out)
