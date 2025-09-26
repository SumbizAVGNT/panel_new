# app/routes/admin/servers.py
from __future__ import annotations

import os
import stat
import time
from typing import Optional

from werkzeug.utils import secure_filename
from flask import request, redirect, url_for, flash, current_app

from ...database import get_db_connection
from ...decorators import superadmin_required
from . import admin_bp
from .admin_common import check_csrf
from .helpers import (
    keys_dir,
    tcp_ping,
    ssh_uptime,
    pick_status_value_for_db,
    utc_now_str,
    servers_last_status_storage,
)


def _clean_str(v: Optional[str]) -> Optional[str]:
    v = (v or "").strip()
    return v or None


def _valid_port(p: Optional[int]) -> int:
    try:
        p = int(p or 22)
    except Exception:
        p = 22
    if not (1 <= p <= 65535):
        p = 22
    return p


@admin_bp.post("/settings/servers/add")
@superadmin_required
def servers_add():
    """
    Добавляет сервер. При совпадении (host,port,username) обновляет только присланные поля:
      - name               -> всегда обновляем (если не пустое), иначе берём <host>:<port>
      - password           -> обновляем только если прислали непустое значение
      - ssh_key_path       -> обновляем только если загружен новый файл
    """
    check_csrf()

    host = _clean_str(request.form.get("host"))
    port = _valid_port(request.form.get("port", type=int))
    username = _clean_str(request.form.get("username"))
    name = _clean_str(request.form.get("name"))
    password = request.form.get("password")  # не чистим — пустую строку интерпретируем как «не обновлять»
    key_file = request.files.get("ssh_key")

    if not (host and username):
        flash("Fill host and username", "error")
        return redirect(url_for("admin.settings"))

    # name обязателен по схеме — дадим дефолт
    if not name:
        name = f"{host}:{port}"

    # Прикручиваем загрузку ключа (опционально)
    ssh_key_path: Optional[str] = None
    if key_file and key_file.filename:
        # имя файла + уникальность
        fname = f"{int(time.time())}_{secure_filename(key_file.filename)}"
        dest_dir = keys_dir()
        dest = os.path.join(dest_dir, fname)
        try:
            key_file.save(dest)
            # выставим права 600, если это приватный ключ — лишним не будет
            try:
                os.chmod(dest, stat.S_IRUSR | stat.S_IWUSR)
            except Exception:
                pass
            ssh_key_path = dest
        except Exception as e:
            flash(f"Failed to save SSH key: {e}", "error")
            return redirect(url_for("admin.settings"))

    # В MySQL используем UPSERT. Важно: не затираем значения, если не присланы.
    # Для этого используем COALESCE(NULLIF(VALUES(field), ''), field) для password
    # и COALESCE(VALUES(ssh_key_path), ssh_key_path) для ключа.
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute(
            """
            INSERT INTO servers(name, host, port, username, password, ssh_key_path)
            VALUES (?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              -- name обновляем, только если прислали непустое:
              name = COALESCE(NULLIF(VALUES(name), ''), name),
              -- пароль: пустая строка = не обновлять
              password = COALESCE(NULLIF(VALUES(password), ''), password),
              -- ключ: обновляем только если реально загружен новый файл (NULL означает «не прислано»)
              ssh_key_path = COALESCE(VALUES(ssh_key_path), ssh_key_path)
            """,
            (name, host, port, username, (password or ""), ssh_key_path),
        )

    flash("Server saved", "success")
    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/servers/<int:server_id>/delete")
@superadmin_required
def servers_delete(server_id: int):
    check_csrf()

    # Сначала уберём файл ключа (если есть)
    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        row = conn.execute(
            "SELECT ssh_key_path FROM servers WHERE id = ?",
            (server_id,),
        ).fetchone()

    if row and row.get("ssh_key_path"):
        try:
            if os.path.isfile(row["ssh_key_path"]):
                os.remove(row["ssh_key_path"])
        except Exception:
            # не блокируем удаление записи
            pass

    # Теперь удаляем запись
    with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
        conn.execute("DELETE FROM servers WHERE id = ?", (server_id,))

    flash("Server removed", "success")
    return redirect(url_for("admin.settings"))


@admin_bp.post("/settings/servers/<int:server_id>/check")
@superadmin_required
def servers_check(server_id: int):
    """
    Проверка доступности сервера:
      1) tcp_ping(host, port)
      2) если онлайн — пытаемся получить uptime по SSH
      3) сохраняем last_status/last_uptime/last_checked
    Статус учитывает тип столбца (ENUM/TEXT/NUMERIC) через pick_status_value_for_db().
    """
    check_csrf()

    with get_db_connection(current_app.config["DB_PATH"]) as conn:
        s = conn.execute(
            """
            SELECT id, host, port, username, password, ssh_key_path
            FROM servers WHERE id = ?
            """,
            (server_id,),
        ).fetchone()

    if not s:
        flash("Server not found", "error")
        return redirect(url_for("admin.settings"))

    host = s["host"]
    port = _valid_port(s.get("port"))
    username = s.get("username")
    password = s.get("password")
    ssh_key_path = s.get("ssh_key_path")

    # 1) ping
    reachable = tcp_ping(host, port)
    status_val = pick_status_value_for_db(current_app.config["DB_PATH"], reachable)

    # 2) uptime по SSH
    uptime = None
    if reachable and username:
        uptime = ssh_uptime(host, port, username, password, ssh_key_path)

    # 3) апдейт полей
    now_utc = utc_now_str()
    try:
        with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
            conn.execute(
                """
                UPDATE servers
                SET last_status = ?, last_uptime = ?, last_checked = ?
                WHERE id = ?
                """,
                (status_val, uptime, now_utc, server_id),
            )
    except Exception as e:
        # На случай несоответствия ENUM — пробуем синонимы up/down.
        info = servers_last_status_storage(current_app.config["DB_PATH"])
        if info["kind"] == "enum":
            alt_val = "up" if reachable else "down"
            try:
                with get_db_connection(current_app.config["DB_PATH"]) as conn, conn:
                    conn.execute(
                        """
                        UPDATE servers
                        SET last_status = ?, last_uptime = ?, last_checked = ?
                        WHERE id = ?
                        """,
                        (alt_val, uptime, now_utc, server_id),
                    )
            except Exception:
                flash(f"Update failed for last_status: {e}", "error")
                return redirect(url_for("admin.settings"))
        else:
            flash(f"Update failed for last_status: {e}", "error")
            return redirect(url_for("admin.settings"))

    # UI-отклик
    if reachable:
        msg = f"Server {host}:{port} is ONLINE"
        if uptime:
            msg += f" (uptime: {uptime})"
        flash(msg, "success")
    else:
        flash(f"Server {host}:{port} is OFFLINE or unreachable", "warning")

    return redirect(url_for("admin.settings"))
