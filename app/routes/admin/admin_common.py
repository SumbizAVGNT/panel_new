# app/routes/admin/admin_common.py
from __future__ import annotations

from typing import Tuple, Optional
import re
import requests
from urllib.parse import urlparse
from flask import request, session, abort

# ---- Константы / настройки ----
ALLOWED_ROLES = {"pending", "user", "admin", "superadmin"}
HTTP_TIMEOUT = 8  # seconds (общий таймаут)
USER_AGENT = "MoonRein/1.0 (+admin-probe)"

_JSON_CT_RE = re.compile(r"^application/(?:json|problem\+json)(?:;|$)", re.I)


def _is_json_response(resp: requests.Response) -> bool:
    return _JSON_CT_RE.match(resp.headers.get("content-type", "")) is not None


# ---- CSRF ----
def check_csrf() -> None:
    """
    Проверяет CSRF-токен как в form-данных, так и в заголовке (для AJAX).
    Бросает 400 при несоответствии.
    """
    token_form = (request.form.get("csrf_token") or "").strip()
    token_hdr = (request.headers.get("X-CSRF-Token") or "").strip()
    token_sess = (session.get("_csrf") or "").strip()

    # для JSON/AJAX часто ещё ставят X-Requested-With
    if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        if not token_hdr or token_hdr != token_sess:
            abort(400, description="CSRF token invalid")
    else:
        if not token_form or token_form != token_sess:
            abort(400, description="CSRF token invalid")


# ---- Discord OAuth2 probe ----
def probe_oauth_status(client_id: str, client_secret: str, redirect_uri: str) -> Tuple[bool, Optional[str]]:
    """
    Живо проверяет связку client_id / client_secret / redirect_uri.
    Ожидаем:
      - 400 + {"error":"invalid_grant"} — креды валидны (код фальшивый) => OK
      - 401 — неверные client id/secret => Fail
      - Иначе — Fail с кодом ответа
    """
    try:
        r = requests.post(
            "https://discord.com/api/oauth2/token",
            data={
                "client_id": client_id or "",
                "client_secret": client_secret or "",
                "grant_type": "authorization_code",
                "code": "dummy",
                "redirect_uri": redirect_uri or "",
            },
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
        )

        if r.status_code == 400 and _is_json_response(r):
            try:
                if (r.json() or {}).get("error") == "invalid_grant":
                    return True, None
            except Exception:
                pass

        if r.status_code == 401:
            return False, "Invalid Client ID/Secret"

        return False, f"Discord responded: {r.status_code}"
    except requests.Timeout:
        return False, "Network: timeout"
    except requests.SSLError as e:
        return False, f"Network: SSL error ({e})"
    except requests.ConnectionError as e:
        return False, f"Network: connection error ({e})"
    except requests.RequestException as e:
        return False, f"Network: {e.__class__.__name__} ({e})"


# ---- Chatwoot helpers ----
def _normalize_base(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _safe_base_url(url: str) -> tuple[bool, str | None]:
    """
    Простая валидация базового URL во избежание SSRF:
    - схема http/https
    - http разрешаем только для localhost/127.0.0.1
    """
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False, "Unsupported scheme"
        host = (p.hostname or "").lower()
        if p.scheme == "http" and host not in ("localhost", "127.0.0.1"):
            return False, "Plain HTTP is only allowed for localhost"
        return True, None
    except Exception:
        return False, "Invalid URL"


def probe_chatwoot(
    base_url: str,
    access_token: str,
    client: str | None = None,
    uid: str | None = None,
) -> Tuple[bool, Optional[str]]:
    """
    Проверка Chatwoot по REST API.
    GET {base}/api/v1/accounts c Access Token.
    Успех: 200/OK и JSON.
    401/403 — неверный токен/доступ.
    """
    if not base_url or not access_token:
        return False, "Base URL and Access Token required"

    base = _normalize_base(base_url)
    ok, reason = _safe_base_url(base)
    if not ok:
        return False, f"Base URL rejected: {reason}"

    test_url = f"{base}/api/v1/accounts"
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        # Chatwoot понимает оба варианта:
        "Authorization": f"Bearer {access_token}",
        "api_access_token": access_token,
    }
    if client:
        headers["X-Chatwoot-Client"] = client
    if uid:
        headers["X-Chatwoot-Uid"] = uid

    try:
        r = requests.get(test_url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code == 200 and _is_json_response(r):
            return True, None
        if r.status_code in (401, 403):
            return False, "Unauthorized (check token)"
        return False, f"Chatwoot responded: {r.status_code}"
    except requests.Timeout:
        return False, "Network: timeout"
    except requests.RequestException as e:
        return False, f"Network: {e.__class__.__name__} ({e})"
