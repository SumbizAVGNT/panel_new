# app/routes/news/nextcord_helpers.py
from __future__ import annotations

"""
Утилиты для «быстрых» одноразовых запросов к Discord через nextcord:
- получить список серверов (guilds), где находится бот;
- получить список текстовых/анонсных каналов на выбранном сервере.

Особенности:
- Запуск клиента обёрнут в asyncio.wait_for(...) с таймаутом.
- Клиент гарантированно закрывается даже при ошибках.
- Списки нормализуются (str(id), name) и сортируются.
"""

from typing import List, Dict, Optional
import asyncio
import contextlib

import nextcord


# -------- Общие настройки для одноразового клиента --------
_GATEWAY_TIMEOUT = 20.0   # сек: сколько ждём on_ready
_RECONNECT = False        # на одноразовый вызов переподключения не нужны


class _EphemeralClient(nextcord.Client):
    """
    Короткоживущий клиент: ждёт on_ready, выполняет работу и закрывается.
    Результат передаётся через futures/promises, чтобы снаружи можно было
    дождаться значения без подписки на события.
    """
    def __init__(self, *, intents: nextcord.Intents):
        super().__init__(intents=intents)
        self.ready_evt = asyncio.Event()
        self.error: Optional[BaseException] = None

    async def on_ready(self):
        # Просто помечаем, что мы вошли и можно выполнять работу
        self.ready_evt.set()

    async def on_error(self, event_method: str, /, *args, **kwargs):
        # Перехватываем исключение, чтобы не потерять его
        # (nextcord по умолчанию логирует в stderr)
        self.error = RuntimeError(f"nextcord event error in {event_method}")
        self.ready_evt.set()


async def _run_with_client(token: str, worker_coro):
    """
    Создаёт клиента, ждёт вход, выполняет worker_coro(client), закрывает клиента.
    Возвращает результат worker_coro. Бросает исключение при неуспехе.
    """
    intents = nextcord.Intents.none()
    intents.guilds = True  # нам нужны только гильдии/каналы
    client = _EphemeralClient(intents=intents)

    try:
        # Стартуем клиента и ждём on_ready с таймаутом
        start_task = asyncio.create_task(client.start(token, reconnect=_RECONNECT))
        try:
            await asyncio.wait_for(client.ready_evt.wait(), timeout=_GATEWAY_TIMEOUT)
        except asyncio.TimeoutError:
            raise TimeoutError("Discord gateway timeout: on_ready not received")

        # Если в on_error что-то упало — пробросим
        if client.error:
            raise client.error

        # Выполняем полезную работу с уже авторизованным клиентом
        result = await worker_coro(client)
        return result
    finally:
        # Аккуратно закрываем: сначала client.close, потом отменяем start_task (если нужно)
        with contextlib.suppress(Exception):
            await client.close()
        # Если start_task всё ещё жив — отменим и дождём
        if 'start_task' in locals() and not start_task.done():
            start_task.cancel()
            with contextlib.suppress(Exception):
                await start_task


# -------- Публичные хелперы --------

async def fetch_guilds_via_nextcord(bot_token: str) -> List[Dict[str, str]]:
    """
    Возвращает список гильдий: [{"id": "123", "name": "Server"}, ...]
    """
    async def _worker(client: nextcord.Client):
        seen = set()
        out: List[Dict[str, str]] = []
        for g in client.guilds:
            gid = str(g.id)
            if gid in seen:
                continue
            seen.add(gid)
            out.append({"id": gid, "name": g.name or f"guild_{gid}"})
        # сортируем по имени (case-insensitive)
        out.sort(key=lambda x: (x["name"] or "").lower())
        return out

    return await _run_with_client(bot_token, _worker)


async def fetch_channels_via_nextcord(bot_token: str, guild_id: int) -> List[Dict[str, str]]:
    """
    Возвращает список текстовых/анонсных каналов гильдии:
      [{"id": "456", "name": "general"}, ...]
    """
    async def _worker(client: nextcord.Client):
        g = client.get_guild(int(guild_id))
        if not g:
            # Может быть, бот не состоит в гильдии или нет прав на просмотр
            return []

        seen = set()
        chans: List[Dict[str, str]] = []

        # Идём по g.channels: тут и категории, и текстовые/голосовые каналы, и т.д.
        for ch in g.channels:
            try:
                # Нас интересуют обычные текстовые и анонсные (news) каналы
                if isinstance(ch, nextcord.TextChannel):
                    ch_type = getattr(ch, "type", None)
                    news_type = getattr(nextcord.ChannelType, "news", None)
                    if ch_type in (nextcord.ChannelType.text, news_type):
                        cid = str(ch.id)
                        if cid not in seen:
                            seen.add(cid)
                            chans.append({"id": cid, "name": ch.name or f"channel_{cid}"})
            except Exception:
                # Игнорируем редкие ошибки доступа к свойствам
                continue

        chans.sort(key=lambda c: (c["name"] or "").lower())
        return chans

    return await _run_with_client(bot_token, _worker)
