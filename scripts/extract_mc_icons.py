# panel_new/scripts/extract_mc_icons.py
# -*- coding: utf-8 -*-
r"""
Извлекает иконки предметов Minecraft напрямую из client.jar Mojang (без локального .minecraft).

Что делает:
 - version_manifest_v2 → version.json выбранной версии → downloads.client.url
 - Скачивает client.jar (кэширует локально)
 - Извлекает PNG из путей:
     assets/minecraft/textures/gui/sprites/items/*.png   (1.20.5+)
     assets/minecraft/textures/items/*.png               (классика)
     assets/minecraft/textures/item/*.png                (иногда встречается)
 - Складывает в app/static/mc/<version>/items (или в --out)
 - Пишет app/static/data/vanilla-items-<version>.json со списком id/name

Примеры:
  py panel_new\scripts\extract_mc_icons.py --version 1.20.6 --force --debug
  python panel_new/scripts/extract_mc_icons.py --version 1.20.6 --out "D:\...\items"
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import zipfile
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, List

# HTTP
try:
    import requests  # type: ignore
except Exception:
    requests = None

VERSION_DEFAULT = "1.20.6"
MANIFEST_URL = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"

# Пути в JAR, где лежат иконки предметов
ITEM_PATHS = (
    "assets/minecraft/textures/gui/sprites/items/",  # 1.20.5+
    "assets/minecraft/textures/items/",              # старая раскладка
    "assets/minecraft/textures/item/",               # редкая вариация
)


# ---------- утилиты пути/вывода ----------

def repo_root() -> Path:
    # panel_new/scripts -> parents[1] = panel_new
    return Path(__file__).resolve().parents[1]


def out_dir_for(version: str, out_arg: Optional[str]) -> Path:
    if out_arg:
        return Path(out_arg).expanduser().resolve()
    return (repo_root() / "app" / "static" / "mc" / version / "items").resolve()


def items_manifest_path(version: str) -> Path:
    return (repo_root() / "app" / "static" / "data" / f"vanilla-items-{version}.json").resolve()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def cache_dir() -> Path:
    d = repo_root() / ".cache" / "mc"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------- HTTP helpers ----------

def http_get_json(url: str, timeout: int = 45) -> dict:
    if requests:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    else:
        import urllib.request, json as _json
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return _json.loads(resp.read().decode("utf-8"))


def http_get_bytes(url: str, timeout: int = 120) -> bytes:
    if requests:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.content
    else:
        import urllib.request
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()


# ---------- Mojang metadata ----------

def fetch_version_json(version: str) -> dict:
    mf = http_get_json(MANIFEST_URL)
    for v in mf.get("versions", []):
        if str(v.get("id")) == version:
            url = v.get("url")
            if not url:
                break
            return http_get_json(url)
    raise FileNotFoundError(f"Версия {version} не найдена в Mojang manifest")


def get_client_download(vjson: dict) -> Tuple[str, Optional[str]]:
    # Возвращает (url, sha1) client.jar
    dl = (vjson.get("downloads") or {}).get("client") or {}
    url = dl.get("url")
    if not url:
        raise FileNotFoundError("В version.json отсутствует downloads.client.url")
    return url, dl.get("sha1")


# ---------- JAR извлечение ----------

def want_item_entry(name: str) -> bool:
    if not name.lower().endswith(".png"):
        return False
    n = name.replace("\\", "/")
    return any(n.startswith(p) for p in ITEM_PATHS)


def compute_sha1(data: bytes) -> str:
    h = hashlib.sha1()
    h.update(data)
    return h.hexdigest()


def download_client_jar(version: str, url: str, sha1: Optional[str]) -> Path:
    dest = cache_dir() / f"client-{version}.jar"
    if dest.exists() and dest.stat().st_size > 0:
        if sha1:
            try:
                if compute_sha1(dest.read_bytes()) == sha1:
                    return dest
            except Exception:
                pass
        else:
            return dest

    blob = http_get_bytes(url)
    if sha1:
        got = compute_sha1(blob)
        if got != sha1:
            raise RuntimeError(f"SHA1 client.jar не совпадает: ожидали {sha1}, получили {got}")
    dest.write_bytes(blob)
    return dest


def extract_icons_from_jar(jar_path: Path, out_dir: Path, force: bool = False, debug: bool = False) -> Tuple[int, int, int, List[str]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    skipped = 0
    missing = 0
    ids: List[str] = []

    with zipfile.ZipFile(jar_path, "r") as zf:
        names = zf.namelist()
        if debug:
            print(f"[debug] файлов в JAR: {len(names)}")
            # покажем для наглядности первые совпадения по путям
            sample = [n for n in names if any(n.startswith(p) for p in ITEM_PATHS)]
            print(f"[debug] совпавших путей по маске: {len(sample)}")
            for s in sample[:30]:
                print("  -", s)

        for name in names:
            if not want_item_entry(name):
                continue
            item_id = Path(name).stem.lower()
            ids.append(item_id)
            dst = out_dir / f"{item_id}.png"
            if dst.exists() and not force:
                skipped += 1
                continue
            try:
                data = zf.read(name)
                dst.write_bytes(data)
                saved += 1
            except Exception:
                missing += 1
                if debug:
                    print(f"[debug] не удалось извлечь {name}")

    return saved, skipped, missing, ids


# ---------- orchestration ----------

def extract_all_icons(version: str, out_dir: Path, force: bool = False, debug: bool = False) -> dict:
    vjson = fetch_version_json(version)
    client_url, client_sha1 = get_client_download(vjson)
    jar_path = download_client_jar(version, client_url, client_sha1)

    saved, skipped, missing, ids = extract_icons_from_jar(jar_path, out_dir, force=force, debug=debug)

    # items manifest для фронта
    manifest = items_manifest_path(version)
    ensure_parent(manifest)
    uniq = sorted(set(ids))
    items_list = [{"id": iid, "name": iid.replace("_", " ").title()} for iid in uniq]
    try:
        manifest.write_text(json.dumps(items_list, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[warn] не удалось записать {manifest}: {e}")

    return {
        "version": version,
        "saved": saved,
        "skipped": skipped,
        "missing": missing,
        "count": len(uniq),
        "out": str(out_dir),
        "items_json": str(manifest),
        "jar": str(jar_path),
    }


# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(description="Извлекает иконки предметов из Mojang client.jar (без локального .minecraft)")
    parser.add_argument("--version", default=VERSION_DEFAULT, help="Версия Minecraft (по умолчанию 1.20.6)")
    parser.add_argument("--out", help="Папка для PNG (по умолчанию app/static/mc/<version>/items)")
    parser.add_argument("--force", action="store_true", help="Переписывать существующие файлы")
    parser.add_argument("--debug", action="store_true", help="Печать отладочной информации")
    args = parser.parse_args()

    out_dir = out_dir_for(args.version, args.out)
    print(f"[cfg] version={args.version} | out={out_dir}")

    try:
        stats = extract_all_icons(args.version, out_dir, force=args.force, debug=args.debug)
    except Exception as e:
        print(f"[fatal] {e}")
        sys.exit(1)

    print(
        "[done] версия: {version} | сохранено: {saved} | пропущено: {skipped} | не извлечено: {missing} | всего_id: {count}".format(
            **stats
        )
    )
    print(f"[jar] {stats['jar']}")
    print(f"[out] icons -> {stats['out']}")
    print(f"[out] items json -> {stats['items_json']}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
