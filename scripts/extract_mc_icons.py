# panel_new/scripts/extract_mc_icons.py
# -*- coding: utf-8 -*-
r"""
Извлекает иконки Minecraft из client.jar Mojang (без локального .minecraft).

Поддержка:
 - ITEMS: 1) assets/minecraft/textures/gui/sprites/items/*.png (1.20.5+)
          2) assets/minecraft/textures/items/*.png
          3) assets/minecraft/textures/item/*.png
 - BLOCKS: assets/minecraft/textures/block/*.png (и старая 'blocks/')

Фичи:
 - --kinds items|blocks|all
 - --face <substring>  (например: side/top/bottom/front/back) — фильтр по части имени файла без учёта регистра
 - Отдельные выходные папки: items → app/static/mc/<ver>/items, blocks → app/static/mc/<ver>/blocks
 - Отдельные списки: app/static/data/vanilla-items-<ver>.json, app/static/data/vanilla-blocks-<ver>.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

# HTTP
try:
    import requests  # type: ignore
except Exception:
    requests = None

VERSION_DEFAULT = "1.20.6"
MANIFEST_URL = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"

# Пути в JAR
ITEM_PATHS: Tuple[str, ...] = (
    "assets/minecraft/textures/gui/sprites/items/",
    "assets/minecraft/textures/items/",
    "assets/minecraft/textures/item/",
)
BLOCK_PATHS: Tuple[str, ...] = (
    "assets/minecraft/textures/block/",
    "assets/minecraft/textures/blocks/",  # старая раскладка
)


# ---------- пути проекта ----------

def repo_root() -> Path:
    # panel_new/scripts -> parents[1] = panel_new
    return Path(__file__).resolve().parents[1]


def out_dir_items(version: str, out_override: Optional[str]) -> Path:
    base = Path(out_override).expanduser().resolve() if out_override else (repo_root() / "app" / "static" / "mc" / version / "items")
    return base.resolve()


def out_dir_blocks(version: str, out_override: Optional[str]) -> Path:
    # если задан --out, то стянуть "родителя" и заменить на blocks рядом
    if out_override:
        p = Path(out_override).expanduser().resolve()
        if p.name == "items":
            p = p.parent / "blocks"
        return p
    return (repo_root() / "app" / "static" / "mc" / version / "blocks").resolve()


def items_manifest_path(version: str) -> Path:
    return (repo_root() / "app" / "static" / "data" / f"vanilla-items-{version}.json").resolve()


def blocks_manifest_path(version: str) -> Path:
    return (repo_root() / "app" / "static" / "data" / f"vanilla-blocks-{version}.json").resolve()


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
    dl = (vjson.get("downloads") or {}).get("client") or {}
    url = dl.get("url")
    if not url:
        raise FileNotFoundError("В version.json отсутствует downloads.client.url")
    return url, dl.get("sha1")


# ---------- общая выборка из JAR ----------

def compute_sha1(data: bytes) -> str:
    import hashlib as _h
    h = _h.sha1()
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


def path_match(name: str, prefixes: Sequence[str]) -> bool:
    n = name.replace("\\", "/")
    return any(n.startswith(p) for p in prefixes)


def stem_of(jar_name: str) -> str:
    return Path(jar_name).stem.lower()


def filter_face(stem: str, face_substr: Optional[str]) -> bool:
    if not face_substr:
        return True
    return face_substr.lower() in stem.lower()


def extract_group(
    zf: zipfile.ZipFile,
    prefixes: Sequence[str],
    out_dir: Path,
    face: Optional[str],
    force: bool,
    debug: bool,
) -> Tuple[int, int, int, List[str]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved = skipped = missing = 0
    ids: List[str] = []

    names = zf.namelist()
    if debug:
        sample = [n for n in names if path_match(n, prefixes)]
        print(f"[debug] в группе {out_dir.name}: найдено по пути {len(sample)} (покажем до 15):")
        for s in sample[:15]:
            print("  -", s)

    for name in names:
        if not name.lower().endswith(".png"):
            continue
        if not path_match(name, prefixes):
            continue
        st = stem_of(name)
        if not filter_face(st, face):
            continue

        ids.append(st)
        dst = out_dir / f"{st}.png"
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


def write_manifest(items: Iterable[str], path: Path) -> None:
    ensure_parent(path)
    uniq = sorted(set(items))
    payload = [{"id": iid, "name": iid.replace("_", " ").title()} for iid in uniq]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- orchestration ----------

def run_extract(version: str, kinds: str, out: Optional[str], face: Optional[str], force: bool, debug: bool) -> None:
    vjson = fetch_version_json(version)
    client_url, client_sha1 = get_client_download(vjson)
    jar_path = download_client_jar(version, client_url, client_sha1)

    total_saved = total_skipped = total_missing = 0

    with zipfile.ZipFile(jar_path, "r") as zf:
        if kinds in ("items", "all"):
            s, k, m, ids = extract_group(
                zf, ITEM_PATHS, out_dir_items(version, out), face, force, debug
            )
            total_saved += s; total_skipped += k; total_missing += m
            try:
                write_manifest(ids, items_manifest_path(version))
            except Exception as e:
                print(f"[warn] не удалось записать items manifest: {e}")

        if kinds in ("blocks", "all"):
            s, k, m, ids = extract_group(
                zf, BLOCK_PATHS, out_dir_blocks(version, out), face, force, debug
            )
            total_saved += s; total_skipped += k; total_missing += m
            try:
                write_manifest(ids, blocks_manifest_path(version))
            except Exception as e:
                print(f"[warn] не удалось записать blocks manifest: {e}")

    print(
        f"[done] версия: {version} | сохранено: {total_saved} | пропущено: {total_skipped} | "
        f"не извлечено: {total_missing}"
    )
    print(f"[jar] {jar_path}")


# ---------- CLI ----------

def main():
    p = argparse.ArgumentParser(description="Извлекает иконки из Mojang client.jar")
    p.add_argument("--version", default=VERSION_DEFAULT, help="Версия Minecraft (по умолчанию 1.20.6)")
    p.add_argument("--out", help="Папка для PNG (items/blocks будут выбраны автоматически)")
    p.add_argument("--kinds", choices=["items", "blocks", "all"], default="items", help="Что извлекать")
    p.add_argument("--face", help="Фильтр по части имени (например side/top/bottom/front/back)")
    p.add_argument("--force", action="store_true", help="Переписывать существующие файлы")
    p.add_argument("--debug", action="store_true", help="Печать отладочной информации")
    args = p.parse_args()

    print(f"[cfg] version={args.version} | kinds={args.kinds} | face={args.face or '—'}")
    try:
        run_extract(args.version, args.kinds, args.out, args.face, args.force, args.debug)
    except Exception as e:
        print(f"[fatal] {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
