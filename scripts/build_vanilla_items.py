# scripts/build_vanilla_items.py
from __future__ import annotations
import argparse, json
from pathlib import Path

def find_static_root(start: Path) -> Path:
    """
    Ищет вверх по дереву:
      - <repo>/app/static  (предпочтительно)
      - <repo>/static      (fallback)
    Возвращает путь к найденной папке static.
    """
    cur = start.resolve()
    for p in [cur, *cur.parents]:
        app_static = p / "app" / "static"
        if app_static.exists():
            return app_static
        root_static = p / "static"
        if root_static.exists():
            return root_static
    # если ничего не нашли — используем ближайший app/static
    guess = cur / "app" / "static"
    guess.mkdir(parents=True, exist_ok=True)
    return guess

def pretty_name_from_id(i: str) -> str:
    parts = [p for p in i.split("_") if p]
    name = " ".join(s.capitalize() for s in parts)
    return name.replace("Tnt", "TNT").replace("Xp", "XP")

def name_from_lang(lang: dict, item_id: str) -> str | None:
    for key in (f"item.minecraft.{item_id}", f"block.minecraft.{item_id}"):
        if key in lang and lang[key]:
            return str(lang[key])
    return None

def load_lang(path: Path | None) -> dict:
    if not path: return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def main():
    here = Path(__file__).resolve()
    static_root = find_static_root(here.parent)

    parser = argparse.ArgumentParser(description="Build vanilla-items-1.20.6.json (для Flask /static)")
    parser.add_argument("--icons", type=Path, default=static_root / "mc" / "1.20.6" / "items",
                        help="Папка с png-иконками предметов")
    parser.add_argument("--out",   type=Path, default=static_root / "data" / "vanilla-items-1.20.6.json",
                        help="Куда сохранить json-список предметов")
    parser.add_argument("--lang",  type=Path, default=None,
                        help="assets/minecraft/lang/en_us.json (опционально)")
    args = parser.parse_args()

    icons_dir: Path = args.icons
    out_path: Path   = args.out
    lang = load_lang(args.lang)

    if not icons_dir.exists():
        raise SystemExit(f"Иконки не найдены: {icons_dir.resolve()}")

    items = []
    for png in sorted(icons_dir.glob("*.png")):
        item_id = png.stem.lower()
        name = name_from_lang(lang, item_id) or pretty_name_from_id(item_id)
        items.append({"id": item_id, "name": name})

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✔ Сохранено: {out_path} ({len(items)} предметов)")
    print(f"Источник иконок: {icons_dir.resolve()}")
    print(f"STATIC root: {static_root.resolve()}")

if __name__ == "__main__":
    main()
