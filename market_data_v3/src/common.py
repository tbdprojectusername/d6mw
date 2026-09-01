from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
DEFAULT_STORE = ROOT / "store"
DEFAULT_MANIFESTS = ROOT / "manifests"
DEFAULT_REPORTS = ROOT / "reports"
DEFAULT_BUILD = ROOT / "build"


def inside_root(path: Path) -> Path:
    resolved = path.resolve()
    root = ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"write outside market_data_v3 refused: {resolved}")
    return resolved


def ensure_dir(path: Path) -> Path:
    path = inside_root(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_text(path: Path, text: str) -> None:
    path = inside_root(path)
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def stable_hash(*parts: Any) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def name_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(c for c in text if not unicodedata.combining(c)).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def book_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", name_key(value))


def american_to_decimal(value: Any) -> float | None:
    try:
        odds = float(value)
    except (TypeError, ValueError):
        return None
    if odds == 0:
        return None
    return 1.0 + (odds / 100.0 if odds > 0 else 100.0 / -odds)


def load_registry(path: Path | None = None) -> dict[str, Any]:
    return json.loads((path or CONFIG / "books.json").read_text(encoding="utf-8"))


def policy_for(book: Any, registry: dict[str, Any]) -> dict[str, Any]:
    key = book_key(book)
    default = dict(registry["default"])
    specific = dict(registry["books"].get(key, {}))
    default.update(specific)
    default["book_key"] = key
    default["canonical_name"] = specific.get("canonical_name", str(book).strip())
    return default

