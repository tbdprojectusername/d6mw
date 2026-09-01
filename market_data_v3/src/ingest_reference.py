from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd

from .common import (
    DEFAULT_MANIFESTS,
    DEFAULT_STORE,
    atomic_write_json,
    ensure_dir,
    inside_root,
    sha256_file,
    stable_hash,
)


SOURCE_FILES: dict[str, dict[str, tuple[str, str]]] = {
    "greco_ufcstats": {
        "event_details": ("ufc_event_details.csv", ","),
        "fight_details": ("ufc_fight_details.csv", ","),
        "fight_results": ("ufc_fight_results.csv", ","),
        "fight_stats": ("ufc_fight_stats.csv", ","),
        "fighter_details": ("ufc_fighter_details.csv", ","),
        "fighter_tott": ("ufc_fighter_tott.csv", ","),
    },
    "ufc_datalab": {
        "scorecards_ocr": (
            "data/scorecards/OCR_parsed_scorecards/SCORECARDS.csv",
            ";",
        ),
        "stats_raw": ("data/stats/stats_raw.csv", ";"),
    },
}


def _column(value: Any) -> str:
    value = str(value).strip().lower().replace("%", " pct ")
    value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    return value or "unnamed"


def _unique_columns(values: list[Any]) -> list[str]:
    used: dict[str, int] = {}
    output = []
    for raw in values:
        base = _column(raw)
        used[base] = used.get(base, 0) + 1
        output.append(base if used[base] == 1 else f"{base}_{used[base]}")
    return output


def _read_source(path: Path, separator: str) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        sep=separator,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        on_bad_lines="error",
    )
    if frame.empty:
        raise ValueError(f"empty reference file: {path}")
    frame.columns = _unique_columns(list(frame.columns))
    return frame


def ingest_reference(
    source: str,
    source_dir: Path,
    source_revision: str | None = None,
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
) -> dict[str, Any]:
    """Preserve maintained public datasets as a provenance-rich raw layer.

    These tables are never model-ready merely because they were ingested.
    `feature_eligible` deliberately remains false until a separate, tested
    point-in-time transformation resolves identity and availability.
    """
    if source not in SOURCE_FILES:
        raise ValueError(f"unsupported reference source: {source}")
    source_dir = source_dir.resolve()
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    outputs = []
    for table, (relative, separator) in SOURCE_FILES[source].items():
        path = source_dir / relative
        if not path.is_file():
            raise FileNotFoundError(f"required {source} input missing: {path}")
        file_hash = sha256_file(path)
        frame = _read_source(path, separator)
        original = list(frame.columns)
        frame.insert(0, "source_row", range(2, len(frame) + 2))
        frame.insert(0, "source_table", table)
        frame.insert(0, "source_system", source)
        frame["source_file"] = relative.replace("\\", "/")
        frame["source_file_sha256"] = file_hash
        frame["availability_class"] = (
            "post_event_outcome" if "fight" in table or "scorecard" in table
            else "current_observation_or_near_immutable"
        )
        frame["feature_eligible"] = False
        content = frame[original].fillna("").astype(str).agg("\x1f".join, axis=1)
        frame.insert(
            0,
            "record_key",
            [stable_hash(source, table, row, value) for row, value in zip(frame["source_row"], content)],
        )
        target = inside_root(
            store_dir / "reference" / f"source={source}" / f"dataset={table}" / "data.parquet"
        )
        ensure_dir(target.parent)
        tmp = target.with_suffix(".parquet.tmp")
        frame.to_parquet(tmp, index=False, compression="zstd")
        os.replace(tmp, target)
        outputs.append(
            {
                "table": table,
                "source_file": relative.replace("\\", "/"),
                "source_file_bytes": path.stat().st_size,
                "source_file_sha256": file_hash,
                "rows": int(len(frame)),
                "columns": original,
                "output": target.relative_to(store_dir.parent).as_posix(),
                "output_sha256": sha256_file(target),
            }
        )
    manifest = {
        "contract": "MARKET-DATA-REFERENCE-RAW-1",
        "generated_utc": generated,
        "source": source,
        "source_revision": source_revision,
        "source_directory": str(source_dir),
        "rows": sum(x["rows"] for x in outputs),
        "direct_feature_use": False,
        "outputs": outputs,
    }
    atomic_write_json(manifests_dir / f"reference_{source}_latest.json", manifest)
    return manifest
