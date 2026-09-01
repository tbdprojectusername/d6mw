from __future__ import annotations

import datetime as dt
import json
import os
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

from .common import DEFAULT_MANIFESTS, DEFAULT_STORE, atomic_write_json, ensure_dir, inside_root, sha256_file, stable_hash


RANKINGS_URL = "https://api.octagon-api.com/rankings"
FIGHTERS_URL = "https://api.octagon-api.com/fighters"
USER_AGENT = "MMA-Market-Research/1.0 (+noncommercial; daily snapshot)"


def _json(url: str) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def _ranking_rows(data: list[dict[str, Any]], observed_at: str) -> list[dict[str, Any]]:
    rows = []
    day = observed_at[:10]
    for division in data:
        name = division.get("categoryName") or "unknown"
        champion = (division.get("champion") or {}).get("championName")
        if champion:
            rows.append({"division": name, "rank": 0, "fighter_name": champion, "is_champion": True})
        for rank, fighter in enumerate(division.get("fighters") or [], 1):
            rows.append({"division": name, "rank": rank, "fighter_name": fighter.get("name"), "is_champion": False})
    for row in rows:
        row.update(
            {
                "record_key": stable_hash("octagon_rankings", day, row["division"], row["rank"], row["fighter_name"]),
                "snapshot_date": day,
                "available_to_model_at": observed_at,
                "availability_class": "prospective_snapshot_only",
                "feature_eligible": False,
            }
        )
    return rows


def _fighter_rows(data: dict[str, dict[str, Any]], observed_at: str) -> list[dict[str, Any]]:
    day = observed_at[:10]
    rows = []
    for fighter_id, values in data.items():
        row = {"fighter_id": str(fighter_id), "payload_json": json.dumps(values, sort_keys=True, separators=(",", ":"))}
        row.update(
            {
                "record_key": stable_hash("octagon_roster", day, fighter_id, row["payload_json"]),
                "snapshot_date": day,
                "available_to_model_at": observed_at,
                "availability_class": "prospective_snapshot_only",
                "feature_eligible": False,
            }
        )
        rows.append(row)
    return rows


def capture_octagon_snapshot(
    rankings: list[dict[str, Any]] | None = None,
    fighters: dict[str, dict[str, Any]] | None = None,
    observed_at: str | None = None,
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
) -> dict[str, Any]:
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    observed_at = observed_at or dt.datetime.now(dt.timezone.utc).isoformat()
    rankings = rankings if rankings is not None else _json(RANKINGS_URL)
    fighters = fighters if fighters is not None else _json(FIGHTERS_URL)
    day = observed_at[:10]
    outputs = []
    for table, rows in (("rankings", _ranking_rows(rankings, observed_at)), ("roster", _fighter_rows(fighters, observed_at))):
        if not rows:
            raise RuntimeError(f"empty Octagon {table} response")
        target = inside_root(store_dir / "snapshots" / "source=octagon_api" / f"dataset={table}" / f"snapshot_date={day}" / "data.parquet")
        if not target.exists():
            ensure_dir(target.parent)
            tmp = target.with_suffix(".parquet.tmp")
            pd.DataFrame(rows).to_parquet(tmp, index=False, compression="zstd")
            os.replace(tmp, target)
        outputs.append({"table": table, "rows": len(rows), "output": target.relative_to(store_dir.parent).as_posix(), "sha256": sha256_file(target)})
    manifest = {
        "contract": "OCTAGON-PROSPECTIVE-SNAPSHOT-1",
        "generated_utc": observed_at,
        "snapshot_date": day,
        "direct_feature_use": False,
        "outputs": outputs,
    }
    atomic_write_json(manifests_dir / "snapshot_octagon_latest.json", manifest)
    return manifest
