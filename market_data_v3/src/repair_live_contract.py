from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .common import (
    DEFAULT_MANIFESTS,
    DEFAULT_STORE,
    atomic_write_json,
    inside_root,
    sha256_file,
    stable_hash,
)


CONTRACT = "LIVE-RAW-CERTIFICATION-1"
ROW_CERTIFICATIONS = ("feature_eligible", "close_eligible", "execution_eligible")


def _truthy(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool)


def _quote_key(row: pd.Series) -> str:
    return stable_hash(
        row.get("source"), row.get("observed_at"), row.get("fight_id"),
        row.get("book_key"), row.get("side_key"), row.get("side_position"),
        row.get("price_american"), row.get("record_status"),
    )


def repair_live_raw_contract(
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
) -> dict[str, Any]:
    """Apply the current raw-live contract to partitions created before it.

    Book-policy flags are retained. Row-level feature/close/execution
    certification is cleared, and Pinnacle observations at or after their
    explicit source cutoff are quarantined. No price, timestamp, identity or
    book-policy field is changed.
    """
    store_dir = inside_root(store_dir)
    manifests_dir = inside_root(manifests_dir)
    paths = sorted((store_dir / "live_quotes").glob("source=*/observed_date=*/quotes.parquet"))
    if not paths:
        raise RuntimeError(f"no live quote partitions found below {store_dir}")

    totals = {
        "rows": 0,
        "feature_certifications_cleared": 0,
        "close_certifications_cleared": 0,
        "execution_certifications_cleared": 0,
        "post_cutoff_rows_quarantined": 0,
        "post_cutoff_rows_total": 0,
    }
    prepared: list[tuple[Path, Path, dict[str, Any]]] = []

    for path in paths:
        frame = pd.read_parquet(path)
        before_sha = sha256_file(path)
        before_rows = len(frame)
        totals["rows"] += before_rows

        # Partitions created before the current raw-live contract do not have
        # these fields. Conservative defaults preserve their raw/uncertified
        # status; explicit post-cutoff Pinnacle rows are set below.
        if "market_phase" not in frame.columns:
            frame["market_phase"] = pd.NA
        if "source_active" not in frame.columns:
            frame["source_active"] = False

        counts: dict[str, int] = {}
        for field in ROW_CERTIFICATIONS:
            if field not in frame.columns:
                frame[field] = False
                counts[field] = 0
            else:
                counts[field] = int(_truthy(frame[field]).sum())
                frame[field] = False
            totals[f"{field.removesuffix('_eligible')}_certifications_cleared"] += counts[field]

        observed = pd.to_datetime(
            frame.get("observed_at"), utc=True, errors="coerce", format="mixed"
        )
        cutoff = pd.to_datetime(
            frame.get("cutoff_at"), utc=True, errors="coerce", format="mixed"
        )
        source = frame.get("source", pd.Series(index=frame.index, dtype="object")).astype(str)
        post_cutoff = (source == "pinnacle_live") & observed.notna() & cutoff.notna() & (observed >= cutoff)
        post_total = int(post_cutoff.sum())
        totals["post_cutoff_rows_total"] += post_total
        post_changed = 0

        if post_total:
            old_keys = frame.loc[post_cutoff, "quote_key"].copy()
            metadata_wrong = (
                frame.loc[post_cutoff, "orientation_status"].ne("quarantined")
                | frame.loc[post_cutoff, "record_status"].ne("quarantined")
                | frame.loc[post_cutoff, "quarantine_reason"].ne("observed_at_at_or_after_cutoff")
                | frame.loc[post_cutoff, "market_phase"].fillna("").ne("post_cutoff")
                | _truthy(frame.loc[post_cutoff, "source_active"])
            )
            frame.loc[post_cutoff, "orientation_status"] = "quarantined"
            frame.loc[post_cutoff, "record_status"] = "quarantined"
            frame.loc[post_cutoff, "quarantine_reason"] = "observed_at_at_or_after_cutoff"
            frame.loc[post_cutoff, "market_phase"] = "post_cutoff"
            frame.loc[post_cutoff, "source_active"] = False
            frame.loc[post_cutoff, "quote_key"] = frame.loc[post_cutoff].apply(_quote_key, axis=1)
            rekeyed = old_keys.ne(frame.loc[post_cutoff, "quote_key"])
            post_changed = int((metadata_wrong | rekeyed).sum())
            totals["post_cutoff_rows_quarantined"] += post_changed

        if frame["quote_key"].duplicated().any():
            raise RuntimeError(f"{path}: repair would create duplicate quote keys")
        if len(frame) != before_rows:
            raise RuntimeError(f"{path}: repair changed row count")

        changed = any(counts.values()) or post_changed > 0
        if not changed:
            continue
        frame = frame.sort_values("quote_key").reset_index(drop=True)
        tmp = path.with_suffix(".parquet.repair.tmp")
        frame.to_parquet(tmp, index=False, compression="zstd")
        verify = pd.read_parquet(tmp)
        if len(verify) != before_rows:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"{path}: staged repair failed row-count verification")
        if any(_truthy(verify[field]).any() for field in ROW_CERTIFICATIONS):
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"{path}: staged repair retained row certification")
        prepared.append((path, tmp, {
            "path": path.relative_to(store_dir.parent).as_posix(),
            "rows": before_rows,
            "before_sha256": before_sha,
            "feature_certifications_cleared": counts["feature_eligible"],
            "close_certifications_cleared": counts["close_eligible"],
            "execution_certifications_cleared": counts["execution_eligible"],
            "post_cutoff_rows_quarantined": post_changed,
        }))

    outputs = []
    for path, tmp, metadata in prepared:
        os.replace(tmp, path)
        outputs.append({**metadata, "after_sha256": sha256_file(path)})

    manifest = {
        "contract": CONTRACT,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "partitions_scanned": len(paths),
        "partitions_changed": len(outputs),
        **totals,
        "outputs": outputs,
    }
    # Preserve the signed-off cumulative repair summary. Ad-hoc idempotence
    # checks write only the disposable run manifest.
    atomic_write_json(manifests_dir / "live_raw_contract_repair_1_run.json", manifest)
    return manifest
