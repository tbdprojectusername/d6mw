from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .common import ROOT, atomic_write_json, ensure_dir, inside_root, sha256_file
from .ingest_live import canonicalize_live, canonicalize_live_props


DEFAULT_RUNTIME = ROOT / "runtime"


SNAPSHOTS = {
    "bfo": ("bfo_snapshot_latest.csv", "bfo_cycle_latest.json", "bfo_9999-12.csv"),
    "fightodds": ("fightodds_snapshot_latest.csv", "fightodds_cycle_latest.json", "fightodds_9999-12.csv"),
    "pinnacle": ("pinnacle_snapshot_latest.csv", "pinnacle_cycle_latest.json", "pinnacle_9999-12.csv"),
    "fightodds_props": (
        "fightodds_props_snapshot_latest.csv",
        "fightodds_props_cycle_latest.json",
        "fightodds_props_9999-12.csv",
    ),
}


class SnapshotHealthError(RuntimeError):
    pass


def _stage_snapshots(source_dir: Path, staging: Path, now: pd.Timestamp,
                     max_cycle_age_minutes: float) -> dict[str, Any]:
    """Validate and stage only atomic current snapshots—not append history."""
    ensure_dir(staging)
    for old in staging.iterdir():
        if old.is_file():
            old.unlink()
    health: dict[str, Any] = {}
    for source, (snapshot_name, manifest_name, staged_name) in SNAPSHOTS.items():
        snapshot_path = source_dir / snapshot_name
        manifest_path = source_dir / manifest_name
        if not snapshot_path.is_file() or not manifest_path.is_file():
            raise SnapshotHealthError(f"{source}: current snapshot or cycle manifest missing")
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        status = str(payload.get("status") or "")
        if status not in {"complete", "partial"}:
            raise SnapshotHealthError(f"{source}: unhealthy cycle status {status!r}")
        poll_time = pd.to_datetime(payload.get("poll_time"), utc=True, errors="coerce")
        if pd.isna(poll_time):
            raise SnapshotHealthError(f"{source}: invalid cycle poll_time")
        age_minutes = (now - poll_time).total_seconds() / 60.0
        if age_minutes < -5 or age_minutes > max_cycle_age_minutes:
            raise SnapshotHealthError(
                f"{source}: cycle age {age_minutes:.1f}m outside [-5,{max_cycle_age_minutes}]"
            )
        metadata = payload.get("snapshot") or {}
        if metadata.get("path") != snapshot_name:
            raise SnapshotHealthError(f"{source}: manifest snapshot path mismatch")
        if metadata.get("sha256") != sha256_file(snapshot_path):
            raise SnapshotHealthError(f"{source}: snapshot hash mismatch")
        frame = pd.read_csv(snapshot_path, dtype=str, keep_default_na=False)
        if int(metadata.get("rows", -1)) != len(frame):
            raise SnapshotHealthError(f"{source}: snapshot row-count mismatch")
        if not frame.empty:
            observed = pd.to_datetime(frame["poll_time"], utc=True, errors="coerce")
            if observed.isna().any() or not (observed == poll_time).all():
                raise SnapshotHealthError(f"{source}: rows do not belong to manifest cycle")
        frame.to_csv(staging / staged_name, index=False)
        health[source] = {
            "status": status,
            "poll_time": str(payload["poll_time"]),
            "age_minutes": round(age_minutes, 2),
            "snapshot_rows": len(frame),
            "snapshot_sha256": metadata["sha256"],
        }
    return health


def build_current_state(
    source_dir: Path,
    output_dir: Path = DEFAULT_RUNTIME,
    lookahead_days: int = 90,
    max_cycle_age_minutes: float = 35.0,
) -> dict[str, Any]:
    """Build the small, latest-observation state used by the live scorer.

    This is deliberately independent of durable Parquet compaction. It reads
    the latest capture revision, validates/canonicalizes it, and retains one
    latest quote per source/fight/book/side for current or upcoming events.
    """
    output_dir = inside_root(output_dir)
    ensure_dir(output_dir)
    staging = inside_root(output_dir / "_hot_input")
    now = pd.Timestamp.now(tz="UTC")
    source_health = _stage_snapshots(
        Path(source_dir).resolve(), staging, now, max_cycle_age_minutes
    )
    frame, inputs = canonicalize_live(staging)
    prop_frame, prop_inputs = canonicalize_live_props(staging)
    frame = frame.copy()
    frame["observed_at_parsed"] = pd.to_datetime(frame["observed_at"], utc=True, errors="coerce")
    frame["event_date_parsed"] = pd.to_datetime(frame["event_date"], utc=True, errors="coerce")
    earliest = now.normalize() - pd.Timedelta(days=1)
    latest = now.normalize() + pd.Timedelta(days=lookahead_days + 1)
    frame = frame[
        frame["observed_at_parsed"].notna()
        & frame["event_date_parsed"].between(earliest, latest, inclusive="left")
        & (frame["record_status"] == "accepted")
        & frame["source_active"].fillna(False)
    ].copy()
    frame["feature_eligible"] = (
        frame["book_feature_eligible"].fillna(False)
        & (frame["source"] != "bfo_live")
    )
    frame["execution_eligible"] = (
        frame["book_execution_eligible"].fillna(False)
        & (frame["source"] != "bfo_live")
    )
    # A current active quote is not a certified final closing quote.
    frame["close_eligible"] = False
    group_key = ["source", "fight_id", "book_key", "side_key", "market_type"]
    frame = (
        frame.sort_values([*group_key, "observed_at_parsed", "quote_key"])
        .groupby(group_key, as_index=False, dropna=False)
        .tail(1)
        .sort_values(group_key)
    )
    frame["quote_age_minutes"] = (
        (now - frame["observed_at_parsed"]).dt.total_seconds() / 60.0
    ).round(2)
    frame = frame.drop(columns=["observed_at_parsed", "event_date_parsed"])

    duplicate = int(frame.duplicated(group_key).sum())
    invalid_price = int((pd.to_numeric(frame["price_decimal"], errors="coerce") <= 1).sum())
    mise = int(frame["book_key"].isin({"miseojeu", "miseojeuplus"}).sum())
    if duplicate or invalid_price or mise:
        raise RuntimeError(
            f"current-state validation failed: duplicates={duplicate}, "
            f"invalid_price={invalid_price}, miseojeu={mise}"
        )

    target = inside_root(output_dir / "current_moneyline_quotes.parquet")
    tmp = target.with_suffix(".parquet.tmp")
    frame.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, target)

    prop_target = inside_root(output_dir / "current_prop_quotes.parquet")
    if not prop_frame.empty:
        prop_frame = prop_frame.copy()
        prop_frame["observed_at_parsed"] = pd.to_datetime(prop_frame["observed_at"], utc=True, errors="coerce")
        prop_frame["event_date_parsed"] = pd.to_datetime(prop_frame["event_date"], utc=True, errors="coerce")
        prop_frame = prop_frame[
            prop_frame["observed_at_parsed"].notna()
            & prop_frame["event_date_parsed"].between(earliest, latest, inclusive="left")
        ].copy()
        prop_group = ["source", "offer_id", "outcome_id", "book_key"]
        prop_frame = (
            prop_frame.sort_values([*prop_group, "observed_at_parsed", "prop_key"])
            .groupby(prop_group, as_index=False, dropna=False)
            .tail(1)
            .sort_values(prop_group)
        )
        prop_frame["quote_age_minutes"] = (
            (now - prop_frame["observed_at_parsed"]).dt.total_seconds() / 60.0
        ).round(2)
        prop_frame = prop_frame.drop(columns=["observed_at_parsed", "event_date_parsed"])
    prop_tmp = prop_target.with_suffix(".parquet.tmp")
    prop_frame.to_parquet(prop_tmp, index=False, compression="zstd")
    os.replace(prop_tmp, prop_target)
    accepted = int((frame["record_status"] == "accepted").sum())
    eligible = int(
        ((frame["record_status"] == "accepted") & frame["feature_eligible"]).sum()
    )
    manifest = {
        "contract": "MARKET-DATA-HOT-STATE-1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_revision": os.environ.get("CAPTURE_SOURCE_REVISION"),
        "source_directory": str(Path(source_dir).resolve()),
        "source_health": source_health,
        "input_files": inputs,
        "prop_input_files": prop_inputs,
        "rows": int(len(frame)),
        "accepted_rows": accepted,
        "feature_eligible_rows": eligible,
        "max_observed_at": None if frame.empty else str(frame["observed_at"].max()),
        "lookahead_days": lookahead_days,
        "max_cycle_age_minutes": max_cycle_age_minutes,
        "output": {
            "path": target.relative_to(ROOT).as_posix(),
            "bytes": target.stat().st_size,
            "sha256": sha256_file(target),
        },
        "prop_rows": int(len(prop_frame)),
        "prop_output": {
            "path": prop_target.relative_to(ROOT).as_posix(),
            "bytes": prop_target.stat().st_size,
            "sha256": sha256_file(prop_target),
        },
    }
    atomic_write_json(output_dir / "current_state.json", manifest)
    return manifest
