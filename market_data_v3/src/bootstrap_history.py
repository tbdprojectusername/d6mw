from __future__ import annotations

import datetime as dt
import os
import sqlite3
from pathlib import Path
from typing import Any
from collections import Counter

import pandas as pd

from .common import (
    DEFAULT_MANIFESTS,
    DEFAULT_STORE,
    american_to_decimal,
    atomic_write_json,
    ensure_dir,
    inside_root,
    load_registry,
    policy_for,
    sha256_file,
)


def _readonly(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db.resolve().as_posix()}?mode=ro", uri=True)


def bootstrap_fightodds(db: Path, store_dir: Path = DEFAULT_STORE,
                        manifests_dir: Path = DEFAULT_MANIFESTS,
                        start_year: int = 2015, end_year: int | None = None) -> dict[str, Any]:
    """Export raw historical moneyline ticks without inventing side identity."""
    db = db.resolve()
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    end_year = end_year or dt.datetime.now(dt.timezone.utc).year
    registry = load_registry()
    con = _readonly(db)
    outputs = []
    total = 0
    source_total = 0
    excluded_total = 0
    invalid_source_total = 0
    invalid_retained_total = 0
    duplicate_total = 0
    excluded_by_book: Counter[str] = Counter()
    try:
        for year in range(start_year, end_year + 1):
            query = """
                select e.pk as event_pk, e.slug as event_slug, e.name as event_name,
                       e.date as event_date, lower(e.promotion) as promotion,
                       f.fight_slug, f.fighter1, f.fighter2,
                       f.fighter1_slug, f.fighter2_slug, f.is_cancelled,
                       t.book, t.outcome_no, t.ts, t.odds
                  from ticks t
                  join fights f on f.fight_slug=t.fight_slug
                  join events e on e.pk=f.event_pk
                 where substr(e.date,1,4)=?
                 order by t.fight_slug, t.book, t.outcome_no, t.ts, t.odds
            """
            frame = pd.read_sql_query(query, con, params=(str(year),))
            if frame.empty:
                continue
            source_rows = len(frame)
            source_total += source_rows
            policies = frame["book"].map(lambda b: policy_for(b, registry))
            frame["capture_enabled"] = policies.map(lambda p: bool(p["capture_enabled"]))
            excluded_frame = frame[~frame["capture_enabled"]]
            excluded_by_book.update({str(k): int(v) for k, v in excluded_frame["book"].value_counts().items()})
            frame = frame[frame["capture_enabled"]].copy()
            excluded_rows = source_rows - len(frame)
            excluded_total += excluded_rows
            if frame.empty:
                continue
            frame["book_key"] = policies[frame.index].map(lambda p: p["book_key"])
            frame["book"] = policies[frame.index].map(lambda p: p["canonical_name"])
            frame["book_family"] = policies[frame.index].map(lambda p: p["family"])
            frame["venue_type"] = policies[frame.index].map(lambda p: p["venue_type"])
            frame["book_feature_eligible"] = policies[frame.index].map(lambda p: bool(p["feature_eligible"]))
            frame["book_close_eligible"] = policies[frame.index].map(lambda p: bool(p["close_eligible"]))
            frame["book_execution_eligible"] = policies[frame.index].map(lambda p: bool(p["execution_eligible"]))
            # Effective row eligibility stays closed until the canonical side
            # overlay resolves source outcome_no to a named fighter.
            frame["feature_eligible"] = False
            frame["close_eligible"] = False
            frame["execution_eligible"] = False
            frame["policy_status"] = policies[frame.index].map(lambda p: p["status"])
            frame["source"] = "fightodds_history"
            frame["source_outcome_no"] = frame["outcome_no"].astype("int64")
            frame["source_timestamp"] = frame["ts"]
            frame["available_to_model_at"] = frame["ts"]
            frame["availability_basis"] = "source_history_timestamp"
            frame["market_type"] = "moneyline"
            numeric_odds = pd.to_numeric(frame["odds"], errors="coerce")
            invalid_price = numeric_odds.isna()
            invalid_source_rows = int(invalid_price.sum())
            invalid_source_total += invalid_source_rows
            frame["price_american"] = numeric_odds.round().astype("Int64")
            frame["price_decimal"] = frame["price_american"].map(american_to_decimal)
            frame["orientation_status"] = "unresolved_source_outcome_no"
            frame["record_status"] = "orientation_pending"
            frame["quarantine_reason"] = None
            frame.loc[invalid_price, "record_status"] = "invalid_price"
            frame.loc[invalid_price, "quarantine_reason"] = "missing_or_nonfinite_price"
            for column in ("feature_eligible", "close_eligible", "execution_eligible"):
                frame.loc[invalid_price, column] = False
            # Vectorized natural identity avoids a Python callback over roughly
            # ten million rows. File-level SHA-256 remains the content-integrity
            # guarantee; this key is the deterministic row identity.
            frame["quote_key"] = (
                "fightodds_history|" + frame["fight_slug"].astype(str) + "|"
                + frame["book_key"].astype(str) + "|"
                + frame["source_outcome_no"].astype(str) + "|"
                + frame["ts"].astype(str) + "|" + frame["odds"].astype(str)
            )
            keep = [
                "quote_key", "source", "event_pk", "event_slug", "event_name",
                "event_date", "promotion", "fight_slug", "fighter1", "fighter2",
                "fighter1_slug", "fighter2_slug", "is_cancelled", "book", "book_key",
                "book_family", "venue_type", "source_outcome_no", "source_timestamp",
                "available_to_model_at", "availability_basis", "market_type",
                "price_american", "price_decimal", "orientation_status", "record_status",
                "quarantine_reason", "capture_enabled", "book_feature_eligible",
                "book_close_eligible", "book_execution_eligible", "feature_eligible", "close_eligible",
                "execution_eligible", "policy_status",
            ]
            frame = frame[keep].sort_values("quote_key")
            pre_dedup_rows = len(frame)
            frame = frame.drop_duplicates("quote_key")
            duplicate_rows = pre_dedup_rows - len(frame)
            duplicate_total += duplicate_rows
            invalid_retained_rows = int((frame["record_status"] == "invalid_price").sum())
            invalid_retained_total += invalid_retained_rows
            target = inside_root(store_dir / "history_ticks" / "source=fightodds_history" / f"event_year={year}" / "ticks.parquet")
            ensure_dir(target.parent)
            tmp = target.with_suffix(".parquet.tmp")
            frame.to_parquet(tmp, index=False, compression="zstd")
            os.replace(tmp, target)
            outputs.append({
                "year": year,
                "path": target.relative_to(store_dir.parent).as_posix(),
                "rows": int(len(frame)),
                "source_rows": int(source_rows),
                "excluded_rows": int(excluded_rows),
                "invalid_price_source_rows": invalid_source_rows,
                "invalid_price_retained_rows": invalid_retained_rows,
                "duplicate_natural_key_rows": int(duplicate_rows),
                "sha256": sha256_file(target),
            })
            total += len(frame)
    finally:
        con.close()
    manifest = {
        "contract": "FIGHTODDS-HISTORY-BOOTSTRAP-1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_db": str(db),
        "source_db_bytes": db.stat().st_size,
        "source_db_sha256": sha256_file(db),
        "book_registry_version": registry["version"],
        "start_year": start_year,
        "end_year": end_year,
        "rows": int(total),
        "source_rows": int(source_total),
        "excluded_rows": int(excluded_total),
        "invalid_price_source_rows": int(invalid_source_total),
        "invalid_price_retained_rows": int(invalid_retained_total),
        "duplicate_natural_key_rows": int(duplicate_total),
        "excluded_rows_by_book": dict(sorted(excluded_by_book.items())),
        "orientation_warning": "outcome_no is preserved but never mapped to fighterN",
        "outputs": outputs,
    }
    atomic_write_json(manifests_dir / "fightodds_history_bootstrap.json", manifest)
    return manifest
