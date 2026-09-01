from __future__ import annotations

import datetime as dt
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from .common import (
    DEFAULT_MANIFESTS,
    DEFAULT_STORE,
    atomic_write_json,
    ensure_dir,
    inside_root,
    load_registry,
    policy_for,
    sha256_file,
)


PRICE_FIELDS = ("odds_current", "odds_open", "odds_best", "odds_worst")


def _readonly(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db.resolve().as_posix()}?mode=ro", uri=True)


def _decimal(series: pd.Series) -> pd.Series:
    odds = pd.to_numeric(series, errors="coerce")
    result = pd.Series(pd.NA, index=series.index, dtype="Float64")
    positive = odds > 0
    negative = odds < 0
    result.loc[positive] = 1.0 + odds.loc[positive] / 100.0
    result.loc[negative] = 1.0 + 100.0 / -odds.loc[negative]
    return result


def bootstrap_fightodds_props(
    db: Path,
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
    start_year: int = 2015,
    end_year: int | None = None,
) -> dict[str, Any]:
    """Export FightOdds prop endpoint summaries without fabricating tick timing.

    Each record is one offer outcome. ``odds_open/current/best/worst`` are kept
    as distinct source fields. They are endpoint summaries, not an intraday
    path, and therefore start feature/close/execution ineligible.
    """
    db = db.resolve()
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    end_year = end_year or dt.datetime.now(dt.timezone.utc).year
    registry = load_registry()
    con = _readonly(db)
    outputs: list[dict[str, Any]] = []
    total = source_total = excluded_total = invalid_total = duplicate_total = 0
    excluded_by_book: Counter[str] = Counter()
    try:
        for year in range(start_year, end_year + 1):
            query = """
                select e.pk as event_pk, e.slug as event_slug,
                       e.name as event_name, e.date as event_date,
                       lower(e.promotion) as promotion,
                       f.fight_slug, f.fighter1, f.fighter2,
                       f.fighter1_slug, f.fighter2_slug, f.is_cancelled,
                       p.offer_id, p.book, p.sb_id, p.status as offer_status,
                       p.disabled, p.ts as offer_timestamp,
                       p.created_at as offer_created_at,
                       p.value as offer_value, p.type_id, p.category,
                       p.subcategory, p.description, p.not_description,
                       p.type_value,
                       o.outcome_id, o.name as outcome_name, o.is_not,
                       o.fighter_slug as outcome_fighter_slug,
                       o.odds as odds_current, o.odds_open,
                       o.odds_best, o.odds_worst
                  from prop_offers p
                  join prop_outcomes o on o.offer_id=p.offer_id
                  join fights f on f.fight_slug=p.fight_slug
                  join events e on e.pk=p.event_pk
                 where substr(e.date,1,4)=?
                 order by p.offer_id, o.outcome_id
            """
            frame = pd.read_sql_query(query, con, params=(str(year),))
            if frame.empty:
                continue
            source_rows = len(frame)
            source_total += source_rows
            policies = frame["book"].map(lambda value: policy_for(value, registry))
            capture = policies.map(lambda p: bool(p["capture_enabled"]))
            excluded = frame.loc[~capture]
            excluded_by_book.update(
                {str(k): int(v) for k, v in excluded["book"].value_counts().items()}
            )
            frame = frame.loc[capture].copy()
            policies = policies.loc[frame.index]
            excluded_rows = source_rows - len(frame)
            excluded_total += excluded_rows
            if frame.empty:
                continue

            frame["book_key"] = policies.map(lambda p: p["book_key"])
            frame["book"] = policies.map(lambda p: p["canonical_name"])
            frame["book_family"] = policies.map(lambda p: p["family"])
            frame["venue_type"] = policies.map(lambda p: p["venue_type"])
            frame["capture_enabled"] = True
            frame["book_feature_eligible"] = policies.map(lambda p: bool(p["feature_eligible"]))
            frame["book_close_eligible"] = policies.map(lambda p: bool(p["close_eligible"]))
            frame["book_execution_eligible"] = policies.map(lambda p: bool(p["execution_eligible"]))
            frame["policy_status"] = policies.map(lambda p: p["status"])

            for column in PRICE_FIELDS:
                numeric = pd.to_numeric(frame[column], errors="coerce")
                frame[f"price_american_{column.removeprefix('odds_')}"] = numeric.round().astype("Int64")
                frame[f"price_decimal_{column.removeprefix('odds_')}"] = _decimal(numeric)

            no_prices = frame[[f"price_decimal_{name.removeprefix('odds_')}" for name in PRICE_FIELDS]].isna().all(axis=1)
            invalid_rows = int(no_prices.sum())
            invalid_total += invalid_rows

            frame["source"] = "fightodds_history"
            frame["market_type"] = "moneyline_summary"
            frame.loc[frame["category"].fillna("") != "A_1", "market_type"] = "prop"
            frame["market_phase"] = "prematch_or_unknown"
            frame.loc[frame["category"].fillna("") == "A_2", "market_phase"] = "source_live_category_excluded"
            frame["timing_status"] = "source_offer_timestamps_present_no_price_ticks"
            if year < 2020:
                frame["timing_status"] = "bulk_backfill_timestamp_unreliable"
            frame["available_to_model_at"] = None
            frame["availability_basis"] = "endpoint_summary_not_point_in_time"
            frame["record_status"] = "archived_unverified"
            frame.loc[no_prices, "record_status"] = "invalid_price"
            frame["quarantine_reason"] = None
            frame.loc[no_prices, "quarantine_reason"] = "all_price_summaries_missing_or_invalid"
            frame["feature_eligible"] = False
            frame["close_eligible"] = False
            frame["execution_eligible"] = False
            frame["prop_key"] = (
                "fightodds_history|" + frame["offer_id"].astype(str) + "|"
                + frame["outcome_id"].astype(str)
            )

            keep = [
                "prop_key", "source", "event_pk", "event_slug", "event_name",
                "event_date", "promotion", "fight_slug", "fighter1", "fighter2",
                "fighter1_slug", "fighter2_slug", "is_cancelled", "offer_id",
                "outcome_id", "book", "book_key", "book_family", "venue_type",
                "sb_id", "offer_status", "disabled", "offer_timestamp",
                "offer_created_at", "offer_value", "type_id", "category",
                "subcategory", "description", "not_description", "type_value",
                "outcome_name", "is_not", "outcome_fighter_slug", "market_type",
                "market_phase", "timing_status", "available_to_model_at",
                "availability_basis",
                "price_american_current", "price_decimal_current",
                "price_american_open", "price_decimal_open",
                "price_american_best", "price_decimal_best",
                "price_american_worst", "price_decimal_worst",
                "record_status", "quarantine_reason", "capture_enabled",
                "book_feature_eligible", "book_close_eligible",
                "book_execution_eligible", "feature_eligible", "close_eligible",
                "execution_eligible", "policy_status",
            ]
            frame = frame[keep].sort_values("prop_key")
            before = len(frame)
            frame = frame.drop_duplicates("prop_key")
            duplicates = before - len(frame)
            duplicate_total += duplicates

            target = inside_root(
                store_dir / "history_props" / "source=fightodds_history"
                / f"event_year={year}" / "props.parquet"
            )
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
                "invalid_price_rows": invalid_rows,
                "duplicate_natural_key_rows": int(duplicates),
                "sha256": sha256_file(target),
            })
            total += len(frame)
    finally:
        con.close()

    manifest = {
        "contract": "FIGHTODDS-PROP-SUMMARY-BOOTSTRAP-1",
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
        "invalid_price_rows": int(invalid_total),
        "duplicate_natural_key_rows": int(duplicate_total),
        "excluded_rows_by_book": dict(sorted(excluded_by_book.items())),
        "timing_warning": (
            "endpoint summaries contain open/current/best/worst, not price ticks; "
            "available_to_model_at is intentionally null and all effective eligibility is false"
        ),
        "outputs": outputs,
    }
    atomic_write_json(manifests_dir / "fightodds_props_bootstrap.json", manifest)
    return manifest
