from __future__ import annotations

import argparse
import json
from pathlib import Path

from .bootstrap_history import bootstrap_fightodds
from .bootstrap_props import bootstrap_fightodds_props
from .build_catalog import build_catalog
from .common import DEFAULT_BUILD, DEFAULT_MANIFESTS, DEFAULT_REPORTS, DEFAULT_STORE
from .current_state import DEFAULT_RUNTIME, build_current_state
from .ingest_live import ingest_live
from .ingest_reference import ingest_reference
from .ingest_scorecards import ingest_recent_scorecards
from .scorecard_history import (
    bootstrap_historical_scorecards,
    direct_historical_backfill,
    reconcile_historical_scorecards,
)
from .ingest_snapshots import capture_octagon_snapshot
from .report_inventory import inventory
from .repair_live_contract import repair_live_raw_contract
from .validate_store import validate_store


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Isolated MMA canonical data pipeline")
    sub = ap.add_subparsers(dest="command", required=True)
    live = sub.add_parser("ingest-live")
    live.add_argument("--source-dir", type=Path, required=True)
    live.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    repair_live = sub.add_parser("repair-live-contract-1")
    repair_live.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    current = sub.add_parser("build-current-state")
    current.add_argument("--source-dir", type=Path, required=True)
    current.add_argument("--output-dir", type=Path, default=DEFAULT_RUNTIME)
    current.add_argument("--lookahead-days", type=int, default=90)
    current.add_argument("--max-cycle-age-minutes", type=float, default=35.0)
    history = sub.add_parser("bootstrap-fightodds")
    history.add_argument("--db", type=Path, required=True)
    history.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    history.add_argument("--start-year", type=int, default=2015)
    history.add_argument("--end-year", type=int)
    props = sub.add_parser("bootstrap-fightodds-props")
    props.add_argument("--db", type=Path, required=True)
    props.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    props.add_argument("--start-year", type=int, default=2015)
    props.add_argument("--end-year", type=int)
    reference = sub.add_parser("ingest-reference")
    reference.add_argument("--source", choices=("greco_ufcstats", "ufc_datalab"), required=True)
    reference.add_argument("--source-dir", type=Path, required=True)
    reference.add_argument("--source-revision")
    reference.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    scorecards = sub.add_parser("ingest-scorecards")
    scorecards.add_argument("--max-events", type=int, default=8)
    scorecards.add_argument("--pace-seconds", type=float, default=0.6)
    scorecards.add_argument("--refresh-known", type=int, default=12)
    scorecards.add_argument("--year", type=int)
    scorecards.add_argument("--workers", type=int, default=6)
    scorecards.add_argument("--requests-per-second", type=float, default=4.0)
    scorecards.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    scorecard_bootstrap = sub.add_parser("bootstrap-scorecards")
    scorecard_bootstrap.add_argument("--source-dir", type=Path, required=True)
    scorecard_bootstrap.add_argument("--source-revision", required=True)
    scorecard_bootstrap.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    scorecard_history = sub.add_parser("backfill-scorecards")
    scorecard_history.add_argument("--start-year", type=int, default=1995)
    scorecard_history.add_argument("--end-year", type=int, default=2024)
    scorecard_history.add_argument("--workers", type=int, default=12)
    scorecard_history.add_argument("--requests-per-second", type=float, default=8.0)
    scorecard_history.add_argument("--retries", type=int, default=2)
    scorecard_history.add_argument("--refresh-existing", action="store_true")
    scorecard_history.add_argument("--max-pages", type=int)
    scorecard_history.add_argument("--cache-only", action="store_true")
    scorecard_history.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    sub.add_parser("reconcile-scorecards")
    snapshots = sub.add_parser("capture-snapshots")
    snapshots.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    validate = sub.add_parser("validate")
    validate.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    catalog = sub.add_parser("build-catalog")
    catalog.add_argument("--store-dir", type=Path, default=DEFAULT_STORE)
    catalog.add_argument("--output", type=Path, default=DEFAULT_BUILD / "canonical.duckdb")
    sub.add_parser("inventory")
    return ap


def main() -> None:
    args = parser().parse_args()
    if args.command == "ingest-live":
        result = ingest_live(args.source_dir, args.store_dir)
    elif args.command == "repair-live-contract-1":
        result = repair_live_raw_contract(args.store_dir)
    elif args.command == "build-current-state":
        result = build_current_state(
            args.source_dir, args.output_dir, args.lookahead_days,
            args.max_cycle_age_minutes,
        )
    elif args.command == "bootstrap-fightodds":
        result = bootstrap_fightodds(args.db, args.store_dir,
                                    start_year=args.start_year, end_year=args.end_year)
    elif args.command == "bootstrap-fightodds-props":
        result = bootstrap_fightodds_props(
            args.db, args.store_dir,
            start_year=args.start_year, end_year=args.end_year,
        )
    elif args.command == "ingest-reference":
        result = ingest_reference(
            args.source,
            args.source_dir,
            args.source_revision,
            args.store_dir,
        )
    elif args.command == "ingest-scorecards":
        result = ingest_recent_scorecards(
            max_events=args.max_events,
            pace_seconds=args.pace_seconds,
            refresh_known=args.refresh_known,
            discovery_year=args.year,
            workers=args.workers,
            requests_per_second=args.requests_per_second,
            store_dir=args.store_dir,
        )
    elif args.command == "bootstrap-scorecards":
        result = bootstrap_historical_scorecards(
            args.source_dir, args.source_revision, args.store_dir
        )
    elif args.command == "backfill-scorecards":
        result = direct_historical_backfill(
            start_year=args.start_year,
            end_year=args.end_year,
            workers=args.workers,
            requests_per_second=args.requests_per_second,
            retries=args.retries,
            refresh_existing=args.refresh_existing,
            max_pages=args.max_pages,
            cache_only=args.cache_only,
            store_dir=args.store_dir,
        )
    elif args.command == "reconcile-scorecards":
        result = reconcile_historical_scorecards()
    elif args.command == "capture-snapshots":
        result = capture_octagon_snapshot(store_dir=args.store_dir)
    elif args.command == "validate":
        result = validate_store(args.store_dir)
    elif args.command == "build-catalog":
        path = build_catalog(args.store_dir, args.output)
        result = {"catalog": str(path)}
    else:
        result = inventory()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
