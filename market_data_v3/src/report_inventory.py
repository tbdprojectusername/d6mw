from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import duckdb

from .build_catalog import build_catalog
from .common import DEFAULT_BUILD, DEFAULT_REPORTS, DEFAULT_STORE, atomic_write_json, atomic_write_text, inside_root


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    names = [d[0] for d in cursor.description]
    output = []
    for row in cursor.fetchall():
        item = dict(zip(names, row))
        output.append({k: v.isoformat() if isinstance(v, (dt.date, dt.datetime)) else v for k, v in item.items()})
    return output


def inventory(store_dir: Path = DEFAULT_STORE,
              reports_dir: Path = DEFAULT_REPORTS,
              catalog: Path = DEFAULT_BUILD / "inventory.duckdb") -> dict[str, Any]:
    store_dir, reports_dir, catalog = inside_root(store_dir), inside_root(reports_dir), inside_root(catalog)
    build_catalog(store_dir, catalog)
    con = duckdb.connect(str(catalog), read_only=True)
    try:
        live_by_source = _rows(con, "select source, record_status, count(*) row_count, min(observed_at) first_observed, max(observed_at) last_observed from live_quotes group by 1,2 order by 1,2")
        live_close = _rows(con, "select book, count(*) row_count from live_quotes where close_eligible group by 1 order by row_count desc")
        history_by_year = _rows(con, "select event_year, count(*) row_count, count(distinct fight_slug) fights, count(distinct book) books from history_ticks_raw group by 1 order by 1")
        history_status = _rows(con, "select record_status, count(*) row_count from history_ticks_raw group by 1 order by 1")
        history_policy = _rows(con, "select book, count(*) row_count from history_ticks_raw where book_feature_eligible group by 1 order by row_count desc")
        props_by_year = _rows(con, "select event_year, market_type, count(*) row_count, count(distinct fight_slug) fights, count(distinct book) books from history_props_raw group by 1,2 order by 1,2")
        props_by_category = _rows(con, "select category, subcategory, market_phase, count(*) row_count from history_props_raw group by 1,2,3 order by row_count desc")
        live_props_by_source = _rows(con, "select source, record_status, count(*) row_count, min(observed_at) first_observed, max(observed_at) last_observed from live_props_raw group by 1,2 order by 1,2")
        reference_by_table = _rows(con, "select source, dataset, count(*) row_count from reference_raw group by 1,2 order by 1,2")
        snapshots_by_table = _rows(con, "select source, dataset, count(*) row_count, min(snapshot_date) first_snapshot, max(snapshot_date) last_snapshot from prospective_snapshots group by 1,2 order by 1,2")
    finally:
        con.close()
    files = list(store_dir.rglob("*.parquet"))
    report = {
        "contract": "MARKET-DATA-INVENTORY-1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "parquet_files": len(files),
        "parquet_bytes": sum(p.stat().st_size for p in files),
        "live_by_source": live_by_source,
        "live_close_eligible_by_book": live_close,
        "history_by_year": history_by_year,
        "history_record_status": history_status,
        "history_book_policy_feature_rows": history_policy,
        "historical_props_by_year": props_by_year,
        "historical_props_by_category": props_by_category,
        "live_props_by_source": live_props_by_source,
        "reference_by_table": reference_by_table,
        "snapshots_by_table": snapshots_by_table,
    }
    atomic_write_json(reports_dir / "inventory_latest.json", report)
    lines = [
        "# Market data inventory", "",
        f"Generated: {report['generated_utc']}",
        f"Parquet: {report['parquet_files']:,} files / {report['parquet_bytes'] / 1024 / 1024:.1f} MiB", "",
        "## Live capture", "", "| Source | Status | Rows | First | Last |", "|---|---|---:|---|---|",
    ]
    for row in live_by_source:
        lines.append(f"| {row['source']} | {row['record_status']} | {row['row_count']:,} | {row['first_observed']} | {row['last_observed']} |")
    lines.extend(["", "## Historical FightOdds", "", "| Year | Rows | Fights | Books |", "|---:|---:|---:|---:|"])
    for row in history_by_year:
        lines.append(f"| {row['event_year']} | {row['row_count']:,} | {row['fights']:,} | {row['books']:,} |")
    lines.extend(["", "## Historical status", "", "| Status | Rows |", "|---|---:|"])
    for row in history_status:
        lines.append(f"| {row['record_status']} | {row['row_count']:,} |")
    lines.extend(["", "## Historical FightOdds prop summaries", "", "| Year | Type | Outcome rows | Fights | Books |", "|---:|---|---:|---:|---:|"])
    for row in props_by_year:
        lines.append(f"| {row['event_year']} | {row['market_type']} | {row['row_count']:,} | {row['fights']:,} | {row['books']:,} |")
    lines.extend(["", "## Live prop capture", "", "| Source | Status | Rows | First | Last |", "|---|---|---:|---|---|"])
    for row in live_props_by_source:
        lines.append(f"| {row['source']} | {row['record_status']} | {row['row_count']:,} | {row['first_observed']} | {row['last_observed']} |")
    lines.extend(["", "## Reference sources", "", "| Source | Table | Rows |", "|---|---|---:|"])
    for row in reference_by_table:
        lines.append(f"| {row['source']} | {row['dataset']} | {row['row_count']:,} |")
    lines.extend(["", "## Prospective snapshots", "", "| Source | Table | Rows | First | Last |", "|---|---|---:|---|---|"])
    for row in snapshots_by_table:
        lines.append(f"| {row['source']} | {row['dataset']} | {row['row_count']:,} | {row['first_snapshot']} | {row['last_snapshot']} |")
    lines.extend(["", "Historical book policy is preserved separately from effective row eligibility; unresolved rows remain unusable. Raw reference and snapshot tables also remain feature-ineligible until point-in-time canonical transforms pass their own gates."])
    atomic_write_text(reports_dir / "INVENTORY_LATEST.md", "\n".join(lines) + "\n")
    return report
