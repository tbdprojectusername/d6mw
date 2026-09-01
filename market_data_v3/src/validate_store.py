from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import duckdb

from .build_catalog import build_catalog
from .common import DEFAULT_BUILD, DEFAULT_MANIFESTS, DEFAULT_REPORTS, DEFAULT_STORE, atomic_write_json, atomic_write_text, inside_root


class ValidationFailed(RuntimeError):
    pass


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def validate_store(store_dir: Path = DEFAULT_STORE,
                   reports_dir: Path = DEFAULT_REPORTS,
                   catalog: Path = DEFAULT_BUILD / "validation.duckdb") -> dict[str, Any]:
    store_dir, reports_dir, catalog = inside_root(store_dir), inside_root(reports_dir), inside_root(catalog)
    build_catalog(store_dir, catalog)
    con = duckdb.connect(str(catalog), read_only=True)
    checks = []
    try:
        total = _one(con, "select count(*) from live_quotes")
        checks.append(("live_rows_positive", total > 0, total))
        if total:
            duplicate = _one(con, "select count(*)-count(distinct quote_key) from live_quotes")
            checks.append(("quote_keys_unique", duplicate == 0, duplicate))
            invalid_price = _one(con, "select count(*) from live_quotes where price_decimal is null or price_decimal <= 1")
            checks.append(("prices_valid", invalid_price == 0, invalid_price))
            missing_time = _one(con, "select count(*) from live_quotes where observed_at is null or available_to_model_at is null")
            checks.append(("decision_time_present", missing_time == 0, missing_time))
            mise = _one(con, "select count(*) from live_quotes where lower(replace(book,'-','')) in ('miseojeu','miseojeu+')")
            checks.append(("miseojeu_absent", mise == 0, mise))
            bad_quarantine = _one(con, "select count(*) from live_quotes where record_status='quarantined' and (feature_eligible or close_eligible or execution_eligible)")
            checks.append(("quarantine_ineligible", bad_quarantine == 0, bad_quarantine))
            exchange_close = _one(con, "select count(*) from live_quotes where venue_type='exchange' and close_eligible")
            checks.append(("exchange_never_close", exchange_close == 0, exchange_close))
            unresolved_close = _one(con, "select count(*) from live_quotes where close_eligible and orientation_status not in ('source_named','canonical_verified')")
            checks.append(("close_orientation_named", unresolved_close == 0, unresolved_close))
            for field in ("feature_eligible", "close_eligible", "execution_eligible"):
                uncertified = _one(con, f"select count(*) from live_quotes where {field}")
                checks.append((
                    f"raw_live_rows_never_{field.removesuffix('_eligible')}_certified",
                    uncertified == 0,
                    uncertified,
                ))
            after_cutoff_eligible = _one(
                con,
                "select count(*) from live_quotes where try_cast(cutoff_at as timestamptz) is not null "
                "and try_cast(observed_at as timestamptz) >= try_cast(cutoff_at as timestamptz) "
                "and (record_status='accepted' or feature_eligible or close_eligible or execution_eligible)",
            )
            checks.append(("post_cutoff_rows_quarantined", after_cutoff_eligible == 0, after_cutoff_eligible))
        history = _one(con, "select count(*) from history_ticks_raw")
        if history:
            invented = _one(con, "select count(*) from history_ticks_raw where orientation_status <> 'unresolved_source_outcome_no'")
            checks.append(("history_orientation_not_invented", invented == 0, invented))
            prematurely_eligible = _one(
                con,
                "select count(*) from history_ticks_raw where "
                "orientation_status='unresolved_source_outcome_no' and "
                "(feature_eligible or close_eligible or execution_eligible)",
            )
            checks.append(("unresolved_history_ineligible", prematurely_eligible == 0, prematurely_eligible))
        props = _one(con, "select count(*) from history_props_raw")
        if props:
            prop_duplicates = _one(con, "select count(*)-count(distinct prop_key) from history_props_raw")
            checks.append(("prop_keys_unique", prop_duplicates == 0, prop_duplicates))
            prop_leak = _one(
                con,
                "select count(*) from history_props_raw where feature_eligible or close_eligible or execution_eligible",
            )
            checks.append(("prop_summaries_not_point_in_time", prop_leak == 0, prop_leak))
            invented_availability = _one(
                con,
                "select count(*) from history_props_raw where available_to_model_at is not null",
            )
            checks.append(("prop_availability_not_invented", invented_availability == 0, invented_availability))
            live_category_eligible = _one(
                con,
                "select count(*) from history_props_raw where market_phase='source_live_category_excluded' "
                "and (feature_eligible or close_eligible or execution_eligible)",
            )
            checks.append(("source_live_props_ineligible", live_category_eligible == 0, live_category_eligible))
        live_props = _one(con, "select count(*) from live_props_raw")
        if live_props:
            live_prop_duplicates = _one(con, "select count(*)-count(distinct prop_key) from live_props_raw")
            checks.append(("live_prop_keys_unique", live_prop_duplicates == 0, live_prop_duplicates))
            live_prop_price = _one(con, "select count(*) from live_props_raw where price_decimal_current is null or price_decimal_current <= 1")
            checks.append(("live_prop_current_prices_valid", live_prop_price == 0, live_prop_price))
            premature_prop_use = _one(con, "select count(*) from live_props_raw where feature_eligible or close_eligible or execution_eligible")
            checks.append(("live_props_await_identity_settlement_gates", premature_prop_use == 0, premature_prop_use))
        reference = _one(con, "select count(*) from reference_raw")
        if reference:
            duplicate_reference = _one(con, "select count(*)-count(distinct record_key) from reference_raw")
            checks.append(("reference_keys_unique", duplicate_reference == 0, duplicate_reference))
            direct_reference = _one(con, "select count(*) from reference_raw where feature_eligible")
            checks.append(("raw_reference_not_direct_feature", direct_reference == 0, direct_reference))
            bad_scores = _one(
                con,
                "select count(*) from reference_raw where source='mmadecisions' and record_status='accepted' "
                "and (try_cast(round as integer) not between 1 and 5 "
                "or try_cast(side1_score as integer) not between 7 and 10 "
                "or try_cast(side2_score as integer) not between 7 and 10)",
            )
            checks.append(("scorecard_values_valid", bad_scores == 0, bad_scores))
            bad_bootstrap_scores = _one(
                con,
                "select count(*) from reference_raw where source='mmadecisions_snapshot' "
                "and dataset='ufc_judge_rounds' and "
                "(try_cast(round as integer) not between 1 and 5 "
                "or try_cast(side1_score as integer) not between 7 and 10 "
                "or try_cast(side2_score as integer) not between 7 and 10 "
                "or try_cast(side1_score_no_deduction as integer) not between 7 and 10 "
                "or try_cast(side2_score_no_deduction as integer) not between 7 and 10)",
            )
            checks.append(("bootstrap_scorecard_values_valid", bad_bootstrap_scores == 0, bad_bootstrap_scores))
            unreconciled_totals = _one(
                con,
                "with cards as (select decision_id, judge_slot, "
                "sum(try_cast(side1_score as integer)) score1, max(try_cast(side1_total as integer)) total1, "
                "sum(try_cast(side2_score as integer)) score2, max(try_cast(side2_total as integer)) total2 "
                "from reference_raw where source='mmadecisions' and record_status='accepted' "
                "group by 1,2) select count(*) from cards where score1<>total1 or score2<>total2",
            )
            checks.append(("direct_scorecard_totals_reconcile", unreconciled_totals == 0, unreconciled_totals))
            bad_reconciliation_use = _one(
                con,
                "select count(*) from reference_raw where source='mmadecisions_reconciliation' "
                "and record_status='quarantined' and feature_eligible",
            )
            checks.append(("scorecard_conflicts_ineligible", bad_reconciliation_use == 0, bad_reconciliation_use))
        snapshots = _one(con, "select count(*) from prospective_snapshots")
        if snapshots:
            duplicate_snapshots = _one(con, "select count(*)-count(distinct record_key) from prospective_snapshots")
            checks.append(("snapshot_keys_unique", duplicate_snapshots == 0, duplicate_snapshots))
            bad_snapshot_use = _one(con, "select count(*) from prospective_snapshots where feature_eligible")
            checks.append(("raw_snapshots_not_direct_feature", bad_snapshot_use == 0, bad_snapshot_use))
    finally:
        con.close()
    failed = [name for name, ok, _ in checks if not ok]
    report = {
        "contract": "MARKET-DATA-VALIDATION-1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "live_rows": total,
        "history_rows": history,
        "historical_prop_rows": props,
        "live_prop_rows": live_props,
        "reference_rows": reference,
        "snapshot_rows": snapshots,
        "checks": [{"name": n, "passed": ok, "value": value} for n, ok, value in checks],
        "failures": failed,
    }
    atomic_write_json(reports_dir / "health_latest.json", report)
    lines = ["# Market data health", "", f"Status: **{report['status']}**", "",
             f"Live rows: {total:,}", f"Historical rows: {history:,}",
             f"Historical prop outcomes: {props:,}",
             f"Live prop observations: {live_props:,}",
             f"Reference rows: {reference:,}", f"Snapshot rows: {snapshots:,}",
             "", "| Check | Result | Value |", "|---|---:|---:|"]
    lines.extend(f"| {n} | {'PASS' if ok else 'FAIL'} | {value} |" for n, ok, value in checks)
    atomic_write_text(reports_dir / "HEALTH_LATEST.md", "\n".join(lines) + "\n")
    if failed:
        raise ValidationFailed(", ".join(failed))
    return report
