from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from .common import DEFAULT_BUILD, DEFAULT_STORE, ensure_dir, inside_root, load_registry


def _registry_frame() -> pd.DataFrame:
    registry = load_registry()
    rows = []
    for key, specific in registry["books"].items():
        row = dict(registry["default"])
        row.update(specific)
        row["book_key"] = key
        rows.append(row)
    # pandas 3 defaults text columns to StringDtype (`str`), which DuckDB 1.4
    # cannot register directly. Object dtype preserves the scalar values and
    # works identically under both pandas inference regimes.
    return pd.DataFrame(rows).astype(object)


def build_catalog(store_dir: Path = DEFAULT_STORE,
                  output: Path = DEFAULT_BUILD / "canonical.duckdb") -> Path:
    store_dir, output = inside_root(store_dir), inside_root(output)
    ensure_dir(output.parent)
    if output.exists():
        output.unlink()
    con = duckdb.connect(str(output))
    try:
        registry = _registry_frame()
        con.register("registry_frame", registry)
        con.execute("create table book_registry as select * from registry_frame")
        live = (store_dir / "live_quotes").as_posix() + "/**/*.parquet"
        history = (store_dir / "history_ticks").as_posix() + "/**/*.parquet"
        props = (store_dir / "history_props").as_posix() + "/**/*.parquet"
        live_props = (store_dir / "live_props").as_posix() + "/**/*.parquet"
        reference = (store_dir / "reference").as_posix() + "/**/*.parquet"
        snapshots = (store_dir / "snapshots").as_posix() + "/**/*.parquet"
        if list((store_dir / "live_quotes").rglob("*.parquet")):
            live_sql = live.replace("'", "''")
            con.execute(
                f"create view live_quotes as select * from read_parquet('{live_sql}', union_by_name=true, hive_partitioning=true)"
            )
            con.execute("create view clean_live_quotes as select * from live_quotes where record_status='accepted' and feature_eligible")
            con.execute("create view book_close_candidate_live_quotes as select * from live_quotes where record_status='accepted' and book_close_eligible")
            con.execute("create view close_candidate_live_quotes as select * from live_quotes where record_status='accepted' and close_eligible")
            con.execute("create view quarantined_live_quotes as select * from live_quotes where record_status='quarantined'")
        else:
            con.execute("create table live_quotes(quote_key varchar, record_status varchar, feature_eligible boolean, close_eligible boolean)")
        if list((store_dir / "history_ticks").rglob("*.parquet")):
            history_sql = history.replace("'", "''")
            con.execute(
                f"create view history_ticks_raw as select * from read_parquet('{history_sql}', union_by_name=true, hive_partitioning=true)"
            )
        else:
            con.execute("create table history_ticks_raw(quote_key varchar, orientation_status varchar)")
        if list((store_dir / "history_props").rglob("*.parquet")):
            props_sql = props.replace("'", "''")
            con.execute(
                f"create view history_props_raw as select * from read_parquet('{props_sql}', union_by_name=true, hive_partitioning=true)"
            )
        else:
            con.execute(
                "create table history_props_raw(prop_key varchar, event_year integer, "
                "fight_slug varchar, book varchar, category varchar, subcategory varchar, "
                "market_type varchar, "
                "market_phase varchar, timing_status varchar, record_status varchar, "
                "available_to_model_at timestamp, feature_eligible boolean, "
                "close_eligible boolean, execution_eligible boolean)"
            )
        if list((store_dir / "live_props").rglob("*.parquet")):
            live_props_sql = live_props.replace("'", "''")
            con.execute(
                f"create view live_props_raw as select * from read_parquet('{live_props_sql}', union_by_name=true, hive_partitioning=true)"
            )
        else:
            con.execute(
                "create table live_props_raw(prop_key varchar, source varchar, observed_at timestamp, "
                "event_date date, fight_id varchar, offer_id varchar, outcome_id varchar, "
                "book varchar, category varchar, subcategory varchar, market_phase varchar, "
                "price_decimal_current double, record_status varchar, feature_eligible boolean, "
                "close_eligible boolean, execution_eligible boolean)"
            )
        if list((store_dir / "reference").rglob("*.parquet")):
            reference_sql = reference.replace("'", "''")
            con.execute(
                f"create view reference_raw as select * from read_parquet('{reference_sql}', union_by_name=true, hive_partitioning=true)"
            )
        else:
            con.execute("create table reference_raw(record_key varchar, source varchar, dataset varchar, feature_eligible boolean)")
        if list((store_dir / "snapshots").rglob("*.parquet")):
            snapshots_sql = snapshots.replace("'", "''")
            con.execute(
                f"create view prospective_snapshots as select * from read_parquet('{snapshots_sql}', union_by_name=true, hive_partitioning=true)"
            )
        else:
            con.execute("create table prospective_snapshots(record_key varchar, source varchar, dataset varchar, snapshot_date date, feature_eligible boolean)")
        con.execute("checkpoint")
    finally:
        con.close()
    return output
