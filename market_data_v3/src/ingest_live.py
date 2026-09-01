from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .common import (
    DEFAULT_MANIFESTS,
    DEFAULT_STORE,
    american_to_decimal,
    atomic_write_json,
    book_key,
    ensure_dir,
    inside_root,
    load_registry,
    name_key,
    policy_for,
    sha256_file,
    stable_hash,
)


QUOTE_COLUMNS = [
    "quote_key", "source", "source_file", "source_row", "observed_at",
    "source_timestamp", "available_to_model_at", "event_id", "event_name",
    "event_date", "promotion", "fight_id", "pair_id", "side_key",
    "side_position", "source_outcome_no", "book", "book_key", "book_family",
    "venue_type", "market_type", "price_decimal", "price_american",
    "max_risk", "currency", "cutoff_at", "orientation_status",
    "market_phase", "source_active",
    "record_status", "quarantine_reason", "capture_enabled",
    "book_feature_eligible", "book_close_eligible", "book_execution_eligible",
    "feature_eligible", "close_eligible", "execution_eligible",
    "policy_status", "availability_basis",
]


PATTERNS = {
    "fightodds_props": re.compile(r"^fightodds_props_\d{4}-\d{2}\.csv$", re.I),
    "fightodds": re.compile(r"^fightodds_\d{4}-\d{2}\.csv$", re.I),
    "fightodds_quarantine": re.compile(
        r"^quarantine_fightodds_\d{4}-\d{2}\.csv$", re.I
    ),
    "bfo": re.compile(r"^bfo_\d{4}-\d{2}\.csv$", re.I),
    "pinnacle": re.compile(r"^pinnacle_\d{4}-\d{2}\.csv$", re.I),
    "bfo_events": re.compile(r"^bfo_events_\d{4}-\d{2}\.csv$", re.I),
}


PROP_COLUMNS = [
    "prop_key", "source", "source_file", "source_row", "observed_at",
    "source_timestamp", "available_to_model_at", "event_id", "event_name",
    "event_date", "promotion", "fight_id", "offer_id", "outcome_id",
    "book", "book_key", "book_family", "venue_type", "type_id", "category",
    "subcategory", "description", "not_description", "offer_value", "type_value",
    "outcome_name", "outcome_fighter_slug", "is_not", "market_type",
    "market_phase", "price_american_current", "price_decimal_current",
    "price_american_open", "price_decimal_open", "price_american_best",
    "price_decimal_best", "price_american_worst", "price_decimal_worst",
    "offer_status", "disabled", "orientation_status", "record_status",
    "quarantine_reason", "capture_enabled", "book_feature_eligible",
    "book_close_eligible", "book_execution_eligible", "feature_eligible",
    "close_eligible", "execution_eligible", "policy_status", "availability_basis",
]


def _text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def _bool(value: Any) -> bool:
    return bool(value is True or str(value).strip().lower() in {"1", "true", "yes"})


def _base_policy(book: Any, registry: dict[str, Any]) -> dict[str, Any]:
    p = policy_for(book, registry)
    return {
        "book": p["canonical_name"],
        "book_key": p["book_key"],
        "book_family": p["family"],
        "venue_type": p["venue_type"],
        "capture_enabled": _bool(p["capture_enabled"]),
        "book_feature_eligible": _bool(p["feature_eligible"]),
        "book_close_eligible": _bool(p["close_eligible"]),
        "book_execution_eligible": _bool(p["execution_eligible"]),
        # Book policy is not row certification. Raw archive rows remain
        # ineligible until a point-in-time/current-state transform proves that
        # the quote is active, prematch and correctly oriented.
        "feature_eligible": False,
        "close_eligible": False,
        "execution_eligible": False,
        "policy_status": p["status"],
    }


def _quote(row: dict[str, Any]) -> dict[str, Any]:
    row["quote_key"] = stable_hash(
        row.get("source"), row.get("observed_at"), row.get("fight_id"),
        row.get("book_key"), row.get("side_key"), row.get("side_position"),
        row.get("price_american"), row.get("record_status"),
    )
    return {column: row.get(column) for column in QUOTE_COLUMNS}


def _fightodds(path: Path, registry: dict[str, Any], quarantined: bool) -> list[dict[str, Any]]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {
        "poll_time", "event_pk", "fight_slug", "event_date", "event_name",
        "promotion", "side1_key", "side2_key", "book", "dec1", "dec2",
        "amer1", "amer2",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
    output: list[dict[str, Any]] = []
    for idx, item in frame.iterrows():
        policy = _base_policy(item["book"], registry)
        if not policy["capture_enabled"]:
            continue
        sides = ((1, item["side1_key"], item["dec1"], item["amer1"]),
                 (2, item["side2_key"], item["dec2"], item["amer2"]))
        pair = "|".join(sorted((name_key(item["side1_key"]), name_key(item["side2_key"]))))
        for position, side, decimal, american in sides:
            reason = _text(item.get("quarantine_reason")) if quarantined else None
            category = _text(item.get("offer_category"))
            offer_status = (_text(item.get("offer_status")) or "").upper()
            disabled = _bool(item.get("disabled"))
            snapshot_metadata = any(
                column in frame.columns
                for column in ("offer_category", "offer_status", "disabled")
            )
            inactive = snapshot_metadata and (
                category != "A_1" or disabled or offer_status not in {"O", "OPEN"}
            )
            if inactive and not quarantined:
                reason = "nonprematch_or_inactive_fightodds_offer"
            row = {
                "source": "fightodds_live",
                "source_file": path.name,
                "source_row": int(idx) + 2,
                "observed_at": item["poll_time"],
                "source_timestamp": _text(item.get("source_offer_ts")),
                "available_to_model_at": item["poll_time"],
                "event_id": f"fightodds:{item['event_pk']}",
                "event_name": item["event_name"],
                "event_date": item["event_date"],
                "promotion": item["promotion"].lower(),
                "fight_id": item["fight_slug"],
                "pair_id": pair,
                "side_key": name_key(side),
                "side_position": position,
                "source_outcome_no": position,
                "market_type": "moneyline",
                "price_decimal": float(decimal) if decimal else american_to_decimal(american),
                "price_american": int(float(american)) if american else None,
                "max_risk": None,
                "currency": None,
                "cutoff_at": None,
                "orientation_status": "quarantined" if quarantined or inactive else "source_named",
                "record_status": "quarantined" if quarantined or inactive else "accepted",
                "quarantine_reason": reason,
                "availability_basis": "observed_poll_time",
                "market_phase": "prematch" if snapshot_metadata and not inactive else "unknown",
                "source_active": bool(snapshot_metadata and not inactive),
                **policy,
            }
            if quarantined or inactive:
                row["feature_eligible"] = False
                row["close_eligible"] = False
                row["execution_eligible"] = False
            output.append(_quote(row))
    return output


def _fightodds_props(path: Path, registry: dict[str, Any]) -> list[dict[str, Any]]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {
        "poll_time", "event_pk", "event_date", "event_name", "promotion",
        "fight_slug", "offer_id", "outcome_id", "book", "type_id", "category",
        "subcategory", "outcome_name", "american", "offer_status", "disabled",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name}: missing prop columns {sorted(missing)}")
    output = []
    for idx, item in frame.iterrows():
        policy = _base_policy(item["book"], registry)
        if not policy["capture_enabled"]:
            continue
        category = _text(item.get("category"))
        reason = None
        status = "raw_unverified"
        if category in (None, "A_1", "A_2"):
            status = "quarantined"
            reason = "unclassified_or_nonprematch_prop_category"
        if _bool(item.get("disabled")) or str(item.get("offer_status", "")).upper() not in {"O", "OPEN"}:
            status = "inactive_offer"
            reason = reason or "source_offer_inactive"
        american = int(float(item["american"])) if _text(item.get("american")) else None
        if american is None or american_to_decimal(american) is None:
            status = "invalid_price"
            reason = "missing_or_invalid_current_price"

        def optional_price(column: str) -> tuple[int | None, float | None]:
            value = _text(item.get(column))
            if value is None:
                return None, None
            parsed = int(float(value))
            return parsed, american_to_decimal(parsed)

        open_a, open_d = optional_price("american_open")
        best_a, best_d = optional_price("american_best")
        worst_a, worst_d = optional_price("american_worst")
        row = {
            "source": "fightodds_live", "source_file": path.name,
            "source_row": int(idx) + 2, "observed_at": item["poll_time"],
            "source_timestamp": _text(item.get("source_offer_ts")),
            "available_to_model_at": item["poll_time"],
            "event_id": f"fightodds:{item['event_pk']}", "event_name": item["event_name"],
            "event_date": item["event_date"], "promotion": item["promotion"].lower(),
            "fight_id": item["fight_slug"], "offer_id": item["offer_id"],
            "outcome_id": item["outcome_id"], "type_id": item["type_id"],
            "category": category, "subcategory": _text(item.get("subcategory")),
            "description": _text(item.get("description")),
            "not_description": _text(item.get("not_description")),
            "offer_value": _text(item.get("offer_value")),
            "type_value": _text(item.get("type_value")),
            "outcome_name": _text(item.get("outcome_name")),
            "outcome_fighter_slug": _text(item.get("outcome_fighter_slug")),
            "is_not": _bool(item.get("is_not")), "market_type": "prop",
            "market_phase": "prematch", "price_american_current": american,
            "price_decimal_current": american_to_decimal(american),
            "price_american_open": open_a, "price_decimal_open": open_d,
            "price_american_best": best_a, "price_decimal_best": best_d,
            "price_american_worst": worst_a, "price_decimal_worst": worst_d,
            "offer_status": _text(item.get("offer_status")),
            "disabled": _bool(item.get("disabled")),
            "orientation_status": "source_named_outcome", "record_status": status,
            "quarantine_reason": reason, "availability_basis": "observed_poll_time",
            **policy,
        }
        # Raw live props are point-in-time observations, but market identity,
        # settlement and source latency have not passed their own gates yet.
        row["feature_eligible"] = False
        row["close_eligible"] = False
        row["execution_eligible"] = False
        row["prop_key"] = stable_hash(
            row["source"], row["observed_at"], row["offer_id"], row["outcome_id"],
            row["book_key"], row["price_american_current"], row["offer_status"], row["disabled"],
        )
        output.append({column: row.get(column) for column in PROP_COLUMNS})
    return output


def _bfo_event_dates(source_dir: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted(source_dir.iterdir()):
        if not PATTERNS["bfo_events"].match(path.name):
            continue
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        for _, row in frame.iterrows():
            result[str(row["event_slug"])] = str(row["event_date"])
    return result


def _bfo(path: Path, registry: dict[str, Any], event_dates: dict[str, str]) -> list[dict[str, Any]]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"poll_time", "event_slug", "matchup_id", "side", "selection", "book", "american"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
    output = []
    for idx, item in frame.iterrows():
        if _text(item.get("row_kind")) not in (None, "moneyline"):
            continue
        policy = _base_policy(item["book"], registry)
        if not policy["capture_enabled"]:
            continue
        event_slug = str(item["event_slug"])
        row = {
            "source": "bfo_live", "source_file": path.name,
            "source_row": int(idx) + 2, "observed_at": item["poll_time"],
            "source_timestamp": None, "available_to_model_at": item["poll_time"],
            "event_id": f"bfo:{event_slug}", "event_name": _text(item.get("event_name")),
            "event_date": _text(item.get("event_date")) or event_dates.get(event_slug), "promotion": None,
            "fight_id": f"bfo:{event_slug}:{item['matchup_id']}",
            "pair_id": f"bfo:{event_slug}:{item['matchup_id']}",
            "side_key": name_key(item["selection"]),
            "side_position": int(float(item["side"])) if item["side"] else None,
            "source_outcome_no": int(float(item["side"])) if item["side"] else None,
            "market_type": "moneyline", "price_decimal": american_to_decimal(item["american"]),
            "price_american": int(float(item["american"])), "max_risk": None,
            "currency": None, "cutoff_at": None,
            "orientation_status": "source_named_unlinked", "record_status": "accepted",
            "quarantine_reason": None, "availability_basis": "observed_poll_time",
            "market_phase": "unknown", "source_active": bool("event_date" in frame.columns),
            **policy,
        }
        output.append(_quote(row))
    return output


def _pinnacle(path: Path, registry: dict[str, Any]) -> list[dict[str, Any]]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"poll_time", "matchup_id", "home", "away", "side", "american"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
    output = []
    for idx, item in frame.iterrows():
        if _text(item.get("bet_type")) not in (None, "ml"):
            continue
        if _text(item.get("period")) not in (None, "0") or _text(item.get("is_alt")) not in (None, "0"):
            continue
        side = str(item["side"]).lower()
        if side not in {"home", "away"}:
            continue
        selected = item[side]
        pair = "|".join(sorted((name_key(item["home"]), name_key(item["away"]))))
        policy = _base_policy("Pinnacle", registry)
        observed = pd.to_datetime(item["poll_time"], utc=True, errors="coerce")
        cutoff = pd.to_datetime(_text(item.get("cutoff_at")), utc=True, errors="coerce")
        after_cutoff = bool(pd.notna(observed) and pd.notna(cutoff) and observed >= cutoff)
        row = {
            "source": "pinnacle_live", "source_file": path.name,
            "source_row": int(idx) + 2, "observed_at": item["poll_time"],
            "source_timestamp": None, "available_to_model_at": item["poll_time"],
            "event_id": f"pinnacle:{_text(item.get('league_id')) or 'mma'}",
            "event_name": None, "event_date": (_text(item.get("start_time")) or "")[:10] or None,
            "promotion": None, "fight_id": f"pinnacle:{item['matchup_id']}",
            "pair_id": pair, "side_key": name_key(selected),
            "side_position": 1 if side == "home" else 2,
            "source_outcome_no": 1 if side == "home" else 2,
            "market_type": "moneyline", "price_decimal": american_to_decimal(item["american"]),
            "price_american": int(float(item["american"])),
            "max_risk": float(item["max_risk"]) if _text(item.get("max_risk")) else None,
            "currency": _text(item.get("currency_hint")), "cutoff_at": _text(item.get("cutoff_at")),
            "orientation_status": "source_named" if not after_cutoff else "quarantined",
            "record_status": "accepted" if not after_cutoff else "quarantined",
            "quarantine_reason": None if not after_cutoff else "observed_at_at_or_after_cutoff",
            "availability_basis": "observed_poll_time",
            "market_phase": "prematch" if not after_cutoff else "post_cutoff",
            "source_active": not after_cutoff,
            **policy,
        }
        output.append(_quote(row))
    return output


def discover(source_dir: Path) -> dict[str, list[Path]]:
    found = {key: [] for key in PATTERNS}
    for path in sorted(source_dir.iterdir()):
        if not path.is_file():
            continue
        for key, pattern in PATTERNS.items():
            if pattern.match(path.name):
                found[key].append(path)
                break
    return found


def _write_partitions(frame: pd.DataFrame, store_dir: Path) -> list[dict[str, Any]]:
    outputs = []
    frame = frame.copy()
    frame["observed_date"] = pd.to_datetime(frame["observed_at"], utc=True, errors="raise").dt.strftime("%Y-%m-%d")
    for (source, day), group in frame.groupby(["source", "observed_date"], sort=True):
        target = inside_root(store_dir / "live_quotes" / f"source={source}" / f"observed_date={day}" / "quotes.parquet")
        ensure_dir(target.parent)
        group = group.drop(columns=["observed_date"]).sort_values("quote_key").drop_duplicates("quote_key")
        changed = _write_if_changed(group, target)
        outputs.append({"path": target.relative_to(store_dir.parent).as_posix(), "rows": len(group), "changed": changed, "sha256": sha256_file(target)})
    return outputs


def _write_if_changed(frame: pd.DataFrame, target: Path) -> bool:
    """Avoid Git churn when a regenerated partition is semantically identical."""
    if target.exists():
        previous = pd.read_parquet(target)
        if list(previous.columns) == list(frame.columns):
            try:
                pd.testing.assert_frame_equal(
                    previous.reset_index(drop=True), frame.reset_index(drop=True),
                    check_dtype=False, check_names=True,
                )
                return False
            except AssertionError:
                pass
    tmp = target.with_suffix(".parquet.tmp")
    frame.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, target)
    return True


def canonicalize_live(source_dir: Path) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    source_dir = source_dir.resolve()
    registry = load_registry()
    found = discover(source_dir)
    event_dates = _bfo_event_dates(source_dir)
    quotes: list[dict[str, Any]] = []
    inputs = []
    for kind in ("fightodds", "fightodds_quarantine", "bfo", "pinnacle"):
        for path in found[kind]:
            inputs.append({"name": path.name, "kind": kind, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
            if kind == "fightodds":
                quotes.extend(_fightodds(path, registry, False))
            elif kind == "fightodds_quarantine":
                quotes.extend(_fightodds(path, registry, True))
            elif kind == "bfo":
                quotes.extend(_bfo(path, registry, event_dates))
            else:
                quotes.extend(_pinnacle(path, registry))
    if not quotes:
        raise RuntimeError(f"no supported quote rows found in {source_dir}")
    frame = pd.DataFrame(quotes, columns=QUOTE_COLUMNS)
    frame = frame.drop_duplicates("quote_key")
    return frame, inputs


def canonicalize_live_props(source_dir: Path) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    source_dir = source_dir.resolve()
    registry = load_registry()
    found = discover(source_dir)
    rows: list[dict[str, Any]] = []
    inputs = []
    for path in found["fightodds_props"]:
        inputs.append({"name": path.name, "kind": "fightodds_props", "bytes": path.stat().st_size, "sha256": sha256_file(path)})
        rows.extend(_fightodds_props(path, registry))
    if not rows:
        return pd.DataFrame(columns=PROP_COLUMNS), inputs
    return pd.DataFrame(rows, columns=PROP_COLUMNS).drop_duplicates("prop_key"), inputs


def ingest_live(source_dir: Path, store_dir: Path = DEFAULT_STORE,
                manifests_dir: Path = DEFAULT_MANIFESTS) -> dict[str, Any]:
    source_dir = source_dir.resolve()
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    registry = load_registry()
    frame, inputs = canonicalize_live(source_dir)
    outputs = _write_partitions(frame, store_dir)
    prop_frame, prop_inputs = canonicalize_live_props(source_dir)
    prop_outputs = []
    if not prop_frame.empty:
        dated = prop_frame.copy()
        dated["observed_date"] = pd.to_datetime(dated["observed_at"], utc=True, errors="raise").dt.strftime("%Y-%m-%d")
        for (source, day), group in dated.groupby(["source", "observed_date"], sort=True):
            target = inside_root(store_dir / "live_props" / f"source={source}" / f"observed_date={day}" / "props.parquet")
            ensure_dir(target.parent)
            group = group.drop(columns=["observed_date"]).sort_values("prop_key").drop_duplicates("prop_key")
            changed = _write_if_changed(group, target)
            prop_outputs.append({"path": target.relative_to(store_dir.parent).as_posix(), "rows": len(group), "changed": changed, "sha256": sha256_file(target)})
    manifest = {
        "contract": "MARKET-DATA-LIVE-1",
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_directory": str(source_dir),
        "source_revision": os.environ.get("CAPTURE_SOURCE_REVISION"),
        "book_registry_version": registry["version"],
        "input_files": inputs,
        "prop_input_files": prop_inputs,
        "rows": int(len(frame)),
        "accepted_rows": int((frame["record_status"] == "accepted").sum()),
        "quarantined_rows": int((frame["record_status"] == "quarantined").sum()),
        "outputs": outputs,
        "prop_rows": int(len(prop_frame)),
        "prop_outputs": prop_outputs,
    }
    atomic_write_json(manifests_dir / "live_latest.json", manifest)
    return manifest
