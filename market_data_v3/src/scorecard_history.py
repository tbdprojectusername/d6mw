from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from lxml import html as lxml_html

from .common import (
    DEFAULT_BUILD,
    DEFAULT_MANIFESTS,
    DEFAULT_REPORTS,
    DEFAULT_STORE,
    atomic_write_json,
    atomic_write_text,
    ensure_dir,
    inside_root,
    name_key,
    sha256_file,
    stable_hash,
)
from .ingest_scorecards import BASE, USER_AGENT, _get, parse_decision_page


BOOTSTRAP_CONTRACT = "MMADECISIONS-UFC-BOOTSTRAP-1"
BACKFILL_CONTRACT = "MMADECISIONS-DIRECT-HISTORY-1"
RECONCILIATION_CONTRACT = "MMADECISIONS-RECONCILIATION-1"


def _read_rds(path: Path) -> pd.DataFrame:
    try:
        import pyreadr
    except ImportError as exc:  # pragma: no cover - exercised by CLI environment
        raise RuntimeError("pyreadr is required only for the one-time RDS bootstrap") from exc
    result = pyreadr.read_r(str(path))
    if None in result:
        return result[None]
    if len(result) != 1:
        raise ValueError(f"expected one frame in {path}, found {len(result)}")
    return next(iter(result.values()))


def _rds(source_dir: Path, relative: str) -> pd.DataFrame:
    path = source_dir / relative
    if not path.exists():
        raise FileNotFoundError(path)
    return _read_rds(path)


def _atomic_parquet(frame: pd.DataFrame, target: Path) -> None:
    target = inside_root(target)
    ensure_dir(target.parent)
    tmp = target.with_suffix(target.suffix + ".tmp")
    frame.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, target)


def _as_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").astype("int64")


def _snapshot_frames(source_dir: Path) -> dict[str, pd.DataFrame]:
    mma = "data/MMA Decisions"
    ufc = "data/UFC Stats"
    return {
        "merged": _rds(source_dir, "data/final_merged.rds"),
        "bouts": _rds(source_dir, f"{mma}/mmadecisions_bouts.rds"),
        "scores": _rds(source_dir, f"{mma}/mmadecisions_bouts_scores.rds"),
        "events": _rds(source_dir, f"{mma}/mmadecisions_events.rds"),
        "fighters": _rds(source_dir, f"{mma}/mmadecisions_fighters.rds"),
        "judges": _rds(source_dir, f"{mma}/mmadecisions_judges.rds"),
        "urls": _rds(source_dir, f"{mma}/checkpoints/bout_urls.rds"),
        "ufc_events": _rds(source_dir, f"{ufc}/ufcstats_events.rds"),
        "ufc_fighters": _rds(source_dir, f"{ufc}/ufcstats_fighters.rds"),
    }


def _build_snapshot_tables(
    frames: dict[str, pd.DataFrame], source_revision: str, generated: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    adjusted = frames["merged"].copy()
    required = {
        "ufcstats_bout_id", "mmadecisions_bout_id", "ufcstats_event_id",
        "red_ufcstats_fighter_id", "blue_ufcstats_fighter_id", "round",
        "judge_num", "judge_id", "red_score", "blue_score",
    }
    missing = required - set(adjusted.columns)
    if missing:
        raise ValueError(f"snapshot merged table missing {sorted(missing)}")
    adjusted = adjusted.dropna(subset=["red_score", "blue_score"]).copy()
    for column in ("mmadecisions_bout_id", "round", "judge_num", "judge_id", "red_score", "blue_score"):
        adjusted[column] = _as_int(adjusted[column])
    invalid = adjusted[
        ~adjusted["round"].between(1, 5)
        | ~adjusted["red_score"].between(7, 10)
        | ~adjusted["blue_score"].between(7, 10)
    ]
    if not invalid.empty:
        raise ValueError(f"snapshot contains {len(invalid)} invalid judge-round scores")
    key_columns = ["mmadecisions_bout_id", "judge_num", "round"]
    if adjusted.duplicated(key_columns).any():
        raise ValueError("snapshot has duplicate decision/judge/round rows")

    # final_merged deliberately adds point deductions back to estimate what
    # the judge scored before the referee's penalty. The canonical scorecard
    # must preserve the raw official score instead; both representations are
    # retained under unambiguous names.
    crosswalk_columns = [
        "mmadecisions_bout_id", "ufcstats_bout_id", "ufcstats_event_id",
        "red_ufcstats_fighter_id", "blue_ufcstats_fighter_id",
        "red_mmadecisions_fighter_id", "blue_mmadecisions_fighter_id",
    ]
    crosswalk = adjusted[crosswalk_columns].drop_duplicates("mmadecisions_bout_id")
    raw = frames["scores"].copy()
    raw_required = {"id", "round", "fighter_id", "judge_num", "judge_id", "score"}
    raw_missing = raw_required - set(raw.columns)
    if raw_missing:
        raise ValueError(f"snapshot raw score table missing {sorted(raw_missing)}")
    for column in raw_required:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")
    raw = raw.dropna(subset=list(raw_required)).copy()
    for column in raw_required:
        raw[column] = raw[column].astype("int64")
    raw = raw[raw["score"].between(7, 10)].copy()
    raw = raw.merge(
        crosswalk,
        left_on="id",
        right_on="mmadecisions_bout_id",
        how="inner",
        validate="many_to_one",
    )
    raw["corner"] = None
    raw.loc[raw["fighter_id"] == raw["red_mmadecisions_fighter_id"], "corner"] = "red"
    raw.loc[raw["fighter_id"] == raw["blue_mmadecisions_fighter_id"], "corner"] = "blue"
    raw = raw.dropna(subset=["corner"])
    paired = raw.pivot(
        index=["mmadecisions_bout_id", "round", "judge_num", "judge_id"],
        columns="corner",
        values="score",
    ).reset_index()
    if "red" not in paired or "blue" not in paired:
        raise ValueError("raw score table cannot be oriented to both UFCStats corners")
    paired = paired.dropna(subset=["red", "blue"]).rename(
        columns={"red": "red_score_official", "blue": "blue_score_official"}
    )
    adjusted_scores = adjusted[
        key_columns + ["red_score", "blue_score"]
    ].rename(
        columns={
            "red_score": "red_score_no_deduction",
            "blue_score": "blue_score_no_deduction",
        }
    )
    merged = paired.merge(
        adjusted_scores,
        on=key_columns,
        how="inner",
        validate="one_to_one",
    ).merge(
        crosswalk,
        on="mmadecisions_bout_id",
        how="left",
        validate="many_to_one",
    )
    if merged.empty:
        raise ValueError("no raw official scores survived UFCStats orientation")

    ufc_events = frames["ufc_events"][["id", "name", "date"]].rename(
        columns={"id": "ufcstats_event_id", "name": "event_name", "date": "event_date"}
    )
    ufc_fighters = frames["ufc_fighters"][["id", "name"]]
    red_names = ufc_fighters.rename(
        columns={"id": "red_ufcstats_fighter_id", "name": "red_name"}
    )
    blue_names = ufc_fighters.rename(
        columns={"id": "blue_ufcstats_fighter_id", "name": "blue_name"}
    )
    judges = frames["judges"][["id", "name"]].copy()
    judges["id"] = _as_int(judges["id"])
    judges = judges.rename(columns={"id": "judge_id", "name": "judge_name"})
    merged = (
        merged.merge(ufc_events, on="ufcstats_event_id", how="left", validate="many_to_one")
        .merge(red_names, on="red_ufcstats_fighter_id", how="left", validate="many_to_one")
        .merge(blue_names, on="blue_ufcstats_fighter_id", how="left", validate="many_to_one")
        .merge(judges, on="judge_id", how="left", validate="many_to_one")
    )
    if merged[["event_name", "event_date", "red_name", "blue_name", "judge_name"]].isna().any().any():
        raise ValueError("snapshot crosswalk is missing event, fighter, or judge identity")

    urls = frames["urls"].iloc[:, 0].astype(str)
    url_index = pd.DataFrame({"relative_url": urls})
    url_index["decision_id"] = _as_int(
        url_index["relative_url"].str.extract(r"decision/(\d+)/", expand=False)
    )
    url_index["decision_url"] = url_index["relative_url"].map(
        lambda value: urllib.parse.urljoin(BASE, value)
    )
    url_index = url_index[["decision_id", "decision_url"]].drop_duplicates("decision_id")
    merged = merged.merge(
        url_index,
        left_on="mmadecisions_bout_id",
        right_on="decision_id",
        how="left",
        validate="many_to_one",
    )
    if merged["decision_url"].isna().any():
        raise ValueError("snapshot score row lacks a decision URL")

    totals = merged.groupby(["mmadecisions_bout_id", "judge_num"], as_index=False).agg(
        side1_total=("red_score_official", "sum"),
        side2_total=("blue_score_official", "sum"),
    )
    merged = merged.merge(
        totals, on=["mmadecisions_bout_id", "judge_num"], how="left", validate="many_to_one"
    )
    score_rows = pd.DataFrame(
        {
            "record_key": [
                stable_hash("mmadecisions_snapshot", bout, judge, round_no)
                for bout, judge, round_no in zip(
                    merged["mmadecisions_bout_id"], merged["judge_num"], merged["round"]
                )
            ],
            "source_system": "mmadecisions_snapshot",
            "source_authority": "community_snapshot_of_mmadecisions_and_ufcstats",
            "decision_id": merged["mmadecisions_bout_id"],
            "decision_url": merged["decision_url"],
            "ufcstats_bout_id": merged["ufcstats_bout_id"],
            "ufcstats_event_id": merged["ufcstats_event_id"],
            "event_name": merged["event_name"],
            "event_date": merged["event_date"].astype(str),
            "result_text": None,
            "judge_name": merged["judge_name"],
            "judge_slot": merged["judge_num"],
            "round": merged["round"],
            "side1_label": merged["red_name"],
            "side2_label": merged["blue_name"],
            "side1_score": merged["red_score_official"],
            "side2_score": merged["blue_score_official"],
            "side1_score_no_deduction": merged["red_score_no_deduction"],
            "side2_score_no_deduction": merged["blue_score_no_deduction"],
            "point_deduction_adjusted": (
                (merged["red_score_official"] != merged["red_score_no_deduction"])
                | (merged["blue_score_official"] != merged["blue_score_no_deduction"])
            ),
            "side1_total": merged["side1_total"],
            "side2_total": merged["side2_total"],
            "orientation_status": "ufcstats_red_blue_crosswalk",
            "record_status": "bootstrap_reference",
            "quarantine_reason": None,
            "first_observed_at": generated,
            "feature_eligible": False,
            "availability_class": "post_event_outcome",
            "snapshot_revision": source_revision,
            "provenance_status": "bootstrap_unverified",
        }
    )
    score_rows = score_rows.sort_values(
        ["event_date", "decision_id", "judge_slot", "round"]
    ).reset_index(drop=True)

    bouts = frames["bouts"].copy()
    events = frames["events"][["id", "name", "date"]].copy()
    fighters = frames["fighters"][["id", "name"]].copy()
    for frame, columns in ((bouts, ["id", "event_id", "fighter1_id", "fighter2_id"]),
                           (events, ["id"]), (fighters, ["id"])):
        for column in columns:
            frame[column] = _as_int(frame[column])
    events = events.rename(columns={"id": "event_id", "name": "event_name", "date": "event_date"})
    first = fighters.rename(columns={"id": "fighter1_id", "name": "fighter1_name"})
    second = fighters.rename(columns={"id": "fighter2_id", "name": "fighter2_name"})
    index = (
        bouts.merge(events, on="event_id", how="left", validate="many_to_one")
        .merge(first, on="fighter1_id", how="left", validate="many_to_one")
        .merge(second, on="fighter2_id", how="left", validate="many_to_one")
        .merge(url_index, left_on="id", right_on="decision_id", how="left", validate="one_to_one")
    )
    crosswalk = adjusted[
        ["mmadecisions_bout_id", "ufcstats_bout_id", "ufcstats_event_id"]
    ].drop_duplicates("mmadecisions_bout_id")
    index = index.merge(
        crosswalk, left_on="id", right_on="mmadecisions_bout_id", how="left", validate="one_to_one"
    )
    index = index[
        index["event_name"].astype(str).str.startswith("UFC")
        | index["ufcstats_bout_id"].notna()
    ].copy()
    index_rows = pd.DataFrame(
        {
            "record_key": [stable_hash("mmadecisions_snapshot_index", value) for value in index["id"]],
            "source_system": "mmadecisions_snapshot",
            "source_authority": "community_snapshot_index",
            "decision_id": index["id"],
            "decision_url": index["decision_url"],
            "ufcstats_bout_id": index["ufcstats_bout_id"],
            "ufcstats_event_id": index["ufcstats_event_id"],
            "event_name": index["event_name"],
            "event_date": index["event_date"].astype(str),
            "side1_label": index["fighter1_name"],
            "side2_label": index["fighter2_name"],
            "record_status": "bootstrap_reference",
            "first_observed_at": generated,
            "feature_eligible": False,
            "availability_class": "post_event_outcome",
            "snapshot_revision": source_revision,
            "provenance_status": "bootstrap_unverified",
        }
    ).dropna(subset=["decision_url"])
    index_rows = index_rows.sort_values(["event_date", "decision_id"]).reset_index(drop=True)
    if index_rows.duplicated("decision_id").any():
        raise ValueError("snapshot decision index has duplicate IDs")
    return score_rows, index_rows


def bootstrap_historical_scorecards(
    source_dir: Path,
    source_revision: str,
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
) -> dict[str, Any]:
    source_dir = source_dir.resolve()
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    scores, index = _build_snapshot_tables(_snapshot_frames(source_dir), source_revision, generated)
    score_target = inside_root(
        store_dir / "reference" / "source=mmadecisions_snapshot" / "dataset=ufc_judge_rounds" / "data.parquet"
    )
    index_target = inside_root(
        store_dir / "reference" / "source=mmadecisions_snapshot" / "dataset=ufc_decision_index" / "data.parquet"
    )
    _atomic_parquet(scores, score_target)
    _atomic_parquet(index, index_target)
    manifest = {
        "contract": BOOTSTRAP_CONTRACT,
        "generated_utc": generated,
        "source_revision": source_revision,
        "source_repository": "https://github.com/ehan03/UFC-Judging-Analysis",
        "license_status": "no_license_file_observed_do_not_redistribute_upstream_raw",
        "judge_round_rows": int(len(scores)),
        "scorecard_fights": int(scores["decision_id"].nunique()),
        "indexed_ufc_decisions": int(index["decision_id"].nunique()),
        "event_date_min": str(index["event_date"].min()),
        "event_date_max": str(index["event_date"].max()),
        "direct_feature_use": False,
        "outputs": [
            {"path": score_target.relative_to(store_dir.parent).as_posix(), "sha256": sha256_file(score_target)},
            {"path": index_target.relative_to(store_dir.parent).as_posix(), "sha256": sha256_file(index_target)},
        ],
    }
    atomic_write_json(manifests_dir / "scorecards_mmadecisions_bootstrap.json", manifest)
    return manifest


class _StartRateLimiter:
    def __init__(self, requests_per_second: float):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.interval = 1.0 / requests_per_second
        self.lock = threading.Lock()
        self.next_start = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_start - now)
            self.next_start = max(now, self.next_start) + self.interval
        if delay:
            time.sleep(delay)


def _cache_path(cache_dir: Path, decision_id: int, digest: str) -> Path:
    return cache_dir / f"decision_id={decision_id}" / f"{digest}.html.gz"


def _write_cache(cache_dir: Path, decision_id: int, document: bytes, digest: str) -> None:
    target = _cache_path(cache_dir, decision_id, digest)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        return
    tmp = target.with_suffix(target.suffix + ".tmp")
    with gzip.open(tmp, "wb", compresslevel=6) as handle:
        handle.write(document)
    os.replace(tmp, target)


def _fetch_parse(
    row: dict[str, Any], limiter: _StartRateLimiter, cache_dir: Path,
    scraped_at: str, retries: int, use_cache: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    decision_id = int(row["decision_id"])
    url = str(row["decision_url"])
    error = None
    digest = None
    cached = sorted((cache_dir / f"decision_id={decision_id}").glob("*.html.gz"))
    if use_cache and cached:
        try:
            with gzip.open(cached[-1], "rb") as handle:
                document = handle.read()
            digest = hashlib.sha256(document).hexdigest()
            parsed = parse_decision_page(url, document, scraped_at)
            for item in parsed:
                item["source_page_sha256"] = digest
            return parsed, {
                "decision_id": decision_id,
                "decision_url": url,
                "event_date": row.get("event_date"),
                "status": "parsed_cached",
                "sha256": digest,
                "rows": len(parsed),
                "attempts": 0,
                "error": None,
            }
        except Exception:
            # A partial/corrupt cache entry is not authoritative. Fall through
            # to a fresh request and overwrite only via a new content hash.
            pass
    for attempt in range(retries + 1):
        try:
            limiter.wait()
            document = _get(url)
            digest = hashlib.sha256(document).hexdigest()
            _write_cache(cache_dir, decision_id, document, digest)
            parsed = parse_decision_page(url, document, scraped_at)
            for item in parsed:
                item["source_page_sha256"] = digest
            return parsed, {
                "decision_id": decision_id,
                "decision_url": url,
                "event_date": row.get("event_date"),
                "status": "parsed",
                "sha256": digest,
                "rows": len(parsed),
                "attempts": attempt + 1,
                "error": None,
            }
        except Exception as exc:  # source/network/parser failures are ledgered
            error = f"{type(exc).__name__}: {exc}"
            if attempt < retries:
                time.sleep(min(8.0, 0.75 * (2 ** attempt)))
    return [], {
        "decision_id": decision_id,
        "decision_url": url,
        "event_date": row.get("event_date"),
        "status": "failed",
        "sha256": digest,
        "rows": 0,
        "attempts": retries + 1,
        "error": error,
    }


def direct_historical_backfill(
    start_year: int = 1995,
    end_year: int = 2024,
    workers: int = 12,
    requests_per_second: float = 8.0,
    retries: int = 2,
    refresh_existing: bool = False,
    max_pages: int | None = None,
    cache_only: bool = False,
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
    reports_dir: Path = DEFAULT_REPORTS,
    cache_dir: Path = DEFAULT_BUILD / "mmadecisions_raw",
) -> dict[str, Any]:
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    reports_dir, cache_dir = inside_root(reports_dir), inside_root(cache_dir)
    if workers < 1 or workers > 24:
        raise ValueError("workers must be between 1 and 24")
    index_path = inside_root(
        store_dir / "reference" / "source=mmadecisions_snapshot" / "dataset=ufc_decision_index" / "data.parquet"
    )
    if not index_path.exists():
        raise FileNotFoundError("run bootstrap-scorecards before the direct backfill")
    index = pd.read_parquet(index_path)
    years = pd.to_datetime(index["event_date"], errors="coerce").dt.year
    index = index[years.between(start_year, end_year)].copy()
    index = index.dropna(subset=["decision_id", "decision_url"])
    target = inside_root(
        store_dir / "reference" / "source=mmadecisions" / "dataset=official_scorecards" / "data.parquet"
    )
    page_target = inside_root(reports_dir / "mmadecisions_page_index_latest.parquet")
    prior_page_ledger = pd.read_parquet(page_target) if page_target.exists() else pd.DataFrame()
    existing = pd.read_parquet(target) if target.exists() else pd.DataFrame()
    existing_ids = set(existing["decision_id"].dropna().astype(int)) if not existing.empty else set()
    if not refresh_existing:
        index = index[~index["decision_id"].astype(int).isin(existing_ids)]
        if not prior_page_ledger.empty:
            permanent_failures = prior_page_ledger[
                (prior_page_ledger["status"] == "failed")
                & prior_page_ledger["error"].astype(str).str.startswith("ValueError:")
            ]
            index = index[
                ~index["decision_id"].astype(int).isin(
                    permanent_failures["decision_id"].astype(int)
                )
            ]
    # Bounded maintenance queues verify the modern/high-coverage era first,
    # then naturally walk backward without a hand-tuned year list.
    index = index.sort_values(["event_date", "decision_id"], ascending=False)
    if cache_only:
        cached_ids = {
            int(path.name.split("=", 1)[1])
            for path in cache_dir.glob("decision_id=*")
            if path.is_dir() and list(path.glob("*.html.gz"))
        }
        index = index[index["decision_id"].astype(int).isin(cached_ids)]
    if max_pages is not None:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        index = index.head(max_pages)
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    limiter = _StartRateLimiter(requests_per_second)
    ensure_dir(cache_dir)
    parsed_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    records = index[["decision_id", "decision_url", "event_date"]].to_dict("records")
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mmadecisions") as pool:
        futures = {
            pool.submit(
                _fetch_parse, row, limiter, cache_dir, generated, retries, not refresh_existing
            ): row
            for row in records
        }
        for future in as_completed(futures):
            parsed, page = future.result()
            parsed_rows.extend(parsed)
            page_rows.append(page)

    refreshed_ids = {int(row["decision_id"]) for row in parsed_rows}
    current = pd.DataFrame(parsed_rows)
    if not current.empty:
        if not existing.empty:
            first_seen = existing.set_index("record_key")["first_observed_at"].to_dict()
            current["first_observed_at"] = current.apply(
                lambda row: first_seen.get(row["record_key"], row["first_observed_at"]), axis=1
            )
            existing = existing[~existing["decision_id"].astype(int).isin(refreshed_ids)]
            current = pd.concat([existing, current], ignore_index=True)
        current = current.sort_values(
            ["decision_id", "judge_slot", "round"]
        ).drop_duplicates("record_key", keep="last")
        _atomic_parquet(current, target)
    elif not target.exists():
        raise RuntimeError("direct backfill produced no scorecard rows")

    pages = pd.DataFrame(page_rows)
    previous_pages = prior_page_ledger
    if not pages.empty and not previous_pages.empty:
        previous_pages = previous_pages[
            ~previous_pages["decision_id"].astype(int).isin(pages["decision_id"].astype(int))
        ]
        page_ledger = pd.concat([previous_pages, pages], ignore_index=True)
    elif not pages.empty:
        page_ledger = pages
    else:
        page_ledger = previous_pages
    if not page_ledger.empty:
        page_ledger = page_ledger.sort_values("decision_id").drop_duplicates("decision_id", keep="last")
    _atomic_parquet(page_ledger, page_target)
    current_failures = pages[pages["status"] == "failed"] if not pages.empty else pages
    failures = (
        page_ledger[page_ledger["status"] == "failed"] if not page_ledger.empty else page_ledger
    )
    manifest = {
        "contract": BACKFILL_CONTRACT,
        "generated_utc": generated,
        "year_range": [start_year, end_year],
        "cache_only": cache_only,
        "max_pages": max_pages,
        "workers": workers,
        "global_requests_per_second": requests_per_second,
        "requested_pages": len(records),
        "parsed_pages": int(pages["status"].astype(str).str.startswith("parsed").sum()) if not pages.empty else 0,
        "failed_pages": int(len(current_failures)),
        "page_ledger_failures": int(len(failures)),
        "page_ledger_pages": int(len(page_ledger)),
        "rows_added_or_refreshed": len(parsed_rows),
        "total_direct_rows": int(len(pd.read_parquet(target))),
        "direct_feature_use": False,
        "raw_cache": cache_dir.relative_to(store_dir.parent).as_posix(),
        "page_index": page_target.relative_to(store_dir.parent).as_posix(),
        "page_index_sha256": sha256_file(page_target),
        "output": target.relative_to(store_dir.parent).as_posix(),
        "output_sha256": sha256_file(target),
    }
    atomic_write_json(manifests_dir / "scorecards_mmadecisions_history_latest.json", manifest)
    atomic_write_text(
        reports_dir / "MMADECISIONS_FAILURES_LATEST.csv",
        failures.to_csv(index=False, lineterminator="\n"),
    )
    return manifest


def _name_orientation(direct: pd.DataFrame, bootstrap: pd.DataFrame) -> str | None:
    if direct.empty or bootstrap.empty:
        return None
    direct1 = name_key(direct.iloc[0]["side1_label"])
    direct2 = name_key(direct.iloc[0]["side2_label"])
    red = name_key(bootstrap.iloc[0]["side1_label"])
    blue = name_key(bootstrap.iloc[0]["side2_label"])
    red_last, blue_last = red.split()[-1], blue.split()[-1]
    same = (direct1 in red or red_last in direct1) and (direct2 in blue or blue_last in direct2)
    flipped = (direct1 in blue or blue_last in direct1) and (direct2 in red or red_last in direct2)
    if same and not flipped:
        return "same"
    if flipped and not same:
        return "flipped"
    return None


def reconcile_historical_scorecards(
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
    reports_dir: Path = DEFAULT_REPORTS,
) -> dict[str, Any]:
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    reports_dir = inside_root(reports_dir)
    direct_path = inside_root(
        store_dir / "reference" / "source=mmadecisions" / "dataset=official_scorecards" / "data.parquet"
    )
    bootstrap_path = inside_root(
        store_dir / "reference" / "source=mmadecisions_snapshot" / "dataset=ufc_judge_rounds" / "data.parquet"
    )
    index_path = inside_root(
        store_dir / "reference" / "source=mmadecisions_snapshot" / "dataset=ufc_decision_index" / "data.parquet"
    )
    direct, bootstrap, index = (
        pd.read_parquet(direct_path), pd.read_parquet(bootstrap_path), pd.read_parquet(index_path)
    )
    direct_valid = direct[
        (direct["record_status"] == "accepted")
        & direct["side1_score"].notna()
        & direct["decision_id"].astype(int).isin(index["decision_id"].astype(int))
    ].copy()
    bootstrap_valid = bootstrap[bootstrap["record_status"] == "bootstrap_reference"].copy()
    comparison = direct_valid.merge(
        bootstrap_valid[
            ["decision_id", "judge_slot", "round", "side1_score", "side2_score",
             "side1_label", "side2_label", "ufcstats_bout_id"]
        ],
        on=["decision_id", "judge_slot", "round"],
        how="left",
        suffixes=("_direct", "_bootstrap"),
    )
    comparison["same"] = (
        (comparison["side1_score_direct"] == comparison["side1_score_bootstrap"])
        & (comparison["side2_score_direct"] == comparison["side2_score_bootstrap"])
    )
    comparison["flipped"] = (
        (comparison["side1_score_direct"] == comparison["side2_score_bootstrap"])
        & (comparison["side2_score_direct"] == comparison["side1_score_bootstrap"])
    )
    rows = []
    for decision_id, group in direct_valid.groupby("decision_id", sort=True):
        matched = comparison[
            (comparison["decision_id"] == decision_id)
            & comparison["side1_score_bootstrap"].notna()
        ]
        bootstrap_group = bootstrap_valid[bootstrap_valid["decision_id"] == decision_id]
        if matched.empty:
            status, orientation = "direct_only_valid", "unresolved"
        else:
            all_same, all_flipped = bool(matched["same"].all()), bool(matched["flipped"].all())
            if all_same and not all_flipped:
                status, orientation = "direct_snapshot_exact", "same"
            elif all_flipped and not all_same:
                status, orientation = "direct_snapshot_exact", "flipped"
            elif all_same and all_flipped:
                orientation = _name_orientation(group, bootstrap_group) or "score_symmetric"
                status = "direct_snapshot_exact"
            else:
                status, orientation = "quarantined_mismatch", "conflict"
        rounds_by_judge = group.groupby("judge_slot")["round"].apply(
            lambda value: tuple(sorted(set(int(v) for v in value)))
        )
        max_round = int(group["round"].max())
        expected_rounds = tuple(range(1, max_round + 1))
        complete = len(rounds_by_judge) == 3 and all(value == expected_rounds for value in rounds_by_judge)
        index_row = index[index["decision_id"] == decision_id]
        rows.append(
            {
                "record_key": stable_hash("mmadecisions_reconciliation", decision_id),
                "source_system": "mmadecisions_reconciliation",
                "decision_id": int(decision_id),
                "event_date": group.iloc[0]["event_date"],
                "event_name": group.iloc[0]["event_name"],
                "ufcstats_bout_id": None if index_row.empty else index_row.iloc[0].get("ufcstats_bout_id"),
                "valid_judge_round_rows": int(len(group)),
                "judges_with_round_scores": int(group["judge_slot"].nunique()),
                "complete_three_judge_card": bool(complete),
                "comparison_rows": int(len(matched)),
                "reconciliation_status": status,
                "canonical_orientation": orientation,
                "record_status": "quarantined" if status.startswith("quarantined") else "accepted",
                "feature_eligible": False,
                "availability_class": "post_event_outcome",
            }
        )
    reconciliation = pd.DataFrame(rows)
    target = inside_root(
        store_dir / "reference" / "source=mmadecisions_reconciliation" / "dataset=ufc_scorecard_identity" / "data.parquet"
    )
    _atomic_parquet(reconciliation, target)

    index_year = pd.to_datetime(index["event_date"], errors="coerce").dt.year
    index = index.assign(year=index_year)
    reconciliation = reconciliation.assign(
        year=pd.to_datetime(reconciliation["event_date"], errors="coerce").dt.year
    )
    coverage = index.groupby("year", as_index=False).agg(indexed_decisions=("decision_id", "nunique"))
    bootstrap_valid = bootstrap_valid.assign(
        year=pd.to_datetime(bootstrap_valid["event_date"], errors="coerce").dt.year
    )
    bootstrap_decisions = bootstrap_valid.groupby("year", as_index=False).agg(
        bootstrap_scored_decisions=("decision_id", "nunique")
    )
    bootstrap_complete_rows = []
    for decision_id, group in bootstrap_valid.groupby("decision_id"):
        rounds_by_judge = group.groupby("judge_slot")["round"].apply(
            lambda value: tuple(sorted(set(int(v) for v in value)))
        )
        expected_rounds = tuple(range(1, int(group["round"].max()) + 1))
        bootstrap_complete_rows.append(
            {
                "year": int(group["year"].iloc[0]),
                "complete": len(rounds_by_judge) == 3
                and all(value == expected_rounds for value in rounds_by_judge),
            }
        )
    bootstrap_complete = pd.DataFrame(bootstrap_complete_rows).groupby("year", as_index=False).agg(
        bootstrap_complete_cards=("complete", "sum")
    )
    observed = reconciliation.groupby("year", as_index=False).agg(
        decisions_with_round_scores=("decision_id", "nunique"),
        complete_three_judge_cards=("complete_three_judge_card", "sum"),
        quarantined=("reconciliation_status", lambda value: int(value.astype(str).str.startswith("quarantined").sum())),
    )
    coverage = (
        coverage.merge(bootstrap_decisions, on="year", how="left")
        .merge(bootstrap_complete, on="year", how="left")
        .merge(observed, on="year", how="left")
        .fillna(0)
    )
    for column in (
        "indexed_decisions", "bootstrap_scored_decisions", "bootstrap_complete_cards",
        "decisions_with_round_scores", "complete_three_judge_cards", "quarantined",
    ):
        coverage[column] = coverage[column].astype(int)
    coverage["bootstrap_coverage_pct"] = (
        100.0 * coverage["bootstrap_scored_decisions"] / coverage["indexed_decisions"]
    ).round(2)
    coverage["round_score_coverage_pct"] = (
        100.0 * coverage["decisions_with_round_scores"] / coverage["indexed_decisions"]
    ).round(2)
    coverage_target = inside_root(reports_dir / "mmadecisions_historical_coverage.csv")
    atomic_write_text(coverage_target, coverage.to_csv(index=False, lineterminator="\n"))
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    report = {
        "contract": RECONCILIATION_CONTRACT,
        "generated_utc": generated,
        "indexed_decisions": int(index["decision_id"].nunique()),
        "bootstrap_scored_decisions": int(bootstrap_valid["decision_id"].nunique()),
        "bootstrap_judge_round_rows": int(len(bootstrap_valid)),
        "point_deduction_adjusted_rows": int(bootstrap_valid["point_deduction_adjusted"].sum()),
        "decisions_with_round_scores": int(reconciliation["decision_id"].nunique()),
        "complete_three_judge_cards": int(reconciliation["complete_three_judge_card"].sum()),
        "exact_or_direct_only": int((reconciliation["record_status"] == "accepted").sum()),
        "quarantined": int((reconciliation["record_status"] == "quarantined").sum()),
        "development_through_2022_only": True,
        "model_fit_performed": False,
        "output": target.relative_to(store_dir.parent).as_posix(),
        "output_sha256": sha256_file(target),
        "direct_scorecards_sha256": sha256_file(direct_path),
        "bootstrap_scorecards_sha256": sha256_file(bootstrap_path),
        "coverage_csv": coverage_target.relative_to(store_dir.parent).as_posix(),
        "coverage_sha256": sha256_file(coverage_target),
    }
    atomic_write_json(manifests_dir / "scorecards_mmadecisions_reconciliation.json", report)
    lines = [
        "# Historical UFC judging coverage",
        "",
        f"Generated: {generated}",
        "",
        f"- Indexed UFC decisions: {report['indexed_decisions']:,}",
        f"- Bootstrap fights with valid judge-round scores: {report['bootstrap_scored_decisions']:,}",
        f"- Directly re-parsed and reconciled fights: {report['decisions_with_round_scores']:,}",
        f"- Directly verified complete three-judge cards: {report['complete_three_judge_cards']:,}",
        f"- Official rows with a separate deduction-neutral value: {report['point_deduction_adjusted_rows']:,}",
        f"- Quarantined direct/snapshot conflicts: {report['quarantined']:,}",
        "- No model fit was performed. Rows dated 2023 onward remain validation-only.",
        "",
        "| Year | Indexed | Bootstrap scored | Bootstrap coverage | Direct verified | Direct complete | Quarantined |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(
        f"| {row.year} | {row.indexed_decisions} | {row.bootstrap_scored_decisions} | "
        f"{row.bootstrap_coverage_pct:.2f}% | {row.decisions_with_round_scores} | "
        f"{row.complete_three_judge_cards} | {row.quarantined} |"
        for row in coverage.itertuples(index=False)
    )
    atomic_write_text(reports_dir / "MMADECISIONS_HISTORICAL_COVERAGE.md", "\n".join(lines) + "\n")
    return report
