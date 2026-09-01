from __future__ import annotations

import datetime as dt
import hashlib
import io
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from lxml import html as lxml_html

from .common import (
    DEFAULT_MANIFESTS,
    DEFAULT_STORE,
    atomic_write_json,
    ensure_dir,
    inside_root,
    name_key,
    sha256_file,
    stable_hash,
)


BASE = "https://mmadecisions.com/"
USER_AGENT = "MMA-Market-Research/1.0 (+noncommercial; one daily incremental check)"


def _get(url: str, timeout: int = 60) -> bytes:
    parts = urllib.parse.urlsplit(url)
    # urllib's Request encodes a raw non-ASCII path as ASCII and otherwise
    # fails before the request is sent. Unquote first to avoid double-encoding
    # source URLs that are already escaped.
    safe_url = urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            urllib.parse.quote(urllib.parse.unquote(parts.path), safe="/%:+-"),
            urllib.parse.quote(urllib.parse.unquote(parts.query), safe="=&%:+-"),
            parts.fragment,
        )
    )
    request = urllib.request.Request(safe_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _links(document: bytes, contains: str) -> list[str]:
    tree = lxml_html.fromstring(document)
    found = []
    for value in tree.xpath("//a/@href"):
        absolute = urllib.parse.urljoin(BASE, str(value))
        if contains in absolute:
            found.append(absolute)
    return sorted(set(found))


def discover_recent_decisions(
    max_events: int = 8, pace_seconds: float = 0.6, year: int | None = None
) -> list[str]:
    home = _get(BASE)
    event_urls = _links(home, "/event/")
    # Numeric event IDs increase over time. The cap prevents a layout change
    # from turning one daily check into an unbounded crawl.
    event_urls = sorted(
        event_urls,
        key=lambda u: int(re.search(r"/event/(\d+)/", u).group(1)) if re.search(r"/event/(\d+)/", u) else -1,
        reverse=True,
    )[:max_events]
    decisions: set[str] = set()
    for event_url in event_urls:
        time.sleep(pace_seconds)
        decisions.update(_links(_get(event_url), "/decision/"))
    decisions.update(discover_year_decisions(year or dt.date.today().year, pace_seconds=pace_seconds))
    return sorted(decisions)


def discover_year_decisions(
    year: int, pace_seconds: float = 0.25, workers: int = 6
) -> list[str]:
    """Discover every UFC decision linked from one annual event index.

    The home page exposes only a handful of events. The annual index makes the
    daily job gap-recovering after missed runs while the existing-ID filter
    prevents a full scorecard redownload.
    """
    annual_url = urllib.parse.urljoin(BASE, f"decisions-by-event/{int(year)}/")
    tree = lxml_html.fromstring(_get(annual_url))
    event_urls = []
    for anchor in tree.xpath("//a[@href]"):
        label = _clean(anchor.text_content())
        href = urllib.parse.urljoin(BASE, str(anchor.get("href")))
        if "/event/" not in href:
            continue
        if label.startswith(("UFC", "The Ultimate Fighter", "TUF")):
            event_urls.append(href)
    event_urls = sorted(set(event_urls))

    def decisions_for_event(event_url: str) -> list[str]:
        if pace_seconds:
            time.sleep(pace_seconds)
        return _links(_get(event_url), "/decision/")

    decisions: set[str] = set()
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as pool:
        for found in pool.map(decisions_for_event, event_urls):
            decisions.update(found)
    return sorted(decisions)


def _clean(value: Any) -> str:
    return " ".join(str(value).replace("\ufffd", " ").replace("\xa0", " ").split())


class _RequestStartLimiter:
    def __init__(self, requests_per_second: float):
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


def _event_date(text: str) -> str | None:
    match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
        text,
    )
    if not match:
        return None
    return dt.datetime.strptime(match.group(0), "%B %d, %Y").date().isoformat()


def parse_decision_page(url: str, document: bytes, scraped_at: str) -> list[dict[str, Any]]:
    decision_match = re.search(r"/decision/(\d+)/", url)
    if not decision_match:
        raise ValueError(f"no decision id in {url}")
    decision_id = int(decision_match.group(1))
    tree = lxml_html.fromstring(document)
    title = _clean("".join(tree.xpath("//title/text()")))
    full_text = _clean(tree.text_content())
    date = _event_date(full_text)
    event_name = title.split("::")[1].strip() if title.count("::") >= 2 else None
    result_text = title.split("::")[0].strip() if "::" in title else title
    tables = pd.read_html(io.BytesIO(document), header=None)
    rows: list[dict[str, Any]] = []
    judge_slot = 0
    for table in tables:
        if table.shape[1] != 3 or len(table) < 4:
            continue
        values = table.fillna("").map(_clean)
        if values.iloc[1, 0].upper() != "ROUND" or values.iloc[-1, 0].upper() != "TOTAL":
            continue
        judge = values.iloc[0, 0]
        side1, side2 = values.iloc[1, 1], values.iloc[1, 2]
        if not judge or not side1 or not side2:
            continue
        judge_slot += 1
        expected_total1 = expected_total2 = 0
        parsed = []
        complete = True
        for _, item in values.iloc[2:-1].iterrows():
            if not item.iloc[0].isdigit():
                continue
            try:
                round_no = int(item.iloc[0])
                score1, score2 = int(item.iloc[1]), int(item.iloc[2])
            except ValueError:
                complete = False
                continue
            if round_no not in range(1, 6) or score1 not in range(7, 11) or score2 not in range(7, 11):
                raise ValueError(f"out-of-range official score in decision {decision_id}")
            expected_total1 += score1
            expected_total2 += score2
            parsed.append((round_no, score1, score2))
        try:
            stated1, stated2 = int(values.iloc[-1, 1]), int(values.iloc[-1, 2])
        except ValueError as exc:
            raise ValueError(f"non-numeric total in decision {decision_id}") from exc
        if complete and (not parsed or (expected_total1, expected_total2) != (stated1, stated2)):
            raise ValueError(f"round totals do not reconcile in decision {decision_id}")
        if not complete:
            parsed = [(0, None, None)]
        for round_no, score1, score2 in parsed:
            rows.append(
                {
                    "record_key": stable_hash("mmadecisions", decision_id, judge_slot, round_no),
                    "source_system": "mmadecisions",
                    "source_authority": "secondary_transcription_of_official_card",
                    "decision_id": decision_id,
                    "decision_url": url,
                    "event_name": event_name,
                    "event_date": date,
                    "result_text": result_text,
                    "judge_name": judge,
                    "judge_slot": judge_slot,
                    "round": round_no,
                    "side1_label": side1,
                    "side2_label": side2,
                    "side1_score": score1,
                    "side2_score": score2,
                    "side1_total": stated1,
                    "side2_total": stated2,
                    "orientation_status": "source_table_named",
                    "record_status": "accepted" if complete else "partial_total_only",
                    "quarantine_reason": None if complete else "round_scores_unavailable",
                    "first_observed_at": scraped_at,
                    "feature_eligible": False,
                    "availability_class": "post_event_outcome",
                }
            )
    if not rows:
        raise ValueError(f"no official judge tables parsed from decision {decision_id}")
    judge_slots = {row["judge_slot"] for row in rows}
    if judge_slots != {1, 2, 3}:
        raise ValueError(f"expected three official scorecards in decision {decision_id}, found {len(judge_slots)}")
    return rows


def ingest_recent_scorecards(
    urls: Iterable[str] | None = None,
    max_events: int = 8,
    pace_seconds: float = 0.6,
    refresh_known: int = 12,
    discovery_year: int | None = None,
    workers: int = 6,
    requests_per_second: float = 4.0,
    store_dir: Path = DEFAULT_STORE,
    manifests_dir: Path = DEFAULT_MANIFESTS,
) -> dict[str, Any]:
    store_dir, manifests_dir = inside_root(store_dir), inside_root(manifests_dir)
    target = inside_root(store_dir / "reference" / "source=mmadecisions" / "dataset=official_scorecards" / "data.parquet")
    existing = pd.read_parquet(target) if target.exists() else pd.DataFrame()
    discovered = urls is None
    urls = list(urls) if urls is not None else discover_recent_decisions(
        max_events, pace_seconds, discovery_year
    )
    if discovered and not existing.empty:
        existing_ids = set(existing["decision_id"].dropna().astype(int))
        identified = [
            (int(match.group(1)), url)
            for url in urls
            if (match := re.search(r"/decision/(\d+)/", url))
        ]
        refresh_ids = {
            decision_id
            for decision_id, _ in sorted(identified, reverse=True)[:max(0, refresh_known)]
        }
        urls = [
            url for decision_id, url in identified
            if decision_id not in existing_ids or decision_id in refresh_ids
        ]
    if not urls:
        raise RuntimeError("no recent MMA Decisions pages discovered")
    generated = dt.datetime.now(dt.timezone.utc).isoformat()
    refreshed_ids: set[int] = set()
    new_rows: list[dict[str, Any]] = []
    page_hashes = []
    limiter = _RequestStartLimiter(requests_per_second)

    def fetch(url: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        limiter.wait()
        document = _get(url)
        parsed = parse_decision_page(url, document, generated)
        digest = hashlib.sha256(document).hexdigest()
        for row in parsed:
            row["source_page_sha256"] = digest
        return parsed, {
            "url": url,
            "sha256": digest,
            "rows": len(parsed),
        }

    with ThreadPoolExecutor(max_workers=max(1, min(workers, 16))) as pool:
        for parsed, page in pool.map(fetch, urls):
            refreshed_ids.add(int(parsed[0]["decision_id"]))
            new_rows.extend(parsed)
            page_hashes.append(page)
    current = pd.DataFrame(new_rows)
    if not existing.empty:
        first_seen = existing.set_index("record_key")["first_observed_at"].to_dict()
        current["first_observed_at"] = current.apply(
            lambda row: first_seen.get(row["record_key"], row["first_observed_at"]),
            axis=1,
        )
        existing = existing[~existing["decision_id"].astype(int).isin(refreshed_ids)]
        current = pd.concat([existing, current], ignore_index=True)
    current = current.sort_values(["decision_id", "judge_slot", "round"]).drop_duplicates("record_key", keep="last")
    ensure_dir(target.parent)
    tmp = target.with_suffix(".parquet.tmp")
    current.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, target)
    manifest = {
        "contract": "MMADECISIONS-INCREMENTAL-1",
        "generated_utc": generated,
        "pages_refreshed": len(urls),
        "decisions_refreshed": len(refreshed_ids),
        "total_rows": int(len(current)),
        "direct_feature_use": False,
        "source_pacing_seconds": pace_seconds,
        "known_pages_refreshed": refresh_known,
        "discovery_year": discovery_year or dt.date.today().year,
        "workers": workers,
        "global_requests_per_second": requests_per_second,
        "pages": page_hashes,
        "output": target.relative_to(store_dir.parent).as_posix(),
        "output_sha256": sha256_file(target),
    }
    atomic_write_json(manifests_dir / "scorecards_mmadecisions_latest.json", manifest)
    return manifest
