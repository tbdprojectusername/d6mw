# Historical UFC judging ingestion — completion report

Date: 2026-08-24
Contract: `JUDGING-DATA-1`
Scope: data ingestion and validation only; no feature frame or model was fit.

## Disposition

**PASS for the judging subsystem.** The normalized historical bootstrap is
available immediately, direct-source validation found no score conflicts, the
modern annual gaps are filled, and the forward job is gap-recovering.

This does not declare the entire market-data store green. The whole-store audit
still reports two pre-existing live-odds failures, listed below.

## Stored data

- Frozen internal bootstrap: 28,690 valid judge-round rows across 3,120 UFC
  decisions, source revision `93b1f4343761695a351a70b3e38ae44fd546964d`.
- Direct MMA Decisions canonical table: 14,649 historical-run rows; 14,662
  after integrating 13 newer remote keys during publication.
- Directly re-parsed fights reconciled to the bootstrap: 1,022.
- Directly verified complete three-judge cards: 911.
- Direct/bootstrap score conflicts: **0**.
- Official judge-round rows with a separately retained deduction-neutral
  assessment: 164. Official and deduction-neutral values are never substituted.
- One 1997 page (`decision_id=169`) uses a non-standard historical score outside
  the 7–10 range. It is retained in the failure ledger and is not coerced into a
  modern 10-point score.

Modern bootstrap coverage against the frozen UFC decision index:

| Year | Indexed decisions | Scored decisions | Coverage |
|---:|---:|---:|---:|
| 2018 | 230 | 212 | 92.17% |
| 2019 | 277 | 265 | 95.67% |
| 2020 | 227 | 224 | 98.68% |
| 2021 | 257 | 256 | 99.61% |
| 2022 | 240 | 240 | 100.00% |
| 2023 | 249 | 249 | 100.00% |
| 2024 snapshot period | 238 | 238 | 100.00% |

The direct annual catch-up contains 281 UFC decisions for 2024, 251 for 2025,
and 156 for 2026 to date. The 2023+ rows remain paper/replay data and are not
authorized for model fitting.

## Provenance and semantics

- `side1_score` / `side2_score`: official transcribed card after any referee
  point deduction.
- `side1_score_no_deduction` / `side2_score_no_deduction`: explicitly derived
  estimate of the judge's underlying round assessment.
- Bootstrap rows are `feature_eligible=false` and carry the source revision.
- Direct pages are parsed with range, judge-count and total-reconciliation
  checks. Page hashes and a resumable page ledger are retained.
- Missing early public cards remain missing. No score or judge identity is
  imputed.

## Autonomous maintenance

The daily enrichment workflow now:

1. discovers UFC decisions from the complete current-year event index, not only
   the five events exposed on the MMA Decisions home page;
2. downloads only unknown decisions plus the newest 12 known pages;
3. advances a bounded 100-page historical direct-verification queue, modern
   years first and then backward;
4. reruns reconciliation, tests and validation before committing the isolated
   data platform.

Cached pages are reused on resume. Permanent historical parser quarantines are
not requested repeatedly.

## Verification

- 22 `market_data_v3` unit tests pass.
- Reference keys are unique.
- Official and bootstrap score ranges pass.
- Every accepted direct card reconciles its per-round sums to the stated total.
- Every raw reference row remains feature-ineligible.
- Direct/bootstrap reconciliation has zero conflicts.

## Separate whole-store blockers

The final whole-store health run remains `FAIL` because of existing live-odds
partitions, not judging data:

- 68,154 raw live rows are already marked `close_eligible`, contrary to the
  raw-layer contract that close certification happens later.
- 42 live rows at or after their declared cutoff remain accepted/eligible.

These conditions predate this scorecard work and were not modified here. They
must be repaired before the entire database can be called green, but they do not
invalidate the judging tables or require repeating this ingestion.
