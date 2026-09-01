# Live raw-contract repair 1

Date: 2026-08-24

Scope: `market_data_v3` canonical live quote archive only

Result: **PASS**

## Finding

The current ingestion code correctly separates book policy from row
certification, but the durable Parquet partitions predated that change. The
validator was upgraded in the same code change without first regenerating the
stored partitions. The writer workflow also did not validate its output before
committing it.

The 638,673-row local baseline exposed stale row-level eligibility:

| Stale field | Rows cleared |
|---|---:|
| `feature_eligible` | 247,501 |
| `close_eligible` | 68,154 |
| `execution_eligible` | 84,680 |

These counts overlap. The book-policy fields were retained. In particular,
BetOnline and Pinnacle remain identifiable as close-policy candidates through
`book_close_eligible`; no raw observation is itself called a certified close.

The archive also contained 42 Pinnacle side quotes from 15 fights at or after
an explicit source `cutoff_at`. They covered 9 observed dates, with a median
lateness of 407 seconds and a maximum of 1,292 seconds. All 42 are now:

- `record_status=quarantined`;
- `quarantine_reason=observed_at_at_or_after_cutoff`;
- `market_phase=post_cutoff`;
- ineligible for features, closing labels, and execution.

## Final integrated state

- The bounded local repair scanned 68 partitions and preserved all 638,673
  rows. A row-multiset comparison against the committed pre-repair Parquet
  files proved every immutable quote, price, timestamp, identity, and
  book-policy field unchanged.
- Before publication, autonomous ingestion had already regenerated the archive
  under the corrected contract and advanced it to 658,533 rows in 71
  partitions. The integration explicitly retained that newer remote archive;
  the older local Parquet rewrite was not replayed over it.
- The six fractional-second cutoff rows missed by the first migration pass were
  caught by the independent DuckDB validation. The migration now uses mixed ISO
  parsing and the exact case is covered by a regression test.
- The durable writer now runs whole-store validation immediately after
  canonicalization and before it can commit data.
- Validation now independently rejects raw feature, close, and execution
  certification; it no longer checks only the close field.

## Verification

- Full `market_data_v3` suite: **24 tests passed**.
- Final 658,533-row repair replay: **idempotent**, 0 partitions changed.
- Whole-store validation: **PASS**, 29/29 checks.
- Duplicate quote keys: 0.
- Invalid prices: 0.
- Post-cutoff accepted/eligible rows: 0.
- Raw certified rows: 0 for feature, close, and execution.
- MiseOJeu rows: 0.
- Rebuilt inventory: Pinnacle 66,766 accepted rows and 42 quarantined rows;
  65,015 accepted FightOdds rows plus 230 orientation-quarantined rows;
  526,480 accepted BFO rows.

No deployed model, model specification, historical odds database, label, or
staking artifact was changed. The hot current-state path remains separate and
derives point-in-time eligibility from verified current snapshots.

Audit manifest:

- `manifests/live_raw_contract_repair_1.json`
