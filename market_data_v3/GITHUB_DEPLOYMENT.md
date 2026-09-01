# GitHub activation checklist

The workflows are implemented but scheduled GitHub Actions do not exist for
GitHub until these files reach the repository's default branch.

## Before activation

1. Review and push the intended branch. The local branch already contained a
   large number of earlier unpushed commits when this package was added, so the
   automation work was **not pushed automatically**.
2. In repository settings, allow GitHub Actions to create commits and issues
   (`contents: write`, `issues: write`). No sportsbook credentials are used by
   these jobs.
3. Confirm that scheduled workflows are enabled on the default branch.
4. Add a `PRIVATE_REPO_TOKEN` Actions secret to the public `kx7v` repository.
   Give it permission only to dispatch workflows in this private repository.
   Without it, the private ten-minute schedule remains the fallback; with it,
   each successful capture push triggers the hot-state build immediately.
5. Manually dispatch `market-data-refresh` once. Confirm validation PASS, the
   bot commits only the new namespace. Then dispatch `market-live-update` and
   confirm its current state records the public capture revision.
6. Dispatch `market-data-enrichment`, then `market-data-reference-audit`.
7. Confirm the daily health job rebuilds the catalog from a fresh checkout.

## Expected first-run behavior

- The historical FightOdds Parquet bootstrap is already materialized; GitHub
  does not need access to the three-gigabyte local SQLite archive.
- Live refresh reads the public capture repository and rebuilds canonical
  partitions only for durable storage; the ten-minute state/scoring path does
  not wait for it.
- The hot path reads atomic per-cycle snapshots, verifies each snapshot against
  its cycle manifest and hash, and refuses stale or aborted source cycles. It
  never reconstructs a current board from append-only monthly history.
- Fast state construction is separate from compaction. It currently completes
  in roughly 2.5 seconds on the full public feed and includes BFO, FightOdds
  and Pinnacle moneylines plus FightOdds props.
- Current scorecard discovery is capped at eight event pages and uses a polite
  delay. It is incremental, not a daily full-site crawl.
- Rankings/roster snapshots are first-observation-per-UTC-day and are never
  retroactively manufactured.
- A weak enrichment source can fail without discarding a healthy UFCStats
  update; the run still finishes red and opens a deduplicated incident.

## Go-live gate for data—not betting

GitHub automation being green means collection, durability and contracts are
healthy. It does **not** certify historical FightOdds orientation, prematch
closing labels, feature joins, forecasting skill, executable edge, or staking.
Those remain separate versioned gates in the model bible.

## Activation and cost note

GitHub scheduled events are best-effort, so ten-minute cron is a fallback, not
a latency guarantee. The preferred production trigger is an
`odds-capture-updated` repository dispatch after a successful public capture
push. For a private repository, a self-hosted runner avoids consuming thousands
of hosted-runner minutes per month. Until a V3 scorer exists, the hot job builds
and validates state but places no bets and sends no model signal.
