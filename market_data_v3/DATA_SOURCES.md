# Data sources and autonomous jobs

Status date: 2026-08-23. This document describes only `market_data_v3`; the
deployed upstream systems are outside its
write boundary.

## Operational sources

| Source | What is retained | Frequency | Direct model use |
|---|---|---:|---|
| Public capture repository (`tbdprojectusername/kx7v`) | Named-book moneylines from FightOdds, BFO and Pinnacle; FightOdds prematch props; quarantine sidecars | source polls about every 9–10 minutes | only rows passing the market-specific gates |
| Local FightOdds archive bootstrap | Historical moneyline ticks, event/fight identity, source outcome slot | one-time, 2015 onward | **no** until orientation overlay resolves the named side |
| Local FightOdds prop bootstrap | 1.45M retained named outcomes with open/current/best/worst endpoint summaries after book exclusions | one-time, 2015 onward | raw only; no invented as-of time or closing label |
| Greco1899 UFCStats mirror | Events, results, round/fight stats, fighter details and tale-of-the-tape | daily | **no** in raw layer; later lagged canonical features only |
| MMA Decisions | Direct per-judge, per-round official-card transcriptions; historical pages are hash-reconciled against a frozen UFCStats-linked snapshot | one historical bootstrap/backfill, then daily gap-recovering increment | **no** in raw layer; later fights may use a separately certified lagged transform |
| Octagon API | Rankings and roster payload | daily first snapshot | prospective only; never backfilled |
| UFC-DataLab | Independent UFC stats and OCR-derived scorecard totals | weekly revision check | audit/reference only until reconciliation passes |

MMA Decisions is a secondary transcription of official cards. UFC-DataLab is
an independent transformation of UFCStats and official UFC scorecard images.
Neither silently overrides the other. Disagreement becomes an auditable
quarantine/review record in the future canonical transform.

The historical bootstrap preserves two different score concepts rather than
mixing them: `side*_score` is the official card after referee point deductions;
`side*_score_no_deduction` is the research snapshot's explicitly derived
deduction-neutral judge assessment. Direct pages reconcile against the former.
The no-license community snapshot is an internal bootstrap/crosswalk only;
direct MMA Decisions pages, hashes and validation determine canonical status.

## Book policy

- BetOnline and Pinnacle are the initial closing-label candidates, subject to
  row-level prematch/orientation checks.
- Circa and Bookmaker are captured but remain provisional closing sources.
- DraftKings, FanDuel and Bovada are retained as useful retail/execution
  observations, not sharp closing labels.
- Polymarket, Kalshi and SXBet are feature-only exchanges and can never define
  the sportsbook closing target.
- BetCRIS/BetDSI/SportsBetting clone families and Sportbet/Sportsbet/Ohmbet are
  excluded under the frozen registry.
- **Mise-o-jeu is permanently inactive**: it is not discovered, ingested, or
  eligible for any model role.

## Availability law

1. Raw ingestion is not feature approval.
2. Every eventual feature must satisfy `available_to_model_at <= decision_time`.
3. Today’s rankings, roster, camp or aggregate career state is not backfilled.
4. Per-fight performance and scorecards are post-event facts and may affect
   only later fights through a declared conservative availability rule.
5. Closing labels are book-specific verified prematch finals. A final exchange
   price, a FightOdds source slot, or an unverified last tick is not CLV.
6. All source rows and output partitions are content-hashed. Ambiguous rows are
   quarantined, not guessed or deleted.
7. Moneyline and prop namespaces are disjoint. Source category `A_2` (live
   betting) is excluded from the prematch prop feed; `A_1` remains moneyline.
8. The live model reads the newest hot state and never waits for Parquet
   compaction or the full-store audit.

## Live versus maintenance clocks

| Layer | Cadence | Purpose | May gate a live decision? |
|---|---:|---|---:|
| Public BFO/Pinnacle capture | nominal 10 min | append changed named moneylines | yes—freshness checked |
| Public FightOdds moneylines | nominal 9 min | append changed named moneylines | yes—freshness checked |
| Public FightOdds props | nominal 9 min after activation | append changed prematch prop outcomes | yes for a future prop model only |
| V3 hot state | 10 min fallback or source event | latest quote per market/book, then score | **this is the live path** |
| Durable Parquet compaction | 3 h | reproducible research/storage; raw source is already committed every poll | no |
| Full validation/catalog | 3 h, after compaction | audit all partitions and open incidents | no, except a P0 source quarantine |

## Scheduled workflow map

| Workflow | Schedule (UTC) | Mutates | Failure behavior |
|---|---:|---|---|
| `market-live-update.yml` | every 10 minutes + dispatch | ephemeral current state; future score/alert | cancel stale run; issue opened |
| `market-data-refresh.yml` | minute 35 every 3 hours | live canonical partitions and manifests | no partial commit; issue opened |
| `market-data-enrichment.yml` | daily 10:45 | stats reference, gap-recovering current scorecards, prospective snapshots | healthy sources persist; degraded optional source raises issue |
| `market-data-reference-audit.yml` | Monday 14:15 | independent audit tables, manifests, reports | no partial commit; issue opened |
| `market-data-health.yml` | minute 50 every 3 hours | no durable data | independent rebuild/validation; issue opened |

All three writers share one GitHub concurrency group, so two bots cannot push
overlapping data states. Every commit stages only `market_data_v3/store`,
`market_data_v3/manifests`, and `market_data_v3/reports`.
