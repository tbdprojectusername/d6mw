# Market data inventory

Generated: 2026-09-03T14:46:23.545545+00:00
Parquet: 169 files / 287.5 MiB

## Live capture

| Source | Status | Rows | First | Last |
|---|---|---:|---|---|
| bfo_live | accepted | 705,268 | 2026-07-27T19:09:21.183156+00:00 | 2026-09-03T10:54:33.196645+00:00 |
| fightodds_live | accepted | 103,315 | 2026-08-13T13:20:49.217295+00:00 | 2026-09-03T11:27:45.239621+00:00 |
| fightodds_live | quarantined | 472 | 2026-08-17T20:03:19.217823+00:00 | 2026-09-02T09:41:49.930095+00:00 |
| pinnacle_live | accepted | 91,224 | 2026-07-26T04:18:01.319120+00:00 | 2026-09-03T11:27:09.663555+00:00 |
| pinnacle_live | quarantined | 60 | 2026-08-01T17:16:55.114156+00:00 | 2026-09-02T00:06:49.807207+00:00 |

## Historical FightOdds

| Year | Rows | Fights | Books |
|---:|---:|---:|---:|
| 2015 | 203,687 | 190 | 6 |
| 2016 | 334,745 | 492 | 9 |
| 2017 | 261,166 | 497 | 10 |
| 2018 | 342,969 | 508 | 8 |
| 2019 | 375,412 | 563 | 9 |
| 2020 | 293,880 | 852 | 13 |
| 2021 | 545,539 | 1,705 | 16 |
| 2022 | 578,293 | 2,111 | 14 |
| 2023 | 849,987 | 2,448 | 15 |
| 2024 | 1,654,141 | 2,548 | 19 |
| 2025 | 952,705 | 2,747 | 23 |
| 2026 | 708,764 | 1,985 | 29 |

## Historical status

| Status | Rows |
|---|---:|
| invalid_price | 3 |
| orientation_pending | 7,101,285 |

## Historical FightOdds prop summaries

| Year | Type | Outcome rows | Fights | Books |
|---:|---|---:|---:|---:|
| 2015 | moneyline_summary | 6,314 | 503 | 7 |
| 2015 | prop | 12,750 | 213 | 5 |
| 2016 | moneyline_summary | 6,998 | 552 | 9 |
| 2016 | prop | 49,850 | 565 | 7 |
| 2017 | moneyline_summary | 9,142 | 522 | 10 |
| 2017 | prop | 64,373 | 522 | 7 |
| 2018 | moneyline_summary | 7,460 | 521 | 8 |
| 2018 | prop | 50,281 | 502 | 7 |
| 2019 | moneyline_summary | 9,226 | 596 | 9 |
| 2019 | prop | 48,683 | 540 | 6 |
| 2020 | moneyline_summary | 13,420 | 741 | 13 |
| 2020 | prop | 61,038 | 551 | 9 |
| 2021 | moneyline_summary | 18,059 | 687 | 16 |
| 2021 | prop | 120,824 | 586 | 12 |
| 2022 | moneyline_summary | 19,549 | 656 | 14 |
| 2022 | prop | 132,280 | 567 | 10 |
| 2023 | moneyline_summary | 18,846 | 650 | 15 |
| 2023 | prop | 177,013 | 576 | 13 |
| 2024 | moneyline_summary | 19,092 | 641 | 19 |
| 2024 | prop | 131,345 | 560 | 13 |
| 2025 | moneyline_summary | 25,059 | 609 | 24 |
| 2025 | prop | 183,277 | 561 | 21 |
| 2026 | moneyline_summary | 21,043 | 675 | 30 |
| 2026 | prop | 240,953 | 442 | 27 |

## Live prop capture

| Source | Status | Rows | First | Last |
|---|---|---:|---|---|
| fightodds_live | inactive_offer | 6,573 | 2026-08-24T11:50:19.772414+00:00 | 2026-09-03T11:28:08.428724+00:00 |
| fightodds_live | quarantined | 2,295 | 2026-08-24T14:57:43.530128+00:00 | 2026-09-03T11:28:08.428724+00:00 |
| fightodds_live | raw_unverified | 105,015 | 2026-08-23T23:29:09.145038+00:00 | 2026-09-03T11:28:08.428724+00:00 |

## Reference sources

| Source | Table | Rows |
|---|---|---:|
| greco_ufcstats | event_details | 786 |
| greco_ufcstats | fight_details | 8,885 |
| greco_ufcstats | fight_results | 8,885 |
| greco_ufcstats | fight_stats | 41,774 |
| greco_ufcstats | fighter_details | 4,615 |
| greco_ufcstats | fighter_tott | 4,616 |
| mmadecisions | official_scorecards | 22,355 |
| mmadecisions_reconciliation | ufc_scorecard_identity | 1,817 |
| mmadecisions_snapshot | ufc_decision_index | 3,547 |
| mmadecisions_snapshot | ufc_judge_rounds | 28,690 |
| ufc_datalab | scorecards_ocr | 2,251 |
| ufc_datalab | stats_raw | 8,737 |

## Prospective snapshots

| Source | Table | Rows | First | Last |
|---|---|---:|---|---|
| octagon_api | rankings | 2,080 | 2026-08-23 | 2026-09-03 |
| octagon_api | roster | 1,740 | 2026-08-23 | 2026-09-03 |

Historical book policy is preserved separately from effective row eligibility; unresolved rows remain unusable. Raw reference and snapshot tables also remain feature-ineligible until point-in-time canonical transforms pass their own gates.
