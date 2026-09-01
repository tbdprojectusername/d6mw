import tempfile
import unittest
from pathlib import Path

import pandas as pd

from market_data_v3.src.common import ROOT, stable_hash
from market_data_v3.src.repair_live_contract import repair_live_raw_contract


def quote_key(row):
    return stable_hash(
        row["source"], row["observed_at"], row["fight_id"], row["book_key"],
        row["side_key"], row["side_position"], row["price_american"],
        row["record_status"],
    )


class LiveContractRepairTests(unittest.TestCase):
    def test_clears_certifications_and_quarantines_explicit_post_cutoff_rows(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "build") as work:
            root = Path(work)
            store = root / "store"
            manifests = root / "manifests"
            target = (
                store / "live_quotes" / "source=pinnacle_live" /
                "observed_date=2026-08-21" / "quotes.parquet"
            )
            target.parent.mkdir(parents=True)
            rows = [
                {
                    "source": "pinnacle_live", "observed_at": "2026-08-21T19:59:00Z",
                    "cutoff_at": "2026-08-21T20:00:00Z", "fight_id": "pinnacle:10",
                    "book_key": "pinnacle", "side_key": "a", "side_position": 1,
                    "price_american": -195, "record_status": "accepted",
                    "orientation_status": "source_named", "quarantine_reason": None,
                    "feature_eligible": True, "close_eligible": True,
                    "execution_eligible": False, "book_feature_eligible": True,
                    "book_close_eligible": True, "book_execution_eligible": False,
                },
                {
                    "source": "pinnacle_live", "observed_at": "2026-08-21T20:00:00Z",
                    "cutoff_at": "2026-08-21T19:59:59.500Z", "fight_id": "pinnacle:10",
                    "book_key": "pinnacle", "side_key": "b", "side_position": 2,
                    "price_american": 175, "record_status": "accepted",
                    "orientation_status": "source_named", "quarantine_reason": None,
                    "feature_eligible": True, "close_eligible": True,
                    "execution_eligible": True, "book_feature_eligible": True,
                    "book_close_eligible": True, "book_execution_eligible": False,
                },
            ]
            for row in rows:
                row["quote_key"] = quote_key(row)
            pd.DataFrame(rows).to_parquet(target, index=False)

            result = repair_live_raw_contract(store, manifests)
            self.assertEqual(result["partitions_changed"], 1)
            self.assertEqual(result["feature_certifications_cleared"], 2)
            self.assertEqual(result["close_certifications_cleared"], 2)
            self.assertEqual(result["execution_certifications_cleared"], 1)
            self.assertEqual(result["post_cutoff_rows_quarantined"], 1)

            repaired = pd.read_parquet(target)
            self.assertEqual(len(repaired), 2)
            self.assertFalse(repaired[list(("feature_eligible", "close_eligible", "execution_eligible"))].any().any())
            before = repaired[repaired["side_key"] == "a"].iloc[0]
            after = repaired[repaired["side_key"] == "b"].iloc[0]
            self.assertEqual(before["record_status"], "accepted")
            self.assertEqual(after["record_status"], "quarantined")
            self.assertEqual(after["quarantine_reason"], "observed_at_at_or_after_cutoff")
            self.assertEqual(after["market_phase"], "post_cutoff")
            self.assertFalse(bool(after["source_active"]))
            self.assertEqual(after["quote_key"], quote_key(after))

            repeated = repair_live_raw_contract(store, manifests)
            self.assertEqual(repeated["partitions_changed"], 0)


if __name__ == "__main__":
    unittest.main()
