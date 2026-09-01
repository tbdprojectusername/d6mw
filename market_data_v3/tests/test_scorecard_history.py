import tempfile
import unittest
from pathlib import Path

import pandas as pd

from market_data_v3.src.common import ROOT
from market_data_v3.src.scorecard_history import _build_snapshot_tables


class HistoricalScorecardTests(unittest.TestCase):
    def frames(self):
        return {
            "merged": pd.DataFrame(
                {
                    "ufcstats_bout_id": ["bout", "bout", "bout"],
                    "mmadecisions_bout_id": [123.0, 123.0, 123.0],
                    "ufcstats_event_id": ["event", "event", "event"],
                    "red_ufcstats_fighter_id": ["red", "red", "red"],
                    "blue_ufcstats_fighter_id": ["blue", "blue", "blue"],
                    "red_mmadecisions_fighter_id": [11.0, 11.0, 11.0],
                    "blue_mmadecisions_fighter_id": [12.0, 12.0, 12.0],
                    "round": [1.0, 2.0, 3.0],
                    "judge_num": [1.0, 1.0, 1.0],
                    "judge_id": [7.0, 7.0, 7.0],
                    "red_score": [10.0, 9.0, 10.0],
                    "blue_score": [9.0, 10.0, 9.0],
                }
            ),
            "bouts": pd.DataFrame(
                {"id": [123.0], "event_id": [5.0], "fighter1_id": [11.0], "fighter2_id": [12.0]}
            ),
            "scores": pd.DataFrame(
                {
                    "id": [123.0] * 6,
                    "round": [1.0, 2.0, 3.0, 1.0, 2.0, 3.0],
                    "fighter_id": [11.0, 11.0, 11.0, 12.0, 12.0, 12.0],
                    "judge_num": [1.0] * 6,
                    "judge_id": [7.0] * 6,
                    "score": [10.0, 9.0, 10.0, 9.0, 10.0, 9.0],
                }
            ),
            "events": pd.DataFrame(
                {"id": [5.0], "name": ["UFC Test"], "date": [pd.Timestamp("2022-01-01").date()]}
            ),
            "fighters": pd.DataFrame(
                {"id": [11.0, 12.0], "name": ["Red Fighter", "Blue Fighter"]}
            ),
            "judges": pd.DataFrame({"id": [7.0], "name": ["Judge One"]}),
            "urls": pd.DataFrame({None: ["decision/123/Red-Fighter-vs-Blue-Fighter"]}),
            "ufc_events": pd.DataFrame(
                {"id": ["event"], "name": ["UFC Test"], "date": [pd.Timestamp("2022-01-01").date()]}
            ),
            "ufc_fighters": pd.DataFrame(
                {"id": ["red", "blue"], "name": ["Red Fighter", "Blue Fighter"]}
            ),
        }

    def test_snapshot_build_is_named_oriented_and_ineligible(self):
        scores, index = _build_snapshot_tables(self.frames(), "abc123", "2026-08-24T00:00:00+00:00")
        self.assertEqual(len(scores), 3)
        self.assertEqual(len(index), 1)
        self.assertEqual(scores["record_key"].nunique(), 3)
        self.assertEqual(scores.iloc[0]["side1_label"], "Red Fighter")
        self.assertEqual(scores.iloc[0]["orientation_status"], "ufcstats_red_blue_crosswalk")
        self.assertEqual(scores.iloc[0]["side1_total"], 29)
        self.assertFalse(scores["feature_eligible"].any())
        self.assertTrue(index.iloc[0]["decision_url"].endswith("decision/123/Red-Fighter-vs-Blue-Fighter"))

    def test_snapshot_build_rejects_invalid_scores(self):
        frames = self.frames()
        frames["merged"].loc[0, "red_score"] = 6
        with self.assertRaisesRegex(ValueError, "invalid judge-round"):
            _build_snapshot_tables(frames, "abc123", "2026-08-24T00:00:00+00:00")

    def test_official_and_deduction_neutral_scores_are_separate(self):
        frames = self.frames()
        frames["scores"].loc[0, "score"] = 9
        scores, _ = _build_snapshot_tables(frames, "abc123", "2026-08-24T00:00:00+00:00")
        first = scores[(scores["judge_slot"] == 1) & (scores["round"] == 1)].iloc[0]
        self.assertEqual(first["side1_score"], 9)
        self.assertEqual(first["side1_score_no_deduction"], 10)
        self.assertTrue(first["point_deduction_adjusted"])


if __name__ == "__main__":
    unittest.main()
