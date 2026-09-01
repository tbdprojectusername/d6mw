import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from market_data_v3.src.common import ROOT, sha256_file
from market_data_v3.src.current_state import SnapshotHealthError, build_current_state


class CurrentStateSnapshotTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "build").mkdir(exist_ok=True)
        self.source_obj = tempfile.TemporaryDirectory()
        self.work_obj = tempfile.TemporaryDirectory(dir=ROOT / "build")
        self.source = Path(self.source_obj.name)
        self.output = Path(self.work_obj.name)
        self.poll = pd.Timestamp.now(tz="UTC").floor("s")
        self.event = (self.poll + pd.Timedelta(days=2)).strftime("%Y-%m-%d")
        self._write_sources()

    def tearDown(self):
        self.source_obj.cleanup()
        self.work_obj.cleanup()

    def _snapshot(self, source, name, manifest, text):
        path = self.source / name
        path.write_text(text, encoding="utf-8")
        payload = {
            "status": "complete",
            "poll_time": self.poll.isoformat(),
            "snapshot": {
                "path": name,
                "rows": max(0, len(text.splitlines()) - 1),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            },
        }
        (self.source / manifest).write_text(json.dumps(payload), encoding="utf-8")

    def _write_sources(self):
        stamp = self.poll.isoformat()
        self._snapshot(
            "bfo", "bfo_snapshot_latest.csv", "bfo_cycle_latest.json",
            "poll_time,event_slug,event_name,event_date,matchup_id,side,selection,row_kind,book_id,book,american,move_arrow\n"
            f"{stamp},ufc-test,UFC Test,{self.event},9,1,A,moneyline,1,FanDuel,-200,\n"
            f"{stamp},ufc-test,UFC Test,{self.event},9,2,B,moneyline,1,FanDuel,+170,\n",
        )
        self._snapshot(
            "fightodds", "fightodds_snapshot_latest.csv", "fightodds_cycle_latest.json",
            "poll_time,event_pk,pair,fight_slug,event_date,event_name,promotion,side1_key,side2_key,book,book_role,dec1,dec2,amer1,amer2,source_offer_ts,source_change_age_h,cycle_status,offer_id,offer_category,offer_status,disabled\n"
            f"{stamp},1,a|b,a-vs-b-1,{self.event},UFC Test,ufc,a,b,BetOnline,sportsbook,1.5,2.8,-200,+180,{stamp},0,complete,o1,A_1,O,0\n",
        )
        cutoff = (self.poll + pd.Timedelta(days=2)).isoformat()
        self._snapshot(
            "pinnacle", "pinnacle_snapshot_latest.csv", "pinnacle_cycle_latest.json",
            "poll_time,league_id,matchup_id,home,away,start_time,period,bet_type,side,line,american,is_alt,max_risk,currency_hint,cutoff_at,version\n"
            f"{stamp},1,10,A,B,{cutoff},0,ml,home,,-195,0,5000,account_ccy,{cutoff},1\n"
            f"{stamp},1,10,A,B,{cutoff},0,ml,away,,+170,0,5000,account_ccy,{cutoff},1\n",
        )
        self._snapshot(
            "fightodds_props", "fightodds_props_snapshot_latest.csv",
            "fightodds_props_cycle_latest.json",
            "poll_time,event_pk,event_date,event_name,promotion,fight_slug,offer_id,outcome_id,book,book_role,type_id,category,subcategory,description,not_description,offer_value,type_value,outcome_name,outcome_fighter_slug,is_not,american,american_open,american_best,american_worst,source_offer_ts,source_created_at,source_change_age_h,offer_status,disabled,cycle_status\n"
            f"{stamp},1,{self.event},UFC Test,ufc,a-vs-b-1,p1,x1,BetOnline,sportsbook,TOTAL,A_5,A_13,Goes distance,,2.5,2.5,Yes,,0,-140,-120,+105,-160,{stamp},{stamp},0,O,0,complete\n",
        )

    def test_current_state_uses_only_healthy_snapshots(self):
        # Append archives must not affect the point-in-time state.
        (self.source / "fightodds_2099-12.csv").write_text("bad,archive\n", encoding="utf-8")
        result = build_current_state(self.source, self.output)
        frame = pd.read_parquet(self.output / "current_moneyline_quotes.parquet")
        self.assertEqual(result["rows"], 6)
        self.assertEqual(set(frame["source"]), {"bfo_live", "fightodds_live", "pinnacle_live"})
        self.assertFalse(frame["close_eligible"].any())
        self.assertFalse(frame.loc[frame["source"] == "bfo_live", "feature_eligible"].any())
        self.assertTrue(frame.loc[frame["source"] != "bfo_live", "feature_eligible"].all())
        self.assertEqual(set(result["source_health"]), {
            "bfo", "fightodds", "pinnacle", "fightodds_props"
        })

    def test_stale_cycle_fails_closed(self):
        path = self.source / "bfo_cycle_latest.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["poll_time"] = (self.poll - pd.Timedelta(hours=2)).isoformat()
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(SnapshotHealthError, "cycle age"):
            build_current_state(self.source, self.output)

    def test_snapshot_hash_mismatch_fails_closed(self):
        with (self.source / "pinnacle_snapshot_latest.csv").open("a", encoding="utf-8") as handle:
            handle.write("tampered\n")
        with self.assertRaisesRegex(SnapshotHealthError, "snapshot hash mismatch"):
            build_current_state(self.source, self.output)


if __name__ == "__main__":
    unittest.main()
