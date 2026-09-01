import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

from market_data_v3.src.build_catalog import build_catalog
from market_data_v3.src.common import ROOT
from market_data_v3.src.ingest_reference import ingest_reference
from market_data_v3.src.ingest_reference import _unique_columns
from market_data_v3.src.ingest_scorecards import _get, parse_decision_page
from market_data_v3.src.ingest_snapshots import capture_octagon_snapshot


SCORECARD_HTML = b"""<html><head><title>A One def. B Two :: UFC Test :: MMA Decisions</title></head><body>
<div>August 20, 2026</div>
<table><tr><td>Judge Alpha</td><td>Judge Alpha</td><td>Judge Alpha</td></tr><tr><td>ROUND</td><td>One</td><td>Two</td></tr><tr><td>1</td><td>10</td><td>9</td></tr><tr><td>2</td><td>9</td><td>10</td></tr><tr><td>3</td><td>10</td><td>9</td></tr><tr><td>TOTAL</td><td>29</td><td>28</td></tr></table>
<table><tr><td>Judge Beta</td><td>Judge Beta</td><td>Judge Beta</td></tr><tr><td>ROUND</td><td>One</td><td>Two</td></tr><tr><td>1</td><td>10</td><td>9</td></tr><tr><td>2</td><td>10</td><td>9</td></tr><tr><td>3</td><td>10</td><td>9</td></tr><tr><td>TOTAL</td><td>30</td><td>27</td></tr></table>
<table><tr><td>Judge Gamma</td><td>Judge Gamma</td><td>Judge Gamma</td></tr><tr><td>ROUND</td><td>One</td><td>Two</td></tr><tr><td>1</td><td>10</td><td>9</td></tr><tr><td>2</td><td>10</td><td>9</td></tr><tr><td>3</td><td>9</td><td>10</td></tr><tr><td>TOTAL</td><td>29</td><td>28</td></tr></table>
</body></html>"""


class EnrichmentTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "build").mkdir(exist_ok=True)
        self.work_obj = tempfile.TemporaryDirectory(dir=ROOT / "build")
        self.work = Path(self.work_obj.name)
        self.store = self.work / "store"
        self.manifests = self.work / "manifests"

    def tearDown(self):
        self.work_obj.cleanup()

    def test_reference_raw_layer_is_ineligible_and_hashed(self):
        source = self.work / "datalab"
        (source / "data/scorecards/OCR_parsed_scorecards").mkdir(parents=True)
        (source / "data/stats").mkdir(parents=True)
        (source / "data/scorecards/OCR_parsed_scorecards/SCORECARDS.csv").write_text(
            "red_fighter_name;blue_fighter_name;event_date\nA;B;01/01/2020\n",
            encoding="utf-8",
        )
        (source / "data/stats/stats_raw.csv").write_text(
            "red_fighter_name;blue_fighter_name;event_date\nA;B;01/01/2020\n",
            encoding="utf-8",
        )
        result = ingest_reference("ufc_datalab", source, "abc123", self.store, self.manifests)
        self.assertEqual(result["rows"], 2)
        catalog = self.work / "reference.duckdb"
        build_catalog(self.store, catalog)
        con = duckdb.connect(str(catalog), read_only=True)
        try:
            self.assertEqual(con.execute("select count(*) from reference_raw").fetchone()[0], 2)
            self.assertEqual(con.execute("select count(*) from reference_raw where feature_eligible").fetchone()[0], 0)
            self.assertEqual(con.execute("select count(distinct record_key) from reference_raw").fetchone()[0], 2)
        finally:
            con.close()

    def test_reference_column_normalization_preserves_pct_semantics(self):
        self.assertEqual(
            _unique_columns(["SIG.STR.", "SIG.STR.%", "TD.%"]),
            ["sig_str", "sig_str_pct", "td_pct"],
        )

    def test_scorecard_parser_reconciles_three_judges_by_round(self):
        rows = parse_decision_page(
            "https://mmadecisions.com/decision/999/A-One-vs-B-Two",
            SCORECARD_HTML,
            "2026-08-21T12:00:00+00:00",
        )
        self.assertEqual(len(rows), 9)
        self.assertEqual(len({row["judge_name"] for row in rows}), 3)
        self.assertEqual({row["judge_slot"] for row in rows}, {1, 2, 3})
        self.assertTrue(all(row["event_date"] == "2026-08-20" for row in rows))
        self.assertTrue(all(not row["feature_eligible"] for row in rows))

    def test_scorecard_parser_keeps_three_unknown_judge_slots_distinct(self):
        document = SCORECARD_HTML.replace(b"Judge Alpha", b"Unknown Judge").replace(
            b"Judge Beta", b"Unknown Judge"
        ).replace(b"Judge Gamma", b"Unknown Judge")
        rows = parse_decision_page(
            "https://mmadecisions.com/decision/1000/A-One-vs-B-Two",
            document,
            "2026-08-21T12:00:00+00:00",
        )
        self.assertEqual(len({row["record_key"] for row in rows}), 9)
        self.assertEqual({row["judge_slot"] for row in rows}, {1, 2, 3})

    def test_scorecard_parser_retains_totals_when_rounds_are_unavailable(self):
        document = SCORECARD_HTML.replace(
            b"<td>1</td><td>10</td><td>9</td>",
            b"<td>1</td><td>-</td><td>-</td>",
            1,
        )
        rows = parse_decision_page(
            "https://mmadecisions.com/decision/1001/A-One-vs-B-Two",
            document,
            "2026-08-21T12:00:00+00:00",
        )
        partial = [row for row in rows if row["record_status"] == "partial_total_only"]
        self.assertEqual(len(partial), 1)
        self.assertEqual(partial[0]["round"], 0)
        self.assertEqual((partial[0]["side1_total"], partial[0]["side2_total"]), (29, 28))

    @patch("market_data_v3.src.ingest_scorecards.urllib.request.urlopen")
    def test_scorecard_fetch_percent_encodes_unicode_paths(self, urlopen):
        response = MagicMock()
        response.read.return_value = b"ok"
        urlopen.return_value.__enter__.return_value = response
        _get("https://mmadecisions.com/decision/1/Marcos-Rogério-de-Lima")
        request = urlopen.call_args.args[0]
        self.assertIn("Rog%C3%A9rio", request.full_url)

    def test_snapshot_is_prospective_only(self):
        rankings = [{"categoryName": "Lightweight", "champion": {"championName": "A"}, "fighters": [{"name": "B"}]}]
        fighters = {"a": {"name": "A", "trainsAt": "Camp"}}
        result = capture_octagon_snapshot(
            rankings,
            fighters,
            "2026-08-21T12:00:00+00:00",
            self.store,
            self.manifests,
        )
        self.assertEqual(sum(x["rows"] for x in result["outputs"]), 3)
        catalog = self.work / "snapshots.duckdb"
        build_catalog(self.store, catalog)
        con = duckdb.connect(str(catalog), read_only=True)
        try:
            self.assertEqual(con.execute("select count(*) from prospective_snapshots where feature_eligible").fetchone()[0], 0)
            self.assertEqual(con.execute("select count(distinct snapshot_date) from prospective_snapshots").fetchone()[0], 1)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
