import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

import duckdb

from market_data_v3.src.common import ROOT
from market_data_v3.src.bootstrap_history import bootstrap_fightodds
from market_data_v3.src.bootstrap_props import bootstrap_fightodds_props
from market_data_v3.src.build_catalog import build_catalog
from market_data_v3.src.ingest_live import discover, ingest_live
from market_data_v3.src.validate_store import validate_store


FIGHTODDS_HEADER = "poll_time,event_pk,pair,fight_slug,event_date,event_name,promotion,side1_key,side2_key,book,book_role,dec1,dec2,amer1,amer2,source_offer_ts,source_change_age_h,cycle_status\n"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "build").mkdir(exist_ok=True)
        self.source_obj = tempfile.TemporaryDirectory()
        self.work_obj = tempfile.TemporaryDirectory(dir=ROOT / "build")
        self.source = Path(self.source_obj.name)
        self.work = Path(self.work_obj.name)
        self.store = self.work / "store"
        self.manifests = self.work / "manifests"
        self.reports = self.work / "reports"

    def tearDown(self):
        self.source_obj.cleanup()
        self.work_obj.cleanup()

    def _write(self, name, text):
        (self.source / name).write_text(text, encoding="utf-8")

    def test_exact_feed_namespace_excludes_sidecars_and_miseojeu(self):
        self._write("fightodds_2026-08.csv", FIGHTODDS_HEADER)
        self._write("fightodds_notes_2026-08.csv", FIGHTODDS_HEADER)
        self._write("miseojeu_2026-08.csv", "x\n")
        found = discover(self.source)
        self.assertEqual([p.name for p in found["fightodds"]], ["fightodds_2026-08.csv"])
        self.assertFalse(any("miseojeu" in p.name for paths in found.values() for p in paths))

    def test_ingest_quarantine_and_validate(self):
        self._write(
            "fightodds_2026-08.csv",
            FIGHTODDS_HEADER
            + "2026-08-20T12:00:00+00:00,1,a|b,a-vs-b-1,2026-08-21,UFC Test,ufc,a,b,BetOnline,sportsbook,1.5000,2.8000,-200,+180,2026-08-20T11:59:00+00:00,0.02,complete\n"
            + "2026-08-20T12:00:00+00:00,1,a|b,a-vs-b-1,2026-08-21,UFC Test,ufc,a,b,Polymarket,exchange,1.4800,2.9000,-208,+190,2026-08-20T11:59:00+00:00,0.02,complete\n",
        )
        self._write(
            "quarantine_fightodds_2026-08.csv",
            FIGHTODDS_HEADER.rstrip("\n") + ",quarantine_reason\n"
            + "2026-08-20T12:00:00+00:00,1,a|b,a-vs-b-1,2026-08-21,UFC Test,ufc,a,b,Pinnacle,sportsbook,2.8000,1.5000,+180,-200,2026-08-20T11:59:00+00:00,0.02,complete,transposed\n",
        )
        self._write(
            "bfo_2026-08.csv",
            "poll_time,event_slug,event_name,matchup_id,side,selection,row_kind,book_id,book,american,move_arrow\n"
            "2026-08-20T12:00:00+00:00,ufc-test,UFC Test,9,1,A,moneyline,1,FanDuel,-200,\n",
        )
        self._write("bfo_events_2026-08.csv", "poll_time,event_slug,event_date\n2026-08-20T12:00:00+00:00,ufc-test,2026-08-21\n")
        self._write(
            "pinnacle_2026-08.csv",
            "poll_time,league_id,matchup_id,home,away,start_time,period,bet_type,side,line,american,is_alt,max_risk,currency_hint,cutoff_at,version\n"
            "2026-08-20T12:00:00+00:00,1,10,A,B,2026-08-21T20:00:00Z,0,ml,home,,-195,0,5000,account_ccy,2026-08-21T20:00:00Z,1\n"
            "2026-08-21T20:00:00+00:00,1,10,A,B,2026-08-21T20:00:00Z,0,ml,away,,+175,0,5000,account_ccy,2026-08-21T20:00:00Z,1\n",
        )
        self._write(
            "fightodds_props_2026-08.csv",
            "poll_time,event_pk,event_date,event_name,promotion,fight_slug,offer_id,outcome_id,book,book_role,type_id,category,subcategory,description,not_description,offer_value,type_value,outcome_name,outcome_fighter_slug,is_not,american,american_open,american_best,american_worst,source_offer_ts,source_created_at,source_change_age_h,offer_status,disabled,cycle_status\n"
            "2026-08-20T12:00:00+00:00,1,2026-08-21,UFC Test,ufc,a-vs-b-1,offer-1,outcome-1,BetOnline,sportsbook,TOTAL,A_5,A_13,Fight goes distance,,2.5,2.5,Yes,,0,-140,-120,+105,-160,2026-08-20T11:59:00+00:00,2026-08-19T11:00:00+00:00,0.02,O,0,complete\n",
        )
        result = ingest_live(self.source, self.store, self.manifests)
        self.assertEqual(result["rows"], 9)
        self.assertEqual(result["prop_rows"], 1)
        partition = next((self.store / "live_quotes").rglob("*.parquet"))
        first_mtime = partition.stat().st_mtime_ns
        repeated = ingest_live(self.source, self.store, self.manifests)
        self.assertFalse(any(item["changed"] for item in repeated["outputs"]))
        self.assertFalse(any(item["changed"] for item in repeated["prop_outputs"]))
        self.assertEqual(partition.stat().st_mtime_ns, first_mtime)
        report = validate_store(self.store, self.reports, self.work / "validation.duckdb")
        self.assertEqual(report["status"], "PASS")
        checks = {item["name"]: item for item in report["checks"]}
        for field in ("feature", "close", "execution"):
            check = checks[f"raw_live_rows_never_{field}_certified"]
            self.assertTrue(check["passed"])
            self.assertEqual(check["value"], 0)
        con = duckdb.connect(str(self.work / "validation.duckdb"), read_only=True)
        try:
            self.assertEqual(con.execute("select count(*) from live_quotes where venue_type='exchange' and close_eligible").fetchone()[0], 0)
            self.assertEqual(
                con.execute(
                    "select count(*) from live_quotes where "
                    "feature_eligible or close_eligible or execution_eligible"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                con.execute("select count(*) from book_close_candidate_live_quotes").fetchone()[0],
                3,
            )
            self.assertEqual(con.execute("select count(*) from quarantined_live_quotes").fetchone()[0], 3)
            post_cutoff = con.execute(
                "select record_status, quarantine_reason, market_phase, source_active, "
                "feature_eligible, close_eligible, execution_eligible "
                "from live_quotes where source='pinnacle_live' "
                "and try_cast(observed_at as timestamptz) >= try_cast(cutoff_at as timestamptz)"
            ).fetchone()
            self.assertEqual(
                post_cutoff,
                (
                    "quarantined", "observed_at_at_or_after_cutoff", "post_cutoff",
                    False, False, False, False,
                ),
            )
            self.assertEqual(con.execute("select count(*) from live_props_raw").fetchone()[0], 1)
            self.assertEqual(con.execute("select count(*) from live_props_raw where feature_eligible or close_eligible or execution_eligible").fetchone()[0], 0)
        finally:
            con.close()

    def test_history_preserves_unresolved_side_and_quarantines_null_price(self):
        db = self.source / "history.db"
        con = sqlite3.connect(db)
        try:
            con.executescript("""
                create table events(pk integer primary key, slug text, name text, date text, promotion text);
                create table fights(fight_slug text primary key, event_pk integer, fighter1 text, fighter2 text,
                                    fighter1_slug text, fighter2_slug text, is_cancelled integer);
                create table ticks(fight_slug text, book text, outcome_no integer, ts text, odds integer);
                insert into events values(1,'ufc-test','UFC Test','2020-01-01','ufc');
                insert into fights values('a-vs-b-1',1,'A','B','a','b',0);
                insert into ticks values('a-vs-b-1','BetOnline',1,'2020-01-01T20:00:00+00:00',-200);
                insert into ticks values('a-vs-b-1','BetOnline',2,'2020-01-01T20:00:00+00:00',null);
            """)
            con.commit()
        finally:
            con.close()
        result = bootstrap_fightodds(db, self.store, self.manifests, 2020, 2020)
        self.assertEqual(result["rows"], 2)
        catalog = self.work / "history_validation.duckdb"
        build_catalog(self.store, catalog)
        con = duckdb.connect(str(catalog), read_only=True)
        try:
            self.assertEqual(con.execute("select count(*) from history_ticks_raw where feature_eligible or close_eligible or execution_eligible").fetchone()[0], 0)
            self.assertEqual(con.execute("select count(*) from history_ticks_raw where record_status='invalid_price'").fetchone()[0], 1)
        finally:
            con.close()

    def test_prop_bootstrap_preserves_summaries_without_inventing_timing(self):
        db = self.source / "props.db"
        con = sqlite3.connect(db)
        try:
            con.executescript("""
                create table events(pk integer primary key, slug text, name text, date text, promotion text);
                create table fights(fight_slug text primary key, event_pk integer, fighter1 text, fighter2 text,
                                    fighter1_slug text, fighter2_slug text, is_cancelled integer);
                create table prop_offers(offer_id text primary key, fight_slug text, event_pk integer,
                    book text, sb_id text, status text, disabled integer, ts text, created_at text,
                    value text, type_id text, category text, subcategory text, description text,
                    not_description text, type_value text);
                create table prop_outcomes(outcome_id text primary key, offer_id text, name text,
                    is_not integer, fighter_slug text, odds integer, odds_open integer,
                    odds_best integer, odds_worst integer);
                insert into events values(1,'ufc-test','UFC Test','2020-01-01','ufc');
                insert into fights values('a-vs-b-1',1,'A','B','a','b',0);
                insert into prop_offers values('offer-1','a-vs-b-1',1,'BetOnline','7','O',0,
                    '2020-01-01T19:00:00Z','2020-01-01T12:00:00Z','2.5','TOTAL','A_5','A_13',
                    'Fight goes distance','','2.5');
                insert into prop_outcomes values('outcome-1','offer-1','Yes',0,null,-140,-120,-150,110);
                insert into prop_outcomes values('outcome-2','offer-1','No',1,null,120,100,130,-125);
            """)
            con.commit()
        finally:
            con.close()
        result = bootstrap_fightodds_props(db, self.store, self.manifests, 2020, 2020)
        self.assertEqual(result["rows"], 2)
        catalog = self.work / "props_validation.duckdb"
        build_catalog(self.store, catalog)
        con = duckdb.connect(str(catalog), read_only=True)
        try:
            row = con.execute(
                "select market_type, timing_status, available_to_model_at, "
                "feature_eligible, close_eligible, execution_eligible, price_american_open "
                "from history_props_raw where outcome_id='outcome-1'"
            ).fetchone()
            self.assertEqual(row[0], "prop")
            self.assertEqual(row[1], "source_offer_timestamps_present_no_price_ticks")
            self.assertIsNone(row[2])
            self.assertEqual(row[3:6], (False, False, False))
            self.assertEqual(row[6], -120)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
