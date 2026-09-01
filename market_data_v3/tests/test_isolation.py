import unittest
from pathlib import Path

from market_data_v3.src.common import ROOT, inside_root


class IsolationTests(unittest.TestCase):
    def test_write_guard_refuses_existing_model_namespaces(self):
        for relative in ("../aa/x", "../bb/x", "../data/x", "../x"):
            with self.assertRaises(ValueError):
                inside_root(ROOT / relative)

    def test_workflows_do_not_stage_existing_model_paths(self):
        workflows = ROOT.parent / ".github" / "workflows"
        for name in (
            "market-data-refresh.yml",
            "market-data-health.yml",
            "market-data-enrichment.yml",
            "market-data-reference-audit.yml",
        ):
            path = workflows / name
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("git add ..", text)
            self.assertNotIn("git add data", text)
            self.assertNotIn("git add market_forecast_v2", text)

    def test_durable_writer_validates_before_staging_output(self):
        path = ROOT.parent / ".github" / "workflows" / "market-data-refresh.yml"
        text = path.read_text(encoding="utf-8")
        ingest = text.index("market_data_v3.src.cli ingest-live")
        validate = text.index("market_data_v3.src.cli validate")
        stage = text.index("git add market_data_v3/store")
        self.assertLess(ingest, validate)
        self.assertLess(validate, stage)


if __name__ == "__main__":
    unittest.main()
