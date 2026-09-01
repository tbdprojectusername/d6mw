import unittest

from market_data_v3.src.common import load_registry, policy_for


class RegistryTests(unittest.TestCase):
    def test_miseojeu_is_permanently_inactive(self):
        policy = policy_for("Mise-O-Jeu+", load_registry())
        self.assertFalse(policy["capture_enabled"])
        self.assertFalse(policy["feature_eligible"])
        self.assertFalse(policy["close_eligible"])
        self.assertFalse(policy["execution_eligible"])

    def test_exchanges_never_define_close(self):
        for book in ("Polymarket", "Kalshi", "SXBet"):
            policy = policy_for(book, load_registry())
            self.assertEqual(policy["venue_type"], "exchange")
            self.assertFalse(policy["close_eligible"])

    def test_only_verified_primary_books_start_close_eligible(self):
        registry = load_registry()
        eligible = {
            policy_for(name, registry)["canonical_name"]
            for name in registry["books"]
            if policy_for(name, registry)["close_eligible"]
        }
        self.assertEqual(eligible, {"BetOnline", "Pinnacle"})


if __name__ == "__main__":
    unittest.main()

