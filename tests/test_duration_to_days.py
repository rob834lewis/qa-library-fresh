
import unittest
import pandas as pd

# adjust import if needed
from common.functions import duration_to_days


class TestDurationToDays(unittest.TestCase):

    def test_valid_day_units(self):

        self.assertEqual(duration_to_days("1 day"), 1)
        self.assertEqual(duration_to_days("5 days"), 5)

    def test_valid_week_units(self):

        self.assertEqual(duration_to_days("1 week"), 7)
        self.assertEqual(duration_to_days("2 weeks"), 14)

    def test_valid_month_units(self):

        self.assertEqual(duration_to_days("1 month"), 30)
        self.assertEqual(duration_to_days("3 months"), 90)

    def test_valid_year_units(self):

        self.assertEqual(duration_to_days("1 year"), 365)
        self.assertEqual(duration_to_days("2 years"), 730)

    def test_whitespace_and_case_handling(self):

        self.assertEqual(duration_to_days(" 2 Weeks "), 14)
        self.assertEqual(duration_to_days("3 DAYS"), 3)

    def test_invalid_format_returns_na(self):

        self.assertTrue(pd.isna(duration_to_days("ten days")))
        self.assertTrue(pd.isna(duration_to_days("5d")))
        self.assertTrue(pd.isna(duration_to_days("week 2")))
        self.assertTrue(pd.isna(duration_to_days("5 decades")))

    def test_missing_value_returns_na(self):
        
        self.assertTrue(pd.isna(duration_to_days(None)))
        self.assertTrue(pd.isna(duration_to_days(pd.NA)))


if __name__ == "__main__":

    unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)