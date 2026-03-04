
import unittest

from common.functions import date_reason

class TestDateReason(unittest.TestCase):

    def test_missing_none(self):

        self.assertEqual(date_reason(None), "Missing")

    def test_missing_empty_or_whitespace(self):

        self.assertEqual(date_reason(""), "Missing")
        self.assertEqual(date_reason("   "), "Missing")

    def test_unexpected_format(self):

        # wrong separator
        self.assertEqual(date_reason("01-02-2026"), "Unexpected format")

        # letters
        self.assertEqual(date_reason("aa/bb/cccc"), "Unexpected format")

        # not enough parts
        self.assertEqual(date_reason("01/02"), "Unexpected format")

        # extra parts
        self.assertEqual(date_reason("01/02/2026/99"), "Unexpected format")

        # time attached
        self.assertEqual(date_reason("01/02/2026 10:00"), "Unexpected format")

    def test_month_out_of_range(self):

        self.assertEqual(date_reason("01/00/2026"), "Month out of range")
        self.assertEqual(date_reason("01/13/2026"), "Month out of range")

    def test_day_out_of_range(self):

        self.assertEqual(date_reason("00/01/2026"), "Day out of range")
        self.assertEqual(date_reason("32/01/2026"), "Day out of range")

    def test_valid_format_but_invalid_date(self):

        # Function doesn’t validate day/month combinations (e.g. 31 Feb),
        # so these fall through to the final message.
        self.assertEqual(date_reason("31/02/2026"), "Invalid date (e.g., day/month mismatch)")
        self.assertEqual(date_reason("29/02/2025"), "Invalid date (e.g., day/month mismatch)")

    def test_stripping_whitespace(self):
        self.assertEqual(date_reason(" 01/02/2026 "), "Invalid date (e.g., day/month mismatch)")

if __name__ == "__main__":
    
    unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)