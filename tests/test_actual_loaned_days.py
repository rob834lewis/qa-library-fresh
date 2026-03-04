# -*- coding: utf-8 -*-
"""
----------------------------------------------------------------------------------------------------------------------
  Written by      : Rob Lewis

  Date            : 03MAR2026

  Purpose         : Data pipeline

  Dependencies    :

  Module name    : pipeline

  Modifications
  -------------
  03MAR2026   RLEWIS  Initial Version
  04MAR2026   RLEWIS  Added deduping and argparse option
----------------------------------------------------------------------------------------------------------------------:
"""

import unittest
import pandas as pd

from common.functions import intck

class TestActualLoanedDays(unittest.TestCase):

    # ---
    # Day test
    # ---

    def test_actual_loaned_days_vectorised_day_diff(self):

        # dummy data for vectorised form

        df = pd.DataFrame(
            {
                "checkout_date": pd.to_datetime(
                    ["2026-03-01", "2026-03-01", "2025-12-31", "2024-02-28", "2026-03-10"]
                ),
                "return_date": pd.to_datetime(
                    ["2026-03-01", "2026-03-04", "2026-01-02", "2024-03-01", "2026-03-07"]
                ),
            }
        )

        # Calculate actual_loaned_days
        df["actual_loaned_days"] = intck("day", df["checkout_date"], df["return_date"])

        # Includes negative case (return before checkout) 
        self.assertEqual(df["actual_loaned_days"].tolist(), [0, 3, 2, 2, -3])

    def test_actual_loaned_days_scalar_inputs(self):

        # test for non vectorised form (string) 
        self.assertEqual(intck("day", "2026-03-01", "2026-03-04"), 3)

    # --- 
    # SHOULD REALLY BE TESTS HERE FOR OTHER TIME PERIODS
    # ---

if __name__ == "__main__":

    unittest.main(argv=["first-arg-is-ignored"], exit=False)