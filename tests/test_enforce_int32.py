import unittest
import pandas as pd
import numpy as np

from common.functions import enforce_int32

class TestEnforceInt32(unittest.TestCase):

    def test_valid_integer_column(self):

        # Column already numeric should convert to int32
        df = pd.DataFrame({"id": [1, 2, 3]})

        result = enforce_int32(df, "id")

        self.assertEqual(result["id"].dtype, np.int32)
        self.assertListEqual(result["id"].tolist(), [1, 2, 3])


    def test_string_numbers_are_converted(self):

        # Numeric strings should convert successfully
        df = pd.DataFrame({"id": ["1", "2", "3"]})

        result = enforce_int32(df, "id")

        self.assertEqual(result["id"].dtype, np.int32)
        self.assertListEqual(result["id"].tolist(), [1, 2, 3])


    def test_non_numeric_values_raise_error(self):

        # Alphabetic values should raise ValueError
        df = pd.DataFrame({"id": ["1", "abc", "3"]})

        with self.assertRaises(ValueError):
            enforce_int32(df, "id")


    def test_decimal_values_raise_error(self):

        # Decimal values should raise ValueError
        df = pd.DataFrame({"id": [1.5, 2.0, 3.0]})

        with self.assertRaises(ValueError):
            enforce_int32(df, "id")


if __name__ == "__main__":

    unittest.main(argv=["first-arg-is-ignored"], exit=False)