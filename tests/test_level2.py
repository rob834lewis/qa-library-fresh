
import unittest
from calculator import Calculator

class TestOperations(unittest.TestCase):

    def setUp(self):
        self.myCalc = Calculator(8, 2)

    def test_sum(self):
        self.assertEqual(self.myCalc.get_add(), 10, "The answer is not 10!!")

    def test_subtraction(self):
        self.assertEqual(self.myCalc.get_subtraction(), 6, "The answer is not 6!!")

    def test_multiplication(self):
        self.assertEqual(self.myCalc.get_multiplication(), 16, "The answer is not 16!!")

    def test_division(self):
        self.assertEqual(self.myCalc.get_division(), 4, "The answer is not 4!!")

if __name__ == '__main__':

    #unittest.main()

    unittest.main(argv=["first-arg-is-ignored"], exit=False)