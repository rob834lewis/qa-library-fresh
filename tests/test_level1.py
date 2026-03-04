import sys
from pathlib import Path

def add_module_parent_to_syspath(module_name: str, start_file: str) -> None:
    """
    Ensure the directory containing module_name.{py|package} is on sys.path.
    Works regardless of whether you're running from VS Code, terminal, or ipykernel.
    """
    start = Path(start_file).resolve().parent

    # Look upward for either:
    #  - <dir>/<module_name>.py
    #  - <dir>/<module_name>/__init__.py
    for p in [start, *start.parents]:
        if (p / f"{module_name}.py").exists() or (p / module_name / "__init__.py").exists():
            sys.path.insert(0, str(p))
            return

    # Also check common case: module is inside tests/
    for p in [start, *start.parents]:
        tests_dir = p / "tests"
        if (tests_dir / f"{module_name}.py").exists() or (tests_dir / module_name / "__init__.py").exists():
            sys.path.insert(0, str(tests_dir))
            return

    raise ModuleNotFoundError(
        f"Could not locate '{module_name}' as {module_name}.py or {module_name}/__init__.py "
        f"starting from {start}"
    )

add_module_parent_to_syspath("calculator", __file__)

import unittest
from calculator import Calculator

class TestOperations(unittest.TestCase):

    def test_sum(self):
        myCalc = Calculator(2, 2)
        self.assertEqual(myCalc.get_add(), 4, "The answer is not 4!!")

    def test_subtraction(self):
        myCalc = Calculator(8, 2)
        self.assertEqual(myCalc.get_subtraction(), 6, "The answer is not 6!!")

    def test_multiplication(self):
        myCalc = Calculator(8, 2)
        self.assertEqual(myCalc.get_multiplication(), 16, "The answer is not 16!!")

    def test_division(self):
        myCalc = Calculator(8, 2)
        self.assertEqual(myCalc.get_division(), 4, "The answer is not 4!!")

if __name__ == '__main__':

    #unittest.main()

    unittest.main(argv=["first-arg-is-ignored"], exit=False)