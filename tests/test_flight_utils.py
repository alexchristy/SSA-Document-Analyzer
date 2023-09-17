import unittest
import sys

sys.path.append("..")

class TestFlightUtils(unittest.TestCase):

    def test_extract_note(self):

        from flight_utils import extract_note

        test_data = [
            {"input": "*This is a note*", "expected": "This is a note"},
            {"input": "**This is a note**", "expected": "This is a note"},
            {"input": "***This is still a note***", "expected": "This is still a note"},
            {"input": "**********This would still be a note**********", "expected": "This would still be a note"},
            {"input": "* This is a note *", "expected": "This is a note"},
            {"input": "**This is a note * *", "expected": "This is a note"},
            {"input": "* *This is a note**", "expected": "This is a note"},
            {"input": "*** *This is a note ** * *", "expected": "This is a note"},
            {"input": "**This is a note*", "expected": "This is a note"},
            {"input": "**This is not a note", "expected": None},
            {"input": "*This is note a note", "expected": None},
            {"input": "***This is a note*", "expected": "This is a note"},
            {"input": "*This is a note***", "expected": "This is a note"},
            {"input": "*This is a note * *", "expected": "This is a note"},
            {"input": "***This is a note ***", "expected": "This is a note"},
            {"input": "*** *This is a note * **", "expected": "This is a note"},
        ]
        
        for i, test_case in enumerate(test_data):
            result = extract_note(test_case["input"])
            self.assertEqual(result, test_case["expected"], f"Test case {i+1} failed: got '{result}', expected '{test_case['expected']}'")
