import unittest
import sys

sys.path.append("..")

class TestFlightUtils(unittest.TestCase):

    def test_extract_asterisk_note(self):

        from note_extract_utils import extract_asterisk_notes

        single_note_test_data = [
            {"input": "*This is a note*", "expected": ["This is a note"]},
            {"input": "**This is a note**", "expected": ["This is a note"]},
            {"input": "***This is still a note***", "expected": ["This is still a note"]},
            {"input": "**********This would still be a note**********", "expected": ["This would still be a note"]},
            {"input": "* This is a note *", "expected": ["This is a note"]},
            {"input": "**This is a note * *", "expected": ["This is a note"]},
            {"input": "* *This is a note**", "expected": ["This is a note"]},
            {"input": "*** *This is a note ** * *", "expected": ["This is a note"]},
            {"input": "**This is a note*", "expected": ["This is a note"]},
            {"input": "**This is not a note", "expected": []},
            {"input": "*This is note a note", "expected": []},
            {"input": "***This is a note*", "expected": ["This is a note"]},
            {"input": "*This is a note***", "expected": ["This is a note"]},
            {"input": "*This is a note * *", "expected": ["This is a note"]},
            {"input": "***This is a note ***", "expected": ["This is a note"]},
            {"input": "*** *This is a note * **", "expected": ["This is a note"]},
        ]
        
        for i, test_case in enumerate(single_note_test_data):
            with self.subTest(i=i):
                self.assertEqual(set(extract_asterisk_notes(test_case['input'])), set(test_case['expected']))

    def test_extract_multiple_asterisk_notes(self):

        from note_extract_utils import extract_asterisk_notes

        multiple_note_test_data = [
            {
                'input': "**PATRIOT EXPRESS** YOKOTA AIR BASE, JAPAN SEATTLE TACOMA WASHINGTON **SHOWTIME FOR BOOKED PASSENGERS 0730-1020L****EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L**",
                'expected': ['PATRIOT EXPRESS', 'SHOWTIME FOR BOOKED PASSENGERS 0730-1020L', 'EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L']
            },
            {
                'input': "**PATRIOT EXPRESS** YOKOTA AIR BASE, JAPAN SEATTLE TACOMA WASHINGTON ** SHOWTIME FOR BOOKED PASSENGERS 0730-1020L** * *EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L **",
                'expected': ['PATRIOT EXPRESS', 'SHOWTIME FOR BOOKED PASSENGERS 0730-1020L', 'EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L']
            },
            {
                'input': "This is note a note but ***This is a note ** * and this is also a note******",
                'expected': ['This is a note', 'and this is also a note']
            },
        ]

        for i, test_case in enumerate(multiple_note_test_data):
            with self.subTest(i=i):
                self.assertEqual(set(extract_asterisk_notes(test_case['input'])), set(test_case['expected']))

    def test_extract_parenthesis_notes(self):

        from note_extract_utils import extract_parenthesis_notes

        test_data = [
            {
                "input": "This is not a note (This is a note) and this is also not a note",
                "expected": ["This is a note"]
            },
            {
                "input": "No notes here",
                "expected": []
            },
            {
                "input": "(First note) some text (Second note) more text",
                "expected": ["First note", "Second note"]
            },
            {
                "input": "(First note) (Second note) (Third note)",
                "expected": ["First note", "Second note", "Third note"]
            },
            {
                "input": "(  Extra spaces  ) only this should be trimmed",
                "expected": ["Extra spaces"]
            }
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(extract_parenthesis_notes(test_case["input"]), test_case["expected"])
