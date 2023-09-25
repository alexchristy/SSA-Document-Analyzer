import unittest
import sys
from datetime import datetime

sys.path.append("..")

class TestNoteExtractionUtils(unittest.TestCase):

    def test_extract_asterisk_note(self):

        from note_extract_utils import _extract_asterisk_notes

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
                self.assertEqual(set(_extract_asterisk_notes(test_case['input'])), set(test_case['expected']))

    def test_extract_multiple_asterisk_notes(self):

        from note_extract_utils import _extract_asterisk_notes

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
                self.assertEqual(set(_extract_asterisk_notes(test_case['input'])), set(test_case['expected']))

    def test_extract_parenthesis_notes(self):

        from note_extract_utils import _extract_parenthesis_notes

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
                self.assertEqual(_extract_parenthesis_notes(test_case["input"]), test_case["expected"])

    def test_extract_notes(self):

        from note_extract_utils import extract_notes

        test_data = [
            {
                'input': "**PATRIOT EXPRESS** (First note) YOKOTA AIR BASE, JAPAN (Second note) SEATTLE TACOMA WASHINGTON",
                'expected': ['PATRIOT EXPRESS', 'First note', 'Second note']
            },
            {
                'input': "**SHOWTIME FOR BOOKED PASSENGERS 0730-1020L** (Extra spaces) No notes here **EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L**",
                'expected': ['SHOWTIME FOR BOOKED PASSENGERS 0730-1020L', 'EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L', 'Extra spaces']
            },
            {
                'input': "This is not a note **This is a note** (This is a note) and this is also not a note",
                'expected': ['This is a note', 'This is a note']
            },
            {
                'input': "**First asterisk note** (First parenthesis note) **Second asterisk note** (Second parenthesis note)",
                'expected': ['First asterisk note', 'Second asterisk note', 'First parenthesis note', 'Second parenthesis note']
            },
            {
                'input': "  **Leading spaces**  (Leading spaces)  ",
                'expected': ['Leading spaces', 'Leading spaces']
            },
            {
                'input': "**Space between asterisks ** ( Space in parenthesis )",
                'expected': ['Space between asterisks', 'Space in parenthesis']
            },
            {
                'input': "**Multiple   spaces** (  Multiple   spaces  )",
                'expected': ['Multiple   spaces', 'Multiple   spaces']
            }
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(set(extract_notes(test_case['input'])), set(test_case['expected']))

class TestCellParsingUtils(unittest.TestCase):

    def test_parse_rollcall_time(self):

        from cell_parsing_utils import parse_rollcall_time

        test_data = [
            {"input": "1234", "expected": "1234"},
            {"input": "0000", "expected": "0000"},
            {"input": "2359", "expected": "2359"},
            {"input": "12:00", "expected": "1200"},
            {"input": "18:59", "expected": "1859"},
            {"input": "", "expected": None},
            {"input": "abcd", "expected": None},
            {"input": " 123", "expected": None},
            {"input": "12 34", "expected": None},
            {"input": "12345", "expected": "1234"},
            {"input": "-123", "expected": None},
            {"input": "12.34", "expected": None},
            {"input": "Null", "expected": None},
            {"input": None, "expected": None},
            {"input": "24:00", "expected": None},
            {"input": "23:60", "expected": None},
            {"input": "2500", "expected": None},
            {"input": "12;00", "expected": None},
            {"input": "18 59", "expected": None},
            {"input": "12 :00", "expected": None}
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(parse_rollcall_time(test_case["input"]), test_case["expected"])

    def test_parse_seat_data(self):

        from cell_parsing_utils import parse_seat_data

        test_data = [
            {"input": "60T", "expected": (60, "T")},
            {"input": "T-60", "expected": (60, "T")},
            {"input": "20 F", "expected": (20, "F")},
            {"input": "T.20", "expected": (20, "T")},
            {"input": "TBD", "expected": (0, "TBD")},
            {"input": "", "expected": (None, None)},
            {"input": "0F", "expected": (0, "F")},
            {"input": "0f", "expected": (0, "F")},
            {"input": "0T", "expected": (0, "T")},
            {"input": "T_100", "expected": (100, 'T')},
            {"input": "H-60", "expected": (None, None)},
            {"input": "60 H", "expected": (None, None)},
            {"input": "T.100.5", "expected": (None, None)},
            {"input": "TBD ", "expected": (0, "TBD")},
            {"input": " 60T", "expected": (None, None)}
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(parse_seat_data(test_case["input"]), test_case["expected"])

    def test_ocr_correction(self):
        
        from cell_parsing_utils import ocr_correction  # Replace 'your_module' with the actual module name where `ocr_correction` resides

        test_data = [
            {'input': 'OIlS', 'expected': '0115'},  # Test OCR error characters
            {'input': 'ZB', 'expected': '28'},  # Test more OCR error characters
            {'input': '12345', 'expected': '12345'},  # Test numbers
            {'input': 'ABCDE', 'expected': 'A8CDE'},  # Test a mix of letters
            {'input': '', 'expected': ''},  # Test empty string
            {'input': 'O_I_l_S', 'expected': '0_1_1_5'},  # Test underscores
            {'input': 'O-I-l-S', 'expected': '0-1-1-5'},  # Test dashes
        ]
        
        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(ocr_correction(test_case['input']), test_case['expected'])

    def test_parse_destination(self):

        from cell_parsing_utils import parse_destination

        test_cases = {
            'MCCHORD FLD, WA ': ['MCCHORD FLD, WA'],
            'MCCHORD FLD, WA (First note) (Second note)': ['MCCHORD FLD, WA'],
            'ANDERSEN AFB, GUAM ** SPACE-REQUIRED PASSENGERS ONLY**': ['ANDERSEN AFB, GUAM'],
            'ANDERSEN AFB, GUAM ** SPACE-REQUIRED PASSENGERS ONLY** (First note) (Second note)': ['ANDERSEN AFB, GUAM'],
            '**PATRIOT EXPRESS** YOKOTA AIR BASE, JAPAN SEATTLE TACOMA WASHINGTON **SHOWTIME FOR BOOKED PASSENGERS 0730-1020L** **EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L**': ['YOKOTA AIR BASE, JAPAN', 'SEATTLE TACOMA WASHINGTON'],
            'ALI AL SALEM AB - **RUMIL**_ ALIAL SALEM AB': ['ALI AL SALEM AB', 'RUMIL', 'ALI AL SALEM AB'],
            'ALI AL SALEM AB - **RUMIL**_ ALIAL SALEM AB (First note) (Second note)': ['ALI AL SALEM AB', 'RUMIL', 'ALI AL SALEM AB'],
            'PRINCE SULTAN BIN ABDULAZIZ INTL- MUFWAFFAQ AL SALTI AB': ['PRINCE SULTAN BIN ABDULAZIZ INTL', 'MUFWAFFAQ AL SALTI AB'],
            'Misawa AB, Japan Osan AB, Korea * Check in from 0130-0530L at the AMC Ticketing Counter*': ['MISAWA AB, JAPAN', 'OSAN AB, KOREA'],
            'Aviano Air Base, Italy (Space R pax only) Adana Air Base, Turkey (Space R pax only)': ['AVIANO AIR BASE, ITALY', 'ADANA AIR BASE, TURKEY'],
            'NO FLIGHTS': None,
            'NO FLIGHTS (First note) (Second note)': None,
            '***NO SCHEDULED DEPARTURES***': None,
        }
        
        for input_data, expected_output in test_cases.items():
            with self.subTest(input=input_data, expected_output=expected_output):
                result = parse_destination(input_data)
                self.assertEqual(result, expected_output, f"For {input_data}, expected {expected_output} but got {result}")

from flight_utils import reformat_date

class TestReformatDate(unittest.TestCase):
    def test_normal_date(self):
        current_date = datetime(2022, 12, 12)
        date_str = "12th December"
        expected_output = "20221212"
        self.assertEqual(reformat_date(date_str, current_date), expected_output)

    def test_early_january_date(self):
        current_date = datetime(2022, 12, 30)
        date_str = "3rd January"
        expected_output = "20230103"
        self.assertEqual(reformat_date(date_str, current_date), expected_output)

    def test_invalid_date(self):
        current_date = datetime(2022, 12, 12)
        date_str = "31st February"
        expected_output = date_str
        self.assertEqual(reformat_date(date_str, current_date), expected_output)

from flight_utils import create_datetime_from_str

class TestCreateDatetimeFromStr(unittest.TestCase):

    def test_valid_date(self):
        date_str = "20211231"
        expected_output = datetime(year=2021, month=12, day=31)
        self.assertEqual(create_datetime_from_str(date_str), expected_output)

    def test_invalid_date(self):
        date_str = "20211331"
        expected_output = None
        self.assertEqual(create_datetime_from_str(date_str), expected_output)

if __name__ == '__main__':
    unittest.main()