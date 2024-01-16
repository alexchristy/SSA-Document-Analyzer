import unittest
import sys

sys.path.append("..")

# Tested function imports
from flight_utils import find_patriot_express, find_similar_dicts


class TestNoteExtractionUtils(unittest.TestCase):
    def test_extract_asterisk_note(self):
        from note_extract_utils import _extract_asterisk_notes

        single_note_test_data = [
            {"input": "*This is a note*", "expected": ["This is a note"]},
            {"input": "**This is a note**", "expected": ["This is a note"]},
            {
                "input": "***This is still a note***",
                "expected": ["This is still a note"],
            },
            {
                "input": "**********This would still be a note**********",
                "expected": ["This would still be a note"],
            },
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
                self.assertEqual(
                    set(_extract_asterisk_notes(test_case["input"])),
                    set(test_case["expected"]),
                )

    def test_extract_multiple_asterisk_notes(self):
        from note_extract_utils import _extract_asterisk_notes

        multiple_note_test_data = [
            {
                "input": "**PATRIOT EXPRESS** YOKOTA AIR BASE, JAPAN SEATTLE TACOMA WASHINGTON **SHOWTIME FOR BOOKED PASSENGERS 0730-1020L****EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L**",
                "expected": [
                    "PATRIOT EXPRESS",
                    "SHOWTIME FOR BOOKED PASSENGERS 0730-1020L",
                    "EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L",
                ],
            },
            {
                "input": "**PATRIOT EXPRESS** YOKOTA AIR BASE, JAPAN SEATTLE TACOMA WASHINGTON ** SHOWTIME FOR BOOKED PASSENGERS 0730-1020L** * *EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L **",
                "expected": [
                    "PATRIOT EXPRESS",
                    "SHOWTIME FOR BOOKED PASSENGERS 0730-1020L",
                    "EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L",
                ],
            },
            {
                "input": "This is note a note but ***This is a note ** * and this is also a note******",
                "expected": ["This is a note", "and this is also a note"],
            },
        ]

        for i, test_case in enumerate(multiple_note_test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    set(_extract_asterisk_notes(test_case["input"])),
                    set(test_case["expected"]),
                )

    def test_extract_parenthesis_notes(self):
        from note_extract_utils import _extract_parenthesis_notes

        test_data = [
            {
                "input": "This is not a note (This is a note) and this is also not a note",
                "expected": ["This is a note"],
            },
            {"input": "No notes here", "expected": []},
            {
                "input": "(First note) some text (Second note) more text",
                "expected": ["First note", "Second note"],
            },
            {
                "input": "(First note) (Second note) (Third note)",
                "expected": ["First note", "Second note", "Third note"],
            },
            {
                "input": "(  Extra spaces  ) only this should be trimmed",
                "expected": ["Extra spaces"],
            },
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    _extract_parenthesis_notes(test_case["input"]),
                    test_case["expected"],
                )

    def test_extract_notes(self):
        from note_extract_utils import extract_notes

        test_data = [
            {
                "input": "**PATRIOT EXPRESS** (First note) YOKOTA AIR BASE, JAPAN (Second note) SEATTLE TACOMA WASHINGTON",
                "expected": ["PATRIOT EXPRESS", "First note", "Second note"],
            },
            {
                "input": "**SHOWTIME FOR BOOKED PASSENGERS 0730-1020L** (Extra spaces) No notes here **EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L**",
                "expected": [
                    "SHOWTIME FOR BOOKED PASSENGERS 0730-1020L",
                    "EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L",
                    "Extra spaces",
                ],
            },
            {
                "input": "This is not a note **This is a note** (This is a note) and this is also not a note",
                "expected": ["This is a note", "This is a note"],
            },
            {
                "input": "**First asterisk note** (First parenthesis note) **Second asterisk note** (Second parenthesis note)",
                "expected": [
                    "First asterisk note",
                    "Second asterisk note",
                    "First parenthesis note",
                    "Second parenthesis note",
                ],
            },
            {
                "input": "  **Leading spaces**  (Leading spaces)  ",
                "expected": ["Leading spaces", "Leading spaces"],
            },
            {
                "input": "**Space between asterisks ** ( Space in parenthesis )",
                "expected": ["Space between asterisks", "Space in parenthesis"],
            },
            {
                "input": "**Multiple   spaces** (  Multiple   spaces  )",
                "expected": ["Multiple   spaces", "Multiple   spaces"],
            },
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    set(extract_notes(test_case["input"])), set(test_case["expected"])
                )


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
            {"input": "12345", "expected": None},
            {"input": "-123", "expected": None},
            {"input": "12.34", "expected": None},
            {"input": "Null", "expected": None},
            {"input": None, "expected": None},
            {"input": "24:00", "expected": None},
            {"input": "23:60", "expected": None},
            {"input": "2500", "expected": None},
            {"input": "12;00", "expected": None},
            {"input": "18 59", "expected": None},
            {"input": "12 :00", "expected": None},
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    parse_rollcall_time(test_case["input"]), test_case["expected"]
                )

    def test_parse_seat_data_single_data(self):
        from cell_parsing_utils import parse_seat_data

        test_data = [
            {"input": "60T", "expected": [[60, "T"]]},
            {"input": "T-60", "expected": [[60, "T"]]},
            {"input": "20 F", "expected": [[20, "F"]]},
            {"input": "T.20", "expected": [[20, "T"]]},
            {"input": "TBD", "expected": [[0, "TBD"]]},
            {"input": "", "expected": []},
            {"input": "0F", "expected": [[0, "F"]]},
            {"input": "0f", "expected": [[0, "F"]]},
            {"input": "0T", "expected": [[0, "T"]]},
            {"input": "T_100", "expected": [[100, "T"]]},
            {"input": "H-60", "expected": []},
            {"input": "60 H", "expected": []},
            {"input": "T.100.5", "expected": [[100, "T"]]},
            {"input": "TBD ", "expected": [[0, "TBD"]]},
            {"input": " 60T", "expected": [[60, "T"]]},
            {"input": "T8D ", "expected": [[0, "TBD"]]},
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    parse_seat_data(test_case["input"]), test_case["expected"]
                )

    def test_parse_seat_data_multiple_data(self):
        from cell_parsing_utils import parse_seat_data

        test_data = [
            {"input": "60T 20F", "expected": [[60, "T"], [20, "F"]]},
            {"input": "20F 60T", "expected": [[20, "F"], [60, "T"]]},
            {"input": "T-60 20F", "expected": [[60, "T"], [20, "F"]]},
            {"input": "20F T-60", "expected": [[20, "F"], [60, "T"]]},
            {"input": "T.60 20F", "expected": [[60, "T"], [20, "F"]]},
            {"input": "20F T.60", "expected": [[20, "F"], [60, "T"]]},
            {"input": "T-60 F.20", "expected": [[60, "T"], [20, "F"]]},
            {"input": "F.20 T-60", "expected": [[20, "F"], [60, "T"]]},
            {"input": "T.60 T.1 F.20", "expected": [[60, "T"], [1, "T"], [20, "F"]]},
            {
                "input": "T.60 T.1 F.20 T.100",
                "expected": [[60, "T"], [1, "T"], [20, "F"], [100, "T"]],
            },
            {"input": "TBD TBD", "expected": [[0, "TBD"], [0, "TBD"]]},
            {"input": "TDB TBD F0", "expected": [[0, "TBD"], [0, "TBD"], [0, "F"]]},
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    parse_seat_data(test_case["input"]), test_case["expected"]
                )

    def test_ocr_correction(self):
        from cell_parsing_utils import (
            ocr_correction,
        )  # Replace 'your_module' with the actual module name where `ocr_correction` resides

        test_data = [
            {"input": "OIlS", "expected": "0115"},  # Test OCR error characters
            {"input": "ZB", "expected": "28"},  # Test more OCR error characters
            {"input": "12345", "expected": "12345"},  # Test numbers
            {"input": "ABCDE", "expected": "A8CDE"},  # Test a mix of letters
            {"input": "", "expected": ""},  # Test empty string
            {"input": "O_I_l_S", "expected": "0_1_1_5"},  # Test underscores
            {"input": "O-I-l-S", "expected": "0-1-1-5"},  # Test dashes
        ]

        for i, test_case in enumerate(test_data):
            with self.subTest(i=i):
                self.assertEqual(
                    ocr_correction(test_case["input"]), test_case["expected"]
                )

    def test_parse_destination(self):
        from cell_parsing_utils import parse_destination

        test_cases = {
            "MCCHORD FLD, WA ": ["MCCHORD FLD, WA"],
            "MCCHORD FLD, WA (First note) (Second note)": ["MCCHORD FLD, WA"],
            "ANDERSEN AFB, GUAM ** SPACE-REQUIRED PASSENGERS ONLY**": [
                "ANDERSEN AFB, GUAM"
            ],
            "ANDERSEN AFB, GUAM ** SPACE-REQUIRED PASSENGERS ONLY** (First note) (Second note)": [
                "ANDERSEN AFB, GUAM"
            ],
            "**PATRIOT EXPRESS** YOKOTA AIR BASE, JAPAN SEATTLE TACOMA WASHINGTON **SHOWTIME FOR BOOKED PASSENGERS 0730-1020L** **EARLY BAGGAGE CHECK-IN FRIDAY 1600-1730L**": [
                "YOKOTA AIR BASE, JAPAN",
                "SEATTLE TACOMA WASHINGTON",
            ],
            "ALI AL SALEM AB - **RUMIL**_ ALIAL SALEM AB": [
                "ALI AL SALEM AB",
                "RUMIL",
                "ALI AL SALEM AB",
            ],
            "ALI AL SALEM AB - **RUMIL**_ ALIAL SALEM AB (First note) (Second note)": [
                "ALI AL SALEM AB",
                "RUMIL",
                "ALI AL SALEM AB",
            ],
            "PRINCE SULTAN BIN ABDULAZIZ INTL- MUFWAFFAQ AL SALTI AB": [
                "PRINCE SULTAN BIN ABDULAZIZ INTL",
                "MUFWAFFAQ AL SALTI AB",
            ],
            "Misawa AB, Japan Osan AB, Korea * Check in from 0130-0530L at the AMC Ticketing Counter*": [
                "MISAWA AB, JAPAN",
                "OSAN AB, KOREA",
            ],
            "Aviano Air Base, Italy (Space R pax only) Adana Air Base, Turkey (Space R pax only)": [
                "AVIANO AIR BASE, ITALY",
                "ADANA AIR BASE, TURKEY",
            ],
            "NO FLIGHTS": None,
            "NO FLIGHTS (First note) (Second note)": None,
            "***NO SCHEDULED DEPARTURES***": None,
            "NORFOLK ( Patriot Express )": ["NORFOLK"],
            "Kuwait, Kuwait": ["KUWAIT, KUWAIT"],
            "Djibouti, Djibouti": ["DJIBOUTI, DJIBOUTI"],
            "Mildenhall AFB, EU": ["MILDENHALL AFB, EU"],
            "**DUSHANBE** - **AMBOULIINTL**": ["DUSHANBE", "AMBOULI INTL"],
            "LAJES FIELD, AZO": ["LAJES FIELD, AZO"],
        }

        for input_data, expected_output in test_cases.items():
            with self.subTest(input=input_data, expected_output=expected_output):
                result = parse_destination(input_data)
                self.assertEqual(
                    result,
                    expected_output,
                    f"For {input_data}, expected {expected_output} but got {result}",
                )


class TestFindPatriotExpress(unittest.TestCase):
    def test_all_lower(self):
        self.assertTrue(find_patriot_express("patriotexpress"))

    def test_mixed_case(self):
        self.assertTrue(find_patriot_express("PatriotExpress"))

    def test_extra_spaces(self):
        self.assertTrue(find_patriot_express(" patriot   express "))

    def test_embedded_in_text(self):
        self.assertTrue(find_patriot_express("This includes patriot express!"))

    def test_with_numbers(self):
        self.assertTrue(find_patriot_express("patriotexpress1"))

    def test_empty_string(self):
        self.assertFalse(find_patriot_express(""))

    def test_incorrect_spelling(self):
        self.assertTrue(find_patriot_express("patrio express"))

    def test_non_valid_variations(self):
        self.assertFalse(find_patriot_express("pat exp"))

    def test_case_sensitive(self):
        self.assertTrue(find_patriot_express("PATRIOTEXPRESS"))

    def test_non_string_input(self):
        self.assertFalse(find_patriot_express(123456))


class TestFindSimilarDicts(unittest.TestCase):
    def setUp(self):
        # Setup common test data
        self.dict1 = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "key4": "value4",
        }
        self.dict2 = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "key5": "value5",
        }
        self.dict3 = {
            "key1": "value10",
            "key2": {"subkey1": "subvalue1"},
            "key3": "value30",
            "key4": "value40",
        }
        self.dict4 = {
            "key1": "value10",
            "key2": {"subkey1": "subvalue2"},
            "key3": "value30",
            "key4": "value40",
        }
        self.empty_dict = {}
        self.different_keys_dict = {
            "keyA": "valueA",
            "keyB": "valueB",
            "keyC": "valueC",
        }
        self.different_length_dict_2 = {"key1": "value1", "key2": "value2"}
        self.different_length_dict_3 = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }

        self.flight_dict_1 = {
            "date": "20210101",
            "rollcall_time": "1234",
            "patriot_express": True,
            "destination": ["YOKOTA AIR BASE, JAPAN", "SEATTLE TACOMA WASHINGTON"],
            "seats": [[60, "T"]],
        }

        self.flight_dict_2 = {
            "date": "20210101",
            "rollcall_time": "1234",
            "patriot_express": True,
            "destination": ["YOKOTA AIR BASE, JAPAN"],
            "seats": [[60, "T"]],
        }

        self.flight_dict_3 = {
            "date": "20210101",
            "rollcall_time": "1234",
            "patriot_express": True,
            "destination": ["YOKOTA AIR BASE, JAPAN", "SEATTLE TACOMA WASHINGTON"],
            "seats": [[60, "T"], [20, "F"]],
        }

    def test_basic_matching(self):
        result = find_similar_dicts([self.dict1], [self.dict1])
        self.assertEqual(result, [self.dict1])

    def test_partial_matching(self):
        result = find_similar_dicts([self.dict1], [self.dict2])
        self.assertEqual(result, [self.dict2])

    def test_nested_matching(self):
        result = find_similar_dicts([self.dict3], [self.dict3])
        self.assertEqual(result, [self.dict3])

    def test_mismatch(self):
        result = find_similar_dicts([self.dict1], [self.different_keys_dict])
        self.assertEqual(result, [])

    def test_empty_dict(self):
        result = find_similar_dicts([self.empty_dict], [self.dict1])
        self.assertEqual(result, [])

    def test_empty_list(self):
        result = find_similar_dicts([], [self.dict1])
        self.assertEqual(result, [])

    def test_non_dict_elements(self):
        with self.assertRaises(TypeError):
            find_similar_dicts(["not_a_dict"], [self.dict1])

    def test_different_keys(self):
        result = find_similar_dicts([self.dict1], [self.different_keys_dict])
        self.assertEqual(result, [])

    def test_variable_length_dicts(self):
        result = find_similar_dicts([self.dict1], [self.different_length_dict_2], 3)
        self.assertEqual(result, [])

    def test_variable_length_dicts_with_correct_num_match_elements(self):
        result = find_similar_dicts([self.dict1], [self.different_length_dict_3], 3)
        self.assertEqual(result, [self.different_length_dict_3])

    def test_matching_with_different_order(self):
        result = find_similar_dicts([self.dict2], [self.dict1])
        self.assertEqual(result, [self.dict1])

    def test_different_types(self):
        result = find_similar_dicts([self.dict3], [self.dict4])
        self.assertEqual(result, [self.dict4])

    def test_large_dicts(self):
        large_dict1 = {f"key{i}": f"value{i}" for i in range(1000)}
        large_dict2 = {f"key{i}": f"value{i}" for i in range(500)}
        result = find_similar_dicts(
            [large_dict1], [large_dict2], min_num_matching_keys=100
        )
        self.assertEqual(result, [large_dict2])

    def test_custom_min_matching_keys(self):
        result = find_similar_dicts([self.dict1], [self.dict2], min_num_matching_keys=4)
        self.assertEqual(result, [])

    def test_custom_min_matching_keys_with_matching(self):
        result = find_similar_dicts([self.dict1], [self.dict2], min_num_matching_keys=1)
        self.assertEqual(result, [self.dict2])

    def test_flight_data_dicts(self):
        result = find_similar_dicts([self.flight_dict_1], [self.flight_dict_2], 3)
        self.assertEqual(result, [self.flight_dict_2])

    def test_flight_data_dicts_with_different_seats(self):
        result = find_similar_dicts([self.flight_dict_1], [self.flight_dict_3], 3)
        self.assertEqual(result, [self.flight_dict_3])

    def test_flight_data_dicts_with_different_seats_stricter(self):
        result = find_similar_dicts([self.flight_dict_2], [self.flight_dict_3], 4)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()


if __name__ == "__main__":
    unittest.main()
