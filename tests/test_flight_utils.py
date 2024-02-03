import datetime
import sys
import unittest
from copy import deepcopy

sys.path.append("..")

# Tested function imports
from flight import Flight
from flight_utils import (
    count_matching_keys,
    find_patriot_express,
    find_similar_dicts,
    prune_recent_old_flights,
    sort_dicts_by_matching_keys,
)


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
            {"input": "9T / 5T / 5T", "expected": [[9, "T"], [5, "T"], [5, "T"]]},
            {
                "input": "9T / 5T / 5T / 5T",
                "expected": [[9, "T"], [5, "T"], [5, "T"], [5, "T"]],
            },
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
            "Baltimore Washington INT'L, MD Early Check-in available starting 26 December 2023, @0930L for Pre-Booked passengers on mission 1LT2 destined Baltimore Washington INT'L, MD": [
                "BALTIMORE WASHINGTON INT'L, MD"
            ],
            "*PATRIOT EXPRESS* / Iwakuni MCAS, Japan / Yokota AB, Japan / Seattle Tacoma INTL., WA / Early Bird: 18 Aug 0900-1400 Check In Starts: 19 Aug @ 0350 Doors Close: 19 Aug @ 0650": [
                "IWAKUNI MCAS, JAPAN",
                "YOKOTA AB, JAPAN",
                "SEATTLE TACOMA INTL., WA",
            ],
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

    def test_multiple_data_dicts_matching(self):
        result = find_similar_dicts(
            [self.dict1, self.dict3], [self.dict2, self.dict4], min_num_matching_keys=3
        )
        self.assertEqual(result, [self.dict2, self.dict4])

    def test_multiple_data_dicts_matching_diff_length_base_list(self):
        result = find_similar_dicts(
            [self.dict3], [self.dict2, self.dict4], min_num_matching_keys=3
        )
        self.assertEqual(result, [self.dict4])

    def test_multiple_data_dicts_matching_diff_length_compare_list(self):
        result = find_similar_dicts(
            [self.dict1, self.dict3], [self.dict4], min_num_matching_keys=3
        )
        self.assertEqual(result, [self.dict4])

    def test_return_maintains_original_order(self):
        result = find_similar_dicts(
            [self.dict1, self.dict3], [self.dict2, self.dict4], min_num_matching_keys=3
        )
        self.assertListEqual(result, [self.dict2, self.dict4])

    def test_return_maintains_original_order_reversed(self):
        result = find_similar_dicts(
            [self.dict2, self.dict4], [self.dict3, self.dict1], min_num_matching_keys=3
        )
        self.assertListEqual(result, [self.dict3, self.dict1])


class TestPruneSimilarOldFlights(unittest.TestCase):
    """Tests whether old flights that are too similar to new flights and the terminal updated too quickly are pruned."""

    def test_same_flights_diff_creation_times(self):
        """Test that all the old flights in this test are pruned.

        Two flights are loaded in. The old flights are the original pickled flights with their creation_time
        set to 1.5 hours ago. The new flights are the two flights with their creation_time set to the current time.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old__creation_time = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_formatted = old__creation_time.strftime("%Y%m%d%H%M")

        old_flight_1 = deepcopy(flight_1)
        old_flight_1.creation_time = old_creation_time_formatted

        old_flight_2 = deepcopy(flight_2)
        old_flight_2.creation_time = old_creation_time_formatted

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, [])
        self.assertCountEqual(removed_flights, old_flights)

    def test_same_flights_same_creation_times(self):
        """Test that all the old flights in this test are pruned.

        Two flights are loaded in. The old flights are the original pickled flights with their creation_time
        set to the current time. The new flights are the two flights with their creation_time set to the current time.

        Both the old flights should be pruned because they are too similar to the new flights and too recent.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_flight_1 = deepcopy(flight_1)
        old_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        old_flight_2 = deepcopy(flight_2)
        old_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, [])
        self.assertCountEqual(removed_flights, old_flights)

    def test_no_old_flights_pruned(self):
        """Test that all the old flights in this test are not pruned.

        Two flights are loaded in. The old flights are the original pickled flights with their original
        creation time some date in 2023. The new flights are the two flights with their creation_time set
        to the current time.

        Both the old flights should not be pruned because even though they are similar to the new flights,
        they are not recent enough.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_flight_1 = deepcopy(flight_1)

        old_flight_2 = deepcopy(flight_2)

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, old_flights)
        self.assertCountEqual(removed_flights, [])

    def test_more_new_flights_than_old_flights(self):
        """Test that all the one old flight in this test is pruned.

        Two flights are loaded in. The old flight is one of the original pickled flights with their original
        1.75 hour in the past hour creation_time. The new flights are the two flights with their creation_time set
        to the current time.

        The old flight should be pruned because it is too similar to only one of the new flights and too recent.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1 = deepcopy(flight_1)

        old_flight_1.creation_time = old_creation_time.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, [])
        self.assertCountEqual(removed_flights, old_flights)

    def test_flights_with_different_creation_times(self):
        """Test that all the old flights are pruned when the new flights and old flights have different creation times.

        The old flights are the original pickled flights with their creation_time set to 1.5 and 1.75 hours ago. The
        current flights are the two flights with their creation_time set to the current time and current time - 2 minutes.

        All old flights should be pruned because they are too similar to the new flights and too recent.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1 = deepcopy(flight_1)
        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")

        old_flight_2 = deepcopy(flight_2)
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = (
            current_time_utc - datetime.timedelta(minutes=2)
        ).strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, [])
        self.assertCountEqual(removed_flights, old_flights)

    def test_correct_time_bounds(self):
        """Test that the correct time bounds are used when pruning old flights.

        The old flights are the original pickled flights with their creation_time set to 1.95 hours ago and 2 hours ago.
        The current flights are the two flights with their creation time set to the current time. Only the first old
        flight should be pruned because it is too similar to the first new flight and too recent because the time bound
        is 2 hours.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.95)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=2)

        old_flight_1 = deepcopy(flight_1)
        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")

        old_flight_2 = deepcopy(flight_2)
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, flight_age_seconds=7200
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, [old_flight_2])
        self.assertCountEqual(removed_flights, [old_flight_1])

    def test_correct_time_bounds_with_custom_age(self):
        """Test that the correct time bounds are used when pruning old flights and a custom age is used.

        The old flights are the original pickled flights with their creation_time set to 6.25 hours ago and 6.3 hours ago.
        The current flights are the two flights with their creation time set to the current time. Only the first old
        flight should be pruned because it is too similar to the first new flight and too recent because the time bound
        is 6.25 hours.
        """
        flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=6.21)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=6.25)

        old_flight_1 = deepcopy(flight_1)
        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")

        old_flight_2 = deepcopy(flight_2)
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1 = deepcopy(flight_1)
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flight_2 = deepcopy(flight_2)
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, flight_age_seconds=22500
        )

        # Assert that only the first old flight was pruned
        self.assertCountEqual(pruned_old_flights, [old_flight_2])
        self.assertCountEqual(removed_flights, [old_flight_1])

    def test_zero_min_matching_keys_with_diff_flights(self):
        """Test that the old completely unrelated flights are pruned when min_num_matching_keys is 0.

        The old flights are the original pickled flights with their creation_time set to 1.5 and 1.75 hours ago. The
        current flights are the two flights with their creation_time set to the current time. All old flights should
        be pruned because they are too recent and min_num_matching_keys is 0.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/andersen_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_2 = Flight.load_state(
            "tests/flight-objects/andersen_1_72hr_table-2_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, min_num_match_keys=0, flight_age_seconds=7200
        )

        # Assert that all old flights were pruned
        self.assertCountEqual(pruned_old_flights, [])
        self.assertCountEqual(removed_flights, old_flights)

    def test_with_osan_1_72hr_flights(self):
        """Test that the old flight is not pruned because it is not similar enough to the new flight.

        Both flights are from Osan AB and are 72hr flights. The old flight is the original pickled flight and the
        new flight is the original pickled flight. The old flight should not be pruned because it is not similar.
        """
        # Load in pickled flights
        osan_1_72hr_flight_0 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_no_archive_tbd_rollcall_flights/osan_1_72hr_flight-0_fs.pkl"
        )

        if not osan_1_72hr_flight_0:
            self.fail("Failed to load flight 0 from pickle file")

        osan_1_72hr_flight_1 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_no_archive_tbd_rollcall_flights/osan_1_72hr_flight-1_fs.pkl"
        )

        if not osan_1_72hr_flight_1:
            self.fail("Failed to load flight 1 from pickle file")

        # Check that the flights are not similar
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            [osan_1_72hr_flight_0], [osan_1_72hr_flight_1]
        )

        self.assertCountEqual(pruned_old_flights, [osan_1_72hr_flight_0])
        self.assertCountEqual(removed_flights, [])

    def test_no_prune_when_no_similar_flights(self):
        """Test that the function does not prune any flights when there are no similar flights.

        One flight is from Al Udeid and the other is from Osan. Both are 72hr flights. Both flights have their
        creation time set to the current time. The old flight should not be pruned because it is not similar.
        """
        osan_flight = Flight.load_state(
            "tests/flight-objects/osan_1_72hr_table-2_flight-1.pkl"
        )

        if not osan_flight:
            self.fail("Failed to load osan flight from pickle file")

        al_udeid_flight = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        if not al_udeid_flight:
            self.fail("Failed to load al udeid flight from pickle file")

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Set both flights creation_time to the current time
        osan_flight.creation_time = current_time_utc.strftime("%Y%m%d%H%M")
        al_udeid_flight.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        # Check that the flights are not similar
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights=[osan_flight], new_flights=[al_udeid_flight]
        )

        self.assertCountEqual(pruned_old_flights, [osan_flight])
        self.assertCountEqual(removed_flights, [])

    def test_1_to_1_pruning_2_to_1(self):
        """Test that the function prunes flighs with 1 to 1 matches.

        This means that if there are two flights to Al Udeid and then the updated flight schedule has one flight to Osan,
        only one of the old flights should be pruned. Each new flight should only be matched to one old flight max.

        This test uses the same flight three times for both the old and new flights. The old flights are the original
        pickled flights with their creation_time set to 1.5 and 1.75 hours ago. The current flight is the original pickled
        flight with its creation_time set to the current time. Only one of the old flights should be pruned because
        of the 1:1 matching and they are too similar to the new flight and too recent.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, min_num_match_keys=3
        )

        # Assert that only one old flight was pruned
        self.assertEqual(len(pruned_old_flights), 1)

    def test_1_to_1_pruning_2_to_2(self):
        """Test that the function prunes flighs with 1 to 1 matches.

        This means that if there are two flights to Al Udeid and then the updated flight schedule has two flights to Osan,
        both of the old flights should be pruned. Each new flight should only be matched to one old flight max.

        This test uses the same flight three times for both the old and new flights. The old flights are the original
        pickled flights with their creation_time set to 1.5 and 1.75 hours ago. The current flights are the original pickled
        flight with its creation_time set to the current time. Both of the old flights should be pruned because
        of the 1:1 matching and they are too similar to the new flight and too recent.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")
        new_flight_2.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, min_num_match_keys=3
        )

        # Assert that only one old flight was pruned
        self.assertCountEqual(pruned_old_flights, [])
        self.assertCountEqual(removed_flights, old_flights)

    def test_1_to_1_pruning_3_to_2(self):
        """Test that the function prunes flighs with 1 to 1 matches.

        This means that if there are three flights to Al Udeid and then the updated flight schedule has two updated flights to Osan,
        only two of the old flights should be pruned. Each new flight should only be matched to one old flight max.

        This test uses the same flight three times for the old and two for new flights. The old flights are the original
        pickled flights with their creation_time set to 1.5, 1.75, and 1.9 hours ago. The current flights are the original pickled
        flight with their creation_time set to the current time. Only two of the old flights should be pruned because
        of the 1:1 matching and they are too similar to the new flight and too recent.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_3 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)
        old_creation_time_3 = current_time_utc - datetime.timedelta(hours=1.9)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")
        old_flight_3.creation_time = old_creation_time_3.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2, old_flight_3]

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1, new_flight_2]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, min_num_match_keys=3
        )

        # Assert that only one old flight was pruned
        self.assertEqual(len(pruned_old_flights), 1)
        self.assertEqual(len(removed_flights), 2)

    def test_default_priority_prune_key(self):
        """Test that the old flight who's priority_prune_key value is the same as the new flight's key is pruned over the other old flight who's priority_prune_key value is different.

        This test uses the default priority_prune_key value of "destination". Both old flights and the new flight come from the same pickled flight object.
        Old flight 1 has it's seat value changed to be different from the new flight's seat value. Old flight 2 has it's destination value changed to be
        different from the new flight's destination value. Since the priority_prune_key is "destination", old flight 1 should be removed because it's
        destination value is the same as the new flight's destination and thus more similar and more likely to be the same flight as the new flight.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Change old_flight_1's seats value
        old_flight_1.seats = [{"number": 666, "status": "F"}]
        old_flight_1.as_string = old_flight_1.generate_as_string()
        old_flight_1.flight_id = old_flight_1.generate_flight_id()

        # Change old_flight_2's destination value
        old_flight_2.destinations = ["YOKOTA AIR BASE, JAPAN"]
        old_flight_2.as_string = old_flight_2.generate_as_string()
        old_flight_2.flight_id = old_flight_2.generate_flight_id()

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, min_num_match_keys=3
        )

        # Assert that only one old flight 1 was pruned
        self.assertCountEqual(pruned_old_flights, [old_flight_2])
        self.assertCountEqual(removed_flights, [old_flight_1])

    def test_custom_priority_prune_key(self):
        """Test that the old flight who's priority_prune_key value is the same as the new flight's key is pruned over the other old flight who's priority_prune_key value is different.

        This test uses the custom priority_prune_key value of "seats". Both old flights and the new flight come from the same pickled flight object.
        Old flight 1 has it's seat value changed to be different from the new flight's seat value. Old flight 2 has it's destination value changed to be
        different from the new flight's destination value. Since the priority_prune_key is "seats", old flight 2 should be pruned because it's
        seats value is the same as the new flight's destination and thus treated as more similar and in this scenario likely to be the same flight
        as the new flight.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Change old_flight_1's seats value
        old_flight_1.seats = [{"number": 666, "status": "F"}]
        old_flight_1.as_string = old_flight_1.generate_as_string()
        old_flight_1.flight_id = old_flight_1.generate_flight_id()

        # Change old_flight_2's destination value
        old_flight_2.destinations = ["YOKOTA AIR BASE, JAPAN"]
        old_flight_2.as_string = old_flight_2.generate_as_string()
        old_flight_2.flight_id = old_flight_2.generate_flight_id()

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights, new_flights, min_num_match_keys=3, priority_prune_key="seats"
        )

        # Assert that only one old flight 1 was pruned
        self.assertCountEqual(pruned_old_flights, [old_flight_1])
        self.assertCountEqual(removed_flights, [old_flight_2])

    def test_many_priorty_prune_key_match(self):
        """Test that the most similar flight is pruned when there is more than one flight with the same priority_prune_key value.

        If there are two similar old flights and both have the same priority_prune_key value as the new flight, only one should be pruned.
        In this case, of the two old flights, the one that is most similar to the new flight should be pruned.

        In this test, both old flights and the new flight come from the same pickled flight object. This means that both the old flights
        have the same destination value as the new flight. Since we are using "destinations" as the priority_prune_key, both old flights
        will match the priority_prune_key. However, we change the seats value of Old flight 1 to be different from the new flight's seats value.
        This means that old flight 2 is more similar to the new flight and should be removed.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Change old_flight_1's seats  to make it less similar to the new flight
        old_flight_1.seats = [{"number": 666, "status": "F"}]
        old_flight_1.as_string = old_flight_1.generate_as_string()
        old_flight_1.flight_id = old_flight_1.generate_flight_id()

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights,
            new_flights,
            min_num_match_keys=3,
            priority_prune_key="destinations",
        )

        # Assert that only one old flight 1 was pruned
        self.assertCountEqual(pruned_old_flights, [old_flight_1])
        self.assertCountEqual(removed_flights, [old_flight_2])

    def test_no_priority_prune_key_match(self):
        """Test that the most similar flight is pruned when none of the similar old flights have the same priority_prune_key value as the new flight.

        If there are two similar old flights and neither have the same priority_prune_key value as the new flight, only one should be pruned.
        In this case, of the two old flights, the one that is most similar to the new flight should be pruned.

        In this test, both old flights and the new flight come from the same pickled flight object. This means that both the old flights
        have the same destination value as the new flight. So we change the destination value of both old flights to be different from the
        new flight's destination value. This means that neither old flight will match the priority_prune_key. However, we change the seats
        value of Old flight 1 to be different from the new flight's seats value. This means that old flight 2 is more similar to the new
        flight and should be removed.
        """
        old_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        old_flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        new_flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        current_time_utc = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create old_flights list
        old_creation_time_1 = current_time_utc - datetime.timedelta(hours=1.5)
        old_creation_time_2 = current_time_utc - datetime.timedelta(hours=1.75)

        old_flight_1.creation_time = old_creation_time_1.strftime("%Y%m%d%H%M")
        old_flight_2.creation_time = old_creation_time_2.strftime("%Y%m%d%H%M")

        old_flights = [old_flight_1, old_flight_2]

        # Change old_flight_1's seats  to make it less similar to the new flight
        # to make sure it is not removed
        old_flight_1.seats = [{"number": 666, "status": "F"}]
        old_flight_1.destinations = ["YOKOTA AIR BASE, JAPAN"]
        old_flight_1.as_string = old_flight_1.generate_as_string()
        old_flight_1.flight_id = old_flight_1.generate_flight_id()

        # Change old_flight_2's destinations
        old_flight_2.destinations = ["YOKOTA AIR BASE, JAPAN"]
        old_flight_2.as_string = old_flight_2.generate_as_string()
        old_flight_2.flight_id = old_flight_2.generate_flight_id()

        # Create new_flights list
        new_flight_1.creation_time = current_time_utc.strftime("%Y%m%d%H%M")

        new_flights = [new_flight_1]

        # Prune old flights
        pruned_old_flights, removed_flights = prune_recent_old_flights(
            old_flights,
            new_flights,
            min_num_match_keys=2,
            priority_prune_key="destinations",
        )

        # Assert that only one old flight 2 was removed
        self.assertCountEqual(pruned_old_flights, [old_flight_1])
        self.assertCountEqual(removed_flights, [old_flight_2])


class TestCountMatchingKeys(unittest.TestCase):
    def test_full_match(self):
        self.assertEqual(count_matching_keys({"a": 1, "b": 2}, {"a": 1, "b": 2}), 2)

    def test_partial_match(self):
        self.assertEqual(
            count_matching_keys({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 4, "d": 3}), 1
        )

    def test_no_match(self):
        self.assertEqual(count_matching_keys({"a": 1, "b": 2}, {"c": 3, "d": 4}), 0)

    def test_both_empty(self):
        self.assertEqual(count_matching_keys({}, {}), 0)

    def test_one_empty(self):
        self.assertEqual(count_matching_keys({}, {"a": 1, "b": 2}), 0)


class TestSortDictsByMatchingKeys(unittest.TestCase):
    def setUp(self):
        self.reference_dict = {"a": 1, "b": 2, "c": 3}
        self.edge_case_reference_dict = {"a": 1, "b": "text", "c": {"d": 4}}

    def test_various_matching(self):
        dicts = [{"a": 1, "b": 2}, {"a": 1, "d": 4}, {"a": 1, "b": 2, "c": 3}, {"e": 5}]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.reference_dict)
        self.assertEqual(
            sorted_dicts,
            [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2}, {"a": 1, "d": 4}, {"e": 5}],
        )

    def test_no_matches(self):
        dicts = [{"d": 4, "e": 5}, {"f": 6}]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.reference_dict)
        self.assertEqual(sorted_dicts, [{"d": 4, "e": 5}, {"f": 6}])

    def test_empty_list(self):
        dicts = []
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.reference_dict)
        self.assertEqual(sorted_dicts, [])

    def test_full_matches(self):
        dicts = [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 3}]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.reference_dict)
        self.assertEqual(
            sorted_dicts, [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2, "c": 3}]
        )

    def test_identical_dicts(self):
        dicts = [
            {"a": 1, "b": "text", "c": {"d": 4}},
            {"a": 1, "b": "text", "c": {"d": 4}},
        ]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.edge_case_reference_dict)
        self.assertEqual(
            sorted_dicts,
            [
                {"a": 1, "b": "text", "c": {"d": 4}},
                {"a": 1, "b": "text", "c": {"d": 4}},
            ],
        )

    def test_empty_reference(self):
        dicts = [{"a": 1, "b": 2}, {"c": 3, "d": 4}]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, {})
        self.assertEqual(
            sorted_dicts, [{"a": 1, "b": 2}, {"c": 3, "d": 4}]
        )  # Order remains the same as input

    def test_mixed_types(self):
        dicts = [{"a": 1, "b": "text"}, {"a": 1, "b": 2, "c": 3}]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.edge_case_reference_dict)
        self.assertEqual(
            sorted_dicts, [{"a": 1, "b": "text"}, {"a": 1, "b": 2, "c": 3}]
        )  # 'b': 'text' is a match

    def test_nested_dicts(self):
        dicts = [
            {"a": 1, "b": "text", "c": {"d": 5}},
            {"a": 2, "b": "text", "c": {"d": 4}},
        ]
        sorted_dicts = sort_dicts_by_matching_keys(dicts, self.edge_case_reference_dict)
        self.assertEqual(
            sorted_dicts,
            [
                {"a": 1, "b": "text", "c": {"d": 5}},
                {"a": 2, "b": "text", "c": {"d": 4}},
            ],
        )  # 'c': {'d': 4} is a non-match


if __name__ == "__main__":
    unittest.main()
