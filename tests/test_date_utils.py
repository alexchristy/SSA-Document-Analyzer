import unittest
from datetime import datetime

# Function tested imports
from flight_utils import reformat_date
from flight_utils import create_datetime_from_str
from table_utils import check_date_string

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

class TestCreateDatetimeFromStr(unittest.TestCase):

    def test_valid_date(self):
        date_str = "20211231"
        expected_output = datetime(year=2021, month=12, day=31)
        self.assertEqual(create_datetime_from_str(date_str), expected_output)

    def test_invalid_date(self):
        date_str = "20211331"
        expected_output = None
        self.assertEqual(create_datetime_from_str(date_str), expected_output)

class TestCheckDateString(unittest.TestCase):
    def test_valid_date_1(self):
        date_str = "1st January, 2022"
        self.assertTrue(check_date_string(date_str))

    def test_valid_date_2(self):
        date_str = "31st December, 2022"
        self.assertTrue(check_date_string(date_str))

    def test_valid_date_3(self):
        date_str = "15th Feb, 2022"
        self.assertTrue(check_date_string(date_str))

    def test_valid_date_4(self):
        date_str = "2022 March 15th"
        self.assertTrue(check_date_string(date_str))

    def test_invalid_date_2(self):
        date_str = "2022-13-31"
        self.assertFalse(check_date_string(date_str))

    def test_invalid_date_3(self):
        date_str = "2022/12/31"
        self.assertFalse(check_date_string(date_str))

    def test_edge_case_min_year(self):
        date_str = "1st January, 0001"
        self.assertTrue(check_date_string(date_str))

    def test_edge_case_max_year(self):
        date_str = "31st December, 9999"
        self.assertTrue(check_date_string(date_str))

    def test_fail_1(self):
        date_str = "2022-31-12"
        self.assertFalse(check_date_string(date_str))

    def test_fail_2(self):
        date_str = "2022/12/31"
        self.assertFalse(check_date_string(date_str))