import unittest
from datetime import datetime

import pytz

from time_utils import get_local_time, pad_time_string  # Import your functions here


class TestGetLocalTime(unittest.TestCase):
    def test_valid_timezone(self):
        """Test getting local time with a valid timezone."""
        timezone = "UTC"
        result = get_local_time(timezone)
        self.assertIsInstance(result, datetime)

    def test_invalid_timezone(self):
        """Test getting local time with an invalid timezone."""
        with self.assertRaises(ValueError):
            get_local_time("Invalid-Timezone")

    def test_complex_timezone(self):
        """Test with a complex timezone format."""
        result = get_local_time("Asia/Kolkata")
        self.assertIsInstance(result, datetime)

    def test_timezone_with_dst(self):
        """Test with a timezone that includes DST changes."""
        result = get_local_time("America/New_York")
        self.assertIsInstance(result, datetime)

    def test_empty_timezone(self):
        """Test with an empty string as the timezone."""
        with self.assertRaises(ValueError):
            get_local_time("")

    def test_numeric_timezone(self):
        """Test with a numeric value as the timezone."""
        with self.assertRaises(ValueError):
            get_local_time(1234)


class TestPadTimeString(unittest.TestCase):
    def test_already_four_characters(self):
        """Test a string that is already 4 characters long."""
        self.assertEqual(pad_time_string("1234"), "1234")

    def test_less_than_four_characters(self):
        """Test padding a string less than 4 characters long."""
        self.assertEqual(pad_time_string("123"), "0123")

    def test_more_than_four_characters(self):
        """Test a string more than 4 characters long."""
        self.assertEqual(pad_time_string("12345"), "12345")

    def test_empty_string(self):
        """Test padding an empty string."""
        self.assertEqual(pad_time_string(""), "0000")

    def test_non_numeric_string(self):
        """Test with a non-numeric string."""
        self.assertEqual(pad_time_string("ab"), "00ab")

    def test_single_character(self):
        """Test with a single character string."""
        self.assertEqual(pad_time_string("1"), "0001")

    def test_none_input(self):
        """Test with None as input."""
        with self.assertRaises(AttributeError):  # str() on None raises AttributeError
            pad_time_string(None)

    def test_non_string_input(self):
        """Test with non-string types."""

        with self.assertRaises(AttributeError):
            pad_time_string(123)

        with self.assertRaises(AttributeError):
            pad_time_string(12.34)


if __name__ == "__main__":
    unittest.main()
