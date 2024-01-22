import unittest
import sys

sys.path.append("..")

# Tested function imports
from flight import Flight


class TestFlightCreationTime(unittest.TestCase):
    """Test that the creation time of a flight is not changed from going from object to dict and back to object."""

    def setUp(self):
        self.flight_1 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"
        )

        if not self.flight_1:
            self.fail(
                "Failed to load al_udeid_1_72hr_table-1_flight-1.pkl in TestFlightCreationTime.setUp()"
            )

        self.flight_2 = Flight.load_state(
            "tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"
        )

        if not self.flight_2:
            self.fail(
                "Failed to load al_udeid_1_72hr_table-1_flight-2.pkl in TestFlightCreationTime.setUp()"
            )

    def test_flight_al_udeid_1_72hr_table_1_flight_1_creation_time(self):
        original_creation_time = self.flight_1.creation_time

        flight_dict = self.flight_1.to_dict()

        flight_dict_creation_time = flight_dict["creation_time"]

        flight = Flight.from_dict(flight_dict)

        flight_creation_time = flight.creation_time

        self.assertEqual(
            original_creation_time,
            flight_dict_creation_time,
            "Flight creation time changed when converting to dict.",
        )

        self.assertEqual(
            original_creation_time,
            flight_creation_time,
            "Flight creation time changed when converting from dict.",
        )

    def test_flight_al_udeid_1_72hr_table_1_flight_2_creation_time(self):
        original_creation_time = self.flight_2.creation_time

        flight_dict = self.flight_2.to_dict()

        flight_dict_creation_time = flight_dict["creation_time"]

        flight = Flight.from_dict(flight_dict)

        flight_creation_time = flight.creation_time

        self.assertEqual(
            original_creation_time,
            flight_dict_creation_time,
            "Flight creation time changed when converting to dict.",
        )

        self.assertEqual(
            original_creation_time,
            flight_creation_time,
            "Flight creation time changed when converting from dict.",
        )
