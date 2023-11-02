import os
import sys
import time
import unittest
import pickle

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from firestore_db import (  # noqa: E402 (Has to be imported after sys.path.append)
    FirestoreClient,
)
from flight import Flight  # noqa: E402 (Has to be imported after sys.path.append)
from s3_bucket import (  # noqa: E402 (Has to be imported after sys.path.append)
    S3Bucket,
)


class TestPdfToFlightsE2E(unittest.TestCase):
    """Unit tests that upload PDFs to S3 and check that the flights are properly extracted to Firebase."""

    def test_flight_convert_iwakuni_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF with a single flight is properly converted to a Firebase object."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        # Load known good flights
        good_flights = []
        for i in range(4):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_iwakuni_1_72hr/iwakuni_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_iwakuni_1_72hr/OSAN_72HR_02NOV2023.pdf",
            s3_path="current/72_HR/OSAN_72HR_02NOV2023.pdf",
        )

        max_retries = 10
        incomplete_get_retry = 1
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("Osan AB Passenger Terminal")

            correct_num_flights = 4
            if len(flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if (
                flights
                and len(flights) < correct_num_flights
                and incomplete_get_retry > 0
            ):
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 10 retries.")

            time.sleep(15)

        # Remove flights that aren't from the test PDF
        test_flights = []
        for flight in flights:
            flight_dict = flight.to_dict()

            if flight_dict["date"] == "20231102" or flight_dict["date"] == "20231104":
                test_flights.append(flight)

        self.assertEqual(len(test_flights), 4)

        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])
