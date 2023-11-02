import os
import sys
import time
import unittest
from typing import List

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

        # Create PDF document for Firestore
        pdf_doc = {
            "cloud_path": "current/72_HR/OSAN_72HR_02NOV2023.pdf",
            "hash": "867d4d5924e3834a5287f60572e8a5d7b6a5907aa0bdef2e9fb8c4877e26e120",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

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

        test_flights: List[Flight] = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("Osan AB Passenger Terminal")

            # Filter out non-test flights
            if flights:
                # Remove flights that aren't from the test PDF
                test_flights = []
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20231102"
                        or flight_dict["date"] == "20231104"
                    ):
                        test_flights.append(flight)

            correct_num_flights = 4
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 10 retries.")

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive",
            doc_id=pdf_doc["hash"],
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check equal number of flights
        self.assertEqual(len(test_flights), 4)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_sigonella_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF with inference columns properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        # Create PDF document for Firestore
        pdf_doc = {
            "cloud_path": "current/72_HR/sigonella_1_72hr_test.pdf",
            "hash": "1e0fb7e55f9ec87b89f3043d4a215ea96182e2257360bcfb2e7fa65eedc40664",
            "terminal": "NAS Sigonella Air Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(14):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_sigonella_1_72hr/sigonella_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_sigonella_1_72hr/sigonella_1_72hr_test.pdf",
            s3_path="current/72_HR/sigonella_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("NAS Sigonella Air Terminal")

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20230818"
                        or flight_dict["date"] == "20230819"
                        or flight_dict["date"] == "20230820"
                    ):
                        test_flights.append(flight)

            correct_num_flights = 14
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 10 retries.")

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive",
            doc_id=pdf_doc["hash"],
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check equal number of flights
        self.assertEqual(len(test_flights), 14)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])
