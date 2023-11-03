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

# When pickling flights for testing use this snippet:
# import pickle

# for i, flight in enumerate(test_flights):
#     filename = f"tests/end-to-end-test-assets/test_flight_convert_al_udeid_1_72hr/al_udeid_1_72hr_{i}.pkl"

#     with open(filename, "wb") as f:
#         pickle.dump(flight, f)


class TestPdfToFlightsE2E(unittest.TestCase):
    """Unit tests that upload PDFs to S3 and check that the flights are properly extracted to Firebase."""

    def test_flight_convert_osan_1_72hr(self: unittest.TestCase) -> None:
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
            filename = f"tests/end-to-end-test-assets/test_flight_convert_osan_1_72hr/osan_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_osan_1_72hr/OSAN_72HR_02NOV2023.pdf",
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
                self.fail("No flights found after 15 retries.")

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
        """Test that a PDF with inference columns and paged Textract response is properly converted to flights in Firebase."""
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
                self.fail("No flights found after 15 retries.")

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

    def test_flight_convert_macdill_2_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF with the MacDill format can be properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/macdill_2_72hr_test.pdf",
            "hash": "1e0fb7e55f9ec87b89f3043d4a215ea96182e2257360bcfb2e7fa65eedc40664",
            "terminal": "MacDill AFB Air Transportation Function",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(2):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_macdill_2_72hr/macdill_2_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_macdill_2_72hr/macdill_2_72hr_test.pdf",
            s3_path="current/72_HR/macdill_2_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal(
                "MacDill AFB Air Transportation Function"
            )

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20231004"
                        or flight_dict["date"] == "20231005"
                    ):
                        test_flights.append(flight)

            correct_num_flights = 2
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 15 retries.")

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
        self.assertEqual(len(test_flights), 2)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_al_udeid_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/al_udeid_1_72hr_test.pdf",
            "hash": "79404d64c76c2d09aaa97b545cae86eb38d6a48a712fcab556d3072fbf0f0f86",
            "terminal": "Al Udeid AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(16):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_al_udeid_1_72hr/al_udeid_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_al_udeid_1_72hr/al_udeid_1_72hr_test.pdf",
            s3_path="current/72_HR/al_udeid_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("Al Udeid AB Passenger Terminal")

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

            correct_num_flights = 16
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 15 retries.")

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
        self.assertEqual(len(test_flights), 16)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_andersen_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/andersen_1_72hr_test.pdf",
            "hash": "74ce7556874b879caec2204e9a7bb961a608f7a0640ab1059c4c8017a3ce18f6",
            "terminal": "Andersen AFB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(2):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_andersen_1_72hr/andersen_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_andersen_1_72hr/andersen_1_72hr_test.pdf",
            s3_path="current/72_HR/andersen_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("Andersen AFB Passenger Terminal")

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20230819"
                        or flight_dict["date"] == "20230820"
                        or flight_dict["date"] == "20230831"
                    ):
                        test_flights.append(flight)

            correct_num_flights = 2
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 15 retries.")

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
        self.assertEqual(len(test_flights), 2)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        # good_flights = sorted(good_flights, key=lambda x: x.flight_id)

    def test_flight_convert_andrews_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/andrews_1_72hr_test.pdf",
            "hash": "4b959dc89d3d2a82b62b442536a4d79b4305e844438a70da032b71d522dd2b47",
            "terminal": "Joint Base Andrews Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(2):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_andrews_1_72hr/andrews_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_andrews_1_72hr/andrews_1_72hr_test.pdf",
            s3_path="current/72_HR/andrews_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal(
                "Joint Base Andrews Passenger Terminal"
            )

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20230818"
                        or flight_dict["date"] == "20230819"
                    ):
                        test_flights.append(flight)

            correct_num_flights = 2
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 15 retries.")

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
        self.assertEqual(len(test_flights), 2)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_bahrain_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/bahrain_1_72hr_test.pdf",
            "hash": "1bc438155bc238f8e6170a5242a467fe0540478ede55ff01d255c8358039ac2d",
            "terminal": "Bahrain Passenger Terminal",
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
            filename = f"tests/end-to-end-test-assets/test_flight_convert_bahrain_1_72hr/bahrain_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_bahrain_1_72hr/bahrain_1_72hr_test.pdf",
            s3_path="current/72_HR/bahrain_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("Bahrain Passenger Terminal")

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20230819"
                        or flight_dict["date"] == "20230820"
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
                self.fail("No flights found after 15 retries.")

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

    def test_flight_convert_bwi_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/bwi_1_72hr_test.pdf",
            "hash": "8ecea923213579f973ae5e7bf07ae18cc0d4e712426d2c8b9fff052fa6f5fc2b",
            "terminal": "Baltimore-Washinton International Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(3):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_bwi_1_72hr/bwi_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_bwi_1_72hr/bwi_1_72hr_test.pdf",
            s3_path="current/72_HR/bwi_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal(
                "Baltimore-Washinton International Passenger Terminal"
            )

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if (
                        flight_dict["date"] == "20230818"
                        or flight_dict["date"] == "20230820"
                    ):
                        test_flights.append(flight)

            correct_num_flights = 3
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 15 retries.")

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
        self.assertEqual(len(test_flights), 3)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        # good_flights = sorted(good_flights, key=lambda x: x.flight_id)

    def test_flight_convert_charleston_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/charleston_1_72hr_test.pdf",
            "hash": "afee8a587166db371bc85e11087f54bd70244ddb1f3fcf7405c1850109ebe848",
            "terminal": "Joint Base Charleston Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(1):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_charleston_1_72hr/charleston_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_charleston_1_72hr/charleston_1_72hr_test.pdf",
            s3_path="current/72_HR/charleston_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1

            flights = fs.get_flights_by_terminal(
                "Joint Base Charleston Passenger Terminal"
            )

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if flight_dict["date"] == "20230819":
                        test_flights.append(flight)

            correct_num_flights = 1
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            if max_retries <= 0:
                self.fail("No flights found after 15 retries.")

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check equal number of flights
        self.assertEqual(len(test_flights), 1)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])
