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
#     filename = f""

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

    def test_flight_convert_dover_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/dover_1_72hr_test.pdf",
            "hash": "132609787ac169c3cb3ac9d31e5d8f2ed7749ebe507ef639a5dc9e45bef6198a",
            "terminal": "Dover AFB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(5):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_dover_1_72hr/dover_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_dover_1_72hr/dover_1_72hr_test.pdf",
            s3_path="current/72_HR/dover_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1

            flights = fs.get_flights_by_terminal("Dover AFB Passenger Terminal")

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

            correct_num_flights = 5
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 5)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_elmendorf_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/elmendorf_1_72hr_test.pdf",
            "hash": "49211ed8c1818790dac1a9c0c3c4626f0b868a6321df6e1b92155d143e9449a5",
            "terminal": "Joint Base Elmendorf-Richardson Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(11):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_elmendorf_1_72hr/elmendorf_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_elmendorf_1_72hr/elmendorf_1_72hr_test.pdf",
            s3_path="current/72_HR/elmendorf_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1

            flights = fs.get_flights_by_terminal(
                "Joint Base Elmendorf-Richardson Passenger Terminal"
            )

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

            correct_num_flights = 11
            if len(test_flights) == correct_num_flights:
                break

            # Only retry once if we have some flights but not all
            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 11)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_fairchild_1_72hr(self: unittest.TestCase) -> None:
        """Test that a valid 72hr pdf with no flights is logged and able to be caught in Firestore.

        Because this this is a valid 72 hour PDF and has empty tables it should make it all the way to
        the Process-72hr-Flights Lambda.
        """
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_hash = "bccbc0e9c1406a0b9bc229a1b4998e9d92567020db91dbb065acec9db3c16455"

        pdf_doc = {
            "cloud_path": "current/72_HR/fairchild_1_72hr_test.pdf",
            "hash": pdf_hash,
            "terminal": "Fairchild AFB Air Transportation Function",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_fairchild_1_72hr/fairchild_1_72hr_test.pdf",
            s3_path="current/72_HR/fairchild_1_72hr_test.pdf",
        )

        # Wait for job to finish
        retries = 10
        while True:
            time.sleep(60)
            retries -= 1
            textract_jobs = fs.get_all_failed_proc_72_flights(
                lookback_seconds=3600, buffer_seconds=360
            )

            if not textract_jobs:
                if retries <= 0:
                    self.fail("No failed jobs found after 10 retries.")
                continue

            for job in textract_jobs:
                if job.get("pdf_hash", None) == pdf_hash:
                    # Delete job from Firestore
                    fs.delete_document_by_id("Textract_Jobs", job["job_id"])

                    self.assertEqual(job["finished_72hr_processing"], None)
                    break
            break

    def test_flight_convert_guantanamo_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/guantanamo_1_72hr_test.pdf",
            "hash": "7d8f2144fd712d8ae128185c4aebf39ba7e8780cb0d3a29871f4a2e940954a3f",
            "terminal": "NS Guantanamo Bay Passenger Terminal",
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
            filename = f"tests/end-to-end-test-assets/test_flight_guantanamo_1_72hr/guantanamo_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_guantanamo_1_72hr/guantanamo_1_72hr_test.pdf",
            s3_path="current/72_HR/guantanamo_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1

            flights = fs.get_flights_by_terminal("NS Guantanamo Bay Passenger Terminal")

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if flight_dict["date"] == "20230818":
                        test_flights.append(flight)

            correct_num_flights = 1
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 1)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_hickam_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firebase."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/hickam_1_72hr_test.pdf",
            "hash": "6be8e8ee18ec1c45fef1aa689aeb2dd33673963ef6d264c4bcb96f0967b9bcb9",
            "terminal": "Joint Base Pearl Harbor-Hickam Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(9):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_hickam_1_72hr/hickam_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_hickam_1_72hr/hickam_1_72hr_test.pdf",
            s3_path="current/72_HR/hickam_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1

            flights = fs.get_flights_by_terminal(
                "Joint Base Pearl Harbor-Hickam Passenger Terminal"
            )

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

            correct_num_flights = 9
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 9)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_incirlik_1_72hr(self: unittest.TestCase) -> None:
        """Test that a valid 72hr pdf with no flights is logged and able to be caught in Firestore.

        Because this this is a valid 72 hour PDF and has empty tables it should make it all the way to
        the Process-72hr-Flights Lambda.
        """
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_hash = "f16e10fa71e129b41d2f751d35e419c6082e4c522b18de0bdf4aedb4e13dccfc"

        pdf_doc = {
            "cloud_path": "current/72_HR/incirlik_1_72hr_test.pdf",
            "hash": pdf_hash,
            "terminal": "Incirlik AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_hash,
        )

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_incirlik_1_72hr/incirlik_1_72hr_test.pdf",
            s3_path="current/72_HR/incirlik_1_72hr_test.pdf",
        )

        # Wait for job to finish
        retries = 10
        while True:
            time.sleep(60)
            retries -= 1
            textract_jobs = fs.get_all_failed_proc_72_flights(
                lookback_seconds=3600, buffer_seconds=360
            )

            if not textract_jobs:
                if retries <= 0:
                    self.fail("No failed jobs found after 10 retries.")
                continue

            for job in textract_jobs:
                if job.get("pdf_hash", None) == pdf_hash:
                    # Delete job from Firestore
                    fs.delete_document_by_id("Textract_Jobs", job["job_id"])

                    self.assertEqual(job["finished_72hr_processing"], None)
                    break
            break

    def test_flight_convert_iwakuni_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/iwakuni_1_72hr_test.pdf",
            "hash": "837eada28683602428004ce9e2f43cfd8be65938ce257f9d8a9c0a54dbb7805d",
            "terminal": "MCAS Iwakuni Passenger Terminal",
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
            filename = f"tests/end-to-end-test-assets/test_flight_convert_iwakuni_1_72hr/iwakuni_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_iwakuni_1_72hr/iwakuni_1_72hr_test.pdf",
            s3_path="current/72_HR/iwakuni_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1

            flights = fs.get_flights_by_terminal("MCAS Iwakuni Passenger Terminal")

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if flight_dict["date"] == "20230819":
                        test_flights.append(flight)

            correct_num_flights = 1
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 1)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_kadena_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/kadena_1_72hr_test.pdf",
            "hash": "9283d6bf70bb6bc2467e9fc80ae90eeaa29c342906c12a3d6361974f26d08829",
            "terminal": "Kadena AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(5):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_kadena_1_72hr/kadena_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_kadena_1_72hr/kadena_1_72hr_test.pdf",
            s3_path="current/72_HR/kadena_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("Kadena AB Passenger Terminal")

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

            correct_num_flights = 5
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 5)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_little_rock_1_72hr(self: unittest.TestCase) -> None:
        """Test that a valid 72hr pdf who's tables do not get picked up by textract is logged and able to be caught in Firestore.

        Because this this is a valid 72 hour PDF and has empty tables it should make it all the way to
        the Process-72hr-Flights Lambda.
        """
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_hash = "f9b6700f3370343a997cc84abbe60c63d0cf577be996618fdae2abcc80bcecd5"

        pdf_doc = {
            "cloud_path": "current/72_HR/little_rock_1_72hr_test.pdf",
            "hash": pdf_hash,
            "terminal": "Little Rock AFB Air Transportation Function",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_hash,
        )

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_little_rock_1_72hr/little_rock_1_72hr_test.pdf",
            s3_path="current/72_HR/little_rock_1_72hr_test.pdf",
        )

        # Wait for job to finish
        retries = 10
        while True:
            time.sleep(60)
            retries -= 1
            textract_jobs = fs.get_all_failed_textract_to_tables(
                lookback_seconds=3600, buffer_seconds=360
            )

            if not textract_jobs:
                if retries <= 0:
                    self.fail("No failed jobs found after 10 retries.")
                continue

            for job in textract_jobs:
                if job.get("pdf_hash", None) == pdf_hash:
                    # Delete job from Firestore
                    fs.delete_document_by_id("Textract_Jobs", job["job_id"])

                    self.assertEqual(job["tables_parsed_finished"], None)
                    break
            break

    def test_flight_convert_macdill_1_72hr(self: unittest.TestCase) -> None:
        """Test that a valid 72hr pdf with no flights is logged and able to be caught in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_hash = "b36644af30645fd279fd9f5f1d4d3991af0c40d70f9d08386e4dd9841acfa602"

        pdf_doc = {
            "cloud_path": "current/72_HR/macdill_1_72hr_test.pdf",
            "hash": pdf_hash,
            "terminal": "MacDill AFB Air Transportation Function",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_hash,
        )

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_macdill_1_72hr/macdill_1_72hr_test.pdf",
            s3_path="current/72_HR/macdill_1_72hr_test.pdf",
        )

        # Wait for job to finish
        retries = 10
        while True:
            time.sleep(60)
            retries -= 1
            textract_jobs = fs.get_all_failed_proc_72_flights(
                lookback_seconds=3600, buffer_seconds=360
            )

            if not textract_jobs:
                if retries <= 0:
                    self.fail("No failed jobs found after 10 retries.")
                continue

            for job in textract_jobs:
                if job.get("pdf_hash", None) == pdf_hash:
                    # Delete job from Firestore
                    fs.delete_document_by_id("Textract_Jobs", job["job_id"])

                    self.assertEqual(job["finished_72hr_processing"], None)
                    break
            break

    def test_flight_convert_mcchord_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/mcchord_1_72hr_test.pdf",
            "hash": "586df8c1f7ad602d7465729c9b1f8a45d4ce348043150956f1a18d80dddfc2cf",
            "terminal": "Joint Base Lewis-McChord Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Load known good flights
        good_flights = []
        for i in range(8):
            filename = f"tests/end-to-end-test-assets/test_flight_convert_mcchord_1_72hr/mcchord_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_mcchord_1_72hr/mcchord_1_72hr_test.pdf",
            s3_path="current/72_HR/mcchord_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal(
                "Joint Base Lewis-McChord Passenger Terminal"
            )

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

            correct_num_flights = 8
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 8)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_mcconnell_1_72hr(self: unittest.TestCase) -> None:
        """Test that a valid 72hr pdf with no flights is logged and able to be caught in Firestore.

        Because this this is a valid 72 hour PDF and has empty tables it should make it all the way to
        the Process-72hr-Flights Lambda.
        """
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_hash = "ae71090ce52221b2f61840697f0d3d50b71933729e12db89e23a2f15d4ce4d67"

        pdf_doc = {
            "cloud_path": "current/72_HR/mcconnell_1_72hr_test.pdf",
            "hash": pdf_hash,
            "terminal": "McConnell AFB Air Transportation Function",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_hash,
        )

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_mcconnell_1_72hr/mcconnell_1_72hr_test.pdf",
            s3_path="current/72_HR/mcconnell_1_72hr_test.pdf",
        )

        # Wait for job to finish
        retries = 10
        while True:
            time.sleep(60)
            retries -= 1
            textract_jobs = fs.get_all_failed_proc_72_flights(
                lookback_seconds=3600, buffer_seconds=360
            )

            if not textract_jobs:
                if retries <= 0:
                    self.fail("No failed jobs found after 10 retries.")
                continue

            for job in textract_jobs:
                if job.get("pdf_hash", None) == pdf_hash:
                    # Delete job from Firestore
                    fs.delete_document_by_id("Textract_Jobs", job["job_id"])

                    self.assertEqual(job["finished_72hr_processing"], None)
                    break
            break

    def test_flight_convert_mcconnell_2_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/mcconnell_2_72hr_test.pdf",
            "hash": "6bd17c5fee5c770cb5d4629203d601e568009e3b71e0bef6c560eb028f32bad1",
            "terminal": "McConnell AFB Air Transportation Function",
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
            filename = f"tests/end-to-end-test-assets/test_flight_convert_mcconnell_2_72hr/mcconnell_2_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_mcconnell_2_72hr/mcconnell_2_72hr_test.pdf",
            s3_path="current/72_HR/mcconnell_2_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal(
                "McConnell AFB Air Transportation Function"
            )

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if flight_dict["date"] == "20231012":
                        test_flights.append(flight)

            correct_num_flights = 1
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 1)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_mcguire_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/mcguire_1_72hr_test.pdf",
            "hash": "2f089ced08a5903c1c5a2f4b08d7a63d4c6037934a64942e087e16cd2c28e3a3",
            "terminal": "Joint Base McGuire Dix Lakehurst Passenger Terminal",
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
            filename = f"tests/end-to-end-test-assets/test_flight_convert_mcguire_1_72hr/mcguire_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_mcguire_1_72hr/mcguire_1_72hr_test.pdf",
            s3_path="current/72_HR/mcguire_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal(
                "Joint Base McGuire Dix Lakehurst Passenger Terminal"
            )

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if flight_dict["date"] == "20230819":
                        test_flights.append(flight)

            correct_num_flights = 2
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue

            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 2)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_mildenhall_1_72hr(self: unittest.TestCase) -> None:
        """Test that a PDF properly converted to flights in Firestore."""
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/mildenhall_1_72hr_test.pdf",
            "hash": "45c869c14a5385ec5ff7193b245c563b6012c216071a81185dec90ecae09c7c2",
            "terminal": "RAF Mildenhall Passenger Terminal",
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
            filename = f"tests/end-to-end-test-assets/test_flight_convert_mildenhall_1_72hr/mildenhall_1_72hr_{i}.pkl"
            flight = Flight.load_state(filename=filename)

            if flight is None:
                self.fail(f"Failed to load {filename}")

            good_flights.append(flight)

        if not good_flights:
            self.fail("No good flights loaded.")

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_mildenhall_1_72hr/mildenhall_1_72hr_test.pdf",
            s3_path="current/72_HR/mildenhall_1_72hr_test.pdf",
        )

        test_flights = []
        max_retries = 15
        incomplete_get_retry = 2
        while True:
            max_retries -= 1
            flights = fs.get_flights_by_terminal("RAF Mildenhall Passenger Terminal")

            # Filter out non-test flights
            if flights:
                for flight in flights:
                    flight_dict = flight.to_dict()

                    if flight_dict["date"] == "20230820":
                        test_flights.append(flight)

            correct_num_flights = 2
            if len(test_flights) == correct_num_flights:
                break

            if test_flights and incomplete_get_retry > 0:
                incomplete_get_retry -= 1
                time.sleep(15)
                continue
            time.sleep(15)

        # Delete PDF document from Firestore
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

        # Delete testing flights from Firestore
        for flight in test_flights:
            flight_dict = flight.to_dict()
            fs.delete_flight_by_id(flight_dict["flight_id"])

        # Check that flights are equal
        self.assertEqual(len(test_flights), 2)

        # Sort flights by flight_id
        test_flights = sorted(test_flights, key=lambda x: x.flight_id)
        good_flights = sorted(good_flights, key=lambda x: x.flight_id)

        # Check that flights are equal
        for i, flight in enumerate(test_flights):
            self.assertEqual(flight, good_flights[i])

    def test_flight_convert_misawa_1_72hr(self: unittest.TestCase) -> None:
        """Test that a valid 72hr pdf with no flights is logged and able to be caught in Firestore.

        Because this this is a valid 72 hour PDF and has empty tables it should make it all the way to
        the Process-72hr-Flights Lambda.
        """
        s3_client = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_hash = "213a96cd478b3de2b5b08671b56ccfb175648d7d9c041d630c3fea56d6c156ab"

        pdf_doc = {
            "cloud_path": "current/72_HR/misawa_1_72hr_test.pdf",
            "hash": pdf_hash,
            "terminal": "Misawa AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_hash,
        )

        s3_client.upload_to_s3(
            local_path="tests/end-to-end-test-assets/test_flight_convert_misawa_1_72hr/misawa_1_72hr_test.pdf",
            s3_path="current/72_HR/misawa_1_72hr_test.pdf",
        )

        # Wait for job to finish
        retries = 10
        while True:
            time.sleep(60)
            retries -= 1
            textract_jobs = fs.get_all_failed_proc_72_flights(
                lookback_seconds=3600, buffer_seconds=360
            )

            if not textract_jobs:
                if retries <= 0:
                    self.fail("No failed jobs found after 10 retries.")
                continue

            for job in textract_jobs:
                if job.get("pdf_hash", None) == pdf_hash:
                    # Delete job from Firestore
                    fs.delete_document_by_id("Textract_Jobs", job["job_id"])

                    self.assertEqual(job["finished_72hr_processing"], None)
                    break
            break
