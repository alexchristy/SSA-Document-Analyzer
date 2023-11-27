import json
import time
import unittest
from datetime import datetime
from typing import cast

from aws_utils import initialize_client
from firestore_db import FirestoreClient
from flight import Flight


class TestEndToEnd(unittest.TestCase):
    """Tests that all the lambda functions work together properly.

    This class will not be as comprehensive as the unit tests combined but it will check that the
    flights at the end are correct. Additionally, it will check that the lambda functions are properly
    writing to the database. There will be tests for each type of general type of terminal PDF format.
    """

    def test_e2e_macdill_2_72hr(self: unittest.TestCase) -> None:
        """Tests the end to end functionality for parsing the macdill_2_72hr PDF."""
        pdf_archive_coll = "**TESTING-E2E**_PDF_Archive"
        terminal_coll = "**TESTING-E2E**_Terminals"
        current_flights_coll = "**TESTING-E2E**_Current_Flights"
        archive_flights_coll = "**TESTING-E2E**_Archive_Flights"
        textract_jobs_coll = "Textract_Jobs"

        test_date = "202310050347"  # 2023-10-05 03:47:00

        lambda_client = initialize_client("lambda")

        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            flight_current_coll=current_flights_coll,
            flight_archive_coll=archive_flights_coll,
            textract_jobs_coll=textract_jobs_coll,
        )

        pdf_doc = {
            "cloud_path": "tests/macdill_2_72hr_test.pdf",
            "hash": "828aad36ed85b1ac0bf19a5cd77f198d9f65cb74c7d2ef1ba5e2e9baa7fc8f43",
            "terminal": "MacDill AFB Air Transportation Function",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            doc_id=pdf_doc["hash"],
            document_data=pdf_doc,
        )

        terminal_doc = {
            "name": "MacDill AFB Air Transportation Function",
            "location": "MacDill AFB, FL",
            "group": "AMC CONUS TERMINALS",
            "archiveDir": "tests",
            "timezone": "America/New_York",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        # Load in previous flights in the current flights collection
        # to test that the archive flights collection is properly updated
        # by the Store-Flights lambda function
        macdill_2_72hr_flight_1 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_macdill_2_72hr/macdill_2_72hr_table-1_flight-1.pkl"
        )

        if not macdill_2_72hr_flight_1:
            self.fail("Flight 1 was empty")

        macdill_2_72hr_flight_2 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_macdill_2_72hr/macdill_2_72hr_table-1_flight-2.pkl"
        )

        if not macdill_2_72hr_flight_2:
            self.fail("Flight 2 was empty")

        macdill_2_72hr_flight_1.make_firestore_compliant()
        macdill_2_72hr_flight_2.make_firestore_compliant()

        previous_flights = [
            macdill_2_72hr_flight_1.to_dict(),
            macdill_2_72hr_flight_2.to_dict(),
        ]

        for flight_dict in previous_flights:
            fs.insert_document_with_id(
                collection_name=current_flights_coll,
                doc_id=flight_dict["flight_id"],
                document_data=flight_dict,
            )

        # =======================================================================
        # =======================(Start Lambda Chain)============================
        # =======================================================================

        start_job_payload = {
            "Records": [
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::ssa-pdf-store",
                        },
                        "object": {
                            "key": "tests/macdill_2_72hr_test.pdf",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ],
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testDateTime": test_date,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        start_job_response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(start_job_payload),
        )

        # Get Terminal document to check for update status and pdf s3 location
        # set by Start-PDF-Textract-Job
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        self.assertEqual(terminal_doc.get("updating72Hour", False), True)
        self.assertEqual(
            terminal_doc.get("pdf72Hour", ""), "tests/macdill_2_72hr_test.pdf"
        )

        # Retrieve payload from Start-PDF-Textract-Job and verify it contains
        # proper information
        self.assertEqual(start_job_response["StatusCode"], 200)

        response_payload = json.loads(start_job_response["Payload"].read().decode())

        if not response_payload:
            self.fail("Payload was empty")

        self.assertEqual(response_payload["body"], "Job started successfully.")

        job_id = str(response_payload.get("job_id", ""))

        if not job_id:
            self.fail("Job ID was empty")

        # Check that the pdf has finished processing
        while True:
            textract_doc = fs.get_textract_job(job_id)

            if not textract_doc:
                self.fail("Textract document was empty")

            if textract_doc.get("finished_store_flights", ""):
                break

            time.sleep(5)

        if not isinstance(textract_doc, dict):
            self.fail("Textract document was not a dictionary")

        # =======================================================================
        # ================(Check that previous flights archived)=================
        # =======================================================================

        # Only the first flight to Elmenforf AFB should be archived
        # since the second flight is still in the future and should be deleted
        archived_flight_dict = fs.get_doc_by_id(
            collection_name=archive_flights_coll,
            doc_id=macdill_2_72hr_flight_1.flight_id,
        )

        if not archived_flight_dict:
            self.fail("Archived flight was empty")

        # Check for archived attributes
        self.assertTrue(archived_flight_dict.get("archived", False))
        del archived_flight_dict["archived"]

        archived_timestamp = archived_flight_dict.get("archived_timestamp")

        if not archived_timestamp or not isinstance(archived_timestamp, datetime):
            self.fail("Archived timestamp was empty or not a datetime")

        del archived_flight_dict["archived_timestamp"]

        # Check that the archived flight is the same as the original flight
        archived_flight = Flight.from_dict(archived_flight_dict)

        if not archived_flight:
            self.fail("Unable to convert archived flight dict to Flight object")

        self.assertEqual(archived_flight, macdill_2_72hr_flight_1)

        # Check that there are no other flights in the archive collection
        archive_docs = fs.db.collection(archive_flights_coll).stream()

        # Only one document for Elmenforf AFB flight tested above
        self.assertEqual(len(list(archive_docs)), 1)

        # =======================================================================
        # ==================(Check Current Flights Collection)===================
        # =======================================================================

        # Only the second flight to Mildenhall AFB should be in the current flights collection
        # since the first flight is in the past and should be deleted
        current_flight_dict = fs.get_doc_by_id(
            collection_name=current_flights_coll,
            doc_id=macdill_2_72hr_flight_2.flight_id,
        )

        if not current_flight_dict:
            self.fail("Current flight was empty")

        # Check that the current flight is the same as the original flight
        current_flight = Flight.from_dict(current_flight_dict)

        if not current_flight:
            self.fail("Unable to convert current flight dict to Flight object")

        self.assertEqual(current_flight, macdill_2_72hr_flight_2)

        # Check that there are no other flights in the current flights collection
        current_docs = fs.db.collection(current_flights_coll).stream()

        # Only one document for Mildenhall AFB flight tested above
        self.assertEqual(len(list(current_docs)), 1)

        # =======================================================================
        # ===============(Check Store-Flights updates Termnal doc)===============
        # =======================================================================

        # Check that the terminal document has been updated
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        # Default to True because the function should update it to False
        self.assertEqual(terminal_doc.get("updating72Hour", True), False)

        # Check that the pdf72Hour attribute was updated to new pdf location
        terminal_pdf_s3_location = terminal_doc.get("pdf72Hour", "")

        if not terminal_pdf_s3_location:
            self.fail("Terminal pdf72Hour attribute was empty")

        self.assertEqual(terminal_pdf_s3_location, "tests/macdill_2_72hr_test.pdf")

        # Check that the correct flight id was added to the terminal document
        # for the flight to Mildenhall AFB
        correct_flight_ids = [macdill_2_72hr_flight_2.flight_id]
        terminal_flight_ids = cast(list, terminal_doc.get("flights72Hour", []))

        if not terminal_flight_ids or not isinstance(terminal_flight_ids, list):
            self.fail("Unable to get terminal flight ids")

        self.assertCountEqual(terminal_flight_ids, correct_flight_ids)

        # =======================================================================
        # ===============(Check Start-PDF-Textract-Job debug info)===============
        # =======================================================================
        start_job_request_id = start_job_response.get("ResponseMetadata", {}).get(
            "RequestId", ""
        )

        # Check request ID stored is correct
        self.assertEqual(
            textract_doc.get("func_start_job_request_id", ""), start_job_request_id
        )

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_start_job_name", ""))

        # Check for test status
        self.assertTrue(textract_doc.get("test", False))

        # Check test parameters are properly written to textract document
        test_parameters_invoke = start_job_payload.get("testParameters", {})
        if not isinstance(test_parameters_invoke, dict):
            self.fail("Test parameters (invoke payload) was not a dictionary")

        test_parameters_textract = textract_doc.get("testParameters", {})
        if not isinstance(test_parameters_textract, dict):
            self.fail("Test parameters (textract document) was not a dictionary")

        self.assertDictEqual(test_parameters_invoke, test_parameters_textract)

        # Check for textract status
        self.assertEqual(textract_doc.get("status", ""), "SUCCEEDED")

        # Check for PDF hash in textract job document
        self.assertEqual(textract_doc.get("pdf_hash", ""), pdf_doc["hash"])

        # Check that the textract timestamps were created and updated with timestamps
        textract_started = textract_doc.get("textract_started")

        if not textract_started or not isinstance(textract_started, datetime):
            self.fail("Textract started timestamp was empty or not a datetime")

        textract_finished = textract_doc.get("textract_finished")

        if not textract_finished or not isinstance(textract_finished, datetime):
            self.fail("Textract finished timestamp was empty or not a datetime")

        # =======================================================================
        # =================(Check Textract-to-Tables debug info)=================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_request_id", ""))

        # Check for timestamps
        textract_to_tables_started = textract_doc.get("tables_parsed_started")

        if not textract_to_tables_started or not isinstance(
            textract_to_tables_started, datetime
        ):
            self.fail(
                "Textract to tables started timestamp was empty or not a datetime"
            )

        textract_to_tables_finished = textract_doc.get("tables_parsed_finished")

        if not textract_to_tables_finished or not isinstance(
            textract_to_tables_finished, datetime
        ):
            self.fail(
                "Textract to tables finished timestamp was empty or not a datetime"
            )

        # =======================================================================
        # ================(Check Process-72HR-Flights debug info)================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_72hr_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_72hr_request_id", ""))

        # Check for timestamps
        func_72hr_started = textract_doc.get("started_72hr_processing")

        if not func_72hr_started or not isinstance(func_72hr_started, datetime):
            self.fail(
                "Process 72hr flights started timestamp was empty or not a datetime"
            )

        func_72hr_finished = textract_doc.get("finished_72hr_processing")

        if not func_72hr_finished or not isinstance(func_72hr_finished, datetime):
            self.fail(
                "Process 72hr flights finished timestamp was empty or not a datetime"
            )

        # Check for number of flights generated from the PDF
        num_flights = textract_doc.get("num_flights", 0)

        self.assertEqual(num_flights, 2)

        # Check the generated flight ids are correct
        correct_flight_ids = [
            macdill_2_72hr_flight_1.flight_id,
            macdill_2_72hr_flight_2.flight_id,
        ]

        flight_ids = textract_doc.get("flight_ids", [])

        if not isinstance(flight_ids, list):
            self.fail("Flight IDs was not a list")

        self.assertCountEqual(flight_ids, correct_flight_ids)

        # =======================================================================
        # ===================(Check Store-Flights debug info)====================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_store_flights_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_store_flights_request_id", ""))

        # Check for timestamps
        store_flights_started = textract_doc.get("started_store_flights")

        if not store_flights_started or not isinstance(store_flights_started, datetime):
            self.fail("Store flights started timestamp was empty or not a datetime")

        store_flights_finished = textract_doc.get("finished_store_flights")

        if not store_flights_finished or not isinstance(
            store_flights_finished, datetime
        ):
            self.fail("Store flights finished timestamp was empty or not a datetime")

        # =======================================================================
        # =====================(Clean up testing documents)======================
        # =======================================================================
        # Delete testing pdf document
        fs.delete_document_by_id(pdf_archive_coll, pdf_doc["hash"])

        # Delete testing terminal document
        fs.delete_document_by_id(terminal_coll, terminal_doc["name"])

        # Delete archived flight document
        fs.delete_document_by_id(archive_flights_coll, archived_flight.flight_id)

        # Delete current flight document
        fs.delete_document_by_id(current_flights_coll, current_flight.flight_id)

        # Delete textract job document
        fs.delete_document_by_id(textract_jobs_coll, job_id)

    def test_e2e_kadena_1_72hr(self: unittest.TestCase) -> None:
        """Tests the end to end functionality for parsing the macdill_2_72hr PDF."""
        pdf_archive_coll = "**TESTING-E2E**_PDF_Archive"
        terminal_coll = "**TESTING-E2E**_Terminals"
        current_flights_coll = "**TESTING-E2E**_Current_Flights"
        archive_flights_coll = "**TESTING-E2E**_Archive_Flights"
        textract_jobs_coll = "Textract_Jobs"

        test_date = "202308190658"  # 2023-08-19 06:58:00

        lambda_client = initialize_client("lambda")

        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            flight_current_coll=current_flights_coll,
            flight_archive_coll=archive_flights_coll,
            textract_jobs_coll=textract_jobs_coll,
        )

        pdf_doc = {
            "cloud_path": "tests/kadena_1_72hr_test.pdf",
            "hash": "8bbbc47fe5f6dfe5f55c878d50419e85bb98c30c5296f50109a7eed8fe257e80",
            "terminal": "Kadena AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            doc_id=pdf_doc["hash"],
            document_data=pdf_doc,
        )

        terminal_doc = {
            "name": "Kadena AB Passenger Terminal",
            "location": "Kadena AB, Japan",
            "group": "INDOPACOM TERMINALS",
            "archiveDir": "tests",
            "timezone": "Asia/Tokyo",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        # Load in previous flights in the current flights collection
        # to test that the archive flights collection is properly updated
        # by the Store-Flights lambda function
        kadena_1_72hr_flight_1 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_kadena_1_72hr/kadena_1_72hr_table-1_flight-1.pkl"
        )

        if not kadena_1_72hr_flight_1:
            self.fail("Flight 1 was empty")

        kadena_1_72hr_flight_2 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_kadena_1_72hr/kadena_1_72hr_table-2_flight-1.pkl"
        )

        if not kadena_1_72hr_flight_2:
            self.fail("Flight 2 was empty")

        kadena_1_72hr_flight_3 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_kadena_1_72hr/kadena_1_72hr_table-2_flight-2.pkl"
        )

        if not kadena_1_72hr_flight_3:
            self.fail("Flight 3 was empty")

        kadena_1_72hr_flight_4 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_kadena_1_72hr/kadena_1_72hr_table-2_flight-3.pkl"
        )

        if not kadena_1_72hr_flight_4:
            self.fail("Flight 4 was empty")

        kadena_1_72hr_flight_5 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_kadena_1_72hr/kadena_1_72hr_table-3_flight-1.pkl"
        )

        if not kadena_1_72hr_flight_5:
            self.fail("Flight 5 was empty")

        kadena_1_72hr_flight_1.make_firestore_compliant()
        kadena_1_72hr_flight_2.make_firestore_compliant()
        kadena_1_72hr_flight_3.make_firestore_compliant()
        kadena_1_72hr_flight_4.make_firestore_compliant()
        kadena_1_72hr_flight_5.make_firestore_compliant()

        all_flights = [
            kadena_1_72hr_flight_1,
            kadena_1_72hr_flight_2,
            kadena_1_72hr_flight_3,
            kadena_1_72hr_flight_4,
            kadena_1_72hr_flight_5,
        ]

        archived_flights = [
            kadena_1_72hr_flight_1,
            kadena_1_72hr_flight_2,
        ]

        current_flights = [
            kadena_1_72hr_flight_3,
            kadena_1_72hr_flight_4,
            kadena_1_72hr_flight_5,
        ]

        previous_flights = [
            kadena_1_72hr_flight_1.to_dict(),
            kadena_1_72hr_flight_2.to_dict(),
            kadena_1_72hr_flight_3.to_dict(),
            kadena_1_72hr_flight_4.to_dict(),
            kadena_1_72hr_flight_5.to_dict(),
        ]

        for flight_dict in previous_flights:
            fs.insert_document_with_id(
                collection_name=current_flights_coll,
                doc_id=flight_dict["flight_id"],
                document_data=flight_dict,
            )

        # =======================================================================
        # =======================(Start Lambda Chain)============================
        # =======================================================================

        start_job_payload = {
            "Records": [
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::ssa-pdf-store",
                        },
                        "object": {
                            "key": "tests/kadena_1_72hr_test.pdf",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ],
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testDateTime": test_date,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        start_job_response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(start_job_payload),
        )

        # Get Terminal document to check for update status and pdf s3 location
        # set by Start-PDF-Textract-Job
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        self.assertEqual(terminal_doc.get("updating72Hour", False), True)
        self.assertEqual(
            terminal_doc.get("pdf72Hour", ""), "tests/kadena_1_72hr_test.pdf"
        )

        # Retrieve payload from Start-PDF-Textract-Job and verify it contains
        # proper information
        self.assertEqual(start_job_response["StatusCode"], 200)

        response_payload = json.loads(start_job_response["Payload"].read().decode())

        if not response_payload:
            self.fail("Payload was empty")

        self.assertEqual(response_payload["body"], "Job started successfully.")

        job_id = str(response_payload.get("job_id", ""))

        if not job_id:
            self.fail("Job ID was empty")

        # Check that the pdf has finished processing
        while True:
            textract_doc = fs.get_textract_job(job_id)

            if not textract_doc:
                self.fail("Textract document was empty")

            if textract_doc.get("finished_store_flights", ""):
                break

            time.sleep(5)

        if not isinstance(textract_doc, dict):
            self.fail("Textract document was not a dictionary")

        # =======================================================================
        # ================(Check that previous flights archived)=================
        # =======================================================================

        # The first two flights to Yokota AB and Iwakuni MCAS should be archived

        for i, flight in enumerate(archived_flights):
            archived_flight_dict = fs.get_doc_by_id(
                collection_name=archive_flights_coll,
                doc_id=flight.flight_id,
            )

            if not archived_flight_dict:
                self.fail(f"Archived flight {i} was empty")

            # Check for archived attributes
            self.assertTrue(archived_flight_dict.get("archived", False))
            del archived_flight_dict["archived"]

            archived_timestamp = archived_flight_dict.get("archived_timestamp")

            if not archived_timestamp or not isinstance(archived_timestamp, datetime):
                self.fail(f"Archived timestamp {i} was empty or not a datetime")

            del archived_flight_dict["archived_timestamp"]

            # Check that the archived flight is the same as the original flight
            archived_flight = Flight.from_dict(archived_flight_dict)

            if not archived_flight:
                self.fail(
                    f"Unable to convert archived flight dict to Flight object. Flight {i}."
                )

            self.assertEqual(archived_flight, flight)

        # Check that there are no other flights in the archive collection
        archive_docs = fs.db.collection(archive_flights_coll).stream()

        # Only one document for Elmenforf AFB flight tested above
        self.assertEqual(len(list(archive_docs)), 2)

        # =======================================================================
        # ==================(Check Current Flights Collection)===================
        # =======================================================================

        for i, flight in enumerate(current_flights):
            current_flight_dict = fs.get_doc_by_id(
                collection_name=current_flights_coll,
                doc_id=flight.flight_id,
            )

            if not current_flight_dict:
                self.fail("Current flight was empty")

            # Check that the current flight is the same as the original flight
            current_flight = Flight.from_dict(current_flight_dict)

            # Add two because this list starts at the 3rd flight in the pdf
            if not current_flight:
                self.fail(
                    f"Unable to convert current flight dict to Flight object. Flight {i+2}."
                )

            self.assertEqual(
                flight, current_flight
            )  # flight is an from a pickled file. current_flight is being tested.

        # Check that there are no other flights in the current flights collection
        current_docs = fs.db.collection(current_flights_coll).stream()

        # Only one document for Mildenhall AFB flight tested above
        self.assertEqual(len(list(current_docs)), 3)

        # =======================================================================
        # ===============(Check Store-Flights updates Termnal doc)===============
        # =======================================================================

        # Check that the terminal document has been updated
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        # Default to True because the function should update it to False
        self.assertEqual(terminal_doc.get("updating72Hour", True), False)

        # Check that the pdf72Hour attribute was updated to new pdf location
        terminal_pdf_s3_location = terminal_doc.get("pdf72Hour", "")

        if not terminal_pdf_s3_location:
            self.fail("Terminal pdf72Hour attribute was empty")

        self.assertEqual(terminal_pdf_s3_location, "tests/kadena_1_72hr_test.pdf")

        # Check that the flights added to current flights collection were added to the terminal document
        correct_flight_ids = [flight.flight_id for flight in current_flights]
        terminal_flight_ids = cast(list, terminal_doc.get("flights72Hour", []))

        if not terminal_flight_ids or not isinstance(terminal_flight_ids, list):
            self.fail("Unable to get terminal flight ids")

        self.assertCountEqual(terminal_flight_ids, correct_flight_ids)

        # =======================================================================
        # ===============(Check Start-PDF-Textract-Job debug info)===============
        # =======================================================================
        start_job_request_id = start_job_response.get("ResponseMetadata", {}).get(
            "RequestId", ""
        )

        # Check request ID stored is correct
        self.assertEqual(
            textract_doc.get("func_start_job_request_id", ""), start_job_request_id
        )

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_start_job_name", ""))

        # Check for test status
        self.assertTrue(textract_doc.get("test", False))

        # Check test parameters are properly written to textract document
        test_parameters_invoke = start_job_payload.get("testParameters", {})
        if not isinstance(test_parameters_invoke, dict):
            self.fail("Test parameters (invoke payload) was not a dictionary")

        test_parameters_textract = textract_doc.get("testParameters", {})
        if not isinstance(test_parameters_textract, dict):
            self.fail("Test parameters (textract document) was not a dictionary")

        self.assertDictEqual(test_parameters_invoke, test_parameters_textract)

        # Check for textract status
        self.assertEqual(textract_doc.get("status", ""), "SUCCEEDED")

        # Check for PDF hash in textract job document
        self.assertEqual(textract_doc.get("pdf_hash", ""), pdf_doc["hash"])

        # Check that the textract timestamps were created and updated with timestamps
        textract_started = textract_doc.get("textract_started")

        if not textract_started or not isinstance(textract_started, datetime):
            self.fail("Textract started timestamp was empty or not a datetime")

        textract_finished = textract_doc.get("textract_finished")

        if not textract_finished or not isinstance(textract_finished, datetime):
            self.fail("Textract finished timestamp was empty or not a datetime")

        # =======================================================================
        # =================(Check Textract-to-Tables debug info)=================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_request_id", ""))

        # Check for timestamps
        textract_to_tables_started = textract_doc.get("tables_parsed_started")

        if not textract_to_tables_started or not isinstance(
            textract_to_tables_started, datetime
        ):
            self.fail(
                "Textract to tables started timestamp was empty or not a datetime"
            )

        textract_to_tables_finished = textract_doc.get("tables_parsed_finished")

        if not textract_to_tables_finished or not isinstance(
            textract_to_tables_finished, datetime
        ):
            self.fail(
                "Textract to tables finished timestamp was empty or not a datetime"
            )

        # =======================================================================
        # ================(Check Process-72HR-Flights debug info)================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_72hr_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_72hr_request_id", ""))

        # Check for timestamps
        func_72hr_started = textract_doc.get("started_72hr_processing")

        if not func_72hr_started or not isinstance(func_72hr_started, datetime):
            self.fail(
                "Process 72hr flights started timestamp was empty or not a datetime"
            )

        func_72hr_finished = textract_doc.get("finished_72hr_processing")

        if not func_72hr_finished or not isinstance(func_72hr_finished, datetime):
            self.fail(
                "Process 72hr flights finished timestamp was empty or not a datetime"
            )

        # Check for number of flights generated from the PDF
        num_flights = textract_doc.get("num_flights", 0)

        self.assertEqual(num_flights, 5)

        # Check the generated flight ids are correct
        correct_flight_ids = [flight.flight_id for flight in all_flights]

        flight_ids = textract_doc.get("flight_ids", [])

        if not isinstance(flight_ids, list):
            self.fail("Flight IDs was not a list")

        self.assertCountEqual(flight_ids, correct_flight_ids)

        # =======================================================================
        # ===================(Check Store-Flights debug info)====================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_store_flights_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_store_flights_request_id", ""))

        # Check for timestamps
        store_flights_started = textract_doc.get("started_store_flights")

        if not store_flights_started or not isinstance(store_flights_started, datetime):
            self.fail("Store flights started timestamp was empty or not a datetime")

        store_flights_finished = textract_doc.get("finished_store_flights")

        if not store_flights_finished or not isinstance(
            store_flights_finished, datetime
        ):
            self.fail("Store flights finished timestamp was empty or not a datetime")

        # =======================================================================
        # =====================(Clean up testing documents)======================
        # =======================================================================
        # Delete testing pdf document
        fs.delete_document_by_id(pdf_archive_coll, pdf_doc["hash"])

        # Delete testing terminal document
        fs.delete_document_by_id(terminal_coll, terminal_doc["name"])

        # Delete archived flight documents
        for flight in archived_flights:
            fs.delete_document_by_id(archive_flights_coll, flight.flight_id)

        # Delete current flight documents
        for flight in current_flights:
            fs.delete_document_by_id(current_flights_coll, flight.flight_id)

        # Delete textract job document
        fs.delete_document_by_id(textract_jobs_coll, job_id)

    def test_e2e_sigonella_1_72hr(self: unittest.TestCase) -> None:
        """Tests the end to end functionality for parsing the macdill_2_72hr PDF."""
        pdf_archive_coll = "**TESTING-E2E**_PDF_Archive"
        terminal_coll = "**TESTING-E2E**_Terminals"
        current_flights_coll = "**TESTING-E2E**_Current_Flights"
        archive_flights_coll = "**TESTING-E2E**_Archive_Flights"
        textract_jobs_coll = "Textract_Jobs"

        test_date = "202308181104"  # 2023-08-18 11:04:00

        pdf_path = "tests/sigonella_1_72hr_test.pdf"

        lambda_client = initialize_client("lambda")

        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            flight_current_coll=current_flights_coll,
            flight_archive_coll=archive_flights_coll,
            textract_jobs_coll=textract_jobs_coll,
        )

        pdf_doc = {
            "cloud_path": pdf_path,
            "hash": "988de49958ea8b78435bd04f99ed1bb21ac86a589eb1904a13834a30c746a63b",
            "terminal": "NAS Sigonella Air Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            doc_id=pdf_doc["hash"],
            document_data=pdf_doc,
        )

        terminal_doc = {
            "name": "NAS Sigonella Air Terminal",
            "location": "NAS Sigonella, Italy",
            "group": "EUCOM TERMINALS",
            "archiveDir": "tests",
            "timezone": "Europe/Rome",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        # Load in previous flights in the current flights collection
        # to test that the archive flights collection is properly updated
        # by the Store-Flights lambda function
        sigonella_1_72hr_flight_1 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-7_flight-1.pkl"
        )

        if not sigonella_1_72hr_flight_1:
            self.fail("Flight 1 was empty")

        sigonella_1_72hr_flight_2 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-7_flight-2.pkl"
        )

        if not sigonella_1_72hr_flight_2:
            self.fail("Flight 2 was empty")

        sigonella_1_72hr_flight_3 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-7_flight-3.pkl"
        )

        if not sigonella_1_72hr_flight_3:
            self.fail("Flight 3 was empty")

        sigonella_1_72hr_flight_4 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-8_flight-1.pkl"
        )

        if not sigonella_1_72hr_flight_4:
            self.fail("Flight 4 was empty")

        sigonella_1_72hr_flight_5 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-8_flight-2.pkl"
        )

        if not sigonella_1_72hr_flight_5:
            self.fail("Flight 5 was empty")

        sigonella_1_72hr_flight_6 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-8_flight-3.pkl"
        )

        if not sigonella_1_72hr_flight_6:
            self.fail("Flight 6 was empty")

        sigonella_1_72hr_flight_7 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-8_flight-4.pkl"
        )

        if not sigonella_1_72hr_flight_7:
            self.fail("Flight 7 was empty")

        sigonella_1_72hr_flight_8 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-1.pkl"
        )

        if not sigonella_1_72hr_flight_8:
            self.fail("Flight 8 was empty")

        sigonella_1_72hr_flight_9 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-2.pkl"
        )

        if not sigonella_1_72hr_flight_9:
            self.fail("Flight 9 was empty")

        sigonella_1_72hr_flight_10 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-3.pkl"
        )

        if not sigonella_1_72hr_flight_10:
            self.fail("Flight 10 was empty")

        sigonella_1_72hr_flight_11 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-4.pkl"
        )

        if not sigonella_1_72hr_flight_11:
            self.fail("Flight 11 was empty")

        sigonella_1_72hr_flight_12 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-5.pkl"
        )

        if not sigonella_1_72hr_flight_12:
            self.fail("Flight 12 was empty")

        sigonella_1_72hr_flight_13 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-6.pkl"
        )

        if not sigonella_1_72hr_flight_13:
            self.fail("Flight 13 was empty")

        sigonella_1_72hr_flight_14 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_sigonella_1_72hr/sigonella_1_72hr_table-9_flight-7.pkl"
        )

        if not sigonella_1_72hr_flight_14:
            self.fail("Flight 14 was empty")

        sigonella_1_72hr_flight_1.make_firestore_compliant()
        sigonella_1_72hr_flight_2.make_firestore_compliant()
        sigonella_1_72hr_flight_3.make_firestore_compliant()
        sigonella_1_72hr_flight_4.make_firestore_compliant()
        sigonella_1_72hr_flight_5.make_firestore_compliant()
        sigonella_1_72hr_flight_6.make_firestore_compliant()
        sigonella_1_72hr_flight_7.make_firestore_compliant()
        sigonella_1_72hr_flight_8.make_firestore_compliant()
        sigonella_1_72hr_flight_9.make_firestore_compliant()
        sigonella_1_72hr_flight_10.make_firestore_compliant()
        sigonella_1_72hr_flight_11.make_firestore_compliant()
        sigonella_1_72hr_flight_12.make_firestore_compliant()
        sigonella_1_72hr_flight_13.make_firestore_compliant()
        sigonella_1_72hr_flight_14.make_firestore_compliant()

        all_flights = [
            sigonella_1_72hr_flight_1,
            sigonella_1_72hr_flight_2,
            sigonella_1_72hr_flight_3,
            sigonella_1_72hr_flight_4,
            sigonella_1_72hr_flight_5,
            sigonella_1_72hr_flight_6,
            sigonella_1_72hr_flight_7,
            sigonella_1_72hr_flight_8,
            sigonella_1_72hr_flight_9,
            sigonella_1_72hr_flight_10,
            sigonella_1_72hr_flight_11,
            sigonella_1_72hr_flight_12,
            sigonella_1_72hr_flight_13,
            sigonella_1_72hr_flight_14,
        ]

        archived_flights = [
            sigonella_1_72hr_flight_3,
        ]

        current_flights = [  # Every flight except the third flight
            sigonella_1_72hr_flight_1,
            sigonella_1_72hr_flight_2,
            sigonella_1_72hr_flight_4,
            sigonella_1_72hr_flight_5,
            sigonella_1_72hr_flight_6,
            sigonella_1_72hr_flight_7,
            sigonella_1_72hr_flight_8,
            sigonella_1_72hr_flight_9,
            sigonella_1_72hr_flight_10,
            sigonella_1_72hr_flight_11,
            sigonella_1_72hr_flight_12,
            sigonella_1_72hr_flight_13,
            sigonella_1_72hr_flight_14,
        ]

        previous_flights = [flight.to_dict() for flight in all_flights]

        for flight_dict in previous_flights:
            fs.insert_document_with_id(
                collection_name=current_flights_coll,
                doc_id=flight_dict["flight_id"],
                document_data=flight_dict,
            )

        # =======================================================================
        # =======================(Start Lambda Chain)============================
        # =======================================================================

        start_job_payload = {
            "Records": [
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::ssa-pdf-store",
                        },
                        "object": {
                            "key": pdf_path,
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ],
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testDateTime": test_date,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        start_job_response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(start_job_payload),
        )

        # Get Terminal document to check for update status and pdf s3 location
        # set by Start-PDF-Textract-Job
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        self.assertEqual(terminal_doc.get("updating72Hour", False), True)
        self.assertEqual(terminal_doc.get("pdf72Hour", ""), pdf_path)

        # Retrieve payload from Start-PDF-Textract-Job and verify it contains
        # proper information
        self.assertEqual(start_job_response["StatusCode"], 200)

        response_payload = json.loads(start_job_response["Payload"].read().decode())

        if not response_payload:
            self.fail("Payload was empty")

        self.assertEqual(response_payload["body"], "Job started successfully.")

        job_id = str(response_payload.get("job_id", ""))

        if not job_id:
            self.fail("Job ID was empty")

        # Check that the pdf has finished processing
        while True:
            textract_doc = fs.get_textract_job(job_id)

            if not textract_doc:
                self.fail("Textract document was empty")

            if textract_doc.get("finished_store_flights", ""):
                break

            time.sleep(5)

        if not isinstance(textract_doc, dict):
            self.fail("Textract document was not a dictionary")

        # =======================================================================
        # ================(Check that previous flights archived)=================
        # =======================================================================

        # The first two flights to Yokota AB and Iwakuni MCAS should be archived

        for i, flight in enumerate(archived_flights):
            archived_flight_dict = fs.get_doc_by_id(
                collection_name=archive_flights_coll,
                doc_id=flight.flight_id,
            )

            if not archived_flight_dict:
                self.fail(f"Archived flight {i} was empty")

            # Check for archived attributes
            self.assertTrue(archived_flight_dict.get("archived", False))
            del archived_flight_dict["archived"]

            archived_timestamp = archived_flight_dict.get("archived_timestamp")

            if not archived_timestamp or not isinstance(archived_timestamp, datetime):
                self.fail(f"Archived timestamp {i} was empty or not a datetime")

            del archived_flight_dict["archived_timestamp"]

            # Check that the archived flight is the same as the original flight
            archived_flight = Flight.from_dict(archived_flight_dict)

            if not archived_flight:
                self.fail(
                    f"Unable to convert archived flight dict to Flight object. Flight {i}."
                )

            self.assertEqual(archived_flight, flight)

        # Check that there are no other flights in the archive collection
        archive_docs = fs.db.collection(archive_flights_coll).stream()

        self.assertEqual(len(list(archive_docs)), len(archived_flights))

        # =======================================================================
        # ==================(Check Current Flights Collection)===================
        # =======================================================================

        for i, flight in enumerate(current_flights):
            current_flight_dict = fs.get_doc_by_id(
                collection_name=current_flights_coll,
                doc_id=flight.flight_id,
            )

            if not current_flight_dict:
                self.fail("Current flight was empty")

            # Check that the current flight is the same as the original flight
            current_flight = Flight.from_dict(current_flight_dict)

            # Add two because this list starts at the 3rd flight in the pdf
            if not current_flight:
                self.fail(
                    f"Unable to convert current flight dict to Flight object. Flight {i+2}."
                )

            self.assertEqual(
                flight, current_flight
            )  # flight is an from a pickled file. current_flight is being tested.

        # Check that there are no other flights in the current flights collection
        current_docs = fs.db.collection(current_flights_coll).stream()

        self.assertEqual(len(list(current_docs)), len(current_flights))

        # =======================================================================
        # ===============(Check Store-Flights updates Termnal doc)===============
        # =======================================================================

        # Check that the terminal document has been updated
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        # Default to True because the function should update it to False
        self.assertEqual(terminal_doc.get("updating72Hour", True), False)

        # Check that the pdf72Hour attribute was updated to new pdf location
        terminal_pdf_s3_location = terminal_doc.get("pdf72Hour", "")

        if not terminal_pdf_s3_location:
            self.fail("Terminal pdf72Hour attribute was empty")

        self.assertEqual(terminal_pdf_s3_location, pdf_path)

        # Check that the flights added to current flights collection were added to the terminal document
        correct_flight_ids = [flight.flight_id for flight in current_flights]
        terminal_flight_ids = cast(list, terminal_doc.get("flights72Hour", []))

        if not terminal_flight_ids or not isinstance(terminal_flight_ids, list):
            self.fail("Unable to get terminal flight ids")

        self.assertCountEqual(terminal_flight_ids, correct_flight_ids)

        # =======================================================================
        # ===============(Check Start-PDF-Textract-Job debug info)===============
        # =======================================================================
        start_job_request_id = start_job_response.get("ResponseMetadata", {}).get(
            "RequestId", ""
        )

        # Check request ID stored is correct
        self.assertEqual(
            textract_doc.get("func_start_job_request_id", ""), start_job_request_id
        )

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_start_job_name", ""))

        # Check for test status
        self.assertTrue(textract_doc.get("test", False))

        # Check test parameters are properly written to textract document
        test_parameters_invoke = start_job_payload.get("testParameters", {})
        if not isinstance(test_parameters_invoke, dict):
            self.fail("Test parameters (invoke payload) was not a dictionary")

        test_parameters_textract = textract_doc.get("testParameters", {})
        if not isinstance(test_parameters_textract, dict):
            self.fail("Test parameters (textract document) was not a dictionary")

        self.assertDictEqual(test_parameters_invoke, test_parameters_textract)

        # Check for textract status
        self.assertEqual(textract_doc.get("status", ""), "SUCCEEDED")

        # Check for PDF hash in textract job document
        self.assertEqual(textract_doc.get("pdf_hash", ""), pdf_doc["hash"])

        # Check that the textract timestamps were created and updated with timestamps
        textract_started = textract_doc.get("textract_started")

        if not textract_started or not isinstance(textract_started, datetime):
            self.fail("Textract started timestamp was empty or not a datetime")

        textract_finished = textract_doc.get("textract_finished")

        if not textract_finished or not isinstance(textract_finished, datetime):
            self.fail("Textract finished timestamp was empty or not a datetime")

        # =======================================================================
        # =================(Check Textract-to-Tables debug info)=================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_request_id", ""))

        # Check for timestamps
        textract_to_tables_started = textract_doc.get("tables_parsed_started")

        if not textract_to_tables_started or not isinstance(
            textract_to_tables_started, datetime
        ):
            self.fail(
                "Textract to tables started timestamp was empty or not a datetime"
            )

        textract_to_tables_finished = textract_doc.get("tables_parsed_finished")

        if not textract_to_tables_finished or not isinstance(
            textract_to_tables_finished, datetime
        ):
            self.fail(
                "Textract to tables finished timestamp was empty or not a datetime"
            )

        # =======================================================================
        # ================(Check Process-72HR-Flights debug info)================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_72hr_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_72hr_request_id", ""))

        # Check for timestamps
        func_72hr_started = textract_doc.get("started_72hr_processing")

        if not func_72hr_started or not isinstance(func_72hr_started, datetime):
            self.fail(
                "Process 72hr flights started timestamp was empty or not a datetime"
            )

        func_72hr_finished = textract_doc.get("finished_72hr_processing")

        if not func_72hr_finished or not isinstance(func_72hr_finished, datetime):
            self.fail(
                "Process 72hr flights finished timestamp was empty or not a datetime"
            )

        # Check for number of flights generated from the PDF
        num_flights = textract_doc.get("num_flights", 0)

        self.assertEqual(num_flights, len(all_flights))

        # Check the generated flight ids are correct
        correct_flight_ids = [flight.flight_id for flight in all_flights]

        flight_ids = textract_doc.get("flight_ids", [])

        if not isinstance(flight_ids, list):
            self.fail("Flight IDs was not a list")

        self.assertCountEqual(flight_ids, correct_flight_ids)

        # =======================================================================
        # ===================(Check Store-Flights debug info)====================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_store_flights_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_store_flights_request_id", ""))

        # Check for timestamps
        store_flights_started = textract_doc.get("started_store_flights")

        if not store_flights_started or not isinstance(store_flights_started, datetime):
            self.fail("Store flights started timestamp was empty or not a datetime")

        store_flights_finished = textract_doc.get("finished_store_flights")

        if not store_flights_finished or not isinstance(
            store_flights_finished, datetime
        ):
            self.fail("Store flights finished timestamp was empty or not a datetime")

        # =======================================================================
        # =====================(Clean up testing documents)======================
        # =======================================================================
        # Delete testing pdf document
        fs.delete_document_by_id(pdf_archive_coll, pdf_doc["hash"])

        # Delete testing terminal document
        fs.delete_document_by_id(terminal_coll, terminal_doc["name"])

        # Delete archived flight documents
        for flight in archived_flights:
            fs.delete_document_by_id(archive_flights_coll, flight.flight_id)

        # Delete current flight documents
        for flight in current_flights:
            fs.delete_document_by_id(current_flights_coll, flight.flight_id)

        # Delete textract job document
        fs.delete_document_by_id(textract_jobs_coll, job_id)

    def test_e2e_bwi_1_72hr(self: unittest.TestCase) -> None:
        """Tests the end to end functionality for parsing the macdill_2_72hr PDF."""
        pdf_archive_coll = "**TESTING-E2E**_PDF_Archive-BWI"
        terminal_coll = "**TESTING-E2E**_Terminals-BWI"
        current_flights_coll = "**TESTING-E2E**_Current_Flights-BWI"
        archive_flights_coll = "**TESTING-E2E**_Archive_Flights-BWI"
        textract_jobs_coll = "Textract_Jobs"

        test_date = "202308190857"  # 2023-08-19 23:57:00

        pdf_path = "tests/bwi_1_72hr_test.pdf"

        lambda_client = initialize_client("lambda")

        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            flight_current_coll=current_flights_coll,
            flight_archive_coll=archive_flights_coll,
            textract_jobs_coll=textract_jobs_coll,
        )

        pdf_doc = {
            "cloud_path": pdf_path,
            "hash": "96f0761f8b454c5dcf716a32dcb879bc6ffd417a10325d997cff8d41bd5a6374",
            "terminal": "Baltimore-Washinton International Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            doc_id=pdf_doc["hash"],
            document_data=pdf_doc,
        )

        terminal_doc = {
            "name": "Baltimore-Washinton International Passenger Terminal",
            "location": "Baltimore-Washington Int'l (BWI), MD",
            "group": "AMC CONUS TERMINALS",
            "archiveDir": "tests",
            "timezone": "America/New_York",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        # Load in previous flights in the current flights collection
        # to test that the archive flights collection is properly updated
        # by the Store-Flights lambda function
        bwi_1_72hr_flight_1 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_bwi_1_72hr/bwi_1_72hr_table-1_flight-1.pkl"
        )

        if not bwi_1_72hr_flight_1:
            self.fail("Flight 1 was empty")

        bwi_1_72hr_flight_2 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_bwi_1_72hr/bwi_1_72hr_table-3_flight-1.pkl"
        )

        if not bwi_1_72hr_flight_2:
            self.fail("Flight 2 was empty")

        bwi_1_72hr_flight_3 = Flight.load_state(
            "tests/end-to-end-test-assets/test_e2e_bwi_1_72hr/bwi_1_72hr_table-3_flight-2.pkl"
        )

        if not bwi_1_72hr_flight_3:
            self.fail("Flight 3 was empty")

        bwi_1_72hr_flight_1.make_firestore_compliant()
        bwi_1_72hr_flight_2.make_firestore_compliant()
        bwi_1_72hr_flight_3.make_firestore_compliant()

        all_flights = [
            bwi_1_72hr_flight_1,
            bwi_1_72hr_flight_2,
            bwi_1_72hr_flight_3,
        ]

        archived_flights = [
            bwi_1_72hr_flight_1,
        ]

        current_flights = [  # Every flight except the first flight
            bwi_1_72hr_flight_2,
            bwi_1_72hr_flight_3,
        ]

        previous_flights = [flight.to_dict() for flight in all_flights]

        for flight_dict in previous_flights:
            fs.insert_document_with_id(
                collection_name=current_flights_coll,
                doc_id=flight_dict["flight_id"],
                document_data=flight_dict,
            )

        # =======================================================================
        # =======================(Start Lambda Chain)============================
        # =======================================================================

        start_job_payload = {
            "Records": [
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "responseElements": {
                        "x-amz-request-id": "EXAMPLE123456789",
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                    },
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                            "name": "ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::ssa-pdf-store",
                        },
                        "object": {
                            "key": pdf_path,
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ],
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testDateTime": test_date,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        start_job_response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(start_job_payload),
        )

        # Get Terminal document to check for update status and pdf s3 location
        # set by Start-PDF-Textract-Job
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        self.assertEqual(terminal_doc.get("updating72Hour", False), True)
        self.assertEqual(terminal_doc.get("pdf72Hour", ""), pdf_path)

        # Retrieve payload from Start-PDF-Textract-Job and verify it contains
        # proper information
        self.assertEqual(start_job_response["StatusCode"], 200)

        response_payload = json.loads(start_job_response["Payload"].read().decode())

        if not response_payload:
            self.fail("Payload was empty")

        self.assertEqual(response_payload["body"], "Job started successfully.")

        job_id = str(response_payload.get("job_id", ""))

        if not job_id:
            self.fail("Job ID was empty")

        # Check that the pdf has finished processing
        while True:
            textract_doc = fs.get_textract_job(job_id)

            if not textract_doc:
                self.fail("Textract document was empty")

            if textract_doc.get("finished_store_flights", ""):
                break

            time.sleep(5)

        if not isinstance(textract_doc, dict):
            self.fail("Textract document was not a dictionary")

        # =======================================================================
        # ================(Check that previous flights archived)=================
        # =======================================================================

        # The first two flights to Yokota AB and Iwakuni MCAS should be archived

        for i, flight in enumerate(archived_flights):
            archived_flight_dict = fs.get_doc_by_id(
                collection_name=archive_flights_coll,
                doc_id=flight.flight_id,
            )

            if not archived_flight_dict:
                self.fail(f"Archived flight {i} was empty")

            # Check for archived attributes
            self.assertTrue(archived_flight_dict.get("archived", False))
            del archived_flight_dict["archived"]

            archived_timestamp = archived_flight_dict.get("archived_timestamp")

            if not archived_timestamp or not isinstance(archived_timestamp, datetime):
                self.fail(f"Archived timestamp {i} was empty or not a datetime")

            del archived_flight_dict["archived_timestamp"]

            # Check that the archived flight is the same as the original flight
            archived_flight = Flight.from_dict(archived_flight_dict)

            if not archived_flight:
                self.fail(
                    f"Unable to convert archived flight dict to Flight object. Flight {i}."
                )

            self.assertEqual(archived_flight, flight)

        # Check that there are no other flights in the archive collection
        archive_docs = fs.db.collection(archive_flights_coll).stream()

        self.assertEqual(len(list(archive_docs)), len(archived_flights))

        # =======================================================================
        # ==================(Check Current Flights Collection)===================
        # =======================================================================

        for i, flight in enumerate(current_flights):
            current_flight_dict = fs.get_doc_by_id(
                collection_name=current_flights_coll,
                doc_id=flight.flight_id,
            )

            if not current_flight_dict:
                self.fail("Current flight was empty")

            # Check that the current flight is the same as the original flight
            current_flight = Flight.from_dict(current_flight_dict)

            # Add two because this list starts at the 3rd flight in the pdf
            if not current_flight:
                self.fail(
                    f"Unable to convert current flight dict to Flight object. Flight {i+2}."
                )

            self.assertEqual(
                flight, current_flight
            )  # flight is an from a pickled file. current_flight is being tested.

        # Check that there are no other flights in the current flights collection
        current_docs = fs.db.collection(current_flights_coll).stream()

        self.assertEqual(len(list(current_docs)), len(current_flights))

        # =======================================================================
        # ===============(Check Store-Flights updates Termnal doc)===============
        # =======================================================================

        # Check that the terminal document has been updated
        terminal_doc = fs.get_terminal_dict_by_name(terminal_doc["name"])

        if not terminal_doc:
            self.fail("Terminal document was empty")

        # Default to True because the function should update it to False
        self.assertEqual(terminal_doc.get("updating72Hour", True), False)

        # Check that the pdf72Hour attribute was updated to new pdf location
        terminal_pdf_s3_location = terminal_doc.get("pdf72Hour", "")

        if not terminal_pdf_s3_location:
            self.fail("Terminal pdf72Hour attribute was empty")

        self.assertEqual(terminal_pdf_s3_location, pdf_path)

        # Check that the flights added to current flights collection were added to the terminal document
        correct_flight_ids = [flight.flight_id for flight in current_flights]
        terminal_flight_ids = cast(list, terminal_doc.get("flights72Hour", []))

        if not terminal_flight_ids or not isinstance(terminal_flight_ids, list):
            self.fail("Unable to get terminal flight ids")

        self.assertCountEqual(terminal_flight_ids, correct_flight_ids)

        # =======================================================================
        # ===============(Check Start-PDF-Textract-Job debug info)===============
        # =======================================================================
        start_job_request_id = start_job_response.get("ResponseMetadata", {}).get(
            "RequestId", ""
        )

        # Check request ID stored is correct
        self.assertEqual(
            textract_doc.get("func_start_job_request_id", ""), start_job_request_id
        )

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_start_job_name", ""))

        # Check for test status
        self.assertTrue(textract_doc.get("test", False))

        # Check test parameters are properly written to textract document
        test_parameters_invoke = start_job_payload.get("testParameters", {})
        if not isinstance(test_parameters_invoke, dict):
            self.fail("Test parameters (invoke payload) was not a dictionary")

        test_parameters_textract = textract_doc.get("testParameters", {})
        if not isinstance(test_parameters_textract, dict):
            self.fail("Test parameters (textract document) was not a dictionary")

        self.assertDictEqual(test_parameters_invoke, test_parameters_textract)

        # Check for textract status
        self.assertEqual(textract_doc.get("status", ""), "SUCCEEDED")

        # Check for PDF hash in textract job document
        self.assertEqual(textract_doc.get("pdf_hash", ""), pdf_doc["hash"])

        # Check that the textract timestamps were created and updated with timestamps
        textract_started = textract_doc.get("textract_started")

        if not textract_started or not isinstance(textract_started, datetime):
            self.fail("Textract started timestamp was empty or not a datetime")

        textract_finished = textract_doc.get("textract_finished")

        if not textract_finished or not isinstance(textract_finished, datetime):
            self.fail("Textract finished timestamp was empty or not a datetime")

        # =======================================================================
        # =================(Check Textract-to-Tables debug info)=================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_textract_to_tables_request_id", ""))

        # Check for timestamps
        textract_to_tables_started = textract_doc.get("tables_parsed_started")

        if not textract_to_tables_started or not isinstance(
            textract_to_tables_started, datetime
        ):
            self.fail(
                "Textract to tables started timestamp was empty or not a datetime"
            )

        textract_to_tables_finished = textract_doc.get("tables_parsed_finished")

        if not textract_to_tables_finished or not isinstance(
            textract_to_tables_finished, datetime
        ):
            self.fail(
                "Textract to tables finished timestamp was empty or not a datetime"
            )

        # =======================================================================
        # ================(Check Process-72HR-Flights debug info)================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_72hr_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_72hr_request_id", ""))

        # Check for timestamps
        func_72hr_started = textract_doc.get("started_72hr_processing")

        if not func_72hr_started or not isinstance(func_72hr_started, datetime):
            self.fail(
                "Process 72hr flights started timestamp was empty or not a datetime"
            )

        func_72hr_finished = textract_doc.get("finished_72hr_processing")

        if not func_72hr_finished or not isinstance(func_72hr_finished, datetime):
            self.fail(
                "Process 72hr flights finished timestamp was empty or not a datetime"
            )

        # Check for number of flights generated from the PDF
        num_flights = textract_doc.get("num_flights", 0)

        self.assertEqual(num_flights, len(all_flights))

        # Check the generated flight ids are correct
        correct_flight_ids = [flight.flight_id for flight in all_flights]

        flight_ids = textract_doc.get("flight_ids", [])

        if not isinstance(flight_ids, list):
            self.fail("Flight IDs was not a list")

        self.assertCountEqual(flight_ids, correct_flight_ids)

        # =======================================================================
        # ===================(Check Store-Flights debug info)====================
        # =======================================================================

        # Check that any function name was stored
        self.assertTrue(textract_doc.get("func_store_flights_name", ""))

        # Check for any request ID stored
        self.assertTrue(textract_doc.get("func_store_flights_request_id", ""))

        # Check for timestamps
        store_flights_started = textract_doc.get("started_store_flights")

        if not store_flights_started or not isinstance(store_flights_started, datetime):
            self.fail("Store flights started timestamp was empty or not a datetime")

        store_flights_finished = textract_doc.get("finished_store_flights")

        if not store_flights_finished or not isinstance(
            store_flights_finished, datetime
        ):
            self.fail("Store flights finished timestamp was empty or not a datetime")

        # =======================================================================
        # =====================(Clean up testing documents)======================
        # =======================================================================
        # Delete testing pdf document
        fs.delete_document_by_id(pdf_archive_coll, pdf_doc["hash"])

        # Delete testing terminal document
        fs.delete_document_by_id(terminal_coll, terminal_doc["name"])

        # Delete archived flight documents
        for flight in archived_flights:
            fs.delete_document_by_id(archive_flights_coll, flight.flight_id)

        # Delete current flight documents
        for flight in current_flights:
            fs.delete_document_by_id(current_flights_coll, flight.flight_id)

        # Delete textract job document
        fs.delete_document_by_id(textract_jobs_coll, job_id)
