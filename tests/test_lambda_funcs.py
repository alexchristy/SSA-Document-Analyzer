import copy
import json
import logging
import time
import unittest
from datetime import datetime
from typing import Any, Dict, List, cast

import pytz

from aws_utils import initialize_client
from firestore_db import FirestoreClient
from flight import Flight
from s3_bucket import S3Bucket
from table import Table
from time_utils import get_local_time, modify_datetime


class TestStartPdfTextractJob(unittest.TestCase):
    """Test the Start-PDF-Textract-Job function."""

    def test_correct_function(self: unittest.TestCase) -> None:
        """Test that the start_pdf_textract_job function starts a Textract job when a PDF is in the S3 bucket."""
        # If this test fails, check that the S3 bucket and object exist
        # S3 bucket: testing-ssa-pdf-store
        # S3 object: current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf
        pdf_archive_coll = "**TESTING**_PDF_Archive"
        terminal_coll = "**TESTING**_Terminals"

        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll, terminal_coll=terminal_coll
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        terminal_doc = {
            "name": "Osan AB Passenger Terminal",
            "location": "Osan AB, ROK",
            "group": "INDOPACOM TERMINALS",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        payload = {
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
                            "name": "testing-ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::testing-ssa-pdf-store",
                        },
                        "object": {
                            "key": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ],
            "test": True,
            "testParameters": {
                "sendPdf": False,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
            },
        }

        response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        self.assertEqual(response["StatusCode"], 200)

        # Reading the payload
        payload_stream = response["Payload"]
        payload_data = payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        payload = json.loads(payload_data.decode())

        if not payload:
            self.fail("Payload is empty")

        self.assertEqual(payload["body"], "Job started successfully.")

        returned_job_id = payload.get("job_id")

        if not returned_job_id:
            self.fail("Job ID not returned")

        textract_job = fs.get_textract_job(str(returned_job_id))

        if not textract_job:
            self.fail("Textract job not found in Firestore")

        self.assertEqual(textract_job["pdf_hash"], pdf_doc["hash"])

        request_id = response.get("ResponseMetadata", {}).get("RequestId", "")

        if not request_id:
            self.fail("Request ID not found")

        self.assertEqual(textract_job["func_start_job_request_id"], request_id)

        # Verify that terminal document was updated properly
        terminal_doc = fs.get_doc_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        if not terminal_doc:
            self.fail("Terminal document not found in Firestore")

        self.assertEqual(terminal_doc.get("pdf72Hour", ""), pdf_doc["cloud_path"])
        self.assertTrue(terminal_doc.get("updating72Hour", False))

        # Clean up
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

    def test_testing_parameters_no_send(self: unittest.TestCase) -> None:
        """Test that the Start-PDF-Textract-Job function correctly handles the testing parameters.

        In this test, the PDF is not sent to Textract because the sendPdf parameter is set to False. The
        job is still created in Firestore, but the job ID is a fake ID.
        """
        pdf_archive_coll = "**TESTING**_PDF_Archive"
        terminal_coll = "**TESTING**_Terminals"
        textract_job_coll = "Textract_Jobs"

        # If this test fails, check that the S3 bucket and object exist
        # S3 bucket: testing-ssa-pdf-store
        # S3 object: current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf
        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        terminal_doc = {
            "name": "Osan AB Passenger Terminal",
            "location": "Osan AB, ROK",
            "group": "INDOPACOM TERMINALS",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        invoke_payload = {
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
                            "name": "testing-ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::testing-ssa-pdf-store",
                        },
                        "object": {
                            "key": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            ],
            "test": True,
            "testParameters": {
                "sendPdf": False,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
            },
        }

        response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(invoke_payload),
        )

        self.assertEqual(response["StatusCode"], 200)

        # Reading the response payload
        response_payload_stream = response["Payload"]
        response_payload_data = response_payload_stream.read()

        # Decode response payload to a string and then load it as JSON
        response_payload = json.loads(response_payload_data.decode())

        if not response_payload:
            self.fail("Response payload is empty")

        self.assertEqual(response_payload["body"], "Job started successfully.")

        returned_job_id = response_payload.get("job_id")
        if not returned_job_id:
            self.fail("Job ID not returned")

        textract_job = fs.get_textract_job(str(returned_job_id))
        if not textract_job:
            self.fail("Textract job not found in Firestore")

        self.assertEqual(textract_job["pdf_hash"], pdf_doc["hash"])
        self.assertTrue(textract_job.get("test", False))

        # Check that the job id is equal to the fake job id that should be returned
        self.assertEqual(str(returned_job_id), "111111111111111111111111111111111111")

        # Ensure that the testParameters in the original invoke_payload and textract_job are the same
        test_parameters_invoke_payload = invoke_payload.get("testParameters", {})
        if not isinstance(test_parameters_invoke_payload, dict):
            self.fail("invoke_payload['testParameters'] is not a dictionary")

        test_parameters_textract_job = textract_job.get("testParameters", {})
        if not isinstance(test_parameters_textract_job, dict):
            self.fail("textract_job['testParameters'] is not a dictionary")

        self.assertDictEqual(
            test_parameters_invoke_payload, test_parameters_textract_job
        )

        request_id = response.get("ResponseMetadata", {}).get("RequestId", "")

        if not request_id:
            self.fail("Request ID not found")

        self.assertEqual(textract_job["func_start_job_request_id"], request_id)

        # Verify that terminal document was updated properly
        terminal_doc = fs.get_doc_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        if not terminal_doc:
            self.fail("Terminal document not found in Firestore")

        self.assertEqual(terminal_doc.get("pdf72Hour", ""), pdf_doc["cloud_path"])
        self.assertTrue(terminal_doc.get("updating72Hour", False))

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )
        # Delete the Textract job
        fs.delete_document_by_id(textract_job_coll, doc_id=str(returned_job_id))

        # Delete the terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

    def test_testing_parameters_send(self: unittest.TestCase) -> None:
        """Test that the Start-PDF-Textract-Job function correctly handles the testing parameters.

        This test is the same as the previous test, except that the sendPdf parameter is set to True. This
        means that the PDF is sent to Textract and a real job ID is returned. This is useful to test to verify
        that the function will work with end-to-end testing.
        """
        pdf_archive_coll = "**TESTING**_PDF_Archive"
        terminal_coll = "**TESTING**_Terminals"
        textract_job_coll = "Textract_Jobs"
        current_flights_coll = "**TESTING**_Current_Flights"
        archive_flights_coll = "**TESTING**_Archive_Flights"

        # If this test fails, check that the S3 bucket and object exist
        # S3 bucket: testing-ssa-pdf-store
        # S3 object: current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf
        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        terminal_doc = {
            "name": "Osan AB Passenger Terminal",
            "location": "Osan AB, ROK",
            "group": "INDOPACOM TERMINALS",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

        invoke_payload = {
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
                            "name": "testing-ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::testing-ssa-pdf-store",
                        },
                        "object": {
                            "key": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
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
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(invoke_payload),
        )

        self.assertEqual(response["StatusCode"], 200)

        # Reading the response payload
        response_payload_stream = response["Payload"]
        response_payload_data = response_payload_stream.read()

        # Decode response payload to a string and then load it as JSON
        response_payload = json.loads(response_payload_data.decode())

        if not response_payload:
            self.fail("Response payload is empty")

        self.assertEqual(response_payload["body"], "Job started successfully.")

        returned_job_id = response_payload.get("job_id")
        if not returned_job_id:
            self.fail("Job ID not returned")

        textract_job = fs.get_textract_job(str(returned_job_id))
        if not textract_job:
            self.fail("Textract job not found in Firestore")

        self.assertEqual(textract_job["pdf_hash"], pdf_doc["hash"])
        self.assertTrue(textract_job.get("test", False))

        # Check that the job id is NOT equal to the fake job id that should be returned
        self.assertNotEqual(
            str(returned_job_id), "111111111111111111111111111111111111"
        )

        # Ensure that the testParameters in the original invoke_payload and textract_job are the same
        test_parameters_invoke_payload = invoke_payload.get("testParameters", {})
        if not isinstance(test_parameters_invoke_payload, dict):
            self.fail("invoke_payload['testParameters'] is not a dictionary")

        test_parameters_textract_job = textract_job.get("testParameters", {})
        if not isinstance(test_parameters_textract_job, dict):
            self.fail("textract_job['testParameters'] is not a dictionary")

        self.assertDictEqual(
            test_parameters_invoke_payload, test_parameters_textract_job
        )

        request_id = response.get("ResponseMetadata", {}).get("RequestId", "")

        if not request_id:
            self.fail("Request ID not found")

        self.assertEqual(textract_job["func_start_job_request_id"], request_id)

        # Verify that terminal document was updated properly
        terminal_doc = fs.get_doc_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        if not terminal_doc:
            self.fail("Terminal document not found in Firestore")

        self.assertEqual(terminal_doc.get("pdf72Hour", ""), pdf_doc["cloud_path"])
        self.assertTrue(terminal_doc.get("updating72Hour", False))

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )
        # Delete the Textract job
        fs.delete_document_by_id(textract_job_coll, doc_id=str(returned_job_id))

        # Delete the terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        # Delete any possible flights
        fs.delete_collection(current_flights_coll)
        fs.delete_collection(archive_flights_coll)


class TestTextractToTables(unittest.TestCase):
    """Test the Textract-to-Tables function."""

    def test_correct_function(self: unittest.TestCase) -> None:
        """Test that the Textract-To-Tables function correctly converts a Textract job to tables.

        This works by invoking the Start-PDF-Textract-Job function, which starts a Textract job. Then, the
        the test function verfies the start job worked and then continues on to invoke the Textract-to-Tables with
        the returned job ID.

        NOTE: When checking logs there will be two log streams for the Textract-to-Tables as
        one will be invoked from Textract finishing the job and the other will be invoked from this test function.
        """
        pdf_archive_coll = "**TESTING**_PDF_Archive"
        terminal_coll = "**TESTING**_Terminals"
        textract_job_coll = "Textract_Jobs"
        current_flights_coll = "**TESTING**_Current_Flights"
        archive_flights_coll = "**TESTING**_Archive_Flights"

        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        terminal_doc = {
            "name": "Osan AB Passenger Terminal",
            "location": "Osan AB, ROK",
            "group": "INDOPACOM TERMINALS",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            doc_id=terminal_doc["name"],
            document_data=terminal_doc,
        )

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
                            "name": "testing-ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::testing-ssa-pdf-store",
                        },
                        "object": {
                            "key": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
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

        self.assertEqual(start_job_response["StatusCode"], 200)

        # Reading the payload
        start_job_payload_stream = start_job_response["Payload"]
        start_job_payload_data = start_job_payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        start_job_payload = json.loads(start_job_payload_data.decode())

        if not start_job_payload:
            self.fail("Payload is empty")

        self.assertEqual(start_job_payload["body"], "Job started successfully.")

        returned_job_id = start_job_payload.get("job_id", "")

        if not returned_job_id or not isinstance(returned_job_id, str):
            self.fail("Job ID not returned")

        textract_message = {
            "JobId": returned_job_id,
            "Status": "SUCCEEDED",
            "API": "StartDocumentAnalysis",
            "Timestamp": 1693745973466,
            "DocumentLocation": {
                "S3ObjectName": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
                "S3Bucket": "testing-ssa-pdf-store",
            },
        }

        sns_message = {
            "Records": [
                {
                    "EventSource": "aws:sns",
                    "EventVersion": "1.0",
                    "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                    "Sns": {
                        "Type": "Notification",
                        "MessageId": "a886eb90-af8e-5733-9107-b6fe2afbae39",
                        "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                        "Subject": "null",
                        "Message": json.dumps(textract_message),
                        "Timestamp": "2023-09-03T12:59:33.516Z",
                        "SignatureVersion": "1",
                        "Signature": "CG3+hONCIidvf9W+BneMu2Q0Jx/CFRmWpaawS1r5WMi3L/BA1Isir9weZmFDtWkFEbeT9hTeCSa+hgWp1tQH4Hrc0Y8j3PUoGE5wVtHMOjNJfRGo9lZxvX7+Lqbgpg+aohxrsdWPE0ryg5yBpBSeFooNAJzrcjcqaqGUk8PTF7lw1SxG8pENUxP0Vy2QJckEnXN7KNEPRdyEMUj/mxy29SFK++uKZysJ00BPWBNBjO/mLoCoY6uWUGNiX8n7J7QCZdhB1wtbFWHEXm0uqfYcC97W7wR+5ywshkF5NCrmbOtYzskBTIuUmaHTNNLbriAFG2foKI9iSRmqWXWcekOqDg==",
                        "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                        "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                        "MessageAttributes": {},
                    },
                }
            ]
        }

        # Wait for the Textract job to finish
        while True:
            # Check if the Textract job has been created in Firestore
            textract_job = fs.get_textract_job(returned_job_id)

            if not textract_job:
                self.fail("Textract job not found in Firestore")

            status = textract_job.get("status", "")

            if status == "SUCCEEDED":
                break

            time.sleep(5)

        textract_to_tables_response = lambda_client.invoke(
            FunctionName="Textract-to-Tables",
            InvocationType="RequestResponse",
            Payload=json.dumps(sns_message),
        )

        self.assertEqual(textract_to_tables_response["StatusCode"], 200)

        # Reading the payload
        textract_to_tables_payload_stream = textract_to_tables_response["Payload"]
        textract_to_tables_payload_data = textract_to_tables_payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        textract_to_tables_payload = json.loads(
            textract_to_tables_payload_data.decode()
        )

        if not textract_to_tables_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            textract_to_tables_payload["body"],
            "Successfully parsed textract to tables.",
        )

        tables_payload = json.loads(textract_to_tables_payload["payload"])

        event_tables = tables_payload.get("tables", [])
        pdf_hash = tables_payload.get("pdf_hash", "")
        job_id = tables_payload.get("job_id", "")

        if not event_tables:
            self.fail("No tables found in Textract-to-Tables payload")

        if not pdf_hash:
            self.fail("PDF hash not found in Textract-to-Tables payload")

        if not job_id:
            self.fail("Job ID not found in Textract-to-Tables payload")

        # Verify the job_id is the same from the start job payload and Textract-to-Tables payload
        self.assertEqual(job_id, returned_job_id)

        textract_doc = fs.get_textract_job(job_id)

        if not textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the textract_finished timestamp exists and is set properly
        textract_finished = textract_doc.get("textract_finished")

        if not textract_finished or not isinstance(textract_finished, datetime):
            self.fail("Textract finished timestamp not found in Textract job")

        # Verfiy the pdf_hash is the same from the start job payload and original pdf_doc
        self.assertEqual(pdf_hash, pdf_doc["hash"])
        self.assertEqual(pdf_hash, textract_doc["pdf_hash"])

        # Verify that the timestamps for the Textract-to-Tables exist
        # and are set properly in the textract job document
        start_time = textract_doc.get("tables_parsed_started")
        end_time = textract_doc.get("tables_parsed_finished")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Textract-to-Tables function exists
        # in the textract job document
        self.assertTrue(textract_doc.get("func_textract_to_tables_request_id"))
        self.assertTrue(textract_doc.get("func_textract_to_tables_name"))

        tables: List[Table] = []
        for i, table_dict in enumerate(event_tables):
            curr_table = Table.from_dict(table_dict)  # type: ignore[arg-type]

            if curr_table is None:
                logging.info("Failed to convert table %d from a dictionary", i)
                continue

            tables.append(curr_table)

        # In this specific PDF, there are 5 tables
        # PDF: tests/lambda-func-tests/textract-to-tables-iwakuni.pdf
        self.assertEqual(len(tables), 5)

        for i, table in enumerate(tables):
            test_table_path = f"tests/lambda-func-tests/TestTextractToTables/test_correct_function/osan_1_72hr_table-{i+1}.pkl"

            test_table = table.load_state(test_table_path)

            self.assertEqual(test_table, table)

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        # Delete any possible flights
        fs.delete_collection(current_flights_coll)
        fs.delete_collection(archive_flights_coll)

    def test_correct_testing_params(self: unittest.TestCase) -> None:
        """Test that the Textract-To-Tables function correctly handles the testing parameters and can find the documents.

        These test parameters are placed in Textract job document when the job is started by Start-PDF-Textract-Job.
        This verfies that inserting critical documents in newly created collections can still be retrieved by setting
        the testing parameters to the correct collections.
        """
        pdf_archive_coll = "FAKE_PDF_Archive"
        terminal_coll = "FAKE_Terminals"
        current_flights_coll = "FAKE_Current_Flights"
        archive_flights_coll = "FAKE_Archive_Flights"
        textract_job_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        terminal_doc = {
            "archiveDir": "archive/Osan_AB_Passenger_Terminal/",
            "group": "INDOPACOM TERMINALS",
            "link": "https://www.amc.af.mil/AMC-Travel-Site/Terminals/PACOM-Terminals/Osan-AB-Passenger-Terminal/",
            "location": "Osan AB, ROK",
            "name": "Osan AB Passenger Terminal",
            "pagePosition": 38,
            "pdf30DayHash": None,
            "pdf72HourHash": "0b72d4290b2a79b904a9bceddc3146fd8c7cd10e7df9b5f326efe085e2b4bad1",
            "pdfRollcallHash": None,
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            document_data=terminal_doc,
            doc_id=str(terminal_doc["name"]),  # Name will always be a string
        )

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
                            "name": "testing-ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::testing-ssa-pdf-store",
                        },
                        "object": {
                            "key": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
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
                "testDateTime": "197001010000",  # January 1, 1970 at 00:00
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

        self.assertEqual(start_job_response["StatusCode"], 200)

        # Reading the payload
        start_job_payload_stream = start_job_response["Payload"]
        start_job_payload_data = start_job_payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        start_job_payload = json.loads(start_job_payload_data.decode())

        if not start_job_payload:
            self.fail("Payload is empty")

        self.assertEqual(start_job_payload["body"], "Job started successfully.")

        returned_job_id = start_job_payload.get("job_id", "")

        if not returned_job_id or not isinstance(returned_job_id, str):
            self.fail("Job ID not returned")

        textract_message = {
            "JobId": returned_job_id,
            "Status": "SUCCEEDED",
            "API": "StartDocumentAnalysis",
            "Timestamp": 1693745973466,
            "DocumentLocation": {
                "S3ObjectName": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
                "S3Bucket": "testing-ssa-pdf-store",
            },
        }

        sns_message = {
            "Records": [
                {
                    "EventSource": "aws:sns",
                    "EventVersion": "1.0",
                    "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                    "Sns": {
                        "Type": "Notification",
                        "MessageId": "a886eb90-af8e-5733-9107-b6fe2afbae39",
                        "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                        "Subject": "null",
                        "Message": json.dumps(textract_message),
                        "Timestamp": "2023-09-03T12:59:33.516Z",
                        "SignatureVersion": "1",
                        "Signature": "CG3+hONCIidvf9W+BneMu2Q0Jx/CFRmWpaawS1r5WMi3L/BA1Isir9weZmFDtWkFEbeT9hTeCSa+hgWp1tQH4Hrc0Y8j3PUoGE5wVtHMOjNJfRGo9lZxvX7+Lqbgpg+aohxrsdWPE0ryg5yBpBSeFooNAJzrcjcqaqGUk8PTF7lw1SxG8pENUxP0Vy2QJckEnXN7KNEPRdyEMUj/mxy29SFK++uKZysJ00BPWBNBjO/mLoCoY6uWUGNiX8n7J7QCZdhB1wtbFWHEXm0uqfYcC97W7wR+5ywshkF5NCrmbOtYzskBTIuUmaHTNNLbriAFG2foKI9iSRmqWXWcekOqDg==",
                        "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                        "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                        "MessageAttributes": {},
                    },
                }
            ]
        }

        # Wait for the Textract job to finish
        while True:
            # Check if the Textract job has been created in Firestore
            textract_job = fs.get_textract_job(returned_job_id)

            if not textract_job:
                self.fail("Textract job not found in Firestore")

            status = textract_job.get("status", "")

            if status == "SUCCEEDED":
                break

            time.sleep(5)

        textract_to_tables_response = lambda_client.invoke(
            FunctionName="Textract-to-Tables",
            InvocationType="RequestResponse",
            Payload=json.dumps(sns_message),
        )

        self.assertEqual(textract_to_tables_response["StatusCode"], 200)

        # Reading the payload
        textract_to_tables_payload_stream = textract_to_tables_response["Payload"]
        textract_to_tables_payload_data = textract_to_tables_payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        textract_to_tables_payload = json.loads(
            textract_to_tables_payload_data.decode()
        )

        if not textract_to_tables_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            textract_to_tables_payload["body"],
            "Successfully parsed textract to tables.",
        )

        tables_payload = json.loads(textract_to_tables_payload["payload"])

        event_tables = tables_payload.get("tables", [])
        pdf_hash = tables_payload.get("pdf_hash", "")
        job_id = tables_payload.get("job_id", "")

        if not event_tables:
            self.fail("No tables found in Textract-to-Tables payload")

        if not pdf_hash:
            self.fail("PDF hash not found in Textract-to-Tables payload")

        if not job_id:
            self.fail("Job ID not found in Textract-to-Tables payload")

        # Verify the job_id is the same from the start job payload and Textract-to-Tables payload
        self.assertEqual(job_id, returned_job_id)

        textract_doc = fs.get_textract_job(job_id)

        if not textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the textract_finished timestamp exists and is set properly
        textract_finished = textract_doc.get("textract_finished")

        if not textract_finished or not isinstance(textract_finished, datetime):
            self.fail("Textract finished timestamp not found in Textract job")

        # Verfiy the pdf_hash is the same from the start job payload and original pdf_doc
        self.assertEqual(pdf_hash, pdf_doc["hash"])
        self.assertEqual(pdf_hash, textract_doc["pdf_hash"])

        # Verify that the timestamps for the Textract-to-Tables exist
        # and are set properly in the textract job document
        start_time = textract_doc.get("tables_parsed_started")
        end_time = textract_doc.get("tables_parsed_finished")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Textract-to-Tables function exists
        # in the textract job document
        self.assertTrue(textract_doc.get("func_textract_to_tables_request_id"))
        self.assertTrue(textract_doc.get("func_textract_to_tables_name"))

        tables: List[Table] = []
        for i, table_dict in enumerate(event_tables):
            curr_table = Table.from_dict(table_dict)  # type: ignore[arg-type]

            if curr_table is None:
                logging.info("Failed to convert table %d from a dictionary", i)
                continue

            tables.append(curr_table)

        # In this specific PDF, there are 5 tables
        # PDF: tests/lambda-func-tests/textract-to-tables-osan.pdf
        self.assertEqual(len(tables), 5)

        for i, table in enumerate(tables):
            test_table_path = f"tests/lambda-func-tests/TestTextractToTables/test_correct_function/osan_1_72hr_table-{i+1}.pkl"

            test_table = table.load_state(test_table_path)

            self.assertEqual(test_table, table)

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )
        # Delete the Terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=str(terminal_doc["name"])
        )

        # Delete any possible created flights
        fs.delete_collection(current_flights_coll)
        fs.delete_collection(archive_flights_coll)

    def test_bad_test_testing_params_fails(self: unittest.TestCase) -> None:
        """Send incorrect testing parameters to the Textract-to-Tables function and verify it fails.

        In this test we create documents in the FAKE_PDF_Archive and FAKE_Terminals collections. Then, we
        tell the Textract-to-Tables function to look in the FAKE_PDF_Archive888 and FAKE_Terminals888 collections
        which do not exist. This should cause the function to fail.

        NOTE: This will generate errors in Sentry. This is expected behavior.
        """
        pdf_archive_coll = "FAKE_PDF_Archive"
        terminal_coll = "FAKE_Terminals"
        textract_job_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        terminal_doc = {
            "archiveDir": "archive/Osan_AB_Passenger_Terminal/",
            "group": "INDOPACOM TERMINALS",
            "link": "https://www.amc.af.mil/AMC-Travel-Site/Terminals/PACOM-Terminals/Osan-AB-Passenger-Terminal/",
            "location": "Osan AB, ROK",
            "name": "Osan AB Passenger Terminal",
            "pagePosition": 38,
            "pdf30DayHash": None,
            "pdf72HourHash": "0b72d4290b2a79b904a9bceddc3146fd8c7cd10e7df9b5f326efe085e2b4bad1",
            "pdfRollcallHash": None,
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            document_data=terminal_doc,
            doc_id=str(terminal_doc["name"]),  # Name will always be a string
        )

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
                            "name": "testing-ssa-pdf-store",
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": "arn:aws:s3:::testing-ssa-pdf-store",
                        },
                        "object": {
                            "key": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
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
                "testDateTime": "197001010000",
                "testPdfArchiveColl": pdf_archive_coll,  # Leave these correct so start job works
                "testTerminalColl": terminal_coll,
            },
        }

        start_job_response = lambda_client.invoke(
            FunctionName="Start-PDF-Textract-Job",
            InvocationType="RequestResponse",
            Payload=json.dumps(start_job_payload),
        )

        self.assertEqual(start_job_response["StatusCode"], 200)

        # Reading the payload
        start_job_payload_stream = start_job_response["Payload"]
        start_job_payload_data = start_job_payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        start_job_payload = json.loads(start_job_payload_data.decode())

        if not start_job_payload:
            self.fail("Payload is empty")

        self.assertEqual(start_job_payload["body"], "Job started successfully.")

        returned_job_id = start_job_payload.get("job_id", "")

        if not returned_job_id or not isinstance(returned_job_id, str):
            self.fail("Job ID not returned")

        textract_message = {
            "JobId": returned_job_id,
            "Status": "SUCCEEDED",
            "API": "StartDocumentAnalysis",
            "Timestamp": 1693745973466,
            "DocumentLocation": {
                "S3ObjectName": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
                "S3Bucket": "testing-ssa-pdf-store",
            },
        }

        sns_message = {
            "Records": [
                {
                    "EventSource": "aws:sns",
                    "EventVersion": "1.0",
                    "EventSubscriptionArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                    "Sns": {
                        "Type": "Notification",
                        "MessageId": "a886eb90-af8e-5733-9107-b6fe2afbae39",
                        "TopicArn": "arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results",
                        "Subject": "null",
                        "Message": json.dumps(textract_message),
                        "Timestamp": "2023-09-03T12:59:33.516Z",
                        "SignatureVersion": "1",
                        "Signature": "CG3+hONCIidvf9W+BneMu2Q0Jx/CFRmWpaawS1r5WMi3L/BA1Isir9weZmFDtWkFEbeT9hTeCSa+hgWp1tQH4Hrc0Y8j3PUoGE5wVtHMOjNJfRGo9lZxvX7+Lqbgpg+aohxrsdWPE0ryg5yBpBSeFooNAJzrcjcqaqGUk8PTF7lw1SxG8pENUxP0Vy2QJckEnXN7KNEPRdyEMUj/mxy29SFK++uKZysJ00BPWBNBjO/mLoCoY6uWUGNiX8n7J7QCZdhB1wtbFWHEXm0uqfYcC97W7wR+5ywshkF5NCrmbOtYzskBTIuUmaHTNNLbriAFG2foKI9iSRmqWXWcekOqDg==",
                        "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                        "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-2:761356229218:Textract-PDF-Flight-Data-Job-Results:7b8ea79b-8065-4587-8303-3128664d7615",
                        "MessageAttributes": {},
                    },
                }
            ]
        }

        # Wait for the Textract job to finish
        while True:
            # Check if the Textract job has been created in Firestore
            textract_job = fs.get_textract_job(returned_job_id)

            if not textract_job:
                self.fail("Textract job not found in Firestore")

            status = textract_job.get("status", "")

            if status == "SUCCEEDED":
                break

            time.sleep(5)

        # Set the testing parameters to the wrong collections
        bad_collections = {
            "testParameters": {
                "sendPdf": True,
                "testDateTime": "197001010000",
                "testPdfArchiveColl": "FAKE_PDF_Archive888",
                "testTerminalColl": "FAKE_Terminals888",
            }
        }

        fs.append_to_doc("Textract_Jobs", returned_job_id, bad_collections)

        textract_to_tables_response = lambda_client.invoke(
            FunctionName="Textract-to-Tables",
            InvocationType="RequestResponse",
            Payload=json.dumps(sns_message),
        )

        self.assertEqual(textract_to_tables_response["StatusCode"], 200)

        # Reading the payload
        textract_to_tables_payload_stream = textract_to_tables_response["Payload"]
        textract_to_tables_payload_data = textract_to_tables_payload_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        textract_to_tables_payload = json.loads(
            textract_to_tables_payload_data.decode()
        )

        if not textract_to_tables_payload:
            self.fail("Payload is empty")

        self.assertEqual(textract_to_tables_payload["errorType"], "ValueError")
        self.assertEqual(
            textract_to_tables_payload["errorMessage"],
            "Failed to get PDF hash using s3 object path (current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf) from Firestore.",
        )

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )
        # Delete the Terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=str(terminal_doc["name"])
        )


class TestProcess72HrFlights(unittest.TestCase):
    """Test the Process-72-Hour-Flights function."""

    def test_correct_function(self: unittest.TestCase) -> None:
        """Test that the Process-72HR-Flights function correctly processes the 72 hour flights in a production context.

        This works by taking pregenerated payloads and sending them to the Process-72HR-Flights function. Then,
        the test case checks that the proper flights and asscoiated data are parsed by comparing them to pickled
        known good flights. Additionally, it checks firestore to make sure that function writes timestamps and correct
        data to firestore.
        """
        pdf_archive_coll = "**TESTING**_PDF_Archive"
        terminal_coll = "**TESTING**_Terminals"
        current_flight_coll = "**TESTING**_Current_Flights"
        archive_flight_coll = "**TESTING**_Archive_Flights"
        textract_job_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Create a fake Textract job document for the lambda function to append values to
        job_id = "TEST_Textract_Job_Doc"
        textract_doc = {
            "desc": "Test Textract Job document for testing Process-72HR-Flights function",
            "test": True,
            "testParameters": {
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flight_coll,
                "testArchiveFlightsColl": archive_flight_coll,
            },
        }

        fs.insert_document_with_id(textract_job_coll, job_id, textract_doc)

        tables: List[Table] = []

        # Load in pickled tables
        osan_1_72hr_table_1 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_table-1.pkl"
        )

        if osan_1_72hr_table_1 is None:
            self.fail("Failed to load table 1 from pickle file")

        osan_1_72hr_table_2 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_table-2.pkl"
        )

        if osan_1_72hr_table_2 is None:
            self.fail("Failed to load table 2 from pickle file")

        osan_1_72hr_table_3 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_table-3.pkl"
        )

        if osan_1_72hr_table_3 is None:
            self.fail("Failed to load table 3 from pickle file")

        osan_1_72hr_table_4 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_table-4.pkl"
        )

        if osan_1_72hr_table_4 is None:
            self.fail("Failed to load table 4 from pickle file")

        osan_1_72hr_table_5 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_table-5.pkl"
        )

        if osan_1_72hr_table_5 is None:
            self.fail("Failed to load table 5 from pickle file")

        tables.append(osan_1_72hr_table_1)
        tables.append(osan_1_72hr_table_2)
        tables.append(osan_1_72hr_table_3)
        tables.append(osan_1_72hr_table_4)
        tables.append(osan_1_72hr_table_5)

        serialized_tables = [table.to_dict() for table in tables]

        payload = json.dumps(
            {
                "tables": serialized_tables,
                "pdf_hash": pdf_doc["hash"],
                "job_id": job_id,
            }
        )

        process_72hr_flights_response = lambda_client.invoke(
            FunctionName="Process-72HR-Flights",
            InvocationType="RequestResponse",
            Payload=payload,
        )

        self.assertEqual(process_72hr_flights_response["StatusCode"], 200)

        # Reading the payload
        process_72hr_flights_stream = process_72hr_flights_response["Payload"]
        process_72hr_flights_data = process_72hr_flights_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        process_72hr_flights_payload = json.loads(process_72hr_flights_data.decode())

        if not process_72hr_flights_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            process_72hr_flights_payload["body"],
            "Finished processing 72-hour flights.",
        )

        flights_payload = json.loads(process_72hr_flights_payload["payload"])

        # Check that the pdf hash is correct
        self.assertEqual(flights_payload["pdf_hash"], pdf_doc["hash"])

        # Check that the job id is correct
        self.assertEqual(flights_payload["job_id"], job_id)

        # Check that the terminal name is correct
        self.assertEqual(flights_payload["terminal"], pdf_doc["terminal"])

        # Check that the flights are correct
        flight_dicts = flights_payload.get("flights", [])

        if not flight_dicts:
            self.fail("No flights found in Process-72HR-Flights payload")

        self.assertEqual(len(flight_dicts), 4)

        # Create a list of flights from the dictionaries
        converted_flights: List[Flight] = []

        for flight_dict in flight_dicts:
            flight = Flight.from_dict(flight_dict)

            if not flight:
                self.fail("Failed to convert flight from dictionary")

            converted_flights.append(flight)

        # Load in pickled flights
        osan_1_72hr_flight_0 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_flight-0_fs.pkl"
        )

        if not osan_1_72hr_flight_0:
            self.fail("Failed to load flight 0 from pickle file")

        osan_1_72hr_flight_1 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_flight-1_fs.pkl"
        )

        if not osan_1_72hr_flight_1:
            self.fail("Failed to load flight 1 from pickle file")

        osan_1_72hr_flight_2 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_flight-2_fs.pkl"
        )

        if not osan_1_72hr_flight_2:
            self.fail("Failed to load flight 2 from pickle file")

        osan_1_72hr_flight_3 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_function/osan_1_72hr_flight-3_fs.pkl"
        )

        if not osan_1_72hr_flight_3:
            self.fail("Failed to load flight 3 from pickle file")

        loaded_flights = [
            osan_1_72hr_flight_0,
            osan_1_72hr_flight_1,
            osan_1_72hr_flight_2,
            osan_1_72hr_flight_3,
        ]

        # Correct year in the loaded flights to the current year
        current_year = str(datetime.now(tz=pytz.UTC).year)

        for flight in loaded_flights:
            flight.as_string = flight.as_string.replace("2023", current_year)
            flight.date = flight.date.replace("2023", current_year)
            flight.flight_id = flight.generate_flight_id()

        # Sort flight by flight_id
        converted_flights.sort(key=lambda x: x.flight_id)
        loaded_flights.sort(key=lambda x: x.flight_id)

        # Check that the flights are equal
        for i, flight in enumerate(converted_flights):
            self.assertEqual(flight, loaded_flights[i])

        # Check that the proper information is written to Textract job document
        testing_textract_doc = fs.get_textract_job(job_id)

        if not testing_textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the timestamps for the Process-72HR-Flights exist
        # and are set properly in the textract job document
        start_time = testing_textract_doc.get("started_72hr_processing")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        end_time = testing_textract_doc.get("finished_72hr_processing")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Process-72HR-Flights function exists
        # in the textract job document
        request_id = testing_textract_doc.get("func_72hr_request_id")

        if not request_id:
            self.fail("Request ID not found in Textract job")

        function_name = testing_textract_doc.get("func_72hr_name")

        if not function_name:
            self.fail("Function name not found in Textract job")

        # Verify that the correct flight ids are written to the Textract job document
        flight_ids = testing_textract_doc.get("flight_ids")

        if not flight_ids:
            self.fail("Flight IDs not found in Textract job")

        self.assertEqual(len(flight_ids), 4)

        flight_ids.sort()

        for i, flight_id in enumerate(flight_ids):
            self.assertEqual(flight_id, converted_flights[i].flight_id)

        # Verify the number of flights is correctly written to the Textract job document
        num_flights = testing_textract_doc.get("num_flights")

        if not num_flights:
            self.fail("Number of flights not found in Textract job")

        self.assertEqual(num_flights, 4)

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the Textract job document
        fs.delete_document_by_id(collection_name=textract_job_coll, doc_id=job_id)

        # Delete any possible created flights
        fs.delete_collection(current_flight_coll)
        fs.delete_collection(archive_flight_coll)

    def test_correct_testing_parameters_no_date(self: unittest.TestCase) -> None:
        """Test that the Process-72HR-Flights function correctly handles the testing collection parameters processes the 72 hour flights.

        This works by taking pregenerated payloads and sending them to the Process-72HR-Flights function. Then,
        setting the PDF_ARCHIVE_COLLECTION to FAKE_PDF_Archive and the TERMINAL_COLLECTION to FAKE_Terminals. It then
        uploads the pdf archive document to this fake collection and verifies that the function correctly processes the
        data using the fake collection.

        NOTE: FAKE_Terminals is not used by Process-72HR-Flights, so it is not tested here.
        """
        fake_pdf_archive_coll = "FAKE_PDF_Archive"
        fake_terminal_coll = "FAKE_Terminals"
        fake_current_flights_coll = "FAKE_Current_Flights"
        fake_archive_flights_coll = "FAKE_Archive_Flights"
        textract_job_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        fs = FirestoreClient(
            pdf_archive_coll=fake_pdf_archive_coll,
            terminal_coll=fake_terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=fake_pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Create a fake Textract job document for the lambda function to append values to
        job_id = "TEST_Textract_Job_Doc"
        textract_doc = {
            "desc": "Test Textract Job document for testing Process-72HR-Flights function",
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testPdfArchiveColl": fake_pdf_archive_coll,
                "testTerminalColl": fake_terminal_coll,
                "testCurrentFlightsColl": fake_current_flights_coll,
                "testArchiveFlightsColl": fake_archive_flights_coll,
            },
        }

        fs.insert_document_with_id(textract_job_coll, job_id, textract_doc)

        tables: List[Table] = []

        # Load in pickled tables
        osan_1_72hr_table_1 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-1.pkl"
        )

        if osan_1_72hr_table_1 is None:
            self.fail("Failed to load table 1 from pickle file")

        osan_1_72hr_table_2 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-2.pkl"
        )

        if osan_1_72hr_table_2 is None:
            self.fail("Failed to load table 2 from pickle file")

        osan_1_72hr_table_3 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-3.pkl"
        )

        if osan_1_72hr_table_3 is None:
            self.fail("Failed to load table 3 from pickle file")

        osan_1_72hr_table_4 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-4.pkl"
        )

        if osan_1_72hr_table_4 is None:
            self.fail("Failed to load table 4 from pickle file")

        osan_1_72hr_table_5 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-5.pkl"
        )

        if osan_1_72hr_table_5 is None:
            self.fail("Failed to load table 5 from pickle file")

        tables.append(osan_1_72hr_table_1)
        tables.append(osan_1_72hr_table_2)
        tables.append(osan_1_72hr_table_3)
        tables.append(osan_1_72hr_table_4)
        tables.append(osan_1_72hr_table_5)

        serialized_tables = [table.to_dict() for table in tables]

        payload = json.dumps(
            {
                "tables": serialized_tables,
                "pdf_hash": pdf_doc["hash"],
                "job_id": job_id,
            }
        )

        process_72hr_flights_response = lambda_client.invoke(
            FunctionName="Process-72HR-Flights",
            InvocationType="RequestResponse",
            Payload=payload,
        )

        self.assertEqual(process_72hr_flights_response["StatusCode"], 200)

        # Reading the payload
        process_72hr_flights_stream = process_72hr_flights_response["Payload"]
        process_72hr_flights_data = process_72hr_flights_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        process_72hr_flights_payload = json.loads(process_72hr_flights_data.decode())

        if not process_72hr_flights_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            process_72hr_flights_payload["body"],
            "Finished processing 72-hour flights.",
        )

        flights_payload = json.loads(process_72hr_flights_payload["payload"])

        # Check that the pdf hash is correct
        self.assertEqual(flights_payload["pdf_hash"], pdf_doc["hash"])

        # Check that the job id is correct
        self.assertEqual(flights_payload["job_id"], job_id)

        # Check that the terminal name is correct
        self.assertEqual(flights_payload["terminal"], pdf_doc["terminal"])

        # Check that the flights are correct
        flight_dicts = flights_payload.get("flights", [])

        if not flight_dicts:
            self.fail("No flights found in Process-72HR-Flights payload")

        self.assertEqual(len(flight_dicts), 4)

        # Create a list of flights from the dictionaries
        converted_flights: List[Flight] = []

        for flight_dict in flight_dicts:
            flight = Flight.from_dict(flight_dict)

            if not flight:
                self.fail("Failed to convert flight from dictionary")

            converted_flights.append(flight)

        # Load in pickled flights
        osan_1_72hr_flight_0 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_flight-0_fs.pkl"
        )

        if not osan_1_72hr_flight_0:
            self.fail("Failed to load flight 0 from pickle file")

        osan_1_72hr_flight_1 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_flight-1_fs.pkl"
        )

        if not osan_1_72hr_flight_1:
            self.fail("Failed to load flight 1 from pickle file")

        osan_1_72hr_flight_2 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_flight-2_fs.pkl"
        )

        if not osan_1_72hr_flight_2:
            self.fail("Failed to load flight 2 from pickle file")

        osan_1_72hr_flight_3 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_flight-3_fs.pkl"
        )

        if not osan_1_72hr_flight_3:
            self.fail("Failed to load flight 3 from pickle file")

        loaded_flights = [
            osan_1_72hr_flight_0,
            osan_1_72hr_flight_1,
            osan_1_72hr_flight_2,
            osan_1_72hr_flight_3,
        ]

        # Sort flight by flight_id
        converted_flights.sort(key=lambda x: x.flight_id)
        loaded_flights.sort(key=lambda x: x.flight_id)

        # Check that the flights are equal
        for i, flight in enumerate(converted_flights):
            self.assertEqual(flight, loaded_flights[i])

        # Check that the proper information is written to Textract job document
        testing_textract_doc = fs.get_textract_job(job_id)

        if not testing_textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the timestamps for the Process-72HR-Flights exist
        # and are set properly in the textract job document
        start_time = testing_textract_doc.get("started_72hr_processing")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        end_time = testing_textract_doc.get("finished_72hr_processing")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Process-72HR-Flights function exists
        # in the textract job document
        request_id = testing_textract_doc.get("func_72hr_request_id")

        if not request_id:
            self.fail("Request ID not found in Textract job")

        function_name = testing_textract_doc.get("func_72hr_name")

        if not function_name:
            self.fail("Function name not found in Textract job")

        # Verify that the correct flight ids are written to the Textract job document
        flight_ids = testing_textract_doc.get("flight_ids")

        if not flight_ids:
            self.fail("Flight IDs not found in Textract job")

        self.assertEqual(len(flight_ids), 4)

        flight_ids.sort()

        for i, flight_id in enumerate(flight_ids):
            self.assertEqual(flight_id, converted_flights[i].flight_id)

        # Verify the number of flights is correctly written to the Textract job document
        num_flights = testing_textract_doc.get("num_flights")

        if not num_flights:
            self.fail("Number of flights not found in Textract job")

        self.assertEqual(num_flights, 4)

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=fake_pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the Textract job document
        fs.delete_document_by_id(collection_name=textract_job_coll, doc_id=job_id)

        # Delete any possible created flights
        fs.delete_collection(fake_current_flights_coll)
        fs.delete_collection(fake_archive_flights_coll)

    def test_correct_testing_parameters_no_date_fail(self: unittest.TestCase) -> None:
        """Test that the Process-72HR-Flights function correctly handles the testing collection parameters processes the 72 hour flights.

        This works by taking pregenerated payloads and sending them to the Process-72HR-Flights function. Then,
        setting the PDF_ARCHIVE_COLLECTION to FAKE_PDF_Archive and the TERMINAL_COLLECTION to FAKE_Terminals. It then
        uploads the pdf archive document to a different fake collection and verifies that the function correctly fails
        to finish because the data is not in the correct collection.

        NOTE: FAKE_Terminals is not used by Process-72HR-Flights, so it is not tested here.

        NOTE 2: This will generate errors in Sentry. This is expected behavior.
        """
        fake_pdf_archive_coll = "FAKE_PDF_Archive"
        fake_terminal_coll = "FAKE_Terminals"
        fake_current_flights_coll = "FAKE_Current_Flights"
        fake_archive_flights_coll = "FAKE_Archive_Flights"
        textract_job_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        fs = FirestoreClient(
            pdf_archive_coll=fake_pdf_archive_coll,
            terminal_coll=fake_terminal_coll,
            textract_jobs_coll=textract_job_coll,
        )

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=fake_pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Create a fake Textract job document for the lambda function to append values to
        job_id = "TEST_Textract_Job_Doc"
        textract_doc = {
            "desc": "Test Textract Job document for testing Process-72HR-Flights function",
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testPdfArchiveColl": fake_pdf_archive_coll + "888",
                "testTerminalColl": fake_terminal_coll,
                "testCurrentFlightsColl": fake_current_flights_coll,
                "testArchiveFlightsColl": fake_archive_flights_coll,
            },
        }

        fs.insert_document_with_id(textract_job_coll, job_id, textract_doc)

        tables: List[Table] = []

        # Load in pickled tables
        osan_1_72hr_table_1 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-1.pkl"
        )

        if osan_1_72hr_table_1 is None:
            self.fail("Failed to load table 1 from pickle file")

        osan_1_72hr_table_2 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-2.pkl"
        )

        if osan_1_72hr_table_2 is None:
            self.fail("Failed to load table 2 from pickle file")

        osan_1_72hr_table_3 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-3.pkl"
        )

        if osan_1_72hr_table_3 is None:
            self.fail("Failed to load table 3 from pickle file")

        osan_1_72hr_table_4 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-4.pkl"
        )

        if osan_1_72hr_table_4 is None:
            self.fail("Failed to load table 4 from pickle file")

        osan_1_72hr_table_5 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_no_date/osan_1_72hr_table-5.pkl"
        )

        if osan_1_72hr_table_5 is None:
            self.fail("Failed to load table 5 from pickle file")

        tables.append(osan_1_72hr_table_1)
        tables.append(osan_1_72hr_table_2)
        tables.append(osan_1_72hr_table_3)
        tables.append(osan_1_72hr_table_4)
        tables.append(osan_1_72hr_table_5)

        serialized_tables = [table.to_dict() for table in tables]

        payload = json.dumps(
            {
                "tables": serialized_tables,
                "pdf_hash": pdf_doc["hash"],
                "job_id": job_id,
            }
        )

        process_72hr_flights_response = lambda_client.invoke(
            FunctionName="Process-72HR-Flights",
            InvocationType="RequestResponse",
            Payload=payload,
        )

        self.assertEqual(process_72hr_flights_response["StatusCode"], 200)

        # Reading the payload
        process_72hr_flights_stream = process_72hr_flights_response["Payload"]
        process_72hr_flights_data = process_72hr_flights_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        process_72hr_flights_payload = json.loads(process_72hr_flights_data.decode())

        if not process_72hr_flights_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            process_72hr_flights_payload["errorMessage"],
            f"Error occurred: Could not retrieve terminal name for pdf_hash: {pdf_doc['hash']}. Searching in {fake_pdf_archive_coll + '888'} collection.",
        )

        self.assertEqual(process_72hr_flights_payload["errorType"], "ValueError")

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=fake_pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the Textract job document
        fs.delete_document_by_id(collection_name=textract_job_coll, doc_id=job_id)

        # Delete any possible created flights
        fs.delete_collection(fake_current_flights_coll)
        fs.delete_collection(fake_archive_flights_coll)

    def test_correct_testing_parameters_with_date(self: unittest.TestCase) -> None:
        """Test that the Process-72HR-Flights function correctly handles the testing collection parameters processes the 72 hour flights.

        This works by taking pregenerated payloads and sending them to the Process-72HR-Flights function. Then,
        setting the PDF_ARCHIVE_COLLECTION to FAKE_PDF_Archive and the TERMINAL_COLLECTION to FAKE_Terminals. It then
        uploads the pdf archive document to this fake collection and verifies that the function correctly processes the
        data using the fake collection.

        Additionally, this test function sets the testDateTime parameter to a date one day in the future. This is to verify
        that setting the date can be used for testing. Specifically, this test sets the current day to a July 10, 2100 and
        checks that the flight dates are correctly reported to be in 2100.

        NOTE: FAKE_Terminals is not used by Process-72HR-Flights, so it is not tested here.
        """
        fake_pdf_archive_coll = "FAKE_PDF_Archive"
        fake_terminal_coll = "FAKE_Terminals"
        fake_current_flights_coll = "FAKE_Current_Flights"
        fake_archive_flights_coll = "FAKE_Archive_Flights"
        textract_job_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=fake_pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Create a fake Textract job document for the lambda function to append values to
        job_id = "TEST_Textract_Job_Doc"
        textract_doc = {
            "desc": "Test Textract Job document for testing Process-72HR-Flights function",
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testPdfArchiveColl": fake_pdf_archive_coll,
                "testTerminalColl": fake_terminal_coll,
                "testDateTime": "210007101755",  # July 10, 2100 at 17:55
                "testCurrentFlightsColl": fake_current_flights_coll,
                "testArchiveFlightsColl": fake_archive_flights_coll,
            },
        }

        fs.insert_document_with_id(textract_job_coll, job_id, textract_doc)

        tables: List[Table] = []

        # Load in pickled tables
        osan_1_72hr_table_1 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_table-1.pkl"
        )

        if osan_1_72hr_table_1 is None:
            self.fail("Failed to load table 1 from pickle file")

        osan_1_72hr_table_2 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_table-2.pkl"
        )

        if osan_1_72hr_table_2 is None:
            self.fail("Failed to load table 2 from pickle file")

        osan_1_72hr_table_3 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_table-3.pkl"
        )

        if osan_1_72hr_table_3 is None:
            self.fail("Failed to load table 3 from pickle file")

        osan_1_72hr_table_4 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_table-4.pkl"
        )

        if osan_1_72hr_table_4 is None:
            self.fail("Failed to load table 4 from pickle file")

        osan_1_72hr_table_5 = Table.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_table-5.pkl"
        )

        if osan_1_72hr_table_5 is None:
            self.fail("Failed to load table 5 from pickle file")

        tables.append(osan_1_72hr_table_1)
        tables.append(osan_1_72hr_table_2)
        tables.append(osan_1_72hr_table_3)
        tables.append(osan_1_72hr_table_4)
        tables.append(osan_1_72hr_table_5)

        serialized_tables = [table.to_dict() for table in tables]

        payload = json.dumps(
            {
                "tables": serialized_tables,
                "pdf_hash": pdf_doc["hash"],
                "job_id": job_id,
            }
        )

        process_72hr_flights_response = lambda_client.invoke(
            FunctionName="Process-72HR-Flights",
            InvocationType="RequestResponse",
            Payload=payload,
        )

        self.assertEqual(process_72hr_flights_response["StatusCode"], 200)

        # Reading the payload
        process_72hr_flights_stream = process_72hr_flights_response["Payload"]
        process_72hr_flights_data = process_72hr_flights_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        process_72hr_flights_payload = json.loads(process_72hr_flights_data.decode())

        if not process_72hr_flights_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            process_72hr_flights_payload["body"],
            "Finished processing 72-hour flights.",
        )

        flights_payload = json.loads(process_72hr_flights_payload["payload"])

        # Check that the pdf hash is correct
        self.assertEqual(flights_payload["pdf_hash"], pdf_doc["hash"])

        # Check that the job id is correct
        self.assertEqual(flights_payload["job_id"], job_id)

        # Check that the terminal name is correct
        self.assertEqual(flights_payload["terminal"], pdf_doc["terminal"])

        # Check that the flights are correct
        flight_dicts = flights_payload.get("flights", [])

        if not flight_dicts:
            self.fail("No flights found in Process-72HR-Flights payload")

        self.assertEqual(len(flight_dicts), 4)

        # Create a list of flights from the dictionaries
        converted_flights: List[Flight] = []

        for flight_dict in flight_dicts:
            flight = Flight.from_dict(flight_dict)

            if not flight:
                self.fail("Failed to convert flight from dictionary")

            converted_flights.append(flight)

        # Load in pickled flights
        osan_1_72hr_flight_0 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_flight-0_fs.pkl"
        )

        if not osan_1_72hr_flight_0:
            self.fail("Failed to load flight 0 from pickle file")

        osan_1_72hr_flight_1 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_flight-1_fs.pkl"
        )

        if not osan_1_72hr_flight_1:
            self.fail("Failed to load flight 1 from pickle file")

        osan_1_72hr_flight_2 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_flight-2_fs.pkl"
        )

        if not osan_1_72hr_flight_2:
            self.fail("Failed to load flight 2 from pickle file")

        osan_1_72hr_flight_3 = Flight.load_state(
            "tests/lambda-func-tests/TestProcess72HrFlights/test_correct_testing_parameters_with_date/osan_1_72hr_flight-3_fs.pkl"
        )

        if not osan_1_72hr_flight_3:
            self.fail("Failed to load flight 3 from pickle file")

        loaded_flights = [
            osan_1_72hr_flight_0,
            osan_1_72hr_flight_1,
            osan_1_72hr_flight_2,
            osan_1_72hr_flight_3,
        ]

        # Sort flight by flight_id
        converted_flights.sort(key=lambda x: x.flight_id)
        loaded_flights.sort(key=lambda x: x.flight_id)

        # Check that the flights are equal
        for i, flight in enumerate(converted_flights):
            self.assertEqual(flight, loaded_flights[i])

        # Check that the proper information is written to Textract job document
        testing_textract_doc = fs.get_textract_job(job_id)

        if not testing_textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the timestamps for the Process-72HR-Flights exist
        # and are set properly in the textract job document
        start_time = testing_textract_doc.get("started_72hr_processing")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        end_time = testing_textract_doc.get("finished_72hr_processing")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Process-72HR-Flights function exists
        # in the textract job document
        request_id = testing_textract_doc.get("func_72hr_request_id")

        if not request_id:
            self.fail("Request ID not found in Textract job")

        function_name = testing_textract_doc.get("func_72hr_name")

        if not function_name:
            self.fail("Function name not found in Textract job")

        # Verify that the correct flight ids are written to the Textract job document
        flight_ids = testing_textract_doc.get("flight_ids")

        if not flight_ids:
            self.fail("Flight IDs not found in Textract job")

        self.assertEqual(len(flight_ids), 4)

        flight_ids.sort()

        for i, flight_id in enumerate(flight_ids):
            self.assertEqual(flight_id, converted_flights[i].flight_id)

        # Verify the number of flights is correctly written to the Textract job document
        num_flights = testing_textract_doc.get("num_flights")

        if not num_flights:
            self.fail("Number of flights not found in Textract job")

        self.assertEqual(num_flights, 4)

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=fake_pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the Textract job document
        fs.delete_document_by_id(collection_name=textract_job_coll, doc_id=job_id)

        # Delete any possible created flights
        fs.delete_collection(fake_current_flights_coll)
        fs.delete_collection(fake_archive_flights_coll)


class TestStoreFlights(unittest.TestCase):
    """Tests the Store-Flights lambda function."""

    def test_correct_function_with_test_values_and_date(
        self: unittest.TestCase,
    ) -> None:
        """Verifies that the Store-Flights function correctly stores the flights in Firestore.

        It does this by inserting 4 flights into the current flights collection and then sending a payload to the
        Store-Flights function with the same 4 flights. The function is set to use November 4, 2023 at 09:08 as the
        current time which means the first two flights are past and the last two flights are in the future. What this means
        is that the past 2 flights in current flights should be archived and the 2 flights in the future will be deleted.

        The 4 same flights are sent to function. It should then delete the two past flights and insert the two future flights
        into current flights collection.
        """
        pdf_archive_coll = "**TESTING**_PDF_Archive666"
        terminal_coll = "**TESTING**_Terminals666"
        current_flights_coll = "**TESTING**_Flights_Current666"
        archive_flights_coll = "**TESTING**_Flights_Archive666"
        textract_jobs_coll = "Textract_Jobs"

        test_date = "202311040908"  # November 4, 2023 at 09:08

        lambda_client = initialize_client("lambda")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_jobs_coll,
            flight_current_coll=current_flights_coll,
            flight_archive_coll=archive_flights_coll,
        )

        # Create a fake Textract job
        job_id = "TEST_Textract_Job_Doc"
        textract_doc = {
            "desc": "Test Textract Job document for testing Store-Flights function",
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testDateTime": test_date,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        fs.insert_document_with_id("Textract_Jobs", job_id, textract_doc)

        # Create a the fake pdf archive document
        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Create a fake terminal document
        terminal_doc = {
            "name": "Osan AB Passenger Terminal",
            "location": "Osan AB, ROK",
            "timezone": "Asia/Seoul",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            document_data=terminal_doc,
            doc_id=terminal_doc["name"],
        )

        # Load in pickled flights
        osan_1_72hr_flight_0 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-0_fs.pkl"
        )

        if not osan_1_72hr_flight_0:
            self.fail("Failed to load flight 0 from pickle file")

        osan_1_72hr_flight_1 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-1_fs.pkl"
        )

        if not osan_1_72hr_flight_1:
            self.fail("Failed to load flight 1 from pickle file")

        osan_1_72hr_flight_2 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-2_fs.pkl"
        )

        if not osan_1_72hr_flight_2:
            self.fail("Failed to load flight 2 from pickle file")

        osan_1_72hr_flight_3 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-3_fs.pkl"
        )

        if not osan_1_72hr_flight_3:
            self.fail("Failed to load flight 3 from pickle file")

        # Create a list of flights
        flights = [
            osan_1_72hr_flight_0.to_dict(),
            osan_1_72hr_flight_1.to_dict(),
            osan_1_72hr_flight_2.to_dict(),
            osan_1_72hr_flight_3.to_dict(),
        ]

        old_flights_copy = copy.deepcopy(flights)

        # Insert flights into current flights collection
        # to test archiving
        for flight in old_flights_copy:
            fs.insert_document_with_id(
                collection_name=current_flights_coll,
                document_data=flight,
                doc_id=flight["flight_id"],
            )

        payload = json.dumps(
            {
                "flights": flights,
                "pdf_hash": pdf_doc["hash"],
                "job_id": job_id,
                "terminal": terminal_doc["name"],
            }
        )

        store_flights_response = lambda_client.invoke(
            FunctionName="Store-Flights",
            InvocationType="RequestResponse",
            Payload=payload,
        )

        self.assertEqual(store_flights_response["StatusCode"], 200)

        # Reading the payload
        store_flights_stream = store_flights_response["Payload"]
        store_flights_data = store_flights_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        store_flights_payload = json.loads(store_flights_data.decode())

        if not store_flights_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            store_flights_payload["body"],
            "Successfully stored flights.",
        )

        # Check that self reported archived flights are correct
        archived_flights = store_flights_payload.get("archivedFlights", [])

        archived_flights = json.loads(archived_flights)

        if not archived_flights:
            self.fail("No archived flights found in Store-Flights payload")

        self.assertEqual(len(archived_flights), 2)

        self.assertCountEqual(
            archived_flights,
            [
                old_flights_copy[0].get("flight_id", ""),
                old_flights_copy[1].get("flight_id", ""),
            ],
        )

        # Check that they are correctly archived in Firestore
        # We check that we can retrieve the flight's by their flight_id
        # and then delete their archived fields and turn them into Flight objects
        # and compare them to the original pickled flights
        for flight_id in archived_flights:
            flight_dict = fs.get_doc_by_id(
                collection_name=archive_flights_coll, doc_id=flight_id
            )

            if not flight_dict:
                self.fail(f"Flight {flight_id} not found in Firestore")

            self.assertEqual(flight_dict.get("archived", False), True)

            del flight_dict["archived"]
            del flight_dict["archived_timestamp"]

            archived_flight = Flight.from_dict(flight_dict)

            if not archived_flight:
                self.fail("Failed to convert archived flight from dictionary")

            for old_flight in old_flights_copy:
                if old_flight["flight_id"] == archived_flight.flight_id:
                    self.assertEqual(Flight.from_dict(old_flight), archived_flight)

                    fs.delete_document_by_id(
                        collection_name=archive_flights_coll,
                        doc_id=archived_flight.flight_id,
                    )

        # Check that other flights were not archived
        flight_archive_collection_ref = fs.db.collection(archive_flights_coll)

        flight_archive_query = flight_archive_collection_ref.where(
            "origin_terminal", "==", "Osan AB Passenger Terminal"
        )

        documents = flight_archive_query.stream()

        self.assertEqual(len(list(documents)), 0)

        # Check that the flights were correctly stored in Firestore
        # current flights collection. This means that only the two
        # latest flights should be in the current flights collection
        # since the test date is set to November 4, 2023 at 09:08
        stored_flights = store_flights_payload.get("storedFlights", [])

        stored_flights = json.loads(stored_flights)

        if not stored_flights:
            self.fail("No stored flights found in Store-Flights payload")

        self.assertEqual(len(stored_flights), 2)

        self.assertCountEqual(
            stored_flights,
            [
                flights[2].get("flight_id", ""),
                flights[3].get("flight_id", ""),
            ],
        )

        for flight_id in stored_flights:
            flight_dict = fs.get_doc_by_id(
                collection_name=current_flights_coll, doc_id=flight_id
            )

            if not flight_dict:
                self.fail(f"Flight {flight_id} not found in Firestore")

            # In current collection, should not be marked as archived
            self.assertEqual(flight_dict.get("archived", False), False)
            self.assertEqual(flight_dict.get("archived_timestamp", None), None)

            stored_flight = Flight.from_dict(flight_dict)

            if not stored_flight:
                self.fail("Failed to convert archived flight from dictionary")

            for pickled_flight in flights:
                if pickled_flight["flight_id"] == stored_flight.flight_id:
                    self.assertEqual(Flight.from_dict(pickled_flight), stored_flight)

                    fs.delete_document_by_id(
                        collection_name=current_flights_coll,
                        doc_id=stored_flight.flight_id,
                    )

        # Check that other flights were not archived
        flight_current_collection_ref = fs.db.collection(current_flights_coll)

        flight_current_query = flight_current_collection_ref.where(
            "origin_terminal", "==", "Osan AB Passenger Terminal"
        )

        documents = flight_current_query.stream()

        self.assertEqual(len(list(documents)), 0)

        # Verify that the terminal document was updated correctly to show that the
        # flights are done being updated.
        terminal_doc = fs.get_doc_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        if not terminal_doc:
            self.fail("Terminal document not found in Firestore")

        # NOTE: This value should be set to false by Store-Flights. Here it defaults to true
        # if it is not found. This is to ensure that the test fails if the value is not set.
        self.assertEqual(terminal_doc.get("updating72Hour", True), False)

        # Verify that the listed flights listed on the terminal are correct.
        # The cast is here because mypy was complaining about terminal_doc.get("flights72Hour", [])
        # returning a str or a list of str's. Still do not know why it is doing this.
        listed_flights: List[str] = cast(
            List[str], terminal_doc.get("flights72Hour", [])
        )

        if not listed_flights:
            self.fail("No listed flights found in terminal document")

        self.assertEqual(len(listed_flights), 2)

        self.assertCountEqual(
            listed_flights,
            [
                flights[2].get("flight_id", ""),
                flights[3].get("flight_id", ""),
            ],
        )

        # Lastly, verify that the Textract job document was updated correctly
        # to show that the function has finished running.
        testing_textract_doc = fs.get_textract_job(job_id)

        if not testing_textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the timestamps for the Store-Flights exist
        # and are set properly in the textract job document
        start_time = testing_textract_doc.get("started_store_flights", "")
        end_time = testing_textract_doc.get("finished_store_flights", "")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Store-Flights function exists
        # in the textract job document
        request_id = testing_textract_doc.get("func_store_flights_request_id", "")
        function_name = testing_textract_doc.get("func_store_flights_name", "")

        if not request_id:
            self.fail("Request ID not found in Textract job")

        if not function_name:
            self.fail("Function name not found in Textract job")

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the Textract job document
        fs.delete_document_by_id(collection_name=textract_jobs_coll, doc_id=job_id)

        # Delete the terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

    def test_correct_function_with_test_values_and_curr_date(
        self: unittest.TestCase,
    ) -> None:
        """Verifies that the Store-Flights function correctly stores the flights in Firestore.

        Uses the current date and time at the Osan AB Passenger Terminal as the test date. To verify that the function works
        correctly.
        """
        pdf_archive_coll = "**TESTING**_PDF_Archive666"
        terminal_coll = "**TESTING**_Terminals666"
        current_flights_coll = "**TESTING**_Flights_Current666"
        archive_flights_coll = "**TESTING**_Flights_Archive666"
        textract_jobs_coll = "Textract_Jobs"

        lambda_client = initialize_client("lambda")
        fs = FirestoreClient(
            pdf_archive_coll=pdf_archive_coll,
            terminal_coll=terminal_coll,
            textract_jobs_coll=textract_jobs_coll,
            flight_current_coll=current_flights_coll,
            flight_archive_coll=archive_flights_coll,
        )

        # Create a fake Textract job
        job_id = "TEST_Textract_Job_Doc"
        textract_doc = {
            "desc": "Test Textract Job document for testing Store-Flights function",
            "test": True,
            "testParameters": {
                "sendPdf": True,
                "testPdfArchiveColl": pdf_archive_coll,
                "testTerminalColl": terminal_coll,
                "testCurrentFlightsColl": current_flights_coll,
                "testArchiveFlightsColl": archive_flights_coll,
            },
        }

        fs.insert_document_with_id("Textract_Jobs", job_id, textract_doc)

        # Create a the fake pdf archive document
        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "Osan AB Passenger Terminal",
            "type": "72_HR",
        }

        fs.insert_document_with_id(
            collection_name=pdf_archive_coll,
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
        )

        # Create a fake terminal document
        terminal_doc = {
            "name": "Osan AB Passenger Terminal",
            "location": "Osan AB, ROK",
            "timezone": "Asia/Seoul",
        }

        fs.insert_document_with_id(
            collection_name=terminal_coll,
            document_data=terminal_doc,
            doc_id=terminal_doc["name"],
        )

        # Load in pickled flights
        osan_1_72hr_flight_0 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-0_fs.pkl"
        )

        if not osan_1_72hr_flight_0:
            self.fail("Failed to load flight 0 from pickle file")

        osan_1_72hr_flight_1 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-1_fs.pkl"
        )

        if not osan_1_72hr_flight_1:
            self.fail("Failed to load flight 1 from pickle file")

        osan_1_72hr_flight_2 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-2_fs.pkl"
        )

        if not osan_1_72hr_flight_2:
            self.fail("Failed to load flight 2 from pickle file")

        osan_1_72hr_flight_3 = Flight.load_state(
            "tests/lambda-func-tests/TestStoreFlights/test_correct_function/osan_1_72hr_flight-3_fs.pkl"
        )

        if not osan_1_72hr_flight_3:
            self.fail("Failed to load flight 3 from pickle file")

        # Create a list of flights
        flights_dicts = [
            osan_1_72hr_flight_0.to_dict(),
            osan_1_72hr_flight_1.to_dict(),
            osan_1_72hr_flight_2.to_dict(),
            osan_1_72hr_flight_3.to_dict(),
        ]

        local_time = get_local_time("Asia/Seoul")

        # Flight 0 set to one day in the past
        flight_0_date = modify_datetime(local_time, days=-1)

        if not flight_0_date:
            self.fail("Failed to modify flight 0 date")

        flights_dicts[0]["date"] = flight_0_date.strftime("%Y%m%d")
        flights_dicts[0]["rollcall_time"] = flight_0_date.strftime("%H%M")

        # Flight 1 set to one hour in the past
        flight_1_date = modify_datetime(local_time, hours=-1)

        if not flight_1_date:
            self.fail("Failed to modify flight 1 date")

        flights_dicts[1]["date"] = flight_1_date.strftime("%Y%m%d")
        flights_dicts[1]["rollcall_time"] = flight_1_date.strftime("%H%M")

        # Flight 2 set to one hour in the future
        flight_2_date = modify_datetime(local_time, hours=1)

        if not flight_2_date:
            self.fail("Failed to modify flight 2 date")

        flights_dicts[2]["date"] = flight_2_date.strftime("%Y%m%d")
        flights_dicts[2]["rollcall_time"] = flight_2_date.strftime("%H%M")

        # Flight 3 set to one day in the future
        flight_3_date = modify_datetime(local_time, days=1)

        if not flight_3_date:
            self.fail("Failed to modify flight 3 date")

        flights_dicts[3]["date"] = flight_3_date.strftime("%Y%m%d")
        flights_dicts[3]["rollcall_time"] = flight_3_date.strftime("%H%M")

        # Convert the flights to Flight objects
        flights: List[Dict[str, Any]] = []
        for flight_dict in flights_dicts:
            flight = Flight.from_dict(flight_dict)

            if not flight:
                self.fail("Failed to convert flight from dictionary")
                continue

            flights.append(flight.to_dict())

        old_flights_copy = copy.deepcopy(flights)

        # Insert flights into current flights collection
        # to test archiving
        for flight_copy in old_flights_copy:
            fs.insert_document_with_id(
                collection_name=current_flights_coll,
                document_data=flight_copy,
                doc_id=flight_copy["flight_id"],
            )

        payload = json.dumps(
            {
                "flights": flights,
                "pdf_hash": pdf_doc["hash"],
                "job_id": job_id,
                "terminal": terminal_doc["name"],
            }
        )

        store_flights_response = lambda_client.invoke(
            FunctionName="Store-Flights",
            InvocationType="RequestResponse",
            Payload=payload,
        )

        self.assertEqual(store_flights_response["StatusCode"], 200)

        # Reading the payload
        store_flights_stream = store_flights_response["Payload"]
        store_flights_data = store_flights_stream.read()

        # The payload is in bytes, so we decode it to a string and then load it as JSON
        store_flights_payload = json.loads(store_flights_data.decode())

        if not store_flights_payload:
            self.fail("Payload is empty")

        self.assertEqual(
            store_flights_payload["body"],
            "Successfully stored flights.",
        )

        # Check that self reported archived flights are correct
        archived_flights = store_flights_payload.get("archivedFlights", [])

        archived_flights = json.loads(archived_flights)

        if not archived_flights:
            self.fail("No archived flights found in Store-Flights payload")

        self.assertEqual(len(archived_flights), 2)

        self.assertCountEqual(
            archived_flights,
            [
                old_flights_copy[0].get("flight_id", ""),
                old_flights_copy[1].get("flight_id", ""),
            ],
        )

        # Check that they are correctly archived in Firestore
        # We check that we can retrieve the flight's by their flight_id
        # and then delete their archived fields and turn them into Flight objects
        # and compare them to the original pickled flights
        for flight_id in archived_flights:
            flight_dict = fs.get_doc_by_id(
                collection_name=archive_flights_coll, doc_id=flight_id
            )

            if not flight_dict:
                self.fail(f"Flight {flight_id} not found in Firestore")

            self.assertEqual(flight_dict.get("archived", False), True)

            del flight_dict["archived"]
            del flight_dict["archived_timestamp"]

            archived_flight = Flight.from_dict(flight_dict)

            if not archived_flight:
                self.fail("Failed to convert archived flight from dictionary")

            for old_flight in old_flights_copy:
                if old_flight["flight_id"] == archived_flight.flight_id:
                    self.assertEqual(Flight.from_dict(old_flight), archived_flight)

                    fs.delete_document_by_id(
                        collection_name=archive_flights_coll,
                        doc_id=archived_flight.flight_id,
                    )

        # Check that other flights were not archived
        flight_archive_collection_ref = fs.db.collection(archive_flights_coll)

        flight_archive_query = flight_archive_collection_ref.where(
            "origin_terminal", "==", "Osan AB Passenger Terminal"
        )

        documents = flight_archive_query.stream()

        self.assertEqual(len(list(documents)), 0)

        # Check that the flights were correctly stored in Firestore
        # current flights collection. This means that only the two
        # latest flights should be in the current flights collection
        # since the test date is set to November 4, 2023 at 09:08
        stored_flights = store_flights_payload.get("storedFlights", [])

        stored_flights = json.loads(stored_flights)

        if not stored_flights:
            self.fail("No stored flights found in Store-Flights payload")

        self.assertEqual(len(stored_flights), 2)

        self.assertCountEqual(
            stored_flights,
            [
                flights[2].get("flight_id", ""),
                flights[3].get("flight_id", ""),
            ],
        )

        for flight_id in stored_flights:
            flight_dict = fs.get_doc_by_id(
                collection_name=current_flights_coll, doc_id=flight_id
            )

            if not flight_dict:
                self.fail(f"Flight {flight_id} not found in Firestore")

            # In current collection, should not be marked as archived
            self.assertEqual(flight_dict.get("archived", False), False)
            self.assertEqual(flight_dict.get("archived_timestamp", None), None)

            stored_flight = Flight.from_dict(flight_dict)

            if not stored_flight:
                self.fail("Failed to convert archived flight from dictionary")

            for pickled_flight in flights:
                if pickled_flight["flight_id"] == stored_flight.flight_id:
                    self.assertEqual(Flight.from_dict(pickled_flight), stored_flight)

                    fs.delete_document_by_id(
                        collection_name=current_flights_coll,
                        doc_id=stored_flight.flight_id,
                    )

        # Check that other flights were not archived
        flight_current_collection_ref = fs.db.collection(current_flights_coll)

        flight_current_query = flight_current_collection_ref.where(
            "origin_terminal", "==", "Osan AB Passenger Terminal"
        )

        documents = flight_current_query.stream()

        self.assertEqual(len(list(documents)), 0)

        # Verify that the terminal document was updated correctly to show that the
        # flights are done being updated.
        terminal_doc = fs.get_doc_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )

        if not terminal_doc:
            self.fail("Terminal document not found in Firestore")

        # NOTE: This value should be set to false by Store-Flights. Here it defaults to true
        # if it is not found. This is to ensure that the test fails if the value is not set.
        self.assertEqual(terminal_doc.get("updating72Hour", True), False)

        # Verify that the listed flights listed on the terminal are correct.
        # The cast is here because mypy was complaining about terminal_doc.get("flights72Hour", [])
        # returning a str or a list of str's. Still do not know why it is doing this.
        listed_flights: List[str] = cast(
            List[str], terminal_doc.get("flights72Hour", [])
        )

        if not listed_flights:
            self.fail("No listed flights found in terminal document")

        self.assertEqual(len(listed_flights), 2)

        self.assertCountEqual(
            listed_flights,
            [
                flights[2].get("flight_id", ""),
                flights[3].get("flight_id", ""),
            ],
        )

        # Lastly, verify that the Textract job document was updated correctly
        # to show that the function has finished running.
        testing_textract_doc = fs.get_textract_job(job_id)

        if not testing_textract_doc:
            self.fail("Textract job not found in Firestore")

        # Verify that the timestamps for the Store-Flights exist
        # and are set properly in the textract job document
        start_time = testing_textract_doc.get("started_store_flights", "")
        end_time = testing_textract_doc.get("finished_store_flights", "")

        if not start_time or not isinstance(start_time, datetime):
            self.fail("Start time not found in Textract job")

        if not end_time or not isinstance(end_time, datetime):
            self.fail("End time not found in Textract job")

        # Verify that the debug info for the Store-Flights function exists
        # in the textract job document
        request_id = testing_textract_doc.get("func_store_flights_request_id", "")
        function_name = testing_textract_doc.get("func_store_flights_name", "")

        if not request_id:
            self.fail("Request ID not found in Textract job")

        if not function_name:
            self.fail("Function name not found in Textract job")

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name=pdf_archive_coll, doc_id=pdf_doc["hash"]
        )

        # Delete the Textract job document
        fs.delete_document_by_id(collection_name=textract_jobs_coll, doc_id=job_id)

        # Delete the terminal document
        fs.delete_document_by_id(
            collection_name=terminal_coll, doc_id=terminal_doc["name"]
        )
