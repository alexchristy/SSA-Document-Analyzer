import json
import logging
import time
import unittest
from datetime import datetime
from typing import List

from aws_utils import initialize_client
from firestore_db import FirestoreClient
from s3_bucket import S3Bucket
from table import Table


class TestStartPdfTextractJob(unittest.TestCase):
    """Test the Start-PDF-Textract-Job function."""

    def test_correct_function(self: unittest.TestCase) -> None:
        """Test that the start_pdf_textract_job function starts a Textract job when a PDF is in the S3 bucket."""
        # If this test fails, check that the S3 bucket and object exist
        # S3 bucket: testing-ssa-pdf-store
        # S3 object: current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf
        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "MCAS Iwakuni Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
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
            ]
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

        # Clean up
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )

    def test_testing_paramters_no_send(self: unittest.TestCase) -> None:
        """Test that the Start-PDF-Textract-Job function correctly handles the testing parameters.

        In this test, the PDF is not sent to Textract because the sendPdf parameter is set to False. The
        job is still created in Firestore, but the job ID is a fake ID.
        """
        # If this test fails, check that the S3 bucket and object exist
        # S3 bucket: testing-ssa-pdf-store
        # S3 object: current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf
        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "MCAS Iwakuni Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
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
                "testDateTime": "197001010000",  # January 1, 1970 at 00:00
                "testPdfArchiveColl": "**TESTING**_PDF_Archive",
                "testTerminalColl": "**TESTING**_Terminals",
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

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )
        # Delete the Textract job
        fs.delete_document_by_id("Textract_Jobs", doc_id=str(returned_job_id))

    def test_testing_paramters_send(self: unittest.TestCase) -> None:
        """Test that the Start-PDF-Textract-Job function correctly handles the testing parameters.

        This test is the same as the previous test, except that the sendPdf parameter is set to True. This
        means that the PDF is sent to Textract and a real job ID is returned. This is useful to test to verify
        that the function will work with end-to-end testing.
        """
        # If this test fails, check that the S3 bucket and object exist
        # S3 bucket: testing-ssa-pdf-store
        # S3 object: current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf
        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "MCAS Iwakuni Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
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
                "testDateTime": "197001010000",  # January 1, 1970 at 00:00
                "testPdfArchiveColl": "**TESTING**_PDF_Archive",
                "testTerminalColl": "**TESTING**_Terminals",
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

        # Clean up
        # Delete the PDF document from the archive
        fs.delete_document_by_id(
            collection_name="**TESTING**_PDF_Archive", doc_id=pdf_doc["hash"]
        )
        # Delete the Textract job
        fs.delete_document_by_id("Textract_Jobs", doc_id=str(returned_job_id))


class TestTextractToTables(unittest.TestCase):
    """Test the Textract-to-Tables function."""

    def test_correct_function(self: unittest.TestCase) -> None:
        """Test that the Textract-To-Tables function correctly converts a Textract job to tables.

        This works by invoking the Start-PDF-Textract-Job function, which starts a Textract job. Then, the
        the test function verfies the start job worked and then continues on to invoke the Textract-to-Tables with
        the returned job ID.

        NOTE: When checking logs there will be two log streams for the Textract-to-Tables as
        one will be invoked from Textract finishing the job and the other will be invoked from the test function.
        """
        lambda_client = initialize_client("lambda")
        s3 = S3Bucket(bucket_name="testing-ssa-pdf-store")
        fs = FirestoreClient()

        pdf_doc = {
            "cloud_path": "current/72_HR/72 Hour Slides AUG 18_fd040263-b.pdf",
            "hash": "80b3f417259982271e57abad302a3caa12d2848f2d13301efc7bcffca12ee4e1",
            "terminal": "MCAS Iwakuni Passenger Terminal",
            "type": "72_HR",
        }

        if not s3.file_exists(pdf_doc["cloud_path"]):
            self.fail("PDF file does not exist in S3 bucket")

        fs.insert_document_with_id(
            collection_name="**TESTING**_PDF_Archive",
            document_data=pdf_doc,
            doc_id=pdf_doc["hash"],
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
            ]
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
            print(table.to_markdown())
            test_table_path = f"tests/lambda-func-tests/TestTextractToTables/test_correct_function/osan_1_72hr_table-{i+1}.pkl"

            test_table = table.load_state(test_table_path)

            self.assertEqual(test_table, table)
