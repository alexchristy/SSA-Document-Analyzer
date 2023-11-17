import json
import unittest

from aws_utils import initialize_client
from firestore_db import FirestoreClient
from s3_bucket import S3Bucket


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

    def test_testing_paramters(self: unittest.TestCase) -> None:
        """Test that the Start-PDF-Textract-Job function correctly handles the testing parameters."""
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
