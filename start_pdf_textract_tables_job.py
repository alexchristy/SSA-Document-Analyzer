import logging
import os
import urllib.parse
from typing import Any, Dict

import boto3  # type: ignore
import sentry_sdk
from aws_lambda_typing import context as lambda_context
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from firestore_db import FirestoreClient

# Set up sentry
sentry_sdk.init(
    dsn="https://5cd0afbfc9ad23474f63e76f5dc199c0@o4506224652713984.ingest.sentry.io/4506224655597568",
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

# Set up logging
logging.getLogger().setLevel(logging.INFO)


lambda_client = boto3.client("lambda")


def lambda_handler(
    event: Dict[str, Any], context: lambda_context.Context
) -> Dict[str, Any]:
    """Start a Textract job to extract tables from a PDF document.

    Entry point for the AWS Lambda function. Starts a Textract job to extract tables from a PDF document
    stored in an S3 bucket, and stores the job ID and logs in Firestore.

    Args:
    ----
        event: AWS Lambda event object
        context: AWS Lambda context object

    Returns:
    -------
        None
    """
    # Initialize Firestore client
    try:
        fs = FirestoreClient(textract_jobs_coll="Textract_Jobs")
        logging.info("Firestore client created")
    except Exception as e:
        msg = "Error initializing Firestore client"
        logging.critical("Error initializing Firestore client: %s", str(e))
        raise Exception(msg) from e

    try:
        # Get S3 bucket and object from AWS SNS event
        s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        s3_object_encoded = event["Records"][0]["s3"]["object"]["key"]

        # Decode S3 object
        s3_object = urllib.parse.unquote(s3_object_encoded)

        logging.info("Creating job for s3://%s/%s", s3_bucket, s3_object)

        # Get SNS topic and role ARNs from ENV variables
        sns_topic_arn = os.environ["SNS_TOPIC_ARN"]
        sns_role_arn = os.environ["SNS_ROLE_ARN"]
        logging.info("SNS topic ARN for Textract: %s", sns_topic_arn)
        logging.info("SNS role ARN for Textract: %s", sns_role_arn)

        # Check if request is a test
        test = event.get("test", False)

        if test:
            logging.info("Test.")
            test_params = event.get("testParameters", {})

            if not test_params:
                msg = "Test parameters not found"
                raise Exception(msg)

            # Set testing terminal and pdf collections
            if "testPdfArchiveColl" in test_params:
                pdf_archive_collection = test_params["testPdfArchiveColl"]

                if isinstance(pdf_archive_collection, str):
                    fs.set_pdf_archive_coll(pdf_archive_collection)
                    logging.info(
                        "Test PDF archive collection: %s", pdf_archive_collection
                    )
                else:
                    logging.warning(
                        "Test PDF archive collection is not a string. Using default."
                    )
            else:
                logging.warning("Test PDF archive collection not found. Using default.")

            if "testTerminalColl" in test_params:
                terminal_collection = test_params["testTerminalColl"]

                if isinstance(terminal_collection, str):
                    fs.set_terminal_coll(terminal_collection)
                    logging.info("Test terminal collection: %s", terminal_collection)
                else:
                    logging.warning(
                        "Test terminal collection is not a string. Using default."
                    )
            else:
                logging.warning("Test terminal collection not found. Using default.")

            # Check for the presence of 'sendPdf' key in test_params
            if "sendPdf" in test_params:
                logging.info("Sending Pdf: %s", test_params["sendPdf"])
                send_pdf = test_params["sendPdf"]
            else:
                logging.warning(
                    "sendPdf key not found in testParameters. Assuming True."
                )

            test_payload = {"test": True, "testParameters": test_params}
        else:
            logging.info("Not a test.")

        # Store document with job ID and log contents
        pdf_hash = fs.get_pdf_hash_with_s3_path(s3_object)

        # Check if hash was successfully retrieved
        if not pdf_hash or pdf_hash == "" or pdf_hash is None:
            msg = f"Could not find PDF hash for S3 object: {s3_object}"
            raise Exception(msg)

        # Update Firestore terminal document to indicate that flights are being processed
        terminal_name = fs.get_terminal_name_by_pdf_hash(pdf_hash)
        pdf_type = fs.get_pdf_type_by_hash(pdf_hash)

        if not terminal_name:
            msg = f"Could not find terminal name for PDF hash: {pdf_hash}"
            raise Exception(msg)

        if not pdf_type:
            msg = f"Could not find PDF type for PDF hash: {pdf_hash}"
            raise Exception(msg)

        # Update terminal document
        fs.set_terminal_update_status(terminal_name, pdf_type, True)
        fs.set_terminal_pdf(terminal_name, pdf_type, s3_object)

        # Start Textract job if not a test or
        # if test and we want to send the PDF to Textract
        if (not test) or (test and send_pdf):
            client = boto3.client("textract")
            response = client.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": s3_bucket, "Name": s3_object}},
                NotificationChannel={
                    "SNSTopicArn": sns_topic_arn,
                    "RoleArn": sns_role_arn,
                },
                FeatureTypes=["TABLES"],
            )
            # Get job ID
            job_id = response["JobId"]
            logging.info("Textract job started with ID: %s", job_id)
        else:
            job_id = "111111111111111111111111111111111111"
            logging.info("Test job started with ID: %s", job_id)

        fs.add_textract_job(job_id, pdf_hash)
        logging.info("Textract job ID %s and logs stored in Firestore", job_id)

        request_id = context.aws_request_id
        function_name = context.function_name

        start_job_info = {
            "func_start_job_request_id": request_id,
            "func_start_job_name": function_name,
        }

        # Append function info to Textract Job
        fs.append_to_doc("Textract_Jobs", job_id, start_job_info)

        null_timestamps = {
            "textract_started": None,
            "textract_finished": None,
        }

        fs.append_to_doc("Textract_Jobs", job_id, null_timestamps)

        fs.add_job_timestamp(job_id, "textract_started")

        if test:
            fs.append_to_doc("Textract_Jobs", job_id, test_payload)

        # Add Terminal name to Textract Job
        terminal_name_append_dict = {
            "terminal_name": terminal_name,
        }

        fs.append_to_doc("Textract_Jobs", job_id, terminal_name_append_dict)

        return {
            "statusCode": 200,
            "body": "Job started successfully.",
            "job_id": job_id,
        }

    except Exception as e:
        raise e
