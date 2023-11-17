import logging
import os
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
        fs = FirestoreClient()
        logging.info("Firestore client created")
    except Exception as e:
        msg = "Error initializing Firestore client"
        logging.critical("Error initializing Firestore client: %s", str(e))
        raise Exception(msg) from e

    try:
        # Get S3 bucket and object from AWS SNS event
        s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        s3_object = event["Records"][0]["s3"]["object"]["key"]
        logging.info("Creating job for s3://%s/%s", s3_bucket, s3_object)

        # Get SNS topic and role ARNs from ENV variables
        sns_topic_arn = os.environ["SNS_TOPIC_ARN"]
        sns_role_arn = os.environ["SNS_ROLE_ARN"]
        logging.info("SNS topic ARN for Textract: %s", sns_topic_arn)
        logging.info("SNS role ARN for Textract: %s", sns_role_arn)

        # Store document with job ID and log contents
        pdf_hash = fs.get_pdf_hash_with_s3_path(s3_object)

        # Check if hash was successfully retrieved
        if not pdf_hash or pdf_hash == "" or pdf_hash is None:
            msg = f"Could not find PDF hash for S3 object: {s3_object}"
            raise Exception(msg)

        # Update Firestore terminal document to indicate that flights are being processed
        terminal_collection = os.getenv("TERMINAL_COLLECTION", "Terminals")
        terminal_name = fs.get_terminal_name_by_pdf_hash(pdf_hash)
        pdf_type = fs.get_pdf_type_by_hash(pdf_hash)

        # Check if request is a test
        test = event.get("test", False)

        if test:
            logging.info("Test.")
            test_params = event.get("testParameters", {})

            if not test_params:
                msg = "Test parameters not found"
                raise Exception(msg)

            if test_params.get("testTerminalColl", ""):
                logging.info(
                    "Test terminal collection: %s", test_params["testTerminalColl"]
                )
                terminal_collection = test_params["testTerminalColl"]
            else:
                msg = "Test terminal collection not found"
                raise Exception(msg)

            test_payload = {"test": True, "testParameters": test_params}

        logging.info("Not a test.")

        payload = {}
        if pdf_type == "72_HR":
            payload = {"updating": pdf_type, "pdf72Hour": s3_object}
        elif pdf_type == "30_DAY":
            payload = {"updating": pdf_type, "pdf30Day": s3_object}
        elif pdf_type == "ROLLCALL":
            payload = {"updating": pdf_type, "pdfRollCall": s3_object}
        else:
            msg = f"Could not find PDF type for S3 object: {s3_object}"
            raise Exception(msg)

        fs.append_to_doc(
            terminal_collection,
            terminal_name,
            payload,
        )

        # Start Textract job
        if not test:
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

        return {
            "statusCode": 200,
            "body": "Job started successfully.",
            "job_id": job_id,
        }

    except Exception as e:
        raise e
