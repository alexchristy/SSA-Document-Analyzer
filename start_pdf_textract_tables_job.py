import logging
import os
from typing import Any, Dict

import boto3  # type: ignore

from firestore_db import FirestoreClient

# Set up logging
logging.getLogger().setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Dict[str, Any]) -> None:
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
        logging.critical("Error initializing Firestore client: %s", str(e))
        msg = "Critical error. Stopping function."
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

        # Start Textract job
        client = boto3.client("textract")
        response = client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": s3_bucket, "Name": s3_object}},
            NotificationChannel={"SNSTopicArn": sns_topic_arn, "RoleArn": sns_role_arn},
            FeatureTypes=["TABLES"],
        )

        # Get job ID
        job_id = response["JobId"]
        logging.info("Textract job started with ID: %s", job_id)

        fs.add_textract_job(job_id, pdf_hash)
        logging.info("Textract job ID %s and logs stored in Firestore", job_id)

        fs.add_job_timestamp(job_id, "textract_started")

    except Exception as e:
        logging.critical("Error processing the Textract job: %s", str(e))
        msg = "Critical error. Stopping function."
        raise Exception(msg) from e
