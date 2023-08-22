import boto3
import logging
from firestore_db import FirestoreClient
import os

# Set up logging
logging.getLogger().setLevel(logging.INFO)

def lambda_handler(event, context):
    # Initialize Firestore client
    try:
        fs = FirestoreClient()
        logging.info('Firestore client created')
    except Exception as e:
        logging.error(f"Error initializing Firestore client: {e}")
        raise e

    try:
        # Get S3 bucket and object from AWS SNS event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_object = event['Records'][0]['s3']['object']['key']
        logging.info(f'Creating job for s3://{s3_bucket}/{s3_object}')

        # Get SNS topic and role ARNs from ENV variables
        sns_topic_arn = os.environ['SNS_TOPIC_ARN']
        sns_role_arn = os.environ['SNS_ROLE_ARN']
        logging.info(f'SNS topic ARN for Textract: {sns_topic_arn}')
        logging.info(f'SNS role ARN for Textract: {sns_role_arn}')

        # Start Textract job
        client = boto3.client('textract')
        response = client.start_document_analysis(
                        DocumentLocation={
                            'S3Object': {'Bucket': s3_bucket, 'Name': s3_object}},
                        NotificationChannel={
                            'SNSTopicArn': sns_topic_arn, 'RoleArn': sns_role_arn},
                        FeatureTypes=['TABLES'])

        # Get job ID
        job_id = response['JobId']
        logging.info(f'Textract job started with ID: {job_id}')

        # Store document with job ID and log contents
        pdf_hash = fs.get_pdf_hash_with_s3_path(s3_object)

        # Check if hash was successfully retrieved
        if pdf_hash:
            fs.add_textract_job(job_id, pdf_hash)
            logging.info(f'Textract job ID {job_id} and logs stored in Firestore')
        else:
            raise Exception(f"Could not find PDF hash for S3 object: {s3_object}")

    except Exception as e:
        logging.error(f"Error processing the Textract job: {e}")
        raise e