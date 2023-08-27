import json
import boto3
import os
import re
from firestore_db import FirestoreClient
from flight import Flight
import logging
from dotenv import load_dotenv
import tests.doc_analysis_responses as doc_analysis_responses
import tests.sns_event_message as sns_event_message
from datetime import datetime as dt  # Importing datetime class as dt to avoid naming conflicts
from destination_correction import find_best_location_match


# REMOVE WHEN FINISHED TESTING
from tests import sns_event_message


def initialize_clients():
    # Set environment variables
    load_dotenv()

    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Check if running in a local environment
    if os.getenv('RUN_LOCAL'):
        logging.info("Running in a local environment.")
        
        # Setup AWS session
        boto3.setup_default_session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )

        # Initialize Textract client
        textract_client = boto3.client('textract')
        
    else:
        logging.info("Running in a cloud environment.")
        
        # Assume the role and environment is already set up in Lambda or EC2 instance, etc.
        textract_client = boto3.client('textract')

    return textract_client

# Initialize Textract client
textract_client = initialize_clients()

# Initialize Firestore client
firestore_client = FirestoreClient()

# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to parse SNS event from Textract
def parse_sns_event(event):
    """Parses the SNS event from Textract.
    
    Args:
        message (str): The SNS event.
        
    Returns:
        dict: Dictionary containing JobId and Status.
    """
    message_json_str = event['Records'][0]['Sns']['Message']
    message_dict = json.loads(message_json_str)
    
    job_id = message_dict.get('JobId', '')
    status = message_dict.get('Status', '')
    return job_id, status

def get_flight_origin_terminal(pdf_hash: str):
    # If pdf_hash is not None we can retrieve
    # origin information from Firestore
    if pdf_hash:
        flight_origin = firestore_client.get_flight_origin_by_pdf_hash(pdf_hash)

        if not flight_origin:
            logging.error(f"Flight origin not found for PDF hash: {pdf_hash}. Is the PDF in the database?")
            flight_origin = "N/A"
    else:
        logging.error(f"Firestore error when searching for PDF hash: {pdf_hash}.")
        flight_origin = "N/A"

    return flight_origin

# Main Lambda function
def lambda_handler(event, context):

    # Parse the SNS message
    job_id, status = parse_sns_event(event)

    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        return

    # Update the job status in Firestore
    firestore_client.update_job_status(job_id, status)

    # Get flight origin from Firestore
    pdf_hash = firestore_client.get_textract_job(job_id).get('pdf_hash', None)

    # flight_origin = get_flight_origin_terminal(pdf_hash)
    flight_origin = "N/A"

    # If the job succeeded, parse the Textract response
    if status == 'SUCCEEDED':
        response = doc_analysis_responses.norfolk_1_textract_response # textract_client.get_document_analysis(JobId=job_id)
        # flights = parse_textract_response_to_flights(response)

        # # Insert each flight into Firestore
        # for flight in flights:
        #     # Set the origin for each flight if it was found
        #     if flight_origin != "N/A":
        #         flight.origin_terminal = flight_origin
            
        #     # firestore_client.insert_flight(flight)
        #     flight.pretty_print()

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }

if __name__ == "__main__":
    lambda_handler(sns_event_message.sns_event_message_textract_successful_job, None)

