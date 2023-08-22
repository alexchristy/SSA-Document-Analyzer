import json
import boto3
import os
import re
from firestore_db import FirestoreClient
from flight import Flight
import logging

def initialize_clients():
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

def extract_tables(blocks):
    tables = []
    for block in blocks:
        if block['BlockType'] == 'TABLE':
            tables.append(block)
    return tables

def process_flight_data(table, job_id):
    destination = find_column_data(table, r'destination[s]?\/?s?')
    rollcall_time = find_column_data(table, r'roll\s?-?\s?call')
    seats_info = find_column_data(table, r'seats')
    
    seat_match = re.match(r'(\d{1,3})([fFtT])|TBD', seats_info)
    if seat_match:
        num_of_seats = int(seat_match.group(1)) if seat_match.group(1) else 0
        seat_status = seat_match.group(2).upper() if seat_match.group(2) else 'TDB'
    else:
        num_of_seats = 0
        seat_status = 'TBD'
    
    pdf_hash = firestore_client.get_pdf_hash_with_s3_path(job_id)
    origin = firestore_client.db.collection(os.getenv('PDF_ARCHIVE_COLLECTION')).document(pdf_hash).get().get('terminal', None)
    
    return Flight(origin, destination, rollcall_time, num_of_seats, seat_status, "")

def find_column_data(table, regex):
    headers = table.get('Headers', [])
    for header in headers:
        if re.search(regex, header['Text'], re.IGNORECASE):
            return header['Text']
    return None

def lambda_handler(event, context):
    message_json_str = event['Records'][0]['Sns']['Message']
    message_dict = json.loads(message_json_str)
    
    job_id = message_dict.get('JobId', '')
    status = message_dict.get('Status', '')
    
    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        return

    # Update the job status in Firestore
    firestore_client.update_job_status(job_id, status)
    
    if status == 'SUCCEEDED':
        response = textract_client.get_document_analysis(JobId=job_id)
        tables = extract_tables(response['Blocks'])
        
        flight_ids = []
        
        for i, table in enumerate(tables):
            flight = process_flight_data(table, job_id)
            firestore_client.insert_flight(flight)
            flight_ids.append(flight.flight_id)
        
        # Add flight IDs to PDF document in Firestore
        pdf_hash = firestore_client.get_pdf_hash_with_s3_path(job_id)
        if pdf_hash:
            firestore_client.add_flight_ids_to_pdf(pdf_hash, flight_ids)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }
