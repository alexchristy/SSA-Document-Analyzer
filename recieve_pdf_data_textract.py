import json
import boto3
import os
from firestore_db import FirestoreClient
import logging
from dotenv import load_dotenv
import tests.doc_analysis_responses as doc_analysis_responses
import tests.sns_event_message as sns_event_message
from datetime import datetime as dt  # Importing datetime class as dt to avoid naming conflicts
from destination_correction import find_best_location_match
from table import Table
from table_utils import *
from s3_bucket import S3Bucket
from screenshot_table import capture_screen_shot_of_table_from_pdf

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
        event (dict): The SNS event.
        
    Returns:
        tuple: Tuple containing JobId, Status, S3 Object Name, and S3 Bucket Name.
    """
    try:
        message_json_str = event.get('Records', [{}])[0].get('Sns', {}).get('Message', '{}')
    except IndexError:
        logging.error("Malformed SNS event: Records array is empty.")
        return None, None, None, None

    try:
        message_dict = json.loads(message_json_str)
    except json.JSONDecodeError:
        logging.error("Failed to decode SNS message.")
        return None, None, None, None
    
    job_id = message_dict.get('JobId', None)
    status = message_dict.get('Status', None)
    
    # Extract S3 Object Name and Bucket Name
    s3_object_name = message_dict.get('DocumentLocation', {}).get('S3ObjectName', None)
    s3_bucket_name = message_dict.get('DocumentLocation', {}).get('S3Bucket', None)
    
    return job_id, status, s3_object_name, s3_bucket_name

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

def gen_tables_from_textract_response(textract_response):
    tables = convert_textract_response_to_tables(textract_response)
    
    if not isinstance(tables, list):
        logging.error("Expected a list of tables, got something else.")
        return []
    
    processed_tables = []
    for table in tables:
        processed_table = remove_incorrect_column_header_rows(table)
        processed_table = rearrange_columns(processed_table)
        processed_tables.append(processed_table)
    
    return processed_tables

def get_lowest_confidence_row(table):

    lowest_confidence = 100

    for index, row in enumerate(table.rows):

        # Ignore first row (column headers)
        if index == 0:
            continue

        row_confidence = table.get_average_row_confidence(row_index=index, ignore_empty_cells=True)
        
        if row_confidence < lowest_confidence:
            lowest_confidence = row_confidence
            lowest_confidence_row_index = index
    
    return lowest_confidence_row_index, lowest_confidence

# Main Lambda function
def lambda_handler(event, context):

    # Parse the SNS message
    job_id, status, s3_object_path, s3_bucket_name = parse_sns_event(event)

    # Initialize S3 client
    s3_client = S3Bucket(bucket_name=s3_bucket_name)

    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        return

    # Update the job status in Firestore
    firestore_client.update_job_status(job_id, status)

    # Get flight origin from Firestore
    pdf_hash = firestore_client.get_textract_job(job_id).get('pdf_hash', None)

    # flight_origin = get_flight_origin_terminal(pdf_hash)
    flight_origin = "N/A"

    # If job failed exit program
    if status != 'SUCCEEDED':
        raise("Job did not succeed.")

    response = doc_analysis_responses.bwi_1_textract_response # textract_client.get_document_analysis(JobId=job_id)

    tables = gen_tables_from_textract_response(response)

    # List to hold tables needing reprocessing
    tables_to_reprocess = []

    # Iterate through tables to find low confidence rows
    for table in tables:

        # Get the lowest confidence row
        _, lowest_confidence = get_lowest_confidence_row(table)

        # If the lowest confidence row is below the threshold
        # add the table to the list of tables to reprocess
        if lowest_confidence < 80:
            tables_to_reprocess.append(table)

    # Check if any tables need to be reprocessed
    if tables_to_reprocess:
        # Get download directory
        download_dir = os.getenv('DOWNLOAD_DIR')
        if not download_dir:
            logging.error("Download directory is not set.")
            raise EnvironmentError("Download directory is not set.")

        # Get PDF filename from S3 object path
        pdf_filename = os.path.basename(s3_object_path)

        # Create local path to download PDF to
        local_pdf_path = os.path.join(download_dir, pdf_filename)

        # Download PDF from S3
        try:
            s3_client.download_from_s3(s3_object_path, local_pdf_path)
        except Exception as e:
            logging.error(f"Failed to download PDF from S3: {e}")
            raise

        # Reprocess tables with low confidence rows
        logging.info(f"Reprocessing {len(tables_to_reprocess)} tables.")
        for table in tables_to_reprocess:
            logging.info(f"Reprocessing table with page number: {table.page_number}")

            # Get table page number
            page_number = table.page_number

            table_screen_shot = capture_screen_shot_of_table_from_pdf(pdf_path=local_pdf_path, page_number=page_number, 
                                                                      textract_response=response, output_folder=download_dir,
                                                                      padding=75, include_title=True)
            
            # Read the local PDF file
            with open(table_screen_shot, "rb") as file:
                file_bytes = bytearray(file.read())

            # Send to screen shot to Textract for reprocessing
            # Call AnalyzeDocument API
            reprocess_response = textract_client.analyze_document(
                Document={'Bytes': file_bytes},
                FeatureTypes=["TABLES"]
            )

            # Parse the response
            reprocessed_table = gen_tables_from_textract_response(reprocess_response)

            # Check if more than one table was found
            if len(reprocessed_table) > 1:
                logging.error("More than one table found in reprocessed table.")
                raise Exception("More than one table found in reprocessed table.")
            
            # Get the only table in the list
            reprocessed_table = reprocessed_table[0]

            # Get the lowest confidence row of the reproccessed table
            _, lowest_confidence_row_reproccessed = get_lowest_confidence_row(reprocessed_table)

            # Compare the lowest confidence row to the original table
            # If the confidence is higher, replace the original table with the reprocessed table
            if lowest_confidence_row_reproccessed > get_lowest_confidence_row(table)[1]:
                logging.info("Reprocessed table has higher confidence than original table. Replacing original table with reprocessed table.")
                table = reprocessed_table

            # Remove the screen shot of the table
            os.remove(table_screen_shot)

    # Remove PDF from local directory
    os.remove(local_pdf_path)

        

        
        

        
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }

if __name__ == "__main__":
    lambda_handler(sns_event_message.sns_event_message_textract_successful_job, None)