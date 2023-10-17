import json
from typing import List
import boto3
import os
from firestore_db import FirestoreClient
import logging
from dotenv import load_dotenv
from datetime import datetime as dt  # Importing datetime class as dt to avoid naming conflicts
from table import Table
from table_utils import *
from s3_bucket import S3Bucket
from screenshot_table import capture_screen_shot_of_table_from_pdf
from flight_utils import *
import uuid
import pickle
from table_utils import gen_tables_from_textract_response

# REMOVE WHEN FINISHED TESTING
import sys
sys.path.append("./tests/textract-responses")
sys.path.append("./tests/sns-event-messages")

from travis_1_72hr_sns_messages import travis_1_72hr_successful_job_sns_message as current_sns_message
from travis_1_72hr_textract_response import travis_1_72hr_textract_response as current_textract_response

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

def get_document_analysis_results(client, job_id):
    # Initialize variables
    results = []
    next_token = None
    
    try:
        while True:
            # Handle paginated responses
            if next_token:
                response = client.get_document_analysis(JobId=job_id, NextToken=next_token)
            else:
                response = client.get_document_analysis(JobId=job_id)
            
            # Process the current page of results
            blocks = response['Blocks']
            results.extend(blocks)
            
            # Log the number of blocks received in the current page
            logging.info(f"Received {len(blocks)} blocks in the current page.")
            
            # Check for more pages
            next_token = response.get('NextToken', None)
            if next_token is None:
                break

    except Exception as e:
        logging.error(f"An error occurred while getting document analysis results: {e}")
    
    return results

def reprocess_tables(tables: List[Table], s3_client: str, s3_object_path: str, response: dict) -> None:
    # Check if any tables need to be reprocessed
    if not tables:
        logging.info("No tables need to be reprocessed.")
        return
    
    download_dir = os.getenv('DOWNLOAD_DIR')
    if not download_dir:
        logging.error("Download directory is not set.")
        raise EnvironmentError("Download directory is not set.")
        
    # Create local path to download PDF to
    unique_dir_name = str(uuid.uuid4())
    download_dir = os.path.join(download_dir, unique_dir_name)
    
    # Check if download directory exists
    if not os.path.isdir(download_dir):
        # Create download directory
        try:
            os.makedirs(download_dir, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create download directory: {e}")
            raise

    # Get PDF filename from S3 object path
    pdf_filename = os.path.basename(s3_object_path)

    # Create local path to PDF
    local_pdf_path = os.path.join(download_dir, pdf_filename)

    # Download PDF from S3
    try:
        s3_client.download_from_s3(s3_object_path, local_pdf_path)
    except Exception as e:
        logging.error(f"Failed to download PDF from S3: {e}")
        raise

    # Reprocess tables with low confidence rows
    logging.info(f"Reprocessing {len(tables)} tables.")
    for table in tables:
        logging.info(f"Reprocessing table with page number: {table.page_number}")

        # Get table page number
        page_number = table.page_number

        table_screen_shot_with_title = capture_screen_shot_of_table_from_pdf(pdf_path=local_pdf_path, page_number=page_number, 
                                                                    textract_response=response, output_folder=download_dir,
                                                                    padding=75, include_title=True)
        
        # Read the local PDF file
        with open(table_screen_shot_with_title, "rb") as file:
            file_bytes = bytearray(file.read())

        # Send to screen shot to Textract for reprocessing
        # Call AnalyzeDocument API
        reprocess_response = textract_client.analyze_document(
            Document={'Bytes': file_bytes},
            FeatureTypes=["TABLES"]
        )

        # Parse the response
        reprocessed_table = gen_tables_from_textract_response(reprocess_response)

        # Remove the screen shot of the table
        os.remove(table_screen_shot_with_title)

        # Check if more than one table was found
        if len(reprocessed_table) > 1:
            logging.error("More than one table found in reprocessed table.")
            raise Exception("More than one table found in reprocessed table.")
        
        # Get the only table in the list if there is a table
        # found. If not, set reprocessed_table to None and 
        # continue to the next table.
        if reprocessed_table:
            reprocessed_table = reprocessed_table[0]
        else:
            logging.warning("No tables found in reprocessed table.")
            continue

        # Get the lowest confidence row of the reproccessed table
        _, lowest_confidence_row_reproccessed = get_lowest_confidence_row(reprocessed_table)

        # Compare the lowest confidence row to the original table
        # If the confidence is higher, replace the original table with the reprocessed table
        if lowest_confidence_row_reproccessed > get_lowest_confidence_row(table)[1]:
            logging.info("Reprocessed table has higher confidence than original table. Replacing original table with reprocessed table.")
            table = reprocessed_table

    # Remove PDF from local directory
    os.remove(local_pdf_path)

    # Remove the local directory
    os.rmdir(download_dir)

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

    # Get Origin terminal from S3 object path
    pdf_hash = firestore_client.get_pdf_hash_with_s3_path(s3_object_path)
    origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

    # If job failed exit program
    if status != 'SUCCEEDED':
        raise("Job did not succeed.")

    # import hashlib

    # response = context # textract_client.get_document_analysis(JobId=job_id)

    # tables = gen_tables_from_textract_response(response)

    # # List to hold tables needing reprocessing
    # tables_to_reprocess = []

    # # Iterate through tables to find low confidence rows
    # for table in tables:

    #     # Get the lowest confidence row
    #     _, lowest_confidence = get_lowest_confidence_row(table)

    #     # If the lowest confidence row is below the threshold
    #     # add the table to the list of tables to reprocess
    #     if lowest_confidence < 80:
    #         tables_to_reprocess.append(table)

    # # Reprocess tables with low confidence rows
    # reprocess_tables(tables=tables_to_reprocess, s3_client=s3_client, s3_object_path=s3_object_path, response=response)

    # table_str = ""
    # for idx, table in enumerate(tables):

    #     print(table.to_markdown())
    #     table_str += table.to_markdown()
    #     print("\n\n\n")
    #     Table.save_state(table, f'table-{idx}.pkl')
    # print(hashlib.sha256(table_str.encode()).hexdigest())
        
    table_pkl_path = 'tests/table-objects/travis_1_72hr_table-3.pkl'

    custom_date = '20230910'

    table = Table.load_state(table_pkl_path)

    # Create flight objects from table
    flights = convert_72hr_table_to_flights(table, origin_terminal=origin_terminal, use_fixed_date=True, fixed_date=custom_date)

    if flights is None:
        logging.error("Failed to convert table to flights.")
        return

    flight_obj_name = os.path.basename(table_pkl_path).split('.')[0]

    i = 1
    for flight in flights:
        flight.pretty_print()

        with open(f'{flight_obj_name}_flight-{i}.pkl', 'wb') as file:
            pickle.dump(flight, file)
        
        i += 1
        
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }


if __name__ == "__main__":
    lambda_handler(current_sns_message, current_textract_response)