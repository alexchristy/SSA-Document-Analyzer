import json
import logging
import os
import uuid
from typing import Any, Dict, List, Tuple

import boto3
from dotenv import load_dotenv

from firestore_db import FirestoreClient
from flight_utils import convert_72hr_table_to_flights
from s3_bucket import S3Bucket
from screenshot_table import capture_screen_shot_of_table_from_pdf
from table import Table
from table_utils import gen_tables_from_textract_response

MIN_CONFIDENCE = 80


def initialize_clients() -> boto3.client:
    """Initialize the Textract client.

    Returns
    -------
        boto3.client: The Textract client.
    """
    # Set environment variables
    load_dotenv()

    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Check if running in a local environment
    if os.getenv("RUN_LOCAL"):
        logging.info("Running in a local environment.")

        # Setup AWS session
        boto3.setup_default_session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )

        # Initialize Textract client
        textract_client = boto3.client("textract")

    else:
        logging.info("Running in a cloud environment.")

        # Assume the role and environment is already set up in Lambda or
        # EC2 instance, etc.
        textract_client = boto3.client("textract")

    return textract_client


# Initialize Textract client
textract_client = initialize_clients()

# Initialize Firestore client
firestore_client = FirestoreClient()

# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_sns_event(event: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Parse the SNS event from Textract.

    Args:
    ----
        event (dict): The SNS event.

    Returns:
    -------
        tuple: Tuple containing JobId, Status, S3 Object Name, and S3 Bucket Name.
    """
    try:
        message_json_str = (
            event.get("Records", [{}])[0].get("Sns", {}).get("Message", "{}")
        )
    except IndexError:
        logging.error("Malformed SNS event: Records array is empty.")
        return "", "", "", ""

    try:
        message_dict = json.loads(message_json_str)
    except json.JSONDecodeError:
        logging.error("Failed to decode SNS message.")
        return "", "", "", ""

    job_id = message_dict.get("JobId", "")
    status = message_dict.get("Status", "")

    # Extract S3 Object Name and Bucket Name
    s3_object_name = message_dict.get("DocumentLocation", {}).get("S3ObjectName", "")
    s3_bucket_name = message_dict.get("DocumentLocation", {}).get("S3Bucket", "")

    return job_id, status, s3_object_name, s3_bucket_name


def get_lowest_confidence_row(table: Table) -> Tuple[int, float]:
    """Get the row index and confidence score of the row with the lowest confidence in a table.

    Args:
    ----
        table (custom Table class): The table to search.

    Returns:
    -------
        tuple: Tuple containing the row index and confidence score.
    """
    lowest_confidence = 100

    for index, _row in enumerate(table.rows):
        # Ignore first row (column headers)
        if index == 0:
            continue

        row_confidence = table.get_average_row_confidence(
            row_index=index, ignore_empty_cells=True
        )

        if row_confidence < lowest_confidence:
            lowest_confidence = row_confidence
            lowest_confidence_row_index = index

    return lowest_confidence_row_index, lowest_confidence


def get_document_analysis_results(client: boto3.client, job_id: str) -> List[Dict]:
    """Get the results of a Textract document analysis job.

    Args:
    ----
        client (boto3 Textract client): The Textract client.
        job_id (str): The ID of the Textract job.

    Returns:
    -------
        list: List of dictionaries containing the results of the Textract job.
    """
    # Initialize variables
    results = []
    next_token = None

    try:
        while True:
            # Handle paginated responses
            if next_token:
                response = client.get_document_analysis(
                    JobId=job_id, NextToken=next_token
                )
            else:
                response = client.get_document_analysis(JobId=job_id)

            # Append results to list
            results.extend(response["Blocks"])

            # Check if there are more pages of results
            next_token = response.get("NextToken", None)
            if not next_token:
                break

    except Exception as e:
        logging.error("Error getting document analysis results: %s", e)
        return []

    return results


def reprocess_tables(
    tables: List[Table], s3_client: str, s3_object_path: str, response: dict
) -> List[Table] | None:
    """Reprocess tables with low confidence scores in a Textract document analysis job.

    Args:
    ----
        tables (List[Table]): The tables to reprocess.
        s3_client (str): The name of the S3 client.
        s3_object_path (str): The path to the S3 object.
        response (dict): The response from the Textract job.

    Returns:
    -------
        None
    """
    # Check if any tables need to be reprocessed
    if not tables:
        logging.info("No tables need to be reprocessed.")
        return None

    download_dir = os.getenv("DOWNLOAD_DIR")
    if not download_dir:
        logging.error("Download directory is not set.")
        msg = "Download directory is not set."
        raise EnvironmentError(msg)

    # Create local path to download PDF to
    unique_dir_name = str(uuid.uuid4())
    download_dir = os.path.join(download_dir, unique_dir_name)

    # Check if download directory exists
    if not os.path.isdir(download_dir):
        # Create download directory
        try:
            os.makedirs(download_dir, exist_ok=True)
        except Exception as e:
            logging.error("Failed to create download directory: %s", e)
            raise

    # Get PDF filename from S3 object path
    pdf_filename = os.path.basename(s3_object_path)

    # Create local path to PDF
    local_pdf_path = os.path.join(download_dir, pdf_filename)

    # Download PDF from S3
    try:
        s3_client.download_from_s3(s3_object_path, local_pdf_path)
    except Exception as e:
        logging.error("Failed to download PDF from S3: %s", e)
        raise

    # Store reporcessed tables
    reporcessed_tables = []

    # Reprocess tables with low confidence rows
    logging.info("Reprocessing %s tables.", len(tables))
    for table in tables:
        logging.info("Reprocessing table with page number: %s", table.page_number)

        # Get table page number
        page_number = table.page_number

        table_screen_shot_with_title = capture_screen_shot_of_table_from_pdf(
            pdf_path=local_pdf_path,
            page_number=page_number,
            textract_response=response,
            output_folder=download_dir,
            padding=75,
            include_title=True,
        )

        # Read the local PDF file
        with open(table_screen_shot_with_title, "rb") as file:
            file_bytes = bytearray(file.read())

        # Send to screen shot to Textract for reprocessing
        # Call AnalyzeDocument API
        reprocess_response = textract_client.analyze_document(
            Document={"Bytes": file_bytes}, FeatureTypes=["TABLES"]
        )

        # Parse the response
        reprocessed_table = gen_tables_from_textract_response(reprocess_response)

        # Remove the screen shot of the table
        os.remove(table_screen_shot_with_title)

        # Check if more than one table was found
        if len(reprocessed_table) > 1:
            logging.error("More than one table found in reprocessed table.")
            msg = "More than one table found in reprocessed table."
            raise Exception(msg)

        # Get the only table in the list if there is a table
        # found. If not, set reprocessed_table to None and
        # continue to the next table.
        if reprocessed_table:
            reprocessed_table = reprocessed_table[0]
        else:
            logging.warning("No tables found in reprocessed table.")
            continue

        # Get the lowest confidence row of the reproccessed table
        _, lowest_confidence_row_reproccessed = get_lowest_confidence_row(
            reprocessed_table
        )

        # Compare the lowest confidence row to the original table
        # If the confidence is higher, replace the original table with the reprocessed table
        if lowest_confidence_row_reproccessed > get_lowest_confidence_row(table)[1]:
            logging.info(
                "Reprocessed table has higher confidence than original table. Replacing original table with reprocessed table."
            )
            reporcessed_tables.append(reprocessed_table)

    # Remove PDF from local directory
    os.remove(local_pdf_path)

    # Remove the local directory
    os.rmdir(download_dir)

    return reporcessed_tables


# Main Lambda function
def lambda_handler(event: dict, context: dict) -> None:
    """Lambda function that handles the event from SNS and processes the PDF using Textract.

    Args:
    ----
        event (dict): The event object passed by AWS Lambda.
        context (dict): The context object passed by AWS Lambda.

    Returns:
    -------
        None
    """
    # Parse the SNS message
    job_id, status, s3_object_path, s3_bucket_name = parse_sns_event(event)

    if not s3_bucket_name:
        logging.error("S3 bucket name missing in SNS message.")
        return None

    if not s3_object_path:
        logging.error("S3 object path missing in SNS message.")
        return None

    # Initialize S3 client
    s3_client = S3Bucket(bucket_name=s3_bucket_name)

    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        return None

    # Update the job status in Firestore
    firestore_client.update_job_status(job_id, status)

    # Get Origin terminal from S3 object path
    pdf_hash = firestore_client.get_pdf_hash_with_s3_path(s3_object_path)

    if not pdf_hash:
        logging.error("Failed to get PDF hash using s3 object path from Firestore.")
        return None

    origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

    # If job failed exit program
    if status != "SUCCEEDED":
        msg = Exception("Job did not succeed.")
        raise (msg)

    response = textract_client.get_document_analysis(JobId=job_id)

    tables = gen_tables_from_textract_response(response)

    # List to hold tables needing reprocessing
    tables_to_reprocess = []

    # Iterate through tables to find low confidence rows
    for table in tables:
        # Get the lowest confidence row
        _, lowest_confidence = get_lowest_confidence_row(table)

        # If the lowest confidence row is below the threshold
        # add the table to the list of tables to reprocess
        if lowest_confidence < MIN_CONFIDENCE:
            tables_to_reprocess.append(table)

    # Reprocess tables with low confidence rows
    reporcessed_tables = reprocess_tables(
        tables=tables_to_reprocess,
        s3_client=s3_client,
        s3_object_path=s3_object_path,
        response=response,
    )

    flights = []
    for i, table in enumerate(reporcessed_tables):
        # Create flight objects from table
        curr_flights = convert_72hr_table_to_flights(
            table, origin_terminal=origin_terminal
        )

        if curr_flights:
            flights.extend(curr_flights)
        else:
            logging.error("Failed to convert table %d to flights.", i)

    if flights is None or not flights:
        logging.error("Failed to any convert table to flights.")
        return None

    # TODO (alexchristy): Upload flights to Firestore

    return {
        "statusCode": 200,
        "body": json.dumps("Lambda function executed successfully!"),
    }
