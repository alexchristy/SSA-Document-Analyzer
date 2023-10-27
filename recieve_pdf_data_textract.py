import json
import logging
import os
import uuid
from typing import Any, Dict, List, Tuple

import boto3  # type: ignore

from firestore_db import FirestoreClient
from flight import Flight
from flight_utils import convert_72hr_table_to_flights
from s3_bucket import S3Bucket
from screenshot_table import capture_screen_shot_of_table_from_pdf
from table import Table
from table_utils import gen_tables_from_textract_response

MIN_CONFIDENCE = 80


def initialize_clients() -> Tuple[boto3.client, boto3.client]:
    """Initialize the Textract and Lambda clients.

    Returns
    -------
        Tuple[boto3.client, boto3.client]: The Textract and Lambda clients.
    """
    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    textract_client = None
    lambda_client = None

    try:
        if os.getenv("RUN_LOCAL"):
            logging.info("Running in a local environment.")

            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            aws_region = os.getenv("AWS_REGION")

            if not all([aws_access_key, aws_secret_key, aws_region]):
                logging.error(
                    "Missing AWS credentials or region for local environment."
                )
                msg = "Missing AWS credentials or region for local environment."
                raise ValueError(msg)

            boto3.setup_default_session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region,
            )
        else:
            logging.info("Running in a cloud environment.")

        textract_client = boto3.client("textract")
        lambda_client = boto3.client("lambda")

    except Exception as e:
        logging.error("Failed to initialize AWS clients: %s", e)
        raise e

    return textract_client, lambda_client


# Initialize Textract client
textract_client, lambda_client = initialize_clients()

# Initialize Firestore client
firestore_client = FirestoreClient()

# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load store_flights_in_firestore Lambda function name from environment variable
STORE_FLIGHTS_LAMBDA = os.getenv("STORE_FLIGHTS_LAMBDA")


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
    lowest_confidence = 100.0

    for index, _row in enumerate(table.rows):
        # Ignore first row (column headers)
        if index == 0:
            continue

        row_confidence = table.get_average_row_confidence(
            row_index=index, ignore_empty_cells=True
        )

        invalid_confidence = -1.0
        if row_confidence < invalid_confidence:
            logging.error(
                "Failed to get average row confidence for row index %s.", index
            )
            continue

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


def array_to_dict(array: List[Any]) -> dict:
    """Convert an array to a dictionary.

    Args:
    ----
        array (list): The array to convert.

    Returns:
    -------
        dict: The resulting dictionary.
    """
    try:
        result_dict = {}
        for index, obj in enumerate(array):
            result_dict[index] = obj
        return result_dict
    except Exception as e:
        print(f"Error: {e!s}")
        raise


def reprocess_tables(
    tables: List[Table], s3_client: S3Bucket, s3_object_path: str, response: dict
) -> List[Table]:
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
        return []

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
    reprocessed_tables_list = []

    # Reprocess tables with low confidence rows
    logging.info("Reprocessing %s tables.", len(tables))
    for i, table in enumerate(tables):
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

        if not table_screen_shot_with_title:
            logging.error(
                "Failed to capture screen shot of table %d in s3 at: %s.",
                i,
                s3_object_path,
            )
            continue

        # Read the local PDF file
        with open(table_screen_shot_with_title, "rb") as file:
            file_bytes = bytearray(file.read())

        # Send to screen shot to Textract for reprocessing
        # Call AnalyzeDocument API
        reprocess_response = textract_client.analyze_document(
            Document={"Bytes": file_bytes}, FeatureTypes=["TABLES"]
        )

        # Parse the response
        new_tables = gen_tables_from_textract_response(reprocess_response)

        # Remove the screen shot of the table
        os.remove(table_screen_shot_with_title)

        # Check if more than one table was found
        if len(new_tables) > 1:
            logging.error("More than one table found in reprocessed table.")
            msg = "More than one table found in reprocessed table."
            raise Exception(msg)

        # Get the only table in the list if there is a table
        # found. If not, set reprocessed_table to None and
        # continue to the next table.
        if new_tables:
            reprocessed_table = new_tables[0]
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
            reprocessed_tables_list.append(reprocessed_table)

    # Remove PDF from local directory
    os.remove(local_pdf_path)

    # Remove the local directory
    os.rmdir(download_dir)

    return reprocessed_tables_list


# Main Lambda function
def lambda_handler(event: dict, context: dict) -> Dict[str, Any]:  # noqa: PLR0911
    """Lambda function that handles the event from SNS and processes the PDF using Textract.

    Args:
    ----
        event (dict): The event object passed by AWS Lambda.
        context (dict): The context object passed by AWS Lambda.

    Returns:
    -------
        None
    """
    print("Current PATH: %s", os.environ["PATH"])
    # Parse the SNS message
    job_id, status, s3_object_path, s3_bucket_name = parse_sns_event(event)

    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        no_job_status_msg = "JobId or Status missing in SNS message."
        return {
            "statusCode": 500,
            "body": json.dumps(no_job_status_msg),
        }

    if not s3_bucket_name:
        logging.error("S3 bucket name missing in SNS message.")
        response_msg = (
            "S3 object path missing in SNS message. JobId: {}, Status: {}".format(
                job_id, status
            )
        )
        return {
            "statusCode": 500,
            "body": json.dumps(response_msg),
        }

    if not s3_object_path:
        logging.error("S3 object path missing in SNS message.")
        response_msg = (
            "S3 object path missing in SNS message. JobId: {}, Status: {}".format(
                job_id, status
            )
        )
        return {
            "statusCode": 500,
            "body": json.dumps(response_msg),
        }

    # Update the job status in Firestore
    firestore_client.update_job_status(job_id, status)
    firestore_client.add_job_finished_timestamp(job_id)

    # Initialize S3 client
    s3_client = S3Bucket(bucket_name=s3_bucket_name)

    # Get Origin terminal from S3 object path
    pdf_hash = firestore_client.get_pdf_hash_with_s3_path(s3_object_path)

    if not pdf_hash:
        logging.error(
            "Failed to get PDF hash using s3 object path (%s) from Firestore.",
            s3_object_path,
        )
        no_pdf_hash_msg = f"Failed to get PDF hash using s3 object path ({s3_object_path}) from Firestore."
        return {
            "statusCode": 500,
            "body": json.dumps(no_pdf_hash_msg),
        }

    origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

    if not origin_terminal:
        logging.error(
            "Failed to get origin terminal using PDF hash (%s) from Firestore.",
            pdf_hash,
        )
        no_origin_terminal_msg = (
            f"Failed to get origin terminal using PDF hash ({pdf_hash}) from Firestore."
        )
        return {
            "statusCode": 500,
            "body": json.dumps(no_origin_terminal_msg),
        }

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
    reprocessed_tables = reprocess_tables(
        tables=tables_to_reprocess,
        s3_client=s3_client,
        s3_object_path=s3_object_path,
        response=response,
    )

    flights: List[Flight] = []
    for i, table in enumerate(reprocessed_tables):
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
        response_msg = f"Failed to convert any tables from terminal: {origin_terminal} in pdf: {pdf_hash} to flights."
        return {
            "statusCode": 500,
            "body": json.dumps(response_msg),
        }

    # Make flight objects compliant with firestore
    for flight in flights:
        flight.convert_seat_data()

    # Save flight IDs to Textract Job
    firestore_client.add_flight_ids_to_job(job_id, flights)

    flights_dict = array_to_dict(flights)

    payload = json.dumps(flights_dict)

    response = lambda_client.invoke(
        FunctionName=STORE_FLIGHTS_LAMBDA,
        InvocationType="Event",
        Payload=payload,
    )

    return {
        "statusCode": 200,
        "body": json.dumps(f"Invoked second lambda asynchronously: {response}"),
    }
