import json
import logging
import os
from typing import Any, Dict, List, Tuple

import boto3  # type: ignore

from firestore_db import FirestoreClient
from flight import Flight
from flight_utils import convert_72hr_table_to_flights

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


# Main Lambda function
def lambda_handler(event: dict, context: dict) -> Dict[str, Any]:
    """Lambda function handler for processing 72-hour flight documents.

    Args:
    ----
        event (dict): The event object passed by AWS Lambda.
        context (dict): The context object passed by AWS Lambda.

    Returns:
    -------
        dict: A dictionary containing the response from the Lambda function.
    """
    try:
        # Extract payload from event
        payload = json.loads(event.get("Payload", "{}"))

        reprocessed_tables = payload.get("tables", [])
        pdf_hash = payload.get("pdf_hash", "")
        job_id = payload.get("job_id", "")

        if not reprocessed_tables:
            response_msg = f"No tables found in payload: {payload}"
            logging.critical(response_msg)
            raise ValueError(response_msg)

        if not pdf_hash:
            response_msg = f"No pdf_hash found in payload: {payload}"
            logging.critical(response_msg)
            raise ValueError(response_msg)

        if not job_id:
            response_msg = f"No job_id found in payload: {payload}"
            logging.critical(response_msg)
            raise ValueError(response_msg)

        # Deserialize JSON to Python object
        reprocessed_tables = json.loads(payload)

        # Get the origin terminal from Firestore
        origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

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
    except Exception as e:
        logger.exception("Error occurred: %s", e)
        return {"statusCode": 500, "body": json.dumps("Internal Server Error.")}
