import json
import logging
import os
from typing import Any, Dict, List, Tuple

import boto3  # type: ignore

from firestore_db import FirestoreClient
from flight import Flight
from flight_utils import convert_72hr_table_to_flights
from table import Table

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
        print(f"Event: {event}")
        # Get tables from event, if any
        event_tables = event.get("tables", [])
        pdf_hash = event.get("pdf_hash", "")
        job_id = event.get("job_id", "")

        print(f"PDF Hash: {pdf_hash}")
        print(f"Job ID: {job_id}")

        tables = []
        for i, table_dict in enumerate(event_tables):
            curr_table = Table.from_dict(table_dict)

            if curr_table is None:
                logging.info("Failed to convert table %d from a dictionary", i)
                continue

            tables.append(curr_table)

        print(f"Recieved {len(tables)} tables from event.")

        if not tables or tables is None:
            response_msg = f"No tables found in payload: {event}"
            logging.critical(response_msg)
            raise ValueError(response_msg)

        if not pdf_hash or pdf_hash is None:
            response_msg = f"No pdf_hash found in payload: {event}"
            logging.critical(response_msg)
            raise ValueError(response_msg)

        if not job_id or job_id is None:
            response_msg = f"No job_id found in payload: {event}"
            logging.critical(response_msg)
            raise ValueError(response_msg)

        firestore_client.add_job_timestamp(job_id, "started_72hr_processing")

        # Get the origin terminal from Firestore
        origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

        if not origin_terminal:
            msg = f"Could not retrieve terminal name for pdf_hash: {pdf_hash}"
            logging.critical(msg)
            raise ValueError(msg)

        flights: List[Flight] = []
        for i, table in enumerate(tables):
            # Create flight objects from table
            curr_flights = convert_72hr_table_to_flights(
                table, origin_terminal=origin_terminal
            )

            if curr_flights:
                flights.extend(curr_flights)
            else:
                logging.error("Failed to convert table %d to flights.", i)

        if flights is None or not flights:
            response_msg = f"Failed to convert any tables to flights from terminal: {origin_terminal} in pdf: {pdf_hash} to flights."
            logging.critical(response_msg)
            raise ValueError(response_msg)

        logging.info("Converted %d tables to %d flights.", len(tables), len(flights))

        # Store flights in Firestore
        for flight in flights:
            flight.convert_seat_data()
            firestore_client.store_flight(flight)

        # Save flight IDs to Textract Job
        firestore_client.add_flight_ids_to_job(job_id, flights)

        firestore_client.add_job_timestamp(job_id, "finished_72hr_processing")

        payload = {
            "terminal": origin_terminal,
        }

        # Invoke second lambda asynchronously
        response = lambda_client.invoke(
            FunctionName=STORE_FLIGHTS_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )

        return {
            "statusCode": 200,
            "body": json.dumps(f"Invoked second lambda asynchronously: {response}"),
        }
    except Exception as e:
        error_msg = f"Error occurred: {e}"
        logger.critical(error_msg)
        raise ValueError(error_msg) from e
