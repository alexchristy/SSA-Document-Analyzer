import json
import logging
import os
from typing import Any, Dict, List

import sentry_sdk
from aws_lambda_typing import context as lambda_context
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from aws_utils import initialize_client
from firestore_db import FirestoreClient
from flight import Flight
from flight_utils import convert_72hr_table_to_flights
from table import Table
from time_utils import get_local_time, pad_time_string

# Set up sentry
sentry_sdk.init(
    dsn="https://5cd0afbfc9ad23474f63e76f5dc199c0@o4506224652713984.ingest.sentry.io/4506224655597568",
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

MIN_CONFIDENCE = 80

lambda_client = initialize_client("lambda")
textract_client = initialize_client("textract")

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
def lambda_handler(event: dict, context: lambda_context.Context) -> Dict[str, Any]:
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
        logging.info("Event: %s", event)
        # Get tables from event, if any
        event_tables = event.get("tables", [])
        pdf_hash = event.get("pdf_hash", "")
        job_id = event.get("job_id", "")

        logging.info("PDF Hash: %s", pdf_hash)
        logging.info("Job ID: %s", job_id)

        request_id = context.aws_request_id
        function_name = context.function_name

        func_72hr_info = {
            "func_72hr_request_id": request_id,
            "func_72hr_name": function_name,
        }

        # Append function info to Textract Job
        firestore_client.append_to_doc("Textract_Jobs", job_id, func_72hr_info)

        # Append null values to timestamp fields in Textract Job.
        # This allows us to query for jobs that failed to process completely.
        null_timestamps = {
            "started_72hr_processing": None,
            "finished_72hr_processing": None,
        }
        firestore_client.append_to_doc("Textract_Jobs", job_id, null_timestamps)

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
            raise ValueError(response_msg)

        if not pdf_hash or pdf_hash is None:
            response_msg = f"No pdf_hash found in payload: {event}"
            raise ValueError(response_msg)

        if not job_id or job_id is None:
            response_msg = f"No job_id found in payload: {event}"
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

        # Save flight IDs to Textract Job
        firestore_client.add_flight_ids_to_job(job_id, flights)

        logging.info("Converted %d tables to %d flights.", len(tables), len(flights))

        # Append number of flights to Textract Job
        append_result = {
            "numFlights": len(flights),
        }
        firestore_client.append_to_doc("Textract_Jobs", job_id, append_result)

        if flights is None or not flights:
            response_msg = f"Failed to convert any tables to flights from terminal: {origin_terminal} in pdf: {pdf_hash} to flights."
            raise ValueError(response_msg)

        # Get localtime for the terminal to avoid inserting flights that have already departed
        terminal = firestore_client.get_terminal_dict_by_name(origin_terminal)

        timezone = terminal.get("timezone", "")

        if not timezone:
            msg = "Timezone is not set for terminal."
            raise ValueError(msg)

        local_time = get_local_time(timezone)

        # Create date time string for lexographical comparison
        current_time_str = local_time.strftime("%Y%m%d%H%M")

        # Store flights in Firestore
        for flight in flights:
            flight.make_firestore_compliant()

            flight_dict = flight.to_dict()

            flight_date = flight_dict.get("date", "")

            if not flight_date:
                logging.critical("Flight date is not set for flight: %s", flight)
                continue

            flight_time = flight_dict.get("time", "")

            if not flight_time:
                logging.critical("Flight time is not set for flight: %s", flight)
                continue

            # Make sure the time has 4 characters to prevent incorrect comparisons
            # 327 becomes 0327
            flight_time = pad_time_string(flight_time)

            flight_time_str = f"{flight_date}{flight_time}"

            if flight_time_str > current_time_str:
                firestore_client.store_flight(flight)
            else:
                logging.info(
                    "Flight %s has already departed. Not storing in Firestore.",
                    flight,
                )

        firestore_client.add_job_timestamp(job_id, "finished_72hr_processing")

        return {
            "statusCode": 200,
            "body": json.dumps("Finished processing 72-hour flights."),
        }
    except Exception as e:
        error_msg = f"Error occurred: {e}"
        logger.critical(error_msg)
        raise ValueError(error_msg) from e
