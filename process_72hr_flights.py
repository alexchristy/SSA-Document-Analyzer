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

# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
        lambda_client = initialize_client("lambda")

        # Initialize Firestore client with a default textract job collection
        firestore_client = FirestoreClient(textract_jobs_coll="Textract_Jobs")

        logging.info("Event: %s", event)
        # Get tables from event, if any
        event_tables = event.get("tables", [])
        pdf_hash = event.get("pdf_hash", "")
        job_id = event.get("job_id", "")

        if not event_tables:
            response_msg = f"No tables found in payload: {event}"
            raise ValueError(response_msg)

        if not pdf_hash:
            response_msg = f"No pdf_hash found in payload: {event}"
            raise ValueError(response_msg)

        if not job_id:
            response_msg = f"No job_id found in payload: {event}"
            raise ValueError(response_msg)

        logging.info("PDF Hash: %s", pdf_hash)
        logging.info("Job ID: %s", job_id)

        # Check for test parameters
        textract_doc = firestore_client.get_textract_job(job_id)

        test = False
        use_test_date = False
        test_date = None
        if textract_doc:
            test = textract_doc.get("test", False)

            pdf_archive_coll = None
            terminal_coll = None

            if test:
                logging.info("Using test values.")
                test_params: Dict[str, Any] = textract_doc.get("testParameters", {})

                if not test_params:
                    msg = "Test parameters not found"
                    raise ValueError(msg)

                # Set testing terminal and pdf collections
                if "testPdfArchiveColl" in test_params:
                    pdf_archive_coll = test_params["testPdfArchiveColl"]

                    if isinstance(pdf_archive_coll, str):
                        firestore_client.set_pdf_archive_coll(pdf_archive_coll)
                else:
                    logging.error("Failed to get test pdf archive collection.")

                if "testTerminalColl" in test_params:
                    terminal_coll = test_params["testTerminalColl"]

                    if isinstance(terminal_coll, str):
                        firestore_client.set_terminal_coll(terminal_coll)
                else:
                    logging.error("Failed to get test terminal collection.")

                valid_date_time_length = 12
                if (
                    "testDateTime" in test_params
                    and len(test_params["testDateTime"]) == valid_date_time_length
                ):
                    time_data = test_params["testDateTime"]
                    use_test_date = True
                    test_date = time_data[0:8]
                else:
                    logging.error("Failed to get testDateTime.")
        else:
            logging.error(
                "Failed to get textract job document when checking for testing values."
            )

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

        logging.info("Received %d tables from event.", len(tables))

        if not tables:
            response_msg = f"No tables found in payload: {event}"
            raise ValueError(response_msg)

        firestore_client.add_job_timestamp(job_id, "started_72hr_processing")

        # Get the origin terminal from Firestore
        origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

        if not origin_terminal:
            msg = f"Could not retrieve terminal name for pdf_hash: {pdf_hash}. Searching in {firestore_client.pdf_archive_coll} collection."
            raise ValueError(msg)

        flights: List[Flight] = []
        for i, table in enumerate(tables):
            # Create flight objects from table
            # Fixed data is used for testing if test flag is set and testDateTime is present
            curr_flights = convert_72hr_table_to_flights(
                table=table,
                origin_terminal=origin_terminal,
                use_fixed_date=use_test_date,
                fixed_date=test_date,
            )

            if curr_flights:
                flights.extend(curr_flights)
            else:
                logging.warning("Failed to convert table %d to flights.", i)

        logging.info("Converted %d tables to %d flights.", len(tables), len(flights))

        # Append number of flights to Textract Job
        append_result = {
            "num_flights": len(flights),
        }
        firestore_client.append_to_doc("Textract_Jobs", job_id, append_result)

        if not flights:
            response_msg = f"Failed to convert any tables to flights from terminal: {origin_terminal} in pdf: {pdf_hash} to flights."
            raise Exception(response_msg)

        # Pass flights to store_flights Lambda function
        payload_flights = []
        for flight in flights:
            flight.make_firestore_compliant()

            flight_dict = flight.to_dict()

            payload_flights.append(flight_dict)

        # Save flight IDs to Textract Job
        firestore_client.add_flight_ids_to_job(job_id, flights)

        # Payload for store_flights Lambda function
        payload = {
            "flights": payload_flights,
            "pdf_hash": pdf_hash,
            "job_id": job_id,
            "terminal": origin_terminal,
        }

        # Load store_flights_in_firestore Lambda function name from environment variable
        store_flights_lambda = os.getenv("STORE_FLIGHTS_LAMBDA")

        # Invoke store_flights Lambda function
        lambda_client.invoke(
            FunctionName=store_flights_lambda,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )

        firestore_client.add_job_timestamp(job_id, "finished_72hr_processing")

        return {
            "statusCode": 200,
            "body": "Finished processing 72-hour flights.",
            "payload": json.dumps(payload),
        }
    except Exception as e:
        error_msg = f"Error occurred: {e}"
        logger.critical(error_msg)
        raise ValueError(error_msg) from e
