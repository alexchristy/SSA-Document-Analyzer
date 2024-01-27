import json
import logging
from typing import Any, Dict, List, Optional

import sentry_sdk
from aws_lambda_typing import context as lambda_context
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from firestore_db import FirestoreClient
from flight import Flight, InvalidDateError, InvalidRollcallTimeError
from time_utils import get_local_time

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


def get_terminal_timezone(firestore_client: FirestoreClient, terminal_name: str) -> str:
    """Retrieve the timezone for a given terminal."""
    terminal_doc = firestore_client.get_terminal_dict_by_name(
        terminal_name=terminal_name
    )

    if not terminal_doc:
        msg = f"No terminal found for {terminal_name}"
        raise ValueError(msg)

    terminal_timezone = terminal_doc.get("timezone", "")
    if not terminal_timezone:
        msg = f"No timezone found for {terminal_name}"
        raise ValueError(msg)

    return terminal_timezone


def get_current_time(
    use_test_date: bool, test_date: Optional[str], terminal_timezone: str
) -> str:
    """Determine the current time based on the test date and terminal timezone."""
    if use_test_date and test_date:
        return test_date

    local_time = get_local_time(timezone_key=terminal_timezone)
    return local_time.strftime("%Y%m%d%H%M")


def lambda_handler(event: dict, context: lambda_context.Context) -> Dict[str, Any]:
    """Archive flights that have departed and delete all old flights current flights collection."""
    try:
        firestore_client = FirestoreClient(textract_jobs_coll="Textract_Jobs")

        logging.info("Received event: %s", event)

        job_id = event.get("job_id", "")
        pdf_hash = event.get("pdf_hash", "")
        terminal = event.get("terminal", "")
        new_flights_dicts = event.get("flights", [])

        if not job_id:
            response_msg = f"No job_id found in payload: {event}"
            raise ValueError(response_msg)

        if not pdf_hash:
            response_msg = f"No pdf_hash found in payload: {event}"
            raise ValueError(response_msg)

        if not terminal:
            response_msg = f"No terminal found in payload: {event}"
            raise ValueError(response_msg)

        if not new_flights_dicts:
            response_msg = f"No flights found in payload: {event}"
            raise ValueError(response_msg)

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
                test_params: Dict[str, str] = textract_doc.get("testParameters", {})

                if not test_params:
                    response_msg = f"No test parameters found in payload: {event}"
                    raise ValueError(response_msg)

                # Set testing terminal and pdf collections
                if "testPdfArchiveColl" in test_params:
                    pdf_archive_coll = test_params["testPdfArchiveColl"]

                    if isinstance(pdf_archive_coll, str):
                        firestore_client.set_pdf_archive_coll(pdf_archive_coll)
                else:
                    logging.warning("Failed to get test pdf archive collection.")

                if "testTerminalColl" in test_params:
                    terminal_coll = test_params["testTerminalColl"]

                    if isinstance(terminal_coll, str):
                        firestore_client.set_terminal_coll(terminal_coll)
                else:
                    logging.warning("Failed to get test terminal collection.")

                if "testCurrentFlightsColl" in test_params:
                    current_flights_coll = test_params["testCurrentFlightsColl"]

                    if isinstance(current_flights_coll, str):
                        firestore_client.set_flight_current_coll(current_flights_coll)
                else:
                    logging.warning("Failed to get test current flights collection.")

                if "testArchiveFlightsColl" in test_params:
                    archive_flights_coll = test_params["testArchiveFlightsColl"]

                    if isinstance(archive_flights_coll, str):
                        firestore_client.set_flight_archive_coll(archive_flights_coll)
                else:
                    logging.warning("Failed to get test archive flights collection.")

                valid_date_time_length = 12
                if (
                    "testDateTime" in test_params
                    and len(test_params["testDateTime"]) == valid_date_time_length
                ):
                    test_date = test_params["testDateTime"]
                    use_test_date = True
                else:
                    logging.warning("Failed to get testDateTime.")

        else:
            logging.error("Failed to get textract job when checking for test values.")

        request_id = context.aws_request_id
        function_name = context.function_name

        func_store_flights_info = {
            "func_store_flights_request_id": request_id,
            "func_store_flights_name": function_name,
        }

        firestore_client.append_to_doc("Textract_Jobs", job_id, func_store_flights_info)

        # Append null values to timestamp fields in Textract Job.
        # This allows us to query for jobs that failed to process completely.
        null_timestamps = {
            "started_store_flights": None,
            "finished_store_flights": None,
        }
        firestore_client.append_to_doc("Textract_Jobs", job_id, null_timestamps)

        # Set timestamp for when store_flights function started
        firestore_client.add_job_timestamp(job_id, "started_store_flights")

        terminal_timezone = get_terminal_timezone(
            firestore_client=firestore_client, terminal_name=terminal
        )

        current_time = get_current_time(
            use_test_date=use_test_date,
            test_date=test_date,
            terminal_timezone=terminal_timezone,
        )

        # Get all old flights
        old_flights = firestore_client.get_flights_by_terminal(terminal=terminal)

        if not old_flights:
            logging.info("No flights found to archive.")

        # Remove old flights in the future
        for flight in old_flights[:]:
            if flight.get_departure_datetime() >= current_time:
                logging.info(
                    "Removing old flight that has not departed yet: %s.",
                    flight.flight_id,
                )
                firestore_client.delete_current_flight(flight)
                old_flights.remove(flight)

        # Create new flights
        new_flights: List[Flight] = []
        for flight_dict in new_flights_dicts:
            new_flight = Flight.from_dict(flight_dict)

            if not new_flight:
                logging.error("Failed to create new flight from dict: %s", flight_dict)
                continue

            new_flights.append(new_flight)

        # Mark new flights that have already departed as do not archive
        for flight in new_flights:
            if flight.get_departure_datetime() < current_time:
                logging.info(
                    "New flight has already departed. Flagging as do not archive: %s.",
                    flight.flight_id,
                )
                flight.should_archive = False

        # # Prevent archiving old flights that are too similar to new flights
        # # which indicates that the new flight is really just an update to the old flight listing.
        # # This is a workaround for the fact that the PDFs are not always updated before
        # # the old flights are listed to depart.
        # pruned_old_flights, removed_flights = prune_recent_old_flights(
        #     old_flights=old_flights, new_flights=new_flights
        # )

        # logging.info(
        #     "Removed %d similar old flights: %s",
        #     len(removed_flights),
        #     ", ".join(str(flight.flight_id) for flight in removed_flights),
        # )

        # # Delete flights that are too similar and recent
        # for removed_flight in removed_flights:
        #     old_flights.remove(removed_flight)
        #     firestore_client.delete_current_flight(removed_flight)

        # Archive old flights
        archived_flights: List[str] = []
        for old_flight in old_flights:
            if old_flight.rollcall_note and old_flight.get_rollcall_note() == "TBD":
                logging.info(
                    "Flight %s has a rollcall note of TBD. Not archiving.",
                    old_flight.flight_id,
                )
                firestore_client.delete_current_flight(old_flight)
                continue

            if not old_flight.should_archive:
                logging.info(
                    "Flight %s should not be archived. Not archiving.",
                    old_flight.flight_id,
                )
                firestore_client.delete_current_flight(old_flight)
                continue

            firestore_client.archive_flight(old_flight)
            archived_flights.append(old_flight.flight_id)

            # Delete all old flights
            firestore_client.delete_current_flight(old_flight)

        # Store new flights
        stored_flights: List[str] = []
        problem_flights: List[Flight] = []
        for flight in new_flights:
            try:
                firestore_client.store_flight_as_current(flight)
                stored_flights.append(flight.flight_id)
            except InvalidRollcallTimeError as e:
                logging.error(
                    "Invalid rollcall time when storing the new flight (%s): %s",
                    flight.flight_id,
                    e,
                )
                problem_flights.append(flight)
                continue
            except InvalidDateError as e:
                logging.error(
                    "Invalid date when storing the new flight (%s): %s",
                    flight.flight_id,
                    e,
                )
                problem_flights.append(flight)
                continue

        # Update terminal with problem flights
        firestore_client.append_to_doc(
            firestore_client.terminal_coll,
            terminal,
            {"problemFlights": problem_flights},
        )

        # Update Terminal document with new flights
        logging.info("Updating Terminal document with new flights.")
        pdf_type = firestore_client.get_pdf_type_by_hash(pdf_hash=pdf_hash)
        terminal_name = terminal  # "terminal" is the name of the terminal that was passed from Process-72HR-Flights

        if not pdf_type:
            msg = "Failed to get pdf type."
            raise ValueError(msg)

        firestore_client.set_terminal_flights(
            terminal_name=terminal_name, pdf_type=pdf_type, flight_ids=stored_flights
        )

        # Set to False to indicate that the terminal is no longer being updated
        firestore_client.set_terminal_update_status(
            terminal_name=terminal_name, pdf_type=pdf_type, status=False
        )

        # Update Textract Job with timestamp for when store_flights function finished
        firestore_client.add_job_timestamp(job_id, "finished_store_flights")

        return_payload = {
            "statusCode": 200,
            "status": "success",
            "body": "Successfully stored flights.",
            "dateTime": current_time,
            "terminal": terminal,
            "archivedFlights": json.dumps(archived_flights),
            "storedFlights": json.dumps(stored_flights),
        }

        logging.info("Returning payload: %s", return_payload)

        return return_payload

    except json.JSONDecodeError:
        logging.critical("Failed to decode JSON payload.")
        return {"status": "failed", "body": "Invalid JSON payload."}
    except Exception as e:
        logging.critical("An unexpected error occurred: %s, %s", type(e).__name__, e)
        return {"status": "failed", "body": str(e)}
