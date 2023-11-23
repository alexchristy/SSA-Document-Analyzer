import json
import logging
from typing import Dict, Optional

from aws_lambda_typing import context as lambda_context

from firestore_db import FirestoreClient
from flight import Flight
from time_utils import get_local_time


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


def lambda_handler(event: dict, context: lambda_context.Context) -> Dict[str, str]:
    """Archive flights that have departed and delete all old flights current flights collection."""
    try:
        firestore_client = FirestoreClient(textract_jobs_coll="Textract_Jobs")

        event = json.loads(event["payload"])

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
                    test_date = test_params["testDateTime"]
                    use_test_date = True
                else:
                    logging.error("Failed to get testDateTime.")

        terminal_timezone = get_terminal_timezone(
            firestore_client=firestore_client, terminal_name=terminal
        )

        current_time = get_current_time(
            use_test_date=use_test_date,
            test_date=test_date,
            terminal_timezone=terminal_timezone,
        )

        # Archive flights that have departed
        old_flights = firestore_client.get_flights_by_terminal(terminal=terminal)

        if not old_flights:
            logging.info("No flights found to archive.")

        for old_flight in old_flights:
            if old_flight.get_departure_datetime() < current_time:
                firestore_client.archive_flight(old_flight)
                logging.info("Archived flight: %s", old_flight.flight_id)

            # Delete all old flights
            firestore_client.delete_current_flight(old_flight)
            logging.info("Deleted flight: %s", old_flight.flight_id)

        # Store new flights
        for flight_dict in new_flights_dicts:
            new_flight = Flight.from_dict(flight_dict)

            if not new_flight:
                logging.error("Failed to create flight from dict: %s", flight_dict)
                continue

            if new_flight.get_departure_datetime() >= current_time:
                firestore_client.store_flight_as_current(new_flight)
                logging.info("Stored flight: %s", new_flight.flight_id)
            else:
                logging.info(
                    "Flight %s has already departed. Not storing.",
                    new_flight.flight_id,
                )

        return {"test": "test"}
    except json.JSONDecodeError:
        logging.critical("Failed to decode JSON payload.")
        return {"status": "failed", "body": "Invalid JSON payload."}
    except Exception as e:
        logging.critical("An unexpected error occurred: %s", e)
        return {"status": "failed", "body": str(e)}
