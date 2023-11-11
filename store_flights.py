import json
import logging
from typing import Dict

from aws_lambda_typing import context as lambda_context

from firestore_db import FirestoreClient
from time_utils import get_local_time, pad_time_string


def lambda_handler(event: str, context: lambda_context.Context) -> Dict[str, str]:
    """Archive flights that have departed and delete all old flights current flights collection."""
    fs = FirestoreClient()

    try:
        # Deserialize the JSON string to a Python dictionary
        terminal_data = json.loads(event)

        terminal_name = terminal_data.get("terminal")

        flights = fs.get_flights_by_terminal(terminal_name)

        if not flights:
            msg = "No flights found."
            logging.info(msg)
            return {"status": "success", "body": msg}

        terminal = fs.get_terminal_dict_by_name(terminal_name)

        timezone = terminal.get("timezone", "")

        if not timezone:
            msg = "Timezone is not set for terminal."
            raise ValueError(msg)

        local_time = get_local_time(timezone)

        # Create date time string for lexographical comparison
        current_time_str = local_time.strftime("%Y%m%d%H%M")

        # Determine which flights have departed
        flights_to_archive = []
        for flight in flights:
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

            if flight_time_str < current_time_str:
                flights_to_archive.append(flight)

            # Delete flight from Current_Flights collection
            flight_id = flight_dict.get("id", "")

            if not flight_id:
                logging.critical(
                    "Failed to delete flight. Flight ID is not set for flight: %s",
                    flight,
                )
                continue

            fs.delete_flight_by_id(flight_id)

        # Archive flights that have departed
        for flight in flights_to_archive:
            fs.archive_flight(flight)

        return {"status": "success", "body": "Flights archived."}

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON payload.")
        return {"status": "failed", "body": "Invalid JSON payload."}
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        return {"status": "failed", "body": str(e)}
        raise e
