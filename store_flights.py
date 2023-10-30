import json
import logging
from typing import Dict, List, Optional, Type

from flight import Flight


def lambda_handler(event, context):
    try:
        # Deserialize the JSON string to a Python dictionary
        flight_data = json.loads(event)

        # Initialize an empty list to store Flight objects
        flights = []

        # Convert each item in the dictionary to a Flight object and append to the list
        for value in flight_data.values():
            flight_obj = Flight.from_dict(value)
            if flight_obj:
                flights.append(flight_obj)

        for flight in flights:
            print(flight.pretty_print())

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON payload.")
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
