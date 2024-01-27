import datetime
import hashlib
import logging
import pickle
from typing import Any, Dict, Optional, Type


# Custom error classes for flight objects
class InvalidRollcallTimeError(Exception):
    """Raised when the roll call time is invalid."""

    pass


class InvalidDateError(Exception):
    """Raised when the date is invalid."""

    pass


class Flight:
    """Represents a flight with information about its origin, destination, roll call time, seats, notes, and date."""

    def __init__(  # noqa: PLR0913 (Makes more sense to have all of these arguments in one place)
        self: "Flight",
        origin_terminal: str,
        destinations: list,
        rollcall_time: Optional[str],
        seats: list,
        notes: Dict[str, Any],
        date: str,
        rollcall_note: bool = False,
        seat_note: bool = False,
        destination_note: bool = False,
        patriot_express: bool = False,
        creation_time: Optional[int] = None,
        should_archive: bool = True,
    ) -> None:
        """Initialize a Flight object with the given parameters.

        Args:
        ----
            origin_terminal (str): The origin terminal of the flight.
            destinations (list): A list of destination terminals for the flight.
            rollcall_time (str): The roll call time for the flight.
            seats (list): A list of seats for the flight.
            notes (str): Any notes for the flight.
            date (str): The date of the flight.
            rollcall_note (bool, optional): Whether there is a note for the roll call time. Defaults to False.
            seat_note (bool, optional): Whether there is a note for the seats. Defaults to False.
            destination_note (bool, optional): Whether there is a note for the destinations. Defaults to False.
            patriot_express (bool, optional): Whether the flight is a Patriot Express flight. Defaults to False.
            creation_time (int, optional): The time the flight was created. Defaults to None.
            should_archive (bool, optional): Whether the flight should be archived. Defaults to True.
        """
        self.origin_terminal = origin_terminal
        self.destinations = destinations
        self.rollcall_time = rollcall_time
        self.seats = seats
        self.notes = notes
        self.date = date
        self.rollcall_note = rollcall_note
        self.seat_note = seat_note
        self.destination_note = destination_note
        self.patriot_express = patriot_express
        self.should_archive = should_archive

        # Generate creation time if not provided
        self.creation_time = creation_time or int(
            datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d%H%M")
        )

        self.as_string = self.generate_as_string()

        self.flight_id = self.generate_flight_id()

    def generate_as_string(self: "Flight") -> str:
        """Generate a string representation of the Flight object.

        Returns
        -------
            str: A string representation of the Flight object.
        """
        return f"{self.origin_terminal}{self.destinations}{self.rollcall_time}{self.seats}{self.notes}{self.date}{self.rollcall_note}{self.seat_note}{self.destination_note}{self.patriot_express}"

    def pretty_print(self: "Flight") -> None:
        """Print a formatted representation of the Flight object."""
        print(f"{'=' * 40}")
        print(f"Flight ID: {self.flight_id}")
        print(f"{'-' * 40}")
        print(
            f"Origin Terminal: {self.origin_terminal if self.origin_terminal else 'N/A'}"
        )
        print(f"Destination/s: {self.destinations if self.destinations else 'N/A'}")
        rollcall_text = (
            "**See note below**"
            if self.rollcall_note and self.rollcall_time is None
            else self.rollcall_time
        )
        print(f"Roll Call Time: {rollcall_text if rollcall_text else 'N/A'}")
        print(f"Seats: {self.seats if self.seats else 'N/A'}")
        print(f"Notes: {self.notes if self.notes else 'N/A'}")
        print(f"Date: {self.date if self.date else 'N/A'}")
        print(f"Patriot Express: {self.patriot_express}")
        print(f"{'=' * 40}")

    def __eq__(self: "Flight", other: object) -> bool:
        """Check if two Flight objects are equal.

        Parameters
        ----------
        other : Flight
            The other Flight object to compare to.

        Returns
        -------
        bool
            True if the two Flight objects are equal, False otherwise.
        """
        if not isinstance(other, Flight):
            return False
        attributes_to_check = [
            "origin_terminal",
            "destinations",
            "rollcall_time",
            "seats",
            "notes",
            "date",
            "flight_id",
            "rollcall_note",
            "seat_note",
            "destination_note",
            "patriot_express",
            "as_string",
        ]
        return all(
            getattr(self, attr) == getattr(other, attr) for attr in attributes_to_check
        )

    def to_string(self: "Flight") -> str:
        """Return a string representation of the Flight object."""
        return self.as_string

    def generate_flight_id(self: "Flight") -> str:
        """Generate a unique ID for the Flight object.

        Returns
        -------
            str: A unique ID for the Flight object.
        """
        return hashlib.sha256(self.as_string.encode()).hexdigest()

    def make_firestore_compliant(self: "Flight") -> None:
        """Convert the seat data from a list of lists to a list of dictionaries."""
        # Convert the seat data to a list of dictionaries
        seats = []
        for data_point in self.seats:
            seat = {}
            seat["number"] = data_point[0]
            seat["status"] = data_point[1]
            seats.append(seat)
        self.seats = seats
        self.as_string = self.generate_as_string()
        self.flight_id = self.generate_flight_id()

    def get_departure_datetime(self: "Flight") -> str:
        """Get the departure day and time in the format YYYYMMDDHHMM.

        Returns
        -------
            str: The departure day and time.
        """
        valid_rollcall_time_length = 4

        if not self.date:
            msg = "Date is missing"
            raise InvalidDateError(msg)

        if not self.rollcall_time:
            if not self.rollcall_note:
                msg = "Rollcall time is missing"
                raise InvalidRollcallTimeError(msg)

            logging.info(
                "Rollcall time was replaced with a note: %s. Using 23:59 for the rollcall time.",
                self.rollcall_note,
            )
            # If there is a rollcall note, return the last minute of the day
            return f"{self.date}2359"

        # Ensure rollcall_time is in HHMM format
        if (
            len(self.rollcall_time) != valid_rollcall_time_length
            or not self.rollcall_time.isdigit()
        ):
            msg = "Invalid rollcall time format"
            raise InvalidRollcallTimeError(msg)

        # Combine date and rollcall time
        return f"{self.date}{self.rollcall_time}"

    def get_rollcall_note(self: "Flight") -> str:
        """Get the roll call note.

        Returns
        -------
            str: The roll call note.
        """
        if self.rollcall_note:
            return self.notes["rollCallNotes"]["rollCallCellNote"]
        return ""

    @classmethod
    def load_state(
        cls: Type["Flight"], filename: str = "flight_state.pkl"
    ) -> Optional["Flight"]:
        """Load the state of the Flight object from a pickle file.

        Parameters
        ----------
        filename : str, optional
            The name of the pickle file to load the state from, by default "flight_state.pkl"

        Returns
        -------
        Flight
            The Flight object with the loaded state, or None if an error occurred.
        """
        try:
            with open(filename, "rb") as f:
                return pickle.load(f)  # noqa: S301 (Only used for testing)
        except Exception as e:
            logging.error("An error occurred while loading the flight state: %s", e)
            return None

    def to_dict(self: "Flight") -> dict:
        """Convert the Flight object to a dictionary.

        Returns
        -------
            dict: A dictionary representation of the Flight object.
        """
        # Directly serialize class attributes
        return vars(self)

    @classmethod
    def _sort_nested_dict(cls: Type["Flight"], d: dict) -> dict:
        """Recursively sorts a nested dictionary lexicographically by keys."""
        if isinstance(d, dict):
            return {k: cls._sort_nested_dict(v) for k, v in sorted(d.items())}
        return d

    @classmethod
    def from_dict(cls: Type["Flight"], data: Dict[str, Any]) -> Optional["Flight"]:
        """Create a Flight object from a dictionary.

        Args:
        ----
            data (Dict[str, Any]): The dictionary containing the Flight attributes.

        Returns:
        -------
            Flight: A Flight object or None if an error occurred.
        """
        try:
            # Only pick the keys that are accepted by __init__
            valid_keys = {
                "origin_terminal",
                "destinations",
                "rollcall_time",
                "seats",
                "notes",
                "date",
                "rollcall_note",
                "seat_note",
                "destination_note",
                "patriot_express",
                "creation_time",
                "should_archive",
            }
            filtered_data = {k: v for k, v in data.items() if k in valid_keys}

            # Sort the 'notes' attribute if it exists and is a dictionary
            if "notes" in filtered_data and isinstance(filtered_data["notes"], dict):
                filtered_data["notes"] = cls._sort_nested_dict(filtered_data["notes"])

            # Initialize Flight object with filtered dictionary attributes
            return cls(**filtered_data)
        except Exception as e:
            logging.error("Failed to create Flight from dict: %s", e)
            return None
