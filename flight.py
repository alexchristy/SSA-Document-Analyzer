import hashlib
import logging
import pickle

class Flight:

    def __init__(self, origin_terminal: str, destinations: list, rollcall_time: str, num_of_seats: int, seat_status: str, notes: str, date: str, rollcall_note=False):

        self.origin_terminal = origin_terminal
        self.destinations = destinations
        self.rollcall_time = rollcall_time
        self.num_of_seats = num_of_seats
        self.seat_status = seat_status
        self.notes = notes
        self.date = date
        self.rollcall_note = rollcall_note

        # Generate a deterministic unique flight ID based on attributes using SHA-256
        attributes_str = f"{self.origin_terminal}{self.destinations}{self.rollcall_time}{self.num_of_seats}{self.seat_status}{self.notes}{self.date}"
        self.flight_id = hashlib.sha256(attributes_str.encode()).hexdigest()


    def to_dict(self):
        """
        Convert the Flight object to a dictionary suitable for Firestore storage.
        """
        return {
            'origin_terminal': self.origin_terminal,
            'destination_terminal': self.destinations,
            'rollcall_time': self.rollcall_time,
            'num_of_seats': self.num_of_seats,
            'seat_status': self.seat_status,
            'notes': self.notes,
            'flight_id': self.flight_id,
            'date': self.date,
            'rollcall_note': self.rollcall_note
        }
    
    def pretty_print(self):
        print(f"{'=' * 40}")
        print(f"Flight ID: {self.flight_id}")
        print(f"{'-' * 40}")
        print(f"Origin Terminal: {self.origin_terminal if self.origin_terminal else 'N/A'}")
        print(f"Destination/s: {self.destinations if self.destinations else 'N/A'}")
        rollcall_text = "**See note below**" if self.rollcall_note else self.rollcall_time
        print(f"Roll Call Time: {rollcall_text if rollcall_text else 'N/A'}")
        print(f"Number of Seats: {self.num_of_seats if self.num_of_seats is not None else 0}")
        print(f"Seat Status: {self.seat_status if self.seat_status else 'N/A'}")
        print(f"Notes: {self.notes if self.notes else 'N/A'}")
        print(f"Date: {self.date if self.date else 'N/A'}")
        print(f"{'=' * 40}")

    def __eq__(self, other):
        if not isinstance(other, Flight):
            return False
        return (
            self.origin_terminal == other.origin_terminal and
            self.destinations == other.destinations and
            self.rollcall_time == other.rollcall_time and
            self.num_of_seats == other.num_of_seats and
            self.seat_status == other.seat_status and
            self.notes == other.notes and
            self.date == other.date and
            self.flight_id == other.flight_id and
            self.rollcall_note == other.rollcall_note
        )
    
    @classmethod
    def load_state(cls, filename="flight_state.pkl") -> "Flight":
        try:
            with open(filename, "rb") as f:
                loaded_flight = pickle.load(f)
            return loaded_flight
        except Exception as e:
            logging.error(f"An error occurred while loading the flight state: {e}")
            return None