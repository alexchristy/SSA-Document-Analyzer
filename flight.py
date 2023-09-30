import hashlib
import logging
import pickle
from datetime import datetime

class Flight:

    def __init__(self, origin_terminal: str, destinations: list, rollcall_time: str, seats: list, notes: str, date: str, rollcall_note=False, seat_note=False, destination_note=False, patriot_express=False):

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
        self.creation_time = int(datetime.now().strftime("%Y%m%d%H%M"))
        self.as_string = self.generate_as_string()

        self.flight_id = self.generate_flight_id()

    def generate_as_string(self):
        return f"{self.origin_terminal}{self.destinations}{self.rollcall_time}{self.seats}{self.notes}{self.date}{self.rollcall_note}{self.seat_note}{self.destination_note}{self.patriot_express}"

    def to_dict(self):
        return {
            'origin_terminal': self.origin_terminal,
            'destination_terminal': self.destinations,
            'rollcall_time': self.rollcall_time,
            'seats': self.seats,
            'notes': self.notes,
            'flight_id': self.flight_id,
            'date': self.date,
            'rollcall_note': self.rollcall_note,
            'seat_note': self.seat_note,
            'destination_note': self.destination_note,
            'patriot_express': self.patriot_express,
            'creation_time': self.creation_time,
            'as_string': self.as_string
        }

    def pretty_print(self):
        print(f"{'=' * 40}")
        print(f"Flight ID: {self.flight_id}")
        print(f"{'-' * 40}")
        print(f"Origin Terminal: {self.origin_terminal if self.origin_terminal else 'N/A'}")
        print(f"Destination/s: {self.destinations if self.destinations else 'N/A'}")
        rollcall_text = "**See note below**" if self.rollcall_note and self.rollcall_time is None else self.rollcall_time
        print(f"Roll Call Time: {rollcall_text if rollcall_text else 'N/A'}")
        print(f"Seats: {self.seats if self.seats else 'N/A'}")
        print(f"Notes: {self.notes if self.notes else 'N/A'}")
        print(f"Date: {self.date if self.date else 'N/A'}")
        print(f"Patriot Express: {self.patriot_express}")
        print(f"{'=' * 40}")

    def __eq__(self, other):
        if not isinstance(other, Flight):
            return False
        return (
            self.origin_terminal == other.origin_terminal and
            self.destinations == other.destinations and
            self.rollcall_time == other.rollcall_time and
            self.seats == other.seats and
            self.notes == other.notes and
            self.date == other.date and
            self.flight_id == other.flight_id and
            self.rollcall_note == other.rollcall_note and
            self.seat_note == other.seat_note and
            self.destination_note == other.destination_note and
            self.patriot_express == other.patriot_express and
            self.as_string == other.as_string
        )
    
    def to_string(self):
        return self.as_string
    
    def generate_flight_id(self):
        return hashlib.sha256(self.as_string.encode()).hexdigest()

    @classmethod
    def load_state(cls, filename="flight_state.pkl") -> "Flight":
        try:
            with open(filename, "rb") as f:
                loaded_flight = pickle.load(f)
            return loaded_flight
        except Exception as e:
            logging.error(f"An error occurred while loading the flight state: {e}")
            return None