import uuid

class Flight:

    def __init__(self, origin, destination, rollcall_time, num_of_seats, seat_status, notes, date, table_footer):
        self.origin = origin
        self.destination = destination
        self.rollcall_time = rollcall_time
        self.num_of_seats = num_of_seats
        self.seat_status = seat_status
        self.notes = notes
        self.date = date
        self.table_footer = table_footer
        self.flight_id = str(uuid.uuid4())  # Generate a unique flight ID

    def to_dict(self):
        """
        Convert the Flight object to a dictionary suitable for Firestore storage.
        """
        return {
            'origin': self.origin,
            'destination': self.destination,
            'rollcall_time': self.rollcall_time,
            'num_of_seats': self.num_of_seats,
            'seat_status': self.seat_status,
            'notes': self.notes,
            'flight_id': self.flight_id,  # Include the unique flight ID
            'date': self.date,
            'table_footer': self.table_footer
        }
    
    def pretty_print(self):
        """
        Pretty-prints the Flight object's attributes in a visually appealing manner.
        """
        print(f"{'-' * 40}")
        print(f"Flight ID: {self.flight_id}")
        print(f"{'-' * 40}")
        print(f"Origin: {self.origin if self.origin else 'N/A'}")
        print(f"Destination: {self.destination if self.destination else 'N/A'}")
        print(f"Roll Call Time: {self.rollcall_time if self.rollcall_time else 'N/A'}")
        print(f"Number of Seats: {self.num_of_seats if self.num_of_seats else 'N/A'}")
        print(f"Seat Status: {self.seat_status if self.seat_status else 'N/A'}")
        print(f"Notes: {self.notes if self.notes else 'N/A'}")
        print(f"Date: {self.date if self.date else 'N/A'}")
        print(f"Table Footer: {self.table_footer if self.table_footer else 'N/A'}")
        print(f"{'-' * 40}")

    def __eq__(self, other):
        if not isinstance(other, Flight):
            return False
        return (
            self.origin == other.origin and
            self.destination == other.destination and
            self.rollcall_time == other.rollcall_time and
            self.num_of_seats == other.num_of_seats and
            self.seat_status == other.seat_status and
            self.notes == other.notes and
            self.date == other.date and
            self.table_footer == other.table_footer
        )