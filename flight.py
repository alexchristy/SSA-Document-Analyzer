import hashlib

class Flight:

    def __init__(self, origin_terminal, destination_terminal, rollcall_time, num_of_seats, seat_status, notes, date, table_footer):

        self.origin_terminal = origin_terminal
        self.destination_terminal = destination_terminal
        self.rollcall_time = rollcall_time
        self.num_of_seats = num_of_seats
        self.seat_status = seat_status
        self.notes = notes
        self.date = date
        self.table_footer = table_footer

        # Generate a deterministic unique flight ID based on attributes using SHA-256
        attributes_str = f"{self.origin_terminal}{self.destination_terminal}{self.rollcall_time}{self.num_of_seats}{self.seat_status}{self.notes}{self.date}{self.table_footer}"
        self.flight_id = hashlib.sha256(attributes_str.encode()).hexdigest()


    def to_dict(self):
        """
        Convert the Flight object to a dictionary suitable for Firestore storage.
        """
        return {
            'origin_terminal': self.origin_terminal,
            'destination_terminal': self.destination_terminal,
            'rollcall_time': self.rollcall_time,
            'num_of_seats': self.num_of_seats,
            'seat_status': self.seat_status,
            'notes': self.notes,
            'flight_id': self.flight_id,
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
        print(f"Origin Terminal: {self.origin_terminal if self.origin_terminal else 'N/A'}")
        print(f"Destination Terminal: {self.destination_terminal if self.destination_terminal else 'N/A'}")
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
            self.origin_terminal == other.origin_terminal and
            self.destination_terminal == other.destination_terminal and
            self.rollcall_time == other.rollcall_time and
            self.num_of_seats == other.num_of_seats and
            self.seat_status == other.seat_status and
            self.notes == other.notes and
            self.date == other.date and
            self.table_footer == other.table_footer
        )