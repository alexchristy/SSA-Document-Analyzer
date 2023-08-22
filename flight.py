import uuid

class Flight:

    def __init__(self, origin, destination, rollcall_time, num_of_seats, seat_status, notes):
        self.origin = origin
        self.destination = destination
        self.rollcall_time = rollcall_time
        self.num_of_seats = num_of_seats
        self.seat_status = seat_status
        self.notes = notes
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
            'flight_id': self.flight_id  # Include the unique flight ID
        }