import unittest
import tests.doc_analysis_responses as doc_analysis_responses
import flight
from recieve_pdf_data_textract import parse_textract_response_to_flights

class TestTextractResponseParsing(unittest.TestCase):

    def test_bwi_1(self):

        mcguire_flight_known = flight.Flight(
            date = "20230818",
            destination = "MCGUIRE AFB, NEW JERSEY",
            notes = "",
            num_of_seats = 5,
            origin = "",
            rollcall_time = "1200L",
            seat_status = "T",
            table_footer = "N/A"
        )

        incirlik_flight_known = flight.Flight(
            date = "20230820",
            destination = "INCIRLIK AB, TURKEY",
            notes = "",
            num_of_seats = 37,
            origin = "",
            rollcall_time = "2300L",
            seat_status = "T",
            table_footer = "SPACE REQUIRED CHECK IN 2010L AND NO LATER THAN 2350L SPACE A MUST BE MARKED PRESENT PRIOR TO 2230L"
        )

        ramstein_flight_known = flight.Flight(
            date = "20230820",
            destination = "RAMSTEIN AB, GERMANY",
            notes = "",
            num_of_seats = 69,
            origin = "",
            rollcall_time = "2300L",
            seat_status = "T",
            table_footer = "SPACE REQUIRED CHECK IN 2010L AND NO LATER THAN 2350L SPACE A MUST BE MARKED PRESENT PRIOR TO 2230L"
        )

        known_flights = [mcguire_flight_known, incirlik_flight_known, ramstein_flight_known]

        parsed_flights = parse_textract_response_to_flights(doc_analysis_responses.bwi_1_textract_response)

        # Check that the number of flights is correct
        self.assertEqual(len(parsed_flights), len(known_flights))

        # Check that each known fligh has a match in the parsed flights
        for known_flight in known_flights:
            self.assertTrue(any(known_flight == parsed_flight for parsed_flight in parsed_flights))

if __name__ == '__main__':
    unittest.main()