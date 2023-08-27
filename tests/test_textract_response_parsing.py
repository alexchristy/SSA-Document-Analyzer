import unittest
import tests.doc_analysis_responses as doc_analysis_responses
import flight
from recieve_pdf_data_textract import parse_textract_response_to_flights

class TestTextractResponseParsing(unittest.TestCase):

    def test_bwi_1(self):

        mcguire_flight_known = flight.Flight(
            date = "20230818",
            destination_terminal = "Joint Base McGuire Dix Lakehurst Passenger Terminal",
            notes = "",
            num_of_seats = 5,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1200L",
            seat_status = "T",
            table_footer = "N/A"
        )

        incirlik_flight_known = flight.Flight(
            date = "20230820",
            destination_terminal = "Incirlik AB Passenger Terminal",
            notes = "",
            num_of_seats = 37,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "2300L",
            seat_status = "T",
            table_footer = "SPACE REQUIRED CHECK IN 2010L AND NO LATER THAN 2350L SPACE A MUST BE MARKED PRESENT PRIOR TO 2230L"
        )

        ramstein_flight_known = flight.Flight(
            date = "20230820",
            destination_terminal = "Ramstein AB Passenger Terminal",
            notes = "",
            num_of_seats = 69,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "2300L",
            seat_status = "T",
            table_footer = "SPACE REQUIRED CHECK IN 2010L AND NO LATER THAN 2350L SPACE A MUST BE MARKED PRESENT PRIOR TO 2230L"
        )

        known_flights = [mcguire_flight_known, incirlik_flight_known, ramstein_flight_known]

        parsed_flights = parse_textract_response_to_flights(doc_analysis_responses.bwi_1_textract_response)

        # Check that the number of flights is correct
        self.assertEqual(len(parsed_flights), len(known_flights))

        # Check that each known flight has a match in the parsed flights
        for known_flight in known_flights:
            self.assertTrue(any(known_flight == parsed_flight for parsed_flight in parsed_flights))

    def test_norfolk_1(self):

        rota_flight_known = flight.Flight(
            date = "20230818",
            destination_terminal = "Naval Air Station Rota Passenger Terminal",
            notes = "S/A Passengers must have a Spanish residency ID or have a Spanish Passport to make Rota, Spain their destination.",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1930",
            seat_status = "TDB",
            table_footer = "N/A"
        )

        sigonella_flight_known = flight.Flight(
            date = "20230818",
            destination_terminal = "NAS Sigonella Air Terminal",
            notes = "S/A Passengers must have a Spanish residency ID or have a Spanish Passport to make Rota, Spain their destination.",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1930",
            seat_status = "TDB",
            table_footer = "N/A"
        )

        bahrain_flight_known = flight.Flight(
            date = "20230818",
            destination_terminal = "Bahrain Passenger Terminal",
            notes = "S/A Passengers must have a Spanish residency ID or have a Spanish Passport to make Rota, Spain their destination.",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1930",
            seat_status = "TDB",
            table_footer = "N/A"
        )

        diego_garcia_flight_known = flight.Flight(
            date = "20230818",
            destination_terminal = "Diego Garcia",
            notes = "DELAYED MISSION;S/A Passengers must have a Spanish residency ID or have a Spanish Passport to make Rota, Spain their destination.",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1930",
            seat_status = "TDB",
            table_footer = "N/A"
        )

        jacksonville_flight_known = flight.Flight(
            date = "20230818",
            destination_terminal = "Naval Air Station Jacksonville Passenger Terminal",
            notes = "",
            num_of_seats = 60,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "2100",
            seat_status = "T",
            table_footer = "N/A"
        )

        jb_andrews_flight_known = flight.Flight(
            date = "20230819",
            destination_terminal = "Joint Base Andrews Passenger Terminal",
            notes = "",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1000",
            seat_status = "TDB",
            table_footer = "Seats: T - Tentative, F - Firm, TBD - To Be Determined"
        )

        biloxi_flight_known = flight.Flight(
            date = "20230819",
            destination_terminal = "GULFPORT-BILOXI INTL., MS",
            notes = "",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1115",
            seat_status = "TDB",
            table_footer = "Seats: T - Tentative, F - Firm, TBD - To Be Determined"
        )

        mcguire_field_flight_known = flight.Flight(
            date = "20230819",
            destination_terminal = "Joint Base McGuire Dix Lakehurst Passenger Terminal",
            notes = "",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1115",
            seat_status = "TDB",
            table_footer = "Seats: T - Tentative, F - Firm, TBD - To Be Determined"
        )

        jackson_flight_known = flight.Flight(
            date = "20230817",
            destination_terminal = "Jackson, MS",
            notes = "",
            num_of_seats = 0,
            origin_terminal = "", # Origin is blank as it is added outside of the parsing function
            rollcall_time = "1115",
            seat_status = "TDB",
            table_footer = "Seats: T - Tentative, F - Firm, TBD - To Be Determined"
        )

        known_flights = [rota_flight_known, sigonella_flight_known, bahrain_flight_known, diego_garcia_flight_known, jacksonville_flight_known, jb_andrews_flight_known, biloxi_flight_known, mcguire_field_flight_known, jackson_flight_known]

        parsed_flights = parse_textract_response_to_flights(doc_analysis_responses.norfolk_1_textract_response)

        # Check that the number of flights is correct
        self.assertEqual(len(parsed_flights), len(known_flights))

        # Check that each known flight has a match in the parsed flights
        for known_flight in known_flights:
            self.assertTrue(any(known_flight == parsed_flight for parsed_flight in parsed_flights))
            

if __name__ == '__main__':
    unittest.main()