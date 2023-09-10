import unittest
from concurrent.futures import ThreadPoolExecutor
import sys
import time

# For Table class
sys.path.append("..")
from table import Table
from flight import Flight
from flight_utils import convert_72hr_table_to_flights

class TestTableToFlights(unittest.TestCase):
    
    def test_al_udeid_1_72hr(self):

        origin_terminal = 'Al Udeid AB Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/al_udeid_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/al_udeid_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/al_udeid_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-1_flight-1.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-1_flight-2.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-1_flight-3.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-1_flight-4.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-2_flight-2.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-2_flight-3.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-2_flight-4.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-2_flight-5.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-2_flight-6.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-3_flight-1.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-3_flight-2.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-3_flight-3.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-3_flight-4.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-3_flight-5.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/al_udeid_1_72hr_table-3_flight-6.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), len(table1_flights))
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), len(table3_flights))

        # Check that the converted flights are the same as the known good flights:
        # Table 1
        for i, flight in enumerate(table1_converted_flights):
            self.assertEqual(flight, table1_flights[i])

        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            self.assertEqual(flight, table2_flights[i])

        # Table 3
        for i, flight in enumerate(table3_converted_flights):
            self.assertEqual(flight, table3_flights[i])

    def test_andersen_1_72hr(self):
        
        origin_terminal = 'Andersen AFB Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/andersen_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/andersen_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/andersen_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/andersen_1_72hr_table-1_flight-1.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/andersen_1_72hr_table-2_flight-1.pkl"))

        # Table 3
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()
        
        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), len(table1_flights))
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), len(table3_flights))

        # Check that the converted flights are the same as the known good flights:
        # Table 1
        for i, flight in enumerate(table1_converted_flights):
            self.assertEqual(flight, table1_flights[i])

        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            self.assertEqual(flight, table2_flights[i])
        
        # Table 3
        for i, flight in enumerate(table3_converted_flights):
            self.assertEqual(flight, table3_flights[i])

    def test_andrews_1_72hr(self):

        origin_terminal = 'Joint Base Andrews Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/andrews_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/andrews_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/andrews_1_72hr_table-3.pkl")

        