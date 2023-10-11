import unittest
from concurrent.futures import ThreadPoolExecutor
import sys

# For Table class
sys.path.append("../")
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
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/andrews_1_72hr_table-1_flight-1.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/andrews_1_72hr_table-2_flight-1.pkl"))

        # Table 3
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

    def test_bahrain_1_72hr(self):

        origin_terminal = 'Bahrain Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/bahrain_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/bahrain_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/bahrain_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/bahrain_1_72hr_table-2_flight-1.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/bahrain_1_72hr_table-3_flight-1.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/bahrain_1_72hr_table-3_flight-2.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/bahrain_1_72hr_table-3_flight-3.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

    def test_bwi_1_72hr(self):

        origin_terminal = 'Baltimore-Washinton International Passenger Terminal' # Note: Incorrect spelling of "Washington" reflects the spelling on the website

        # Load tables
        table1 = Table.load_state("tests/table-objects/bwi_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/bwi_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/bwi_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/bwi_1_72hr_table-1_flight-1.pkl"))

        # Table 2
        # No flights

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/bwi_1_72hr_table-3_flight-1.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/bwi_1_72hr_table-3_flight-2.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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
    
    def test_charleston_1_72hr(self):

        origin_terminal = 'Joint Base Charleston Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/charleston_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/charleston_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/charleston_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/charleston_1_72hr_table-2_flight-1.pkl"))

        # Table 3
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

    def test_dover_1_72hr(self):

        origin_terminal = 'Dover AFB Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/dover_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/dover_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/dover_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/dover_1_72hr_table-1_flight-1.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/dover_1_72hr_table-1_flight-2.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/dover_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/dover_1_72hr_table-2_flight-2.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/dover_1_72hr_table-3_flight-1.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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
    
    def test_elmendorf_1_72hr(self):

        origin_terminal = 'Joint Base Elmendorf-Richardson Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/elmendorf_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/elmendorf_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/elmendorf_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-1_flight-1.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-1_flight-2.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-1_flight-3.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-1_flight-4.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-2_flight-2.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-3_flight-1.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-3_flight-2.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-3_flight-3.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-3_flight-4.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/elmendorf_1_72hr_table-3_flight-5.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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
        
    def test_fairchild_1_72hr(self):
        
        origin_terminal = 'Fairchild AFB Air Transportation Function'

        # Load tables
        table1 = Table.load_state("tests/table-objects/fairchild_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/fairchild_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/fairchild_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        # No flights

        # Table 2
        # No flights

        # Table 3
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, False, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, False, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, False, fixed_date)
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

    def test_guantanamo_1_72hr(self):

        origin_terminal = 'NS Guantanamo Bay Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/guantanamo_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/guantanamo_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/guantanamo_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/guantanamo_1_72hr_table-2_flight-1.pkl"))

        # Table 3
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

    def test_hickam_1_72hr(self):

        origin_terminal = 'Joint Base Pearl Harbor-Hickam Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/hickam_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/hickam_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/hickam_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-1_flight-1.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-1_flight-2.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-1_flight-3.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-1_flight-4.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-1_flight-5.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-2_flight-2.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-3_flight-1.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/hickam_1_72hr_table-3_flight-2.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

    def test_incirlik_1_72hr(self):

        origin_terminal = 'Incirlik AB Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/incirlik_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/incirlik_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/incirlik_1_72hr_table-3.pkl")
        table4 = Table.load_state("tests/table-objects/incirlik_1_72hr_table-4.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []
        table4_flights = []

        # Table 1
        # No flights

        # Table 2
        # No flights

        # Table 3
        # No flights

        # Table 4
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, False, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, False, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, False, fixed_date),
                'table4': executor.submit(convert_72hr_table_to_flights, table4, origin_terminal, False, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()
            table4_converted_flights = futures['table4'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), len(table1_flights))
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), len(table3_flights))
        self.assertEqual(len(table4_converted_flights), len(table4_flights))

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

        # Table 4
        for i, flight in enumerate(table4_converted_flights):
            self.assertEqual(flight, table4_flights[i])

    def test_kadena_1_72hr(self):


        origin_terminal = 'Kadena AB Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/kadena_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/kadena_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/kadena_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/kadena_1_72hr_table-1_flight-1.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/kadena_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/kadena_1_72hr_table-2_flight-2.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/kadena_1_72hr_table-2_flight-3.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/kadena_1_72hr_table-3_flight-1.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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

    def test_macdill_2_72hr(self):

        origin_terminal = 'MacDill AFB Air Transportation Function'

        # Load tables
        table1 = Table.load_state("tests/table-objects/macdill_2_72hr_table-1.pkl")

        # Load known good flights
        table1_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/macdill_2_72hr_table-1_flight-1.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/macdill_2_72hr_table-1_flight-2.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), len(table1_flights))

        # Check that the converted flights are the same as the known good flights:
        # Table 1
        for i, flight in enumerate(table1_converted_flights):
            self.assertEqual(flight, table1_flights[i])

    def test_iwakuni_1_72hr(self):

        origin_terminal = 'MCAS Iwakuni Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/iwakuni_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/iwakuni_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/iwakuni_1_72hr_table-3.pkl")
        table4 = Table.load_state("tests/table-objects/iwakuni_1_72hr_table-4.pkl")

        # Load known good flights
        table2_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/iwakuni_1_72hr_table-2_flight-1.pkl"))

        # Table 3
        # No flights

        # Table 4
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date),
                'table4': executor.submit(convert_72hr_table_to_flights, table4, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()
            table4_converted_flights = futures['table4'].result()
    
        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), 0)
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), 0)
        self.assertEqual(len(table4_converted_flights), 0)

        # Check that the converted flights are the same as the known good flights:
        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            are_equal, diff_message = flight == table2_flights[i]
            self.assertTrue(are_equal, f"Difference in flight {i}: {diff_message}")

    def test_mcchord_1_72hr(self):

        origin_terminal = 'Joint Base Lewis-McChord Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/mcchord_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/mcchord_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/mcchord_1_72hr_table-3.pkl")
        table4 = Table.load_state("tests/table-objects/mcchord_1_72hr_table-4.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []
        table4_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-2_flight-2.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-2_flight-3.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-2_flight-4.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-3_flight-1.pkl"))

        # Table 4
        table4_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-4_flight-1.pkl"))
        table4_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-4_flight-2.pkl"))
        table4_flights.append(Flight.load_state("tests/flight-objects/mcchord_1_72hr_table-4_flight-3.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date),
                'table4': executor.submit(convert_72hr_table_to_flights, table4, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()
            table4_converted_flights = futures['table4'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), 0)
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), len(table3_flights))
        self.assertEqual(len(table4_converted_flights), len(table4_flights))

        # Check that the converted flights are the same as the known good flights:
        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            self.assertEqual(flight, table2_flights[i])

        # Table 3
        for i, flight in enumerate(table3_converted_flights):
            self.assertEqual(flight, table3_flights[i])

        # Table 4
        for i, flight in enumerate(table4_converted_flights):
            self.assertEqual(flight, table4_flights[i])
    
    def test_mcconnell_2_72hr(self):

        origin_terminal = 'McConnell AFB Air Transportation Function'

        # Load tables
        table1 = Table.load_state("tests/table-objects/mcconnell_2_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/mcconnell_2_72hr_table-2.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/mcconnell_2_72hr_table-2_flight-1.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = { 
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), 0)
        self.assertEqual(len(table2_converted_flights), len(table2_flights))

        # Check that the converted flights are the same as the known good flights:
        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            self.assertEqual(flight, table2_flights[i])

    def test_mcguire_1_72hr(self):

        origin_terminal = 'Joint Base McGuire Dix Lakehurst Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/mcguire_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/mcguire_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/mcguire_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/mcguire_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/mcguire_1_72hr_table-2_flight-2.pkl"))

        # Table 3
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), 0)
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), 0)

        # Check that the converted flights are the same as the known good flights:
        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            self.assertEqual(flight, table2_flights[i])

    def test_mildenhall_1_72hr(self):

        origin_terminal = 'RAF Mildenhall Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/mildenhall_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/mildenhall_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/mildenhall_1_72hr_table-3.pkl")
        table4 = Table.load_state("tests/table-objects/mildenhall_1_72hr_table-4.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []
        table4_flights = []

        # Table 1
        # No flights

        # Table 2
        # No flights

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/mildenhall_1_72hr_table-3_flight-1.pkl"))
        table3_flights.append(Flight.load_state("tests/flight-objects/mildenhall_1_72hr_table-3_flight-2.pkl"))

        # Table 4
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date),
                'table4': executor.submit(convert_72hr_table_to_flights, table4, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()
            table4_converted_flights = futures['table4'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), 0)
        self.assertEqual(len(table2_converted_flights), 0)
        self.assertEqual(len(table3_converted_flights), len(table3_flights))
        self.assertEqual(len(table4_converted_flights), 0)

        # Check that the converted flights are the same as the known good flights:
        # Table 3
        for i, flight in enumerate(table3_converted_flights):
            self.assertEqual(flight, table3_flights[i])

    def test_norfolk_1_72hr(self):

        origin_terminal = 'Naval Station Norfolk Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/norfolk_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/norfolk_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/norfolk_1_72hr_table-3.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []

        # Table 1
        table1_flights.append(Flight.load_state("tests/flight-objects/norfolk_1_72hr_table-1_flight-1.pkl"))
        table1_flights.append(Flight.load_state("tests/flight-objects/norfolk_1_72hr_table-1_flight-2.pkl"))

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/norfolk_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/norfolk_1_72hr_table-2_flight-2.pkl"))

        # Table 3
        table3_flights.append(Flight.load_state("tests/flight-objects/norfolk_1_72hr_table-3_flight-1.pkl"))

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date)
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
        
    def test_osan_1_72hr(self):

        origin_terminal = 'Osan AB Passenger Terminal'

        # Load tables
        table1 = Table.load_state("tests/table-objects/osan_1_72hr_table-1.pkl")
        table2 = Table.load_state("tests/table-objects/osan_1_72hr_table-2.pkl")
        table3 = Table.load_state("tests/table-objects/osan_1_72hr_table-3.pkl")
        table4 = Table.load_state("tests/table-objects/osan_1_72hr_table-4.pkl")
        table5 = Table.load_state("tests/table-objects/osan_1_72hr_table-5.pkl")

        # Load known good flights
        table1_flights = []
        table2_flights = []
        table3_flights = []
        table4_flights = []
        table5_flights = []

        # Table 1
        # No flights

        # Table 2
        table2_flights.append(Flight.load_state("tests/flight-objects/osan_1_72hr_table-2_flight-1.pkl"))
        table2_flights.append(Flight.load_state("tests/flight-objects/osan_1_72hr_table-2_flight-2.pkl"))

        # Table 3
        # No flights

        # Table 4
        # No flights

        # Table 5
        # No flights

        # Use ThreadPoolExecutor to run conversions in parallel
        with ThreadPoolExecutor() as executor:
            fixed_date = "20230910"
            futures = {
                'table1': executor.submit(convert_72hr_table_to_flights, table1, origin_terminal, True, fixed_date),
                'table2': executor.submit(convert_72hr_table_to_flights, table2, origin_terminal, True, fixed_date),
                'table3': executor.submit(convert_72hr_table_to_flights, table3, origin_terminal, True, fixed_date),
                'table4': executor.submit(convert_72hr_table_to_flights, table4, origin_terminal, True, fixed_date),
                'table5': executor.submit(convert_72hr_table_to_flights, table5, origin_terminal, True, fixed_date)
            }

            table1_converted_flights = futures['table1'].result()
            table2_converted_flights = futures['table2'].result()
            table3_converted_flights = futures['table3'].result()
            table4_converted_flights = futures['table4'].result()
            table5_converted_flights = futures['table5'].result()

        # Check that the flights are the same
        self.assertEqual(len(table1_converted_flights), 0)
        self.assertEqual(len(table2_converted_flights), len(table2_flights))
        self.assertEqual(len(table3_converted_flights), 0)
        self.assertEqual(len(table4_converted_flights), 0)
        self.assertEqual(len(table5_converted_flights), 0)

        # Check that the converted flights are the same as the known good flights:
        # Table 2
        for i, flight in enumerate(table2_converted_flights):
            self.assertEqual(flight, table2_flights[i])

    
