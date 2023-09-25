from datetime import datetime
import logging
from typing import List
from table import Table
import json
from flight import Flight
from date_utils import check_date_string
from cell_parsing_utils import parse_rollcall_time, parse_seat_data, ocr_correction, parse_destination
from table_utils import get_roll_call_column_index, get_destination_column_index, get_seats_column_index, convert_note_column_to_notes
from date_utils import create_datetime_from_str, reformat_date

def convert_72hr_table_to_flights(table: Table, origin_terminal: str, use_fixed_date=False, fixed_date=None) -> List[Flight]:
    """
    Converts a 72-hour flight schedule table to a list of Flight objects.

    Parameters:
        table (Table): The table object containing the 72-hour flight schedule.
        origin_terminal (str): The terminal where the flights originate.
        use_fixed_date (bool, optional): Whether to use a fixed date for testing purposes. Default is False.
        fixed_date (str, optional): The fixed date to use for testing in 'YYYYMMDD' format. Required if `use_fixed_date` is True.

    Returns:
        List[Flight]: A list of Flight objects created from the table.

    Notes:
        - The function can handle special cases like note columns and OCR errors.
        - Logging is used extensively for error handling and debugging.
        - The function expects at least three columns in the table: roll call time, destination, and seats.
        - Extra columns are treated as notes.
        - If the table title contains a date, it will be used for the Flight objects' date attribute.

    Raises:
        Logs an error and returns an empty list if:
            - The table is empty or has no rows/columns.
            - Required data like origin_terminal or table title is missing.
            - Date parsing from the table title fails.

    Examples:
        >>> table = Table(rows=[...], title='Title with Date', footer='Footer note')
        >>> origin_terminal = 'Terminal 1'
        >>> flights = convert_72hr_table_to_flights(table, origin_terminal)
    """

    # Does table have extra columns for notes?
    has_note_columns = False

    # Initialize list of flights
    flights = []

    # Check if table is empty
    if table is None:
        logging.error(f"Table is empty.")
        return flights
    
    # Check that there are rows in table
    if table.rows is None:
        logging.error(f"There are no rows in the table.")
        return flights

    # Check there are columns in table
    if table.get_num_of_columns() == 0:
        logging.error(f"There are no columns in the table.")
        return flights
    
    # Check that there are at least 3 columns in table
    if table.get_num_of_columns() < 3:
        logging.error(f"There are not enough columns in the table. Only {table.get_num_of_columns()} columns found.")
        return flights
    
    # Get column indices
    roll_call_column_index = get_roll_call_column_index(table)
    destination_column_index = get_destination_column_index(table)
    seats_column_index = get_seats_column_index(table)

    if roll_call_column_index is None:
        logging.error(f"Failed to get roll call column index. Skipping table.")
        return flights

    if destination_column_index is None:
        logging.error(f"Failed to get destination column index. Skipping table.")
        return flights

    if seats_column_index is None:
        logging.error(f"Failed to get seats column index. Skipping table.")
        return flights

    # Create list of note column indices if there are more than 3 columns
    if table.get_num_of_columns() > 3:
        logging.info(f"There are more than 3 columns in the table. Treating extra columns as notes.")

        # Make extra columns into notes
        note_column_indices = []
        has_note_columns = True
        for index, column_header in enumerate(table.rows[0]):
            if index not in [roll_call_column_index, destination_column_index, seats_column_index]:
                note_column_indices.append(index)
        logging.info(f"Note column indices found: {note_column_indices}")

    # Check origin terminal
    if origin_terminal is None:
        logging.error(f"Origin terminal is empty.")
        return flights

    # Iterate through each row
    for row_index, row in enumerate(table.rows):
        logging.info(f"Processing row {row_index}.")

        # Special flight data variables
        roll_call_note = False
        seat_note = False
        notes = {}

        # Skip header row
        if row_index == 0:
            continue

        # Convert note columns to notes
        if has_note_columns:
            logging.info(f"Converting note columns to notes.")
            notes = convert_note_column_to_notes(table, row_index, note_column_indices)

        # Add table footer to notes if it exists
        if table.footer is not None and table.footer != '':
            notes['footnote'] = table.footer

        destinations = parse_destination(row[destination_column_index][0])

        # Parse roll call time
        roll_call_time = parse_rollcall_time(row[roll_call_column_index][0])

        # Parse seat data
        num_of_seats, seat_status = parse_seat_data(row[seats_column_index][0])

        # Skip row if it doesn't have complete data
        if roll_call_time is None and num_of_seats is None and seat_status is None and destinations is None:
            logging.info(f"Skipping row {row_index} due to incomplete data.")
            continue

        # Handle special cases:
        # Case 1: If roll call time is not a number, check if it's a note
        if roll_call_time is None:
            
            if len(row[roll_call_column_index][0]) > 0:
                roll_call_note = True
                logging.info(f'Appears to be special roll call format or note. Saving as \"Roll Call Note\" in notes.')
                notes['Roll Call Note'] = row[roll_call_column_index][0]

        # Case 2: If seat data parsing fails, try correcting common OCR errors
        if num_of_seats is None and seat_status is None:
            new_seat_data = ocr_correction(row[seats_column_index][0])
            logging.info(f'Atempting to correct OCR errors. New seat data: {new_seat_data}')
            num_of_seats, seat_status = parse_seat_data(new_seat_data)

        # Case 2.5: If seat data parsing still fails, check if it's a note
        if num_of_seats is None and seat_status is None:
            if len(row[seats_column_index][0]) > 0:
                seat_note = True
                num_of_seats = -1
                seat_status = ''
                logging.info(f'Appears to be special seat format or note. Saving as \"Seat Note\" in notes.')
                notes['Seat Note'] = row[seats_column_index][0]

        # Get date for flight from table title
        if table.title is None:
            logging.error(f"Table title is empty.")
            return flights
        
        match = check_date_string(table.title, return_match=True)

        if match is None:
            logging.error(f"Failed to get date from table title.")
            return flights
        
        # Added this functionality to allow for testing with a fixed date
        # which allows for proper testing of year inference functionality
        # of the reformat_date function
        if use_fixed_date:
            logging.info(f"Using fixed date: {fixed_date}")
            if fixed_date is None:
                logging.error(f"Fixed date is empty.")
                return flights

            custom_date = create_datetime_from_str(fixed_date)
            date = reformat_date(match, custom_date)
        else:
            date = reformat_date(match, datetime.now())

        # Check if any notes were added for the flight
        if len(notes) == 0:
            notes = None
        else:
            notes = json.dumps(notes)

        # Create flight object
        flight = Flight(origin_terminal=origin_terminal, destinations=destinations, rollcall_time=roll_call_time, num_of_seats=num_of_seats, seat_status=seat_status, notes=notes, date=date, rollcall_note=roll_call_note, seat_note=seat_note)

        flights.append(flight)
    
    return flights