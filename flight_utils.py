from datetime import datetime
import logging
from typing import List
from table import Table
import re
from gpt3_turbo_analysis import GPT3TurboAnalysis
import json
from flight import Flight
from table_utils import check_date_string

def parse_seat_data(seat_data):
    """
    Parse seat data from a given string and return the number of seats and seat status.
    
    Args:
    - seat_data (str): The string containing seat data in specific formats.
    
    Returns:
    - tuple: A tuple containing the number of seats (int) and seat status (str).
    """
    
    num_of_seats = -1
    seat_status = ''

    # Case 0: If seat data is empty, return TDB
    if seat_data == '':
        logging.info("Seat data is empty.")
        seat_status = 'Empty'
        return num_of_seats, seat_status

    # Case 1: Seat data is TBD
    if seat_data == 'TBD':
        logging.info("Seat data is TBD.")
        seat_status = 'TBD'
        num_of_seats = 0
        return num_of_seats, seat_status

    # Case 2.5: Special case for '0F' and '0T', case insensitive
    elif seat_data.upper() == '0F' or seat_data.upper() == '0T':
        num_of_seats = 0
        seat_status = seat_data[-1].upper()
        logging.info(f"Parsed special case '0F' or '0T'. num_of_seats = {num_of_seats}, seat_status = {seat_status}")

    # Case 2: Format is number followed by letter (e.g., 60T)
    elif seat_data[:-1].isdigit() and len(seat_data[:-1]) > 0 and seat_data[-1].upper() in ['T', 'F']:
        num_of_seats = int(seat_data[:-1])
        seat_status = seat_data[-1].upper()
        logging.info(f"Parsed data in format 'number letter' (60T). num_of_seats = {num_of_seats}, seat_status = {seat_status}")

    # Case 3: Format is letter dash number (e.g., T-60)
    elif seat_data[0].upper() in ['T', 'F'] and seat_data[1] == '-' and seat_data[2:].isdigit():
        num_of_seats = int(seat_data[2:])
        seat_status = seat_data[0].upper()
        logging.info(f"Parsed data in format 'letter - number' (T-60). num_of_seats = {num_of_seats}, seat_status = {seat_status}")
     
    else:
        logging.error(f"Failed to parse seat data: Invalid format '{seat_data}'")
        return None, None
    
    return num_of_seats, seat_status

def parse_rollcall_time(time_str):
    """
    Parse rollcall time from a given string and return it in 24-hour format.
    
    Args:
    - time_str (str): The string containing rollcall time in specific formats.
    
    Returns:
    - str: A string representing the rollcall time in 24-hour format.
    """
    
    # If time string is empty, return empty string
    if time_str == '':
        logging.info("Rollcall time is empty.")
        return ''

    # Extract the first 4 digits to represent the 24-hour time
    rollcall_time = time_str[:4]
    
    if len(rollcall_time) != 4 or not rollcall_time.isdigit():
        logging.error(f"Failed to parse rollcall time: Invalid format '{time_str}'")
        return None
    
    # Log the parsed rollcall time
    logging.info(f"Parsed rollcall time: {rollcall_time}")
    
    return rollcall_time

def parse_destination(destination_data: str):

    # If destination data is empty, return empty string
    if destination_data == '':
        logging.info("Destination data is empty.")
        return ''
    
    # Initialize GPT analysis object
    gpt3_turbo_analysis = GPT3TurboAnalysis()

    # Split the destination data into two parts: one without the text in parenthesis and one with only the text in parenthesis
    no_parenth_text, parenth_text = split_parenthesis(destination_data)

    # If no text within parenthesis is found, use the entire destination data as the input text
    if not parenth_text:
        input_text = destination_data
    else:
        input_text = no_parenth_text

    # Analyze the input text using GPT-3 Turbo
    returned_str = gpt3_turbo_analysis.get_destination_analysis(input_text)

    # If GPT determines there are no destinations, return None
    if returned_str is None or returned_str == 'None':
        return None
    
    # Parse the returned string into a list of destinations
    try:
        destinations = json.loads(returned_str)
    except Exception as e:
        logging.error(f"An error occurred processing GPT returned destinations. Error: {e}")
        logging.error(f"Returned string: {returned_str}")
        return None
    
    return destinations

def split_parenthesis(text):
    """
    Splits a string into two parts: one without the text in parenthesis and one with only the text in parenthesis.
    
    Parameters:
        text (str): The input string that may contain text in parenthesis.
    
    Returns:
        tuple: A tuple containing two strings: one without the text in parenthesis and one with only the text in parenthesis.
    """
    try:
        # Use regular expression to find text within parenthesis
        parenth_text = re.findall(r'\(.*?\)', text)
        
        # If no text within parenthesis is found, log a warning
        if not parenth_text:
            logging.info(f"No text within parenthesis found in '{text}'.")
        
        # Remove text within parenthesis from the original string
        no_parenth_text = re.sub(r'\(.*?\)', '', text).strip()
        
        return no_parenth_text, ''.join(parenth_text)
    
    except Exception as e:
        # Log any exceptions that occur during the process
        logging.error(f"An error occurred: {e}")
        return None, None

def parse_row(table: Table, row_index: int):

    row = table.get_row(row_index)
    logging.info(f"Processing row {row_index}: {row}")

    # Guard statements for basic validation
    if row is None:
        logging.error(f"Skipping row {row_index} due to None value.")
        return None

    # Skip header row
    if row_index == 0:
        logging.error(f"Skipping row {row_index} due to header row.")
        return None

    # Skip row if it has less than 3 cells
    if len(row) < 3:
        logging.error(f"Skipping row {row_index} due to insufficient number of cells.")
        return None
    
    roll_call_time_cell, destination_cell, seat_cell = row[0], row[1], row[2]

    # Parse and check rollcall time
    roll_call_time = parse_rollcall_time(roll_call_time_cell[0])
    if roll_call_time is None:
        logging.info(f'Exiting parse_row_to_flight due to invalid rollcall time data.')
        return None
    
    # Parse and check seat data
    num_of_seats, seat_status = parse_seat_data(seat_cell[0])
    if num_of_seats is None or seat_status is None:
        logging.info(f'Exiting parse_row_to_flight due to invalid seat data.')        
        return None
    
    # Parse and check destination
    destinations = parse_destination(destination_cell[0])
    if destinations is None:
        logging.info(f'Exiting parse_row_to_flight due to invalid destination data.')
        return None
    
    return roll_call_time, destinations, num_of_seats, seat_status

def ocr_correction(input_str):
    correction_map = {
        'O': '0',
        'I': '1',
        'l': '1',
        'S': '5',
        'Z': '2',
        'B': '8'
    }
    return ''.join(correction_map.get(char, char) for char in input_str)

def has_complete_data(row: list):
    '''
    This checks that a row has a rollcall time, destination, and seat data.
    '''

    logging.info(f"Processing row: {row}")

    # Guard statements for basic validation
    if row is None:
        logging.error(f"Skipping row due to None value.")
        return False

    # Get cell values
    roll_call_time_cell, destination_cell, seat_cell = row[0], row[1], row[2]

    # Parse and check rollcall time
    roll_call_time = parse_rollcall_time(roll_call_time_cell[0])
    if roll_call_time == '':
        logging.info(f'Empty rollcall time.')
        return False
    
    # Parse and check seat data
    num_of_seats, seat_status = parse_seat_data(seat_cell[0])
    if num_of_seats is None or seat_status is None:
        logging.info(f'Empty seat data.')
        return False
    
    # Parse and check destination
    destinations = parse_destination(destination_cell[0])
    if destinations == []:
        logging.info(f'Empty destination data.')
        return False
    
    return True

def get_row_confidence_diff(row1: list, row2: list):

    # Get confidence scores for each cell
    rollcall_1_confidence = row1[0][1]
    destination_1_confidence = row1[1][1]
    seat_1_confidence = row1[2][1]

    rollcall_2_confidence = row2[0][1]
    destination_2_confidence = row2[1][1]
    seat_2_confidence = row2[2][1]

    # Average the confidence scores for each row
    row1_avg_confidence = (rollcall_1_confidence + destination_1_confidence + seat_1_confidence) / 3
    row2_avg_confidence = (rollcall_2_confidence + destination_2_confidence + seat_2_confidence) / 3

    # Get the difference between the two rows
    row_diff = abs(row1_avg_confidence - row2_avg_confidence)

    return row_diff

def get_roll_call_column_index(table: Table) -> int:
    """
    This function returns the index number of the column containing roll call times.
    """
    
    logging.info(f"Retrieving roll call column index.")

    if table is None:
        logging.error(f"Exiting function! Table is empty.")
        return None
    
    if table.rows is None:
        logging.error(f"Exiting function! There are no rows in the table.")
        return None
    
    if table.get_num_of_columns() == 0:
        logging.error(f"Exiting function! There are no columns in the table.")
        return None

    # Define regex pattern to match roll call time
    # column header
    patterns = [r'(?i)\broll\s*call\s*(time)?\b']

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info(f"Found roll call time column header: {column_header[0]}")
                return index

def get_destination_column_index(table: Table) -> int:
    """
    This function returns the index number of the column containing roll call times.
    """
    
    logging.info(f"Retrieving roll call column index.")

    if table is None:
        logging.error(f"Exiting function! Table is empty.")
        return None
    
    if table.rows is None:
        logging.error(f"Exiting function! There are no rows in the table.")
        return None
    
    if table.get_num_of_columns() == 0:
        logging.error(f"Exiting function! There are no columns in the table.")
        return None

    # Define regex pattern to match destination
    # column header
    patterns = [r'(?i)\bdestination(s)?\b']

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info(f"Found roll call time column header: {column_header[0]}")
                return index
            
def get_seats_column_index(table: Table) -> int:
    """
    This function returns the index number of the column containing roll call times.
    """
    
    logging.info(f"Retrieving roll call column index.")

    if table is None:
        logging.error(f"Exiting function! Table is empty.")
        return None
    
    if table.rows is None:
        logging.error(f"Exiting function! There are no rows in the table.")
        return None
    
    if table.get_num_of_columns() == 0:
        logging.error(f"Exiting function! There are no columns in the table.")
        return None

    # Define regex pattern to match seats
    # column header
    patterns = [r'(?i)\bseat(s)?\b']
    
    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info(f"Found roll call time column header: {column_header[0]}")
                return index

def convert_note_column_to_notes(table: Table, current_row: int, note_columns: List[int]) -> dict:
    """
    This function takes in a list of columns that contain information not related
    to roll call time, seats, or destinations and turns them into a json string.
    """

    # Check if table is empty
    if table is None:
        logging.error(f"Nothing to covert to notes. Table is empty.")
        return ''
    
    # Check if note columns is empty
    if note_columns is None:
        logging.error(f"Nothing to convert to notes. There are no note columns.")
        return ''
    
    # Create a dictionary to store notes
    notes = {}

    # Get notes from each cell
    for note_column in note_columns:
        note = table.get_cell_text(note_column, current_row)

        note_column_header_text = table.get_cell_text(note_column, 0)

        if note_column_header_text is None:
            logging.error(f"Failed to get note column header text from cell (0, {note_column}).")
            return ''
        
        if note is None:
            logging.error(f"Failed to get note from cell ({current_row}, {note_column}).")
            return ''
        
        notes[note_column_header_text] = note
    
    return notes

def reformat_date(date_str, current_date):
    pattern = r"(?P<day>\d{1,2})(?:th|st|nd|rd)?(?:\s*,?\s*)?(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    try:
        search_result = re.search(pattern, date_str, re.IGNORECASE)
        if not search_result:
            raise ValueError("Could not parse date")
        
        day = int(search_result.group('day'))
        month = search_result.group('month')[:3].lower()
        
        current_year = current_date.year
        current_month = current_date.month
        
        # Special case for early January when the current month is December
        if month == "jan" and day <= 4 and current_month == 12:
            inferred_year = current_year + 1
        else:
            inferred_year = current_year

        date_obj = datetime.strptime(f"{day} {month} {inferred_year}", "%d %b %Y")
        return date_obj.strftime('%Y%m%d')
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return date_str

def convert_72hr_table_to_flights(table: Table, origin_terminal: str) -> List[Flight]:
    """
    This function takes in a table from a 72hr flight schedule and 
    converts it to a list of Flight objects.
    """

    # Special bool vars for handling special cases
    has_note_columns = False

    # Initialize list of flights
    flights = []

    # Check if table is empty
    if table is None:
        logging.error(f"Table is empty.")
        return flights
    
    # Get number of rows in table
    if table.rows is None:
        logging.error(f"There are no rows in the table.")
        return flights

    # Get number of columns in table
    if table.get_num_of_columns() == 0:
        logging.error(f"There are no columns in the table.")
        return flights
    
    if table.get_num_of_columns() < 3:
        logging.error(f"There are not enough columns in the table. Only {table.get_num_of_columns()} columns found.")
        return flights
    
    # Get column indices
    if table.get_num_of_columns() > 3:
        logging.info(f"There are more than 3 columns in the table. Treating extra columns as notes.")
        roll_call_column_index = get_roll_call_column_index(table)
        destination_column_index = get_destination_column_index(table)
        seats_column_index = get_seats_column_index(table)

        # Make extra columns into notes
        note_column_indices = []
        has_note_columns = True
        for index, column_header in enumerate(table.rows[0]):
            if index not in [roll_call_column_index, destination_column_index, seats_column_index]:
                note_column_indices.append(index)
        logging.info(f"Note column indices found: {note_column_indices}")
    else:
        roll_call_column_index = 0
        destination_column_index = 1
        seats_column_index = 2

    # Check origin terminal
    if origin_terminal is None:
        logging.error(f"Origin terminal is empty.")
        return flights

    # Iterate through each row
    for row_index, row in enumerate(table.rows):

        # Special flight data variables
        roll_call_note = False
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

        # Parse roll call time
        roll_call_time = parse_rollcall_time(row[roll_call_column_index][0])

        # Parse destination
        destinations = parse_destination(row[destination_column_index][0])

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

        # Get date for flight from table title
        if table.title is None:
            logging.error(f"Table title is empty.")
            return flights
        
        match = check_date_string(table.title, return_match=True)

        if match is None:
            logging.error(f"Failed to get date from table title.")
            return flights
        
        date = reformat_date(match, datetime.now())

        # Check if any notes were added for the flight
        if len(notes) == 0:
            notes = None
        else:
            notes = json.dumps(notes)

        # Create flight object
        flight = Flight(origin_terminal=origin_terminal, destinations=destinations, rollcall_time=roll_call_time, num_of_seats=num_of_seats, seat_status=seat_status, notes=notes, date=date, rollcall_note=roll_call_note)

        flights.append(flight)
    
    return flights