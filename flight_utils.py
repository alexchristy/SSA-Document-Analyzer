import logging
from table import Table
import re
from gpt3_turbo_analysis import GPT3TurboAnalysis
import json

def parse_seat_data(seat_data):
    """
    Parse seat data from a given string and return the number of seats and seat status.
    
    Args:
    - seat_data (str): The string containing seat data in specific formats.
    
    Returns:
    - tuple: A tuple containing the number of seats (int) and seat status (str).
    """
    
    num_of_seats = 0
    seat_status = ''

    # If seat data is empty, return empty values
    if seat_data == '':
        logging.info("Seat data is empty.")
        return num_of_seats, seat_status

    # Case 1: Seat data is TBD
    if seat_data == 'TBD':
        logging.info("Seat data is TBD.")
        seat_status = 'TBD'
        return num_of_seats, seat_status

    # Case 2: Format is number followed by letter (e.g., 60T)
    elif seat_data[:-1].isdigit() and seat_data[-1].upper() in ['T', 'F']:
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

    row = table.rows[row_index]
    logging.info(f"Processing row {row_index}: {row}")

    # Guard statements for basic validation
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





if __name__ == "__main__":
    # Create a table object
    table = Table.load_state(filename="table3_state.pkl")
    print(parse_row(table, 1))