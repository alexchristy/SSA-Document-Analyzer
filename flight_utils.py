import logging
from table import Table
import re

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
    
    # Case 1: Format is number followed by letter (e.g., 60T)
    if seat_data[:-1].isdigit() and seat_data[-1].upper() in ['T', 'F']:
        num_of_seats = int(seat_data[:-1])
        seat_status = seat_data[-1].upper()
        logging.info(f"Parsed data in format 'number letter' (60T). num_of_seats = {num_of_seats}, seat_status = {seat_status}")
    
    # Case 2: Format is letter dash number (e.g., T-60)
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
    
    # Extract the first 4 digits to represent the 24-hour time
    rollcall_time = time_str[:4]
    
    if len(rollcall_time) != 4 or not rollcall_time.isdigit():
        logging.error(f"Failed to parse rollcall time: Invalid format '{time_str}'")
        return None
    
    # Log the parsed rollcall time
    logging.info(f"Parsed rollcall time: {rollcall_time}")
    
    return rollcall_time

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
            logging.warning(f"No text within parenthesis found in '{text}'.")
        
        # Remove text within parenthesis from the original string
        no_parenth_text = re.sub(r'\(.*?\)', '', text).strip()
        
        return no_parenth_text, ''.join(parenth_text)
    
    except Exception as e:
        # Log any exceptions that occur during the process
        logging.error(f"An error occurred: {e}")
        return None, None

def convert_table_to_flights(table: Table):

    table_no_column_headers = table.rows[1:]

    flights = []

    # Iterate through each row with flight data
    for row in table_no_column_headers:

        # Get rollcall time
        rollcall_time = parse_rollcall_time(row[0])

        # Get destination/s
        destination_data = row[1]

        # Get seat information
        num_of_seats, seat_status = parse_seat_data(row[2])

        
        
        