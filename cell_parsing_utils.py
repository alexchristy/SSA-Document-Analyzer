import re
import logging
from typing import Tuple
import json
from table import Table

def parse_rollcall_time(time_str: str) -> str:
    """
    Parse rollcall time from a given string and return it in 24-hour format.
    
    Args:
    - time_str (str): The string containing rollcall time in specific formats.
    
    Returns:
    - str: A string representing the rollcall time in 24-hour format, or None if invalid.
    """

    # If time string is empty or None, log and return None
    if not time_str:
        logging.info("Rollcall time is empty or None.")
        return None

    # Regex to match time in HH:MM or HHMM format
    match = re.match(r"(\d{1,2}):?(\d{2})", time_str)
    if not match:
        logging.error(f"Failed to parse rollcall time: Invalid format '{time_str}'")
        return None
    
    hour, minute = map(int, match.groups())

    # Check for valid time
    if hour > 23 or minute > 59:
        logging.error(f"Failed to parse rollcall time: Invalid time '{time_str}'")
        return None
    
    # Format to 24-hour time string
    rollcall_time = f"{hour:02d}{minute:02d}"
    logging.info(f"Parsed rollcall time: {rollcall_time}")
    
    return rollcall_time

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

def parse_seat_data(seat_data: str) -> Tuple[int, str]:
    """
    Parse seat data from a given string and return the number of seats and seat status.
    
    Args:
    - seat_data (str): The string containing seat data in specific formats.
    
    Returns:
    - tuple: A tuple containing the number of seats (int) and seat status (str).
    """

    # Fix special case for '_' to '-'
    seat_data = seat_data.replace('_', '-')

    num_of_seats = -1
    seat_status = ''

    # Case 0: If seat data is empty, return TDB
    if seat_data == '':
        logging.info("Seat data is empty.")
        return None, None

    # Case 1: Seat data is TBD
    if seat_data.strip() == 'TBD':
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
    
    # Case 4: Format is number space letter (e.g., '20 T')
    elif len(seat_data.split()) == 2 and seat_data.split()[0].isdigit() and seat_data.split()[1].upper() in ['T', 'F']:
        num_of_seats = int(seat_data.split()[0])
        seat_status = seat_data.split()[1].upper()
        logging.info(f"Parsed data in format 'number space letter' (20 T). num_of_seats = {num_of_seats}, seat_status = {seat_status}")

    # Case 5: Format is letter dot number (e.g., 'T.20')
    elif '.' in seat_data and seat_data[0].upper() in ['T', 'F'] and seat_data[2:].isdigit():
        num_of_seats = int(seat_data[2:])
        seat_status = seat_data[0].upper()
        logging.info(f"Parsed data in format 'letter dot number' (T.20). num_of_seats = {num_of_seats}, seat_status = {seat_status}")

    else:
        logging.error(f"Failed to parse seat data: Invalid format '{seat_data}'")
        return None, None
    
    return num_of_seats, seat_status

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

def parse_destination(destination_data: str):

    from gpt3_turbo_analysis import GPT3TurboAnalysis
    from note_extract_utils import _extract_parenthesis_notes

    # If destination data is empty, return empty string
    if destination_data == '':
        logging.info("Destination data is empty.")
        return None
    
    # Initialize GPT analysis object
    gpt3_turbo_analysis = GPT3TurboAnalysis()

    # Use the extract_notes function to remove any notes from the input text
    notes = _extract_parenthesis_notes(destination_data)
    for note in notes:
        destination_data = destination_data.replace(note, '')

    # Analyze the input text using GPT-3 Turbo
    returned_str = gpt3_turbo_analysis.get_destination_analysis(destination_data)

    # If GPT determines there are no destinations, return None
    if returned_str is None or returned_str == 'None':
        return None
    
    # Capitalize all the destinations
    returned_str = returned_str.upper()

    # Parse the returned string into a list of destinations
    try:
        destinations = json.loads(returned_str)
    except Exception as e:
        logging.error(f"An error occurred processing GPT returned destinations. Error: {e}")
        logging.error(f"Returned string: {returned_str}")
        return None
    
    return destinations