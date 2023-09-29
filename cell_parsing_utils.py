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

    # Regex to match time in HH:MM or HHMM format within a string
    match = re.search(r"(\d{1,2}):?(\d{2})", time_str)
    
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

def parse_seat_data(seat_data: str):
    """
    Parse seat data from a given string and return the number of seats and seat status.
    
    Args:
    - seat_data (str): The string containing seat data in specific formats.
    
    Returns:
    - tuple: A tuple containing the number of seats (int) and seat status (str).
    """
    
    # Fix special case for '_' to '-'
    seat_data = seat_data.replace('_', '-')
    
    # Split off any notes in parenthesis
    seat_data, _ = split_parenthesis(seat_data)
    
    # Case 0: If seat data is empty, return None, None
    if seat_data == '':
        logging.info("Seat data is empty.")
        return None, None

    # Case 1: Seat data is TBD
    if re.search(r'(?i)tbd', seat_data):
        logging.info("Seat data is TBD.")
        return 0, 'TBD'

    # Case 2: Various other patterns
    patterns = [
        r'(?P<num>\d+)(?P<status>[tf])',  # e.g., "60T"
        r'(?P<status>[tf])-?(?P<num>\d+)',  # e.g., "T-60", "T60"
        r'(?P<num>\d+)\s*(?P<status>[tf])',  # e.g., "20 T"
        r'(?P<status>[tf])\.?(?P<num>\d+)'  # e.g., "T.20", "T20"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, seat_data, re.IGNORECASE)
        if match:
            num_of_seats = int(match.group('num'))
            seat_status = match.group('status').upper()
            logging.info(f"Parsed seat data. num_of_seats = {num_of_seats}, seat_status = {seat_status}")
            return num_of_seats, seat_status

    logging.error(f"Failed to parse seat data: Invalid format '{seat_data}'")
    return None, None

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