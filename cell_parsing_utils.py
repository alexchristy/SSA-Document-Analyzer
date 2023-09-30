import re
import logging
from typing import Tuple
import json
from table import Table

def parse_rollcall_time(time_str: str) -> str:
    if not time_str:
        logging.info("Rollcall time is empty or None.")
        return None

    # Regex patterns to exclude unwanted matches
    exclusion_patterns = [
        re.compile(r"(?:\s|-)\d{3}(?:\D|$)"),  # exclude " 123" or "-123"
    ]
    
    for pattern in exclusion_patterns:
        match = pattern.search(time_str)
        if match:
            logging.info(f"Excluding match due to exclusion pattern: '{match.group()}'")
            return None

    # Two different regex patterns for valid time formats
    patterns = [
        re.compile(r"(?<=\D)(\d{1,2}):?(\d{2})(?=\D|$)"),
        re.compile(r"^(?:\D)*(\d{1,2}):?(\d{2})(?=\D|$)")
    ]

    match = None
    for pattern in patterns:
        match = pattern.search(time_str)
        if match:
            break

    if not match:
        logging.error(f"Failed to parse rollcall time: Invalid format '{time_str}'")
        return None
    
    hour, minute = map(int, match.groups())

    if hour > 23 or minute > 59:
        logging.error(f"Failed to parse rollcall time: Invalid time '{time_str}'")
        return None

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
    Parse seat data from a given string and return a list of tuples containing the number of seats and seat status.
    
    Args:
    - seat_data (str): The string containing seat data in specific formats.
    
    Returns:
    - list: A list of tuples containing the number of seats (int) and seat status (str).
    """
    
    # Fix special case for '_' to '-'
    seat_data = seat_data.replace('_', '-')
    
    # Initialize list to store results
    results = []
    
    # Case 0: If seat data is empty, return an empty list
    if seat_data == '':
        logging.info("Seat data is empty.")
        return []
    
    # Case 1: Seat data is TBD
    if re.search(r'(?i)tbd', seat_data):
        logging.info("Seat data is TBD.")
        return [[0, 'TBD']]
    
    # Single pattern to cover all cases
    combined_pattern = r'(?P<num>\d+)(?P<status>[tf])|(?P<status1>[tf])-?(?P<num1>\d+)|(?P<num2>\d+)\s*(?P<status2>[tf])|(?P<status3>[tf])\.?(?P<num3>\d+)'
    
    # Use finditer to find all matches while maintaining their order
    for match in re.finditer(combined_pattern, seat_data, re.IGNORECASE):
        num_of_seats = int(match.group('num') or match.group('num1') or match.group('num2') or match.group('num3'))
        seat_status = (match.group('status') or match.group('status1') or match.group('status2') or match.group('status3')).upper()
        results.append([num_of_seats, seat_status])
    
    if results:
        logging.info(f"Parsed seat data: {results}")
    else:
        logging.error(f"Failed to parse seat data: Invalid format '{seat_data}'")
    
    return results

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