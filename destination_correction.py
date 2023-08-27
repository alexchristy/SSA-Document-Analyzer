import logging
from collections import namedtuple
from fuzzywuzzy import fuzz
import re
from destination_dicts import state_abbreviations, country_abbreviations

# Get a logger for this module, allowing it to inherit settings from the parent application
logger = logging.getLogger(__name__)

# Define a namedtuple to encapsulate information about a location.
# The LocationInfo class has three attributes: base_name, state, and country.
LocationInfo = namedtuple("LocationInfo", ["base_name", "state", "country"])

def normalize_string(s):
    """
    Normalizes a string by removing all non-alphanumeric characters and converting it to lowercase.
    
    Parameters:
        s (str): The string to be normalized.
        
    Returns:
        str: The normalized string.
    """
    try:
        # Log the operation
        logger.info(f"Normalizing string: {s}")
        
        # Use a regular expression to remove non-alphanumeric characters and convert to lowercase
        normalized = re.sub(r'[^a-zA-Z0-9\\s]', '', s).lower()
        return normalized
    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"Error while normalizing string: {e}")
        return s

def replace_abbreviations(location_str, state_dict, country_dict):
    """
    Replaces state and country abbreviations in the input location string with their full names.
    
    Parameters:
        location_str (str): The location string to be processed.
        state_dict (dict): A dictionary mapping state abbreviations to full names.
        country_dict (dict): A dictionary mapping country abbreviations to full names.
        
    Returns:
        str: The location string with abbreviations replaced.
    """
    try:
        # Log the operation
        logger.info(f"Replacing abbreviations in: {location_str}")
        
        # Split the location string into words
        words = location_str.split()
        
        # Iterate through each word to replace abbreviations
        for i, word in enumerate(words):
            if word.upper() in state_dict:
                words[i] = state_dict[word.upper()]
            elif word.upper() in country_dict:
                words[i] = country_dict[word.upper()]
        return ' '.join(words)
    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"Error while replacing abbreviations: {e}")
        return location_str

def parse_location(location_str):
    """
    Parses a location string into its component parts: base_name, state, and country.
    
    Parameters:
        location_str (str): The location string to be parsed.
        
    Returns:
        LocationInfo: A namedtuple containing the parsed components.
    """
    try:
        # Log the operation
        logger.info(f"Parsing location: {location_str}")
        
        # Initialize variables to hold the parsed components
        base_name = ''
        state = ''
        country = ''
        
        # Check if the location string contains commas, indicating multiple components
        if ', ' in location_str:
            # Split the location string into its components
            parts = location_str.split(', ')
            base_name = parts[0]
            state = parts[1] if len(parts) > 1 else ''
            country = parts[2] if len(parts) > 2 else ''
        else:
            # If no commas are present, the entire string is considered as the base_name
            base_name = location_str
            
        return LocationInfo(base_name=base_name, state=state, country=country)
    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"Error while parsing location: {e}")
        return None

def calculate_location_match_score(location1, location2):
    """
    Calculates a similarity score between two LocationInfo objects based on fuzzy string matching.
    
    Parameters:
        location1 (LocationInfo): The first location object.
        location2 (LocationInfo): The second location object.
        
    Returns:
        float: The similarity score.
    """
    try:
        # Log the operation
        logger.info(f"Calculating match score between {location1} and {location2}")
        
        # Use fuzzy string matching to calculate similarity scores for each component
        base_name_score = fuzz.token_set_ratio(location1.base_name, location2.base_name)
        state_score = fuzz.token_set_ratio(location1.state, location2.state)
        country_score = fuzz.token_set_ratio(location1.country, location2.country)
        
        # Calculate the weighted average of the similarity scores
        return (0.6 * base_name_score) + (0.3 * state_score) + (0.1 * country_score)
    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"Error while calculating location match score: {e}")
        return 0

def find_best_location_match(location_to_check: str, potential_location_tuples: list):
    """
    Finds the best matching location from a list of potential locations based on fuzzy string matching and abbreviation replacement.
    
    Parameters:
        location_to_check (str): The location to be matched.
        potential_location_tuples (list): A list of tuples, each containing a terminal name and a potential matching location.
        state_dict (dict): A dictionary mapping state abbreviations to full names.
        country_dict (dict): A dictionary mapping country abbreviations to full names.
        
    Returns:
        tuple: A tuple containing the best matching terminal-location tuple and its score.
    """
    try:
        # Log the operation
        logger.info(f"Finding best location match for: {location_to_check}")
        
        # Replace abbreviations in the location string to be checked
        location_to_check = replace_abbreviations(location_to_check, state_abbreviations, country_abbreviations)
        
        # Parse the location string into its components
        location_to_check_info = parse_location(location_to_check)
        if location_to_check_info is None:
            logger.warning("Could not parse the location to check. Exiting.")
            return None, 0

        best_match = None
        best_match_score = 0

        # Iterate through each potential location to find the best match
        for terminal_name, potential_location in potential_location_tuples:
            # Replace abbreviations in the potential location string
            potential_location = replace_abbreviations(potential_location, state_abbreviations, country_abbreviations)
            
            # Parse the potential location into its components
            potential_location_info = parse_location(potential_location)
            if potential_location_info is None:
                logger.warning(f"Could not parse the potential location: {potential_location}. Skipping.")
                continue

            # Calculate the match score between the two locations
            match_score = calculate_location_match_score(location_to_check_info, potential_location_info)
            
            # Update the best match if the current score is higher
            if match_score > best_match_score:
                best_match = (terminal_name, potential_location)
                best_match_score = match_score

        # Log the best match found
        logger.info(f"Best match found: {best_match} with score: {best_match_score}")
        return best_match, best_match_score

    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"Error while finding best location match: {e}")
        return None, 0
