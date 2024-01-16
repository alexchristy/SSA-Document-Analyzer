import datetime
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from fuzzywuzzy import fuzz  # type: ignore

from cell_parsing_utils import parse_destination, parse_rollcall_time, parse_seat_data
from date_utils import check_date_string, create_datetime_from_str, reformat_date
from flight import Flight
from note_extract_utils import extract_notes
from table import Table
from table_utils import (
    convert_note_column_to_notes,
    infer_destinations_column_index,
    infer_roll_call_column_index,
    infer_seats_column_index,
    merge_table_rows,
)


def find_patriot_express(input_str: str) -> bool:
    """Return if string has patriot express in it.

    Searches for the string "patriotexpress" in the input string in a case-insensitive manner.
    If an exact match is not found, fuzzy string matching is used to determine if the input string is similar enough.

    Args:
    ----
        input_str (str): The string to search for "patriotexpress".

    Returns:
    -------
        bool: True if "patriotexpress" is found or if the input string is similar enough, False otherwise.
    """
    try:
        # Remove all white spaces from the input string
        sanitized_str = "".join(input_str.split()).lower()

        # Regular expression pattern to find "patriotexpress"
        pattern = re.compile(r"patriotexpress")

        # Search for the pattern in the sanitized string
        match = pattern.search(sanitized_str)

        # Use fuzzy string matching if no exact match is found
        if not match:
            threshold = 80  # Similarity threshold (0-100)
            similarity = fuzz.partial_ratio("patriotexpress", sanitized_str)
            return similarity >= threshold

        return True
    except Exception as e:
        # Robust error handling
        logging.info("An error occurred in find_patriot_express: %s", e)
        return False


def search_key_recursive_dict(
    dictionary: Dict[str, Any], search_key: str
) -> Tuple[str, str]:
    """Search for a key in a dictionary in a case-insensitive and recursive manner.

    Args:
    ----
        dictionary (Dict[str, Any]): The dictionary to search.
        search_key (str): The key to search for.

    Returns:
    -------
        Tuple[str, str]: A tuple containing the found key and its corresponding value,
                         or two empty strings if not found.
    """
    for key, value in dictionary.items():
        if key.lower() == search_key.lower():
            logging.info(
                "Key '%s' found in dictionary. Corresponding value: %s",
                search_key,
                value,
            )
            return key, value
        if isinstance(value, dict):
            found_key, found_value = search_key_recursive_dict(value, search_key)
            if found_key != "":
                return found_key, found_value

    logging.info("Key '%s' not found in dictionary.", search_key)
    return "", ""


def recursively_remove_keys(data: Dict, keys_to_remove: List[str]) -> Dict:
    """Remove keys recursively from a dictionary.

    This function removes keys from a dictionary that are specified in the `keys_to_remove` list.
    If a value associated with a key is a dictionary, this function is called recursively on that dictionary.

    Args:
    ----
        data (Dict): The dictionary to remove keys from.
        keys_to_remove (List[str]): A list of keys to remove from the dictionary.

    Returns:
    -------
        Dict: The dictionary with the specified keys removed.
    """
    if not isinstance(data, dict):
        msg = "Input data must be a dictionary"
        raise ValueError(msg)

    if not isinstance(keys_to_remove, list):
        msg = "keys_to_remove must be a list"
        raise ValueError(msg)

    to_remove = [
        k
        for k in data
        if k in keys_to_remove or (isinstance(data[k], dict) and not data[k])
    ]
    for key in to_remove:
        del data[key]

    for _, v in data.items():
        if isinstance(v, dict):
            recursively_remove_keys(v, keys_to_remove)
    return data


def prune_empty_values(dictionary: Dict[str, Any]) -> Dict[str, Any]:
    """Remove keys with empty values from a dictionary recursively.

    Args:
    ----
        dictionary (dict): The dictionary to prune.

    Returns:
    -------
        dict: The pruned dictionary.
    """
    error_msg = "Input data must be a dictionary"
    if not isinstance(dictionary, dict):
        raise ValueError(error_msg)

    to_prune = {
        k: prune_empty_values(v) if isinstance(v, dict) else v
        for k, v in dictionary.items()
    }
    return {k: v for k, v in to_prune.items() if v or v == 0}


def sort_nested_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Sort a nested dictionary and its contents alphabetically.

    Args:
    ----
        d (Dict[str, Any]): The dictionary to sort.

    Returns:
    -------
        Dict[str, Any]: The sorted dictionary.
    """
    result = {}
    for key, value in sorted(d.items(), key=lambda x: str(x[0])):
        if isinstance(value, dict):
            logging.info("Sorting nested dictionary for key: %s", key)
            result[key] = sort_nested_dict(value)
        elif isinstance(value, list):
            logging.info("Sorting list for key: %s", key)
            try:
                # Cast sorted list to Any
                result[key] = cast(Any, sorted(value))
            except TypeError:
                # If values within the list are unsortable, leave the list as is
                # Cast the value to Any
                result[key] = cast(Any, value)
        else:
            # Cast the value to Any
            result[key] = cast(Any, value)
    return result


def convert_72hr_table_to_flights(  # noqa: PLR0911 (To be refactored later)
    table: Table,
    origin_terminal: str,
    use_fixed_date: bool = False,
    fixed_date: Optional[str] = None,
) -> List[Flight]:
    """Convert a 72-hour flight schedule table to a list of Flight objects.

    Args:
    ----
        table (Table): The table object containing the 72-hour flight schedule.
        origin_terminal (str): The terminal where the flights originate.
        use_fixed_date (bool, optional): Whether to use a fixed date for testing purposes. Default is False.
        fixed_date (str, optional): The fixed date to use for testing in 'YYYYMMDD' format. Required if `use_fixed_date` is True.

    Returns:
    -------
        List[Flight]: A list of Flight objects created from the table.

    Notes:
    -----
        - The function can handle special cases like note columns and OCR errors.
        - Logging is used extensively for error handling and debugging.
        - The function expects at least three columns in the table: roll call time, destination, and seats.
        - Extra columns are treated as notes.
        - If the table title contains a date, it will be used for the Flight objects' date attribute.

    Raises:
    ------
        Logs an error and returns an empty list if:
            - The table is empty or has no rows/columns.
            - Required data like origin_terminal or table title is missing.
            - Date parsing from the table title fails.

    Examples:
    --------
        >>> table = Table(rows=[...], title='Title with Date', footer='Footer note')
        >>> origin_terminal = 'Terminal 1'
        >>> flights = convert_72hr_table_to_flights(table, origin_terminal)
    """
    logging.info("Converting 72-hour table to flights...")

    # Does table have extra columns for notes?
    has_note_columns = False

    # Initialize list of flights
    flights: List[Flight] = []

    # Check if table is empty
    if table is None:
        logging.error("Table is empty. Exiting..")
        return flights

    # Check that there are rows in table
    if table.rows is None:
        logging.error("There are no rows in the table. Exiting...")
        return flights

    # Check there are columns in table
    if table.get_num_of_columns() == 0:
        logging.error("There are no columns in the table. Exiting...")
        return flights

    # Check that there are at least 3 columns in table
    min_num_of_columns = 3
    if table.get_num_of_columns() < min_num_of_columns:
        logging.warning(
            "There are not enough columns in the table. Only %s columns found. Expected at least 3. Exiting...",
            table.get_num_of_columns(),
        )
        return flights

    # Get column indices
    # All are set to return warnings to avoid a lot of false positives
    # when non 72-hour tables are passed in.
    roll_call_column_index = infer_roll_call_column_index(table)

    if roll_call_column_index == -1:
        logging.warning("Failed to get roll call column index. Exiting...")
        return flights
    logging.info("Roll call column index: %s", roll_call_column_index)

    seats_column_index = infer_seats_column_index(table)

    if seats_column_index == -1:
        logging.warning("Failed to get seats column index. Exiting...")
        return flights
    logging.info("Seats column index: %s", seats_column_index)

    destination_column_index = infer_destinations_column_index(table)

    if destination_column_index == -1:
        logging.warning("Failed to get destination column index. Exiting...")
        return flights
    logging.info("Destination column index: %s", destination_column_index)

    # Create list of note column indices if there are more than 3 columns
    if table.get_num_of_columns() > min_num_of_columns:
        logging.info(
            "There are more than 3 columns in the table. Treating extra columns as notes."
        )

        # Make extra columns into notes
        note_column_indices = []
        has_note_columns = True
        for index, _column_header in enumerate(table.rows[0]):
            if index not in [
                roll_call_column_index,
                destination_column_index,
                seats_column_index,
            ]:
                note_column_indices.append(index)
        logging.info("Note column indices found: %s", note_column_indices)

    # Check origin terminal
    if origin_terminal is None:
        logging.error("Origin terminal is empty. Exinting...")
        return flights

    # Merge rows in table as needed
    merged_table = merge_table_rows(table)
    if merged_table is None:
        logging.error("Failed to merge rows in table. Exiting...")
        return flights

    table = merged_table

    if table is None:
        logging.error("Failed to merge rows in table. Exiting...")
        return flights

    # Iterate through each row
    for row_index, row in enumerate(table.rows):
        logging.info("Processing row %s.", row_index)

        # Skip first row (headers)
        if row_index == 0:
            continue

        # Special flight data variables
        has_roll_call_note = False
        roll_call_notes: Dict[str, Any] = {}
        has_seat_note = False
        seat_notes: Dict[str, Any] = {}
        has_dest_note = False
        dest_notes = {}
        notes: Dict[str, Any] = {}

        # Define cells for parsing
        dest_cell = row[destination_column_index]
        roll_call_cell = row[roll_call_column_index]
        seats_cell = row[seats_column_index]

        # Parse the cell text
        destinations = parse_destination(dest_cell[0])
        roll_call_time = parse_rollcall_time(roll_call_cell[0])
        seats = parse_seat_data(seats_cell[0])

        # Skip row if it doesn't have complete data
        if roll_call_time is None and not seats and destinations is None:
            logging.info("Skipping row %s due to incomplete data.", row_index)
            continue

        if destinations is None:
            logging.error("Failed to parse destinations. Skipping row...")
            continue

        # Check if the roll call time is a valid roll call time
        if roll_call_time is None:
            logging.info(
                "Row %s roll call time is not a valid roll call time.", row_index
            )

            # Check if the roll call is a note
            if len(row[roll_call_column_index][0]) > 0:
                has_roll_call_note = True
                logging.info(
                    'Appears to be special roll call format or note. Saving as "Roll Call Note" in notes.'
                )
                roll_call_notes["rollCallCellNote"] = row[roll_call_column_index][0]

        # If seat data parsing fails, check if there is no seat data but there is a note
        if not seats and len(row[seats_column_index][0]) > 0:
            has_seat_note = True
            logging.info(
                'Appears to be special seat format or note. Saving as "Seat Note" in notes.'
            )
            seat_notes["seatCellNote"] = row[seats_column_index][0]
        elif not seats:
            logging.info(
                "Row %s has no seat data. Setting seats to [0, 'TBD'].", row_index
            )
            seats.append([0, "TBD"])

        # Check each of the three cells for extra notes
        # This is any **Notes** or (Notes) when they accompany the data
        extra_roll_call_notes = extract_notes(roll_call_cell[0])
        extra_dest_notes = extract_notes(dest_cell[0])
        extra_seat_notes = extract_notes(seats_cell[0])

        # Add extra notes to notes dict
        if extra_roll_call_notes:
            logging.info("Found extra roll call notes.")
            roll_call_notes["markedRollCallCellNotes"] = extra_roll_call_notes
            has_roll_call_note = True

        if extra_dest_notes:
            logging.info("Found extra destination notes.")
            dest_notes["markedDestinationCellNotes"] = extra_dest_notes
            has_dest_note = True

        if extra_seat_notes:
            logging.info("Found extra seat notes.")
            seat_notes["markedSeatCellNotes"] = extra_seat_notes
            has_seat_note = True

        # Build notes for flight
        if has_roll_call_note:
            notes["rollCallNotes"] = roll_call_notes

        if has_dest_note:
            notes["destinationNotes"] = dest_notes

        if has_seat_note:
            notes["seatNotes"] = seat_notes

        # Add table footer to notes if it exists
        if table.footer is not None and table.footer != "":
            notes["footnote"] = table.footer

        # Add extra columns as notes
        if has_note_columns:
            logging.info("Adding extra columns as notes.")
            extra_column_notes = convert_note_column_to_notes(
                table, row_index, note_column_indices
            )
            notes["extraColumnNotes"] = extra_column_notes

        # Remove empty keys from notes
        notes = recursively_remove_keys(notes, [""])

        # Check if the flight is a Patriot Express flight
        patriot_express = False
        row_text_string = f"{dest_cell[0]} {roll_call_cell[0]} {seats_cell[0]}"
        if find_patriot_express(row_text_string):
            logging.info("Found Patriot Express flight.")
            patriot_express = True

        # Check if the table is in the macdill format
        # See macdill_1_72hr_output_tablefied.txt in tests/pdf-table-textract-output folder for example
        date_key, date_string = search_key_recursive_dict(notes, "date")
        if date_key and date_string:
            logging.info("Table is in macdill format.")

            # Check if date is a valid date string
            match = check_date_string(date_string, return_match=True)

            if match is None:
                logging.error("Failed to get date from table title. Skipping row...")
                continue

            # Remove key from notes
            notes = recursively_remove_keys(notes, [date_key])

            if not match:
                logging.error("Failed to get date from table title. Skipping row...")
                continue

        # Standard format
        else:
            # Get date for flight from table title
            if table.title is None:
                logging.error("Table title is empty. Skipping row...")
                continue

            # Check there is a valid date in the table title
            match = check_date_string(table.title, return_match=True)

            if match is None:
                logging.error("Failed to get date from table title. Skipping row...")
                continue

        # Remove keys from notes that have empty values
        notes = prune_empty_values(notes)
        notes = sort_nested_dict(notes)

        # Confirm match is a string before passing to reformat_date
        if not isinstance(match, str):
            logging.error("Match is not a string. Exiting...")
            return flights

        # Added this functionality to allow for testing with a fixed date
        # which allows for proper testing of year inference functionality
        # of the reformat_date function
        if use_fixed_date:
            logging.info("Using fixed date: %s", fixed_date)
            if fixed_date is None:
                logging.error("Fixed date is empty. Exiting...")
                return flights

            custom_date = create_datetime_from_str(fixed_date)

            if custom_date is None:
                logging.error("Failed to create datetime object from fixed date.")
                return flights

            date = reformat_date(match, custom_date)
        else:
            date = reformat_date(match, datetime.datetime.now(tz=datetime.UTC))

        # Create flight object
        flight = Flight(
            origin_terminal=origin_terminal,
            destinations=destinations,
            rollcall_time=roll_call_time,
            seats=seats,
            notes=notes,
            date=date,
            rollcall_note=has_roll_call_note,
            seat_note=has_seat_note,
            destination_note=has_dest_note,
            patriot_express=patriot_express,
        )

        flights.append(flight)

    return flights


# Define a type alias for dictionary elements, which could include nested dictionaries
Element = Union[str, int, float, Dict[str, "Element"]]
GenericDict = Dict[str, Element]


def compare_nested_dicts(elem1: Element, elem2: Element) -> int:
    """Compare two elements, which could be nested dictionaries.

    Args:
    ----
        elem1 (Element): The first element to compare.
        elem2 (Element): The second element to compare.

    Returns:
    -------
        int: The number of matching elements.
    """
    if isinstance(elem1, dict) and isinstance(elem2, dict):
        # Count matching elements in nested dictionaries
        return sum(
            compare_nested_dicts(elem1[key], elem2[key])
            for key in elem1
            if key in elem2
        )

    # Direct comparison for non-dictionary elements
    return int(elem1 == elem2)


def find_similar_dicts(
    base_dict_list: List[GenericDict],
    comp_dict_list: List[GenericDict],
    min_num_matching_keys: int = 3,
) -> List[GenericDict]:
    """Find dictionaries in comp_dict_list that are similar to a dictionary in base_dict_list.

    Args:
    ----
        base_dict_list (List[GenericDict]): Base list of dictionaries to compare against.
        comp_dict_list (List[GenericDict]): List of dictionaries to compare.
        min_num_matching_keys (int, optional): The minimum number of matching keys required to consider the dictionaries similar. Default is 3.

    Returns:
    -------
        List[GenericDict]: A list of dictionaries from comp_dict_list that are similar to a dictionary in base_dict_list.
    """
    # Ensure that all elements in the lists are dictionaries
    for d in base_dict_list + comp_dict_list:
        if not isinstance(d, dict):
            msg = "All elements in the lists must be dictionaries"
            raise TypeError(msg)

    similar_dicts = []

    for comp_dict in comp_dict_list:
        for new_dict in base_dict_list:
            # Count matching elements (considering nested dictionaries)
            match_count = sum(
                compare_nested_dicts(comp_dict[key], new_dict[key])
                for key in comp_dict
                if key in new_dict
            )

            # Check if there are at least 3 matches
            if match_count >= min_num_matching_keys:
                similar_dicts.append(comp_dict)
                break  # Break inner loop if a match is found

    return similar_dicts
