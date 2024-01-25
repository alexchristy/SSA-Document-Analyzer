import copy
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


def count_matching_keys(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> int:
    """Count the number of matching keys between two dictionaries.

    Args:
    ----
        dict1 (Dict[str, Any]): The first dictionary.
        dict2 (Dict[str, Any]): The second dictionary.

    Returns:
    -------
        int: The number of keys that match in both value and existence.
    """
    matching_keys = 0

    for key in dict1:
        if key in dict2 and dict1[key] == dict2[key]:
            matching_keys += 1

    return matching_keys


def sort_dicts_by_matching_keys(
    dict_list: List[Dict[str, Any]], reference_dict: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Sort a list of dictionaries based on the number of matching keys with a reference dictionary.

    Args:
    ----
        dict_list (List[Dict[str, Any]]): List of dictionaries to be sorted.
        reference_dict (Dict[str, Any]): The reference dictionary to compare against.

    Returns:
    -------
        List[Dict[str, Any]]: Sorted list of dictionaries, with most matching keys first.
    """
    return sorted(
        dict_list, key=lambda d: count_matching_keys(d, reference_dict), reverse=True
    )


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


def prune_recent_old_flights(
    old_flights: List[Flight],
    new_flights: List[Flight],
    flight_age_seconds: int = 7200,
    min_num_match_keys: int = 3,
    keys_to_compare: Optional[List[str]] = None,
    priority_prune_key: str = "destinations",
) -> Tuple[List[Flight], List[Flight]]:
    """Prune old flights from the old flights list if they are similar to a new flight and are so many seconds old.

    Returns a list of old flights with similar + recent flights removed and then a seperate list of old flights that were removed.
    Prunes on a 1:1 basis. If there are multiple similar old flights, only the highest priority flight will be pruned.

    Priority is determined by the `priority_prune_key` argument. If the `priority_prune_key` matches between a new flight and an old flight, the old flight will be pruned.
    If two old flights match the `priority_prune_key`, the old flight with the most similarity to the new flight will be pruned.

    Args:
    ----
        old_flights (List[Flight]): List of old flights to prune.
        new_flights (List[Flight]): List of new flights to compare against.
        flight_age_seconds (int, optional): The age of a flight in seconds to consider it old enough to not prune. Default is 7200 (2 hours) inclusive.
        min_num_match_keys (int, optional): The minimum number of matching keys required to consider the flights similar. Default is 3.
        keys_to_compare (Optional[List[str]], optional): A list of keys to compare between the flights. Default is ["date", "seats", "destinations", "rollcall_time"].
        priority_prune_key (Optional[str], optional): Key that if it matches gives that flight priority to be pruned. Must be in keys_to_compare. Default is destinations.

    Returns:
    -------
        Tuple(List[Flight], List[Flight]): A tuple containing the pruned old flights and the removed old flights.
    """
    working_old_flights = copy.deepcopy(
        old_flights
    )  # We remove flights from this list and return it

    current_date = datetime.datetime.now(
        tz=datetime.UTC
    )  # Flight creation_time is in UTC

    if keys_to_compare is None:
        keys_to_compare = ["date", "seats", "destinations", "rollcall_time"]

    keys_to_compare = [key.lower() for key in keys_to_compare]

    keys_to_compare.append("flight_id")  # Used for tracking. Will never match.

    # Ensure that all elements in the lists are Flight objects
    for f in working_old_flights + new_flights:
        if not isinstance(f, Flight):
            msg = "All elements in the lists must be Flight objects"
            raise TypeError(msg)

        if not all(key in f.to_dict() for key in keys_to_compare):
            msg = f"One or more flights do not contain all the required keys for comparison. Keys reques: {keys_to_compare}"
            raise ValueError(msg)

    # Check priority_prune_key is in keys_to_compare
    if priority_prune_key not in keys_to_compare:
        msg = f"priority_prune_key '{priority_prune_key}' is not in keys_to_compare. Keys reques: {keys_to_compare}"
        raise ValueError(msg)

    removed_old_flights: List[Flight] = []
    new_flights_dicts_reduced: List[Dict[str, Any]] = []
    old_flights_dicts_reduced: List[Dict[str, Any]] = []

    # Create dynamically reduced dictionaries for new flights
    for flight in new_flights:
        flight_dict = flight.to_dict()
        reduced_flight_dict = {key: flight_dict[key] for key in keys_to_compare}
        new_flights_dicts_reduced.append(reduced_flight_dict)

    for new_flight_dict in new_flights_dicts_reduced:
        logging.info("New flight dict: %s", new_flight_dict)

        # Create dynamically reduced dictionaries for old flights
        for flight in working_old_flights:
            flight_dict = flight.to_dict()
            reduced_flight_dict = {key: flight_dict[key] for key in keys_to_compare}
            old_flights_dicts_reduced.append(reduced_flight_dict)

        similar_old_flights = find_similar_dicts(
            base_dict_list=[new_flight_dict],
            comp_dict_list=old_flights_dicts_reduced,
            min_num_matching_keys=min_num_match_keys,
        )

        logging.info("Found %s similar flights.", len(similar_old_flights))

        if not similar_old_flights:
            continue

        # List similar flights ordered by priority to be pruned
        ordered_sim_old_flights: List[Dict[str, Any]] = []

        # First old flights to be pruned are the ones that match the priority_prune_key
        # in a first come first serve basis.
        for sim_old_flight in similar_old_flights[:]:  # Iterate over a copy of the list
            # Check if the new flight matches the prune key
            if (
                sim_old_flight[priority_prune_key]
                == new_flight_dict[priority_prune_key]
            ):
                logging.info(
                    "Old flight matches new flight's prune key '%s'. Old flight: %s",
                    priority_prune_key,
                    sim_old_flight,
                )
                ordered_sim_old_flights.append(sim_old_flight)
                similar_old_flights.remove(sim_old_flight)

        # Sort the flights that match the priority_prune_key by the number of matching keys
        if len(ordered_sim_old_flights) > 1:
            ordered_sim_old_flights = sort_dicts_by_matching_keys(
                dict_list=ordered_sim_old_flights, reference_dict=new_flight_dict
            )

        # Sort the remaining old flights by the number of matching keys
        if len(similar_old_flights) > 1:
            no_match_sim_old_flights = sort_dicts_by_matching_keys(
                dict_list=similar_old_flights, reference_dict=new_flight_dict
            )
            ordered_sim_old_flights.extend(no_match_sim_old_flights)
        else:
            ordered_sim_old_flights.extend(similar_old_flights)

        # Get the flight IDs of the similar flights
        old_flight_ids = [flight.flight_id for flight in old_flights]

        for sim_flight in ordered_sim_old_flights:
            # Get the flight ID of the similar flight
            similar_flight_id = sim_flight["flight_id"]

            if similar_flight_id not in old_flight_ids:
                logging.error(
                    "Similar flight ID '%s' not found in the original old flights list. Skipping...",
                    similar_flight_id,
                )
                continue

            # Get the similar flight
            similar_flight = next(
                (f for f in working_old_flights if f.flight_id == similar_flight_id),
                None,
            )

            if similar_flight is None:
                logging.error(
                    "Failed to get similar flight with ID %s. Skipping...",
                    similar_flight_id,
                )
                continue

            flight_creation_datetime = datetime.datetime.strptime(
                str(similar_flight.creation_time), "%Y%m%d%H%M"
            ).replace(tzinfo=datetime.timezone.utc)

            time_diff = current_date - flight_creation_datetime

            logging.info(
                "Time delta (secs): %s for flight: %s",
                time_diff.total_seconds(),
                similar_flight.flight_id,
            )

            if time_diff.total_seconds() <= flight_age_seconds:
                logging.info(
                    "Old flight with ID %s is younger than %s seconds. Pruning...",
                    similar_flight.flight_id,
                    flight_age_seconds,
                )
                removed_old_flights.append(similar_flight)
                working_old_flights.remove(similar_flight)
                break  # Break out of inner loop if a flight is pruned to avoid pruning multiple flights (1:1)

            logging.info(
                "Old flight with ID %s is older than %s seconds. Not removing...",
                similar_flight.flight_id,
                flight_age_seconds,
            )

    return working_old_flights, removed_old_flights
